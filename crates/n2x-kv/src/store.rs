//! 文件 KV：追加日志 + 内存索引 + 定期压缩。
//!
//! 读取不经过文件系统：索引给出值在日志中的偏移，值直接从 mmap 页拷贝。
//! 写入是一帧顺序追加；帧带 CRC，恢复时只接受完整帧，因此批量写具有
//! 全有或全无语义。容量接近上限或显式调用时，把活记录重写到新文件并
//! 原子 rename，回收死数据。

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock};

use crate::KVError;
use crate::backend::{Backend, BackendKind, open_backend};
use crate::format::{
    FILE_HEADER_LEN, FRAME_HEADER_LEN, FrameBuf, FrameHeader, HARD_MAX_FRAME_BYTES, RECORD_CLEAR,
    RECORD_HEADER_LEN, RECORD_REMOVE, RECORD_SET, decode_frame_header, file_header,
    for_each_record, validate_file_header,
};
use crate::index::Index;

/// 何时使用哪个文件后端。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BackendPreference {
    /// 优先 mmap；映射失败时自动降级为 buffered。
    #[default]
    Auto,
    /// 强制 mmap；不可用时打开失败。
    Mmap,
    /// 强制 buffered，适合测试或明确不需要映射的平台。
    Buffered,
}

/// 文件存储配置。
///
/// `max_bytes` 是文件长度上限，不是浏览器式的 5 MB 配额；默认 64 MiB，
/// 由宿主按设备与业务调整。
#[derive(Clone, Debug)]
pub struct StoreOptions {
    /// 映射文件的最大长度。
    pub max_bytes: u64,
    /// 新建或增容时的初始长度下限。
    pub initial_bytes: u64,
    /// 单键最大字节数。
    pub max_key_bytes: usize,
    /// 单值最大字节数。
    pub max_value_bytes: usize,
    /// 单帧（含 20 字节帧头）最大字节数，也是批量写的上限。
    pub max_frame_bytes: usize,
    /// 日志占用达到 `max_bytes` 的百分之多少后，允许自动压缩。
    ///
    /// 默认 `80`；`100` 关闭按容量比例触发。容量满时仍会尝试回收，但只有
    /// 压缩后确实能容纳新帧才执行，避免活数据已满时反复重写整个文件。
    pub compact_at_percent: u8,
    /// 单键累计多少条历史记录后触发自动压缩；`0` 禁用该触发器。
    ///
    /// 计数包含当前活记录与已覆盖记录，跨重开按有效日志重建，压缩后重置。
    /// 显式删除一个键会清掉它的计数，避免被删键长期占用索引内存。例如 `100`
    /// 表示同一键累计约 100 条日志记录后回收旧版本。它只影响回收时机，不改变
    /// 最后一次写入生效的语义。
    pub compact_key_versions: u32,
    /// 后端偏好。
    pub backend: BackendPreference,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            max_bytes: 64 * 1024 * 1024,
            initial_bytes: 1024 * 1024,
            max_key_bytes: 8 * 1024,
            max_value_bytes: 16 * 1024 * 1024,
            max_frame_bytes: 32 * 1024 * 1024,
            compact_at_percent: 80,
            compact_key_versions: 128,
            backend: BackendPreference::Auto,
        }
    }
}

impl StoreOptions {
    /// 校验配置组合，避免打开后才发现单条记录放不进一帧。
    pub fn validate(&self) -> Result<(), KVError> {
        if self.initial_bytes < FILE_HEADER_LEN {
            return Err(invalid_options(
                "initial_bytes is smaller than the file header",
            ));
        }
        if self.initial_bytes > self.max_bytes {
            return Err(invalid_options("initial_bytes exceeds max_bytes"));
        }
        if self.compact_at_percent == 0 || self.compact_at_percent > 100 {
            return Err(invalid_options("compact_at_percent must be in 1..=100"));
        }
        if self.max_bytes < FILE_HEADER_LEN + FRAME_HEADER_LEN as u64 {
            return Err(invalid_options("max_bytes leaves no room for a frame"));
        }
        if self.max_key_bytes > u32::MAX as usize || self.max_value_bytes > u32::MAX as usize {
            return Err(invalid_options(
                "key or value limit does not fit the on-disk format",
            ));
        }
        if self.max_frame_bytes < FRAME_HEADER_LEN + RECORD_HEADER_LEN {
            return Err(invalid_options("max_frame_bytes is too small"));
        }
        if self.max_frame_bytes > u32::MAX as usize {
            return Err(invalid_options(
                "max_frame_bytes does not fit the on-disk format",
            ));
        }
        if self.max_frame_bytes as u64 > self.max_bytes - FILE_HEADER_LEN {
            return Err(invalid_options("max_frame_bytes exceeds max_bytes"));
        }
        let largest_record = FRAME_HEADER_LEN
            .checked_add(RECORD_HEADER_LEN)
            .and_then(|size| size.checked_add(self.max_key_bytes))
            .and_then(|size| size.checked_add(self.max_value_bytes))
            .ok_or_else(|| invalid_options("record limits overflow"))?;
        if largest_record > self.max_frame_bytes {
            return Err(invalid_options(
                "max_frame_bytes cannot hold max_key_bytes + max_value_bytes",
            ));
        }
        Ok(())
    }
}

/// 一次写入。`Set` 与 `Remove` 按出现顺序生效，`Clear` 清空此前所有键。
#[derive(Clone, Copy, Debug)]
pub enum Mutation<'a> {
    Set { key: &'a str, value: &'a [u8] },
    Remove { key: &'a str },
    Clear,
}

/// 存储统计快照。
#[derive(Clone, Copy, Debug, Default)]
pub struct Stats {
    /// 当前键数量。
    pub entries: u64,
    /// 活键 + 值的字节数。
    pub live_bytes: u64,
    /// 可回收的字节数（含帧头与已覆盖记录），是压缩收益的近似值。
    pub dead_bytes: u64,
    /// 日志有效数据长度（不含文件头）。
    pub data_bytes: u64,
    /// 当前文件或映射长度。
    pub file_bytes: u64,
    /// 配置的容量上限。
    pub max_bytes: u64,
    /// 实际后端。
    pub backend: BackendKind,
    /// 是否因 mmap 不可用而降级。
    pub degraded: bool,
    /// 成功压缩次数。
    pub compactions: u64,
}

/// 文件型键值存储。所有操作都是同步的；跨 FFI 调用由上层包装。
pub struct Store {
    path: PathBuf,
    options: StoreOptions,
    registration: Mutex<Option<PathRegistration>>,
    inner: RwLock<Option<Inner>>,
}

struct Inner {
    backend: Box<dyn Backend>,
    index: Index,
    valid_end: u64,
    degraded: bool,
    max_key_versions: u32,
    compactions: u64,
    writes: u64,
    flushes: u64,
    /// 发生无法自动修复的不一致后置位；后续操作返回 `Corrupt`。
    poisoned: Option<String>,
}

impl Inner {
    fn check(&self) -> Result<(), KVError> {
        match &self.poisoned {
            Some(reason) => Err(KVError::Corrupt {
                reason: reason.clone(),
            }),
            None => Ok(()),
        }
    }
}

const MIN_COMPACT_BYTES: u64 = 1024 * 1024;
const COMPACT_FRAME_TARGET: usize = 256 * 1024;

/// 进程内已打开的存储路径。`valid_end` 由每个实例独立维护，同路径的两个
/// 写入者会互相覆盖，因此进程内只允许一个实例。跨进程仍没有文件锁。
static OPEN_PATHS: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static NEXT_OPEN_TOKEN: AtomicU64 = AtomicU64::new(1);

/// 路径占用凭证。`Store` 持有它，`close()` 或析构时释放。
struct PathRegistration {
    path: PathBuf,
    token: u64,
}

impl PathRegistration {
    fn acquire(path: PathBuf) -> Result<Self, KVError> {
        let token = NEXT_OPEN_TOKEN.fetch_add(1, Ordering::Relaxed);
        let mut open = OPEN_PATHS.lock();
        if open.contains_key(&path) {
            return Err(KVError::AlreadyOpen {
                path: path.display().to_string(),
            });
        }
        open.insert(path.clone(), token);
        Ok(Self { path, token })
    }

    fn release(&self) {
        let mut open = OPEN_PATHS.lock();
        // 只删除自己登记的 token；路径可能已被关闭后重新打开的新实例占用。
        if open.get(&self.path) == Some(&self.token) {
            open.remove(&self.path);
        }
    }
}

impl Drop for PathRegistration {
    fn drop(&mut self) {
        self.release();
    }
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("path", &self.path)
            .field("options", &self.options)
            .finish()
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        self.registration.lock().take();
    }
}

impl Store {
    /// 打开或创建文件存储。
    ///
    /// 打开时会顺序扫描日志并重建索引；被撕裂的尾帧会被截断。`Auto`
    /// 后端在 mmap 不可用（映射失败或首尾页不可访问）时自动降级为
    /// buffered，并通过统计中的 `degraded` 暴露降级事实。
    ///
    /// 同一路径在本进程内已被另一个实例打开时返回 `AlreadyOpen`，
    /// 避免两个写入者各自维护 `valid_end` 互相覆盖；`close()` 或析构后
    /// 可以重新打开。跨进程不提供文件锁。
    pub fn open(path: impl AsRef<Path>, options: StoreOptions) -> Result<Self, KVError> {
        options.validate()?;
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        // 规范化后再登记路径，符号链接与相对路径也会归一到同一个键。
        let canonical = std::fs::canonicalize(&path)?;
        let registration = PathRegistration::acquire(canonical)?;
        prepare_file(&mut file, options.initial_bytes)?;

        let (mut backend, degraded) = open_backend(file, &options)?;
        let (index, valid_end, dirty_tail) = recover(backend.as_ref())?;
        if dirty_tail && valid_end < backend.len() {
            log::warn!(
                "n2x-kv: truncating torn tail in {} at offset {valid_end}",
                path.display()
            );
            backend.truncate(valid_end)?;
        }
        let max_key_versions = index.max_versions;

        let store = Self {
            path,
            options,
            registration: Mutex::new(Some(registration)),
            inner: RwLock::new(Some(Inner {
                backend,
                index,
                valid_end,
                degraded,
                max_key_versions,
                compactions: 0,
                writes: 0,
                flushes: 0,
                poisoned: None,
            })),
        };
        if store.should_compact_on_open()
            && let Err(error) = store.compact()
        {
            log::warn!(
                "n2x-kv: compaction after opening {} failed: {error}",
                store.path.display()
            );
        }
        warn_stale_compact_file(&store.path);
        Ok(store)
    }

    /// 文件路径。
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 当前配置。
    pub fn options(&self) -> &StoreOptions {
        &self.options
    }

    /// 读取一个键；不存在返回 `None`，空值返回 `Some(vec![])`。
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, KVError> {
        let guard = self.inner.read();
        let inner = guard.as_ref().ok_or(KVError::Closed)?;
        inner.check()?;
        let Some(entry) = inner.index.get(key) else {
            return Ok(None);
        };
        let mut value = vec![0u8; entry.value_len as usize];
        inner
            .backend
            .read(entry.value_offset, value.len(), &mut value)?;
        Ok(Some(value))
    }

    /// 是否存在该键。与 `get` 一样，超长键只会查不到，不当作输入错误。
    pub fn contains(&self, key: &str) -> Result<bool, KVError> {
        let guard = self.inner.read();
        let inner = guard.as_ref().ok_or(KVError::Closed)?;
        inner.check()?;
        Ok(inner.index.contains(key))
    }

    /// 写入一个键值。
    pub fn set(&self, key: &str, value: &[u8]) -> Result<(), KVError> {
        self.write(&[Mutation::Set { key, value }])
    }

    /// 删除一个键，返回删除前是否存在。
    ///
    /// 日志已满、连 remove 帧都放不下时，改为压缩并丢弃该键：效果相同，且总会
    /// 腾出空间。否则满容量的库会连“减少数据”都做不到，永久只读。
    pub fn remove(&self, key: &str) -> Result<bool, KVError> {
        self.validate_key(key)?;
        let mut guard = self.inner.write();
        let inner = guard.as_mut().ok_or(KVError::Closed)?;
        inner.check()?;
        if !inner.index.contains(key) {
            return Ok(false);
        }
        let mut frame = FrameBuf::new();
        frame.push_remove(key)?;
        let (frame, count) = frame.finish();
        match self.append_frame(inner, &frame, count) {
            Ok(()) => Ok(true),
            Err(KVError::Full { .. }) => {
                log::warn!(
                    "n2x-kv: {} is full; reclaiming space by compaction instead of appending a remove frame",
                    self.path.display()
                );
                self.reclaim_locked(inner, &[Mutation::Remove { key }])?;
                Ok(true)
            }
            Err(error) => Err(error),
        }
    }

    /// 原子批量写入。整批记录放在一帧里，恢复时不会只应用一半。
    ///
    /// 整批只含 `Remove` / `Clear` 时，即使满容量也能完成：此时这批操作只会减少
    /// 活数据，直接按压缩实现，语义与“追加一帧再回收”一致。
    pub fn write(&self, mutations: &[Mutation<'_>]) -> Result<(), KVError> {
        if mutations.is_empty() {
            return Ok(());
        }
        self.validate_mutations(mutations)?;

        let mut frame = FrameBuf::new();
        for mutation in mutations {
            match mutation {
                Mutation::Set { key, value } => frame.push_set(key, value)?,
                Mutation::Remove { key } => frame.push_remove(key)?,
                Mutation::Clear => frame.push_clear()?,
            }
        }
        let (frame, count) = frame.finish();

        let mut guard = self.inner.write();
        let inner = guard.as_mut().ok_or(KVError::Closed)?;
        inner.check()?;
        match self.append_frame(inner, &frame, count) {
            Ok(()) => Ok(()),
            Err(KVError::Full { .. }) if shrinks_only(mutations) => {
                log::warn!(
                    "n2x-kv: {} is full; applying a removal batch by compaction",
                    self.path.display()
                );
                self.reclaim_locked(inner, mutations)
            }
            Err(error) => Err(error),
        }
    }

    /// 清空全部键。清空本身是原子帧；随后尽力压缩回收空间。
    ///
    /// 日志已满、clear 帧放不下时，批量写的减压兜底会把清空实现为一次压缩，
    /// 因此清空不会因为放不下一帧而被拒绝。
    pub fn clear(&self) -> Result<(), KVError> {
        self.write(&[Mutation::Clear])?;
        if let Err(error) = self.compact_if_large() {
            log::warn!(
                "n2x-kv: compaction after clearing {} failed: {error}",
                self.path.display()
            );
        }
        Ok(())
    }

    /// 返回按字典序排序的键；`prefix` 为 `None` 或空串时返回全部。
    pub fn keys(&self, prefix: Option<&str>) -> Result<Vec<String>, KVError> {
        let guard = self.inner.read();
        let inner = guard.as_ref().ok_or(KVError::Closed)?;
        inner.check()?;
        let mut keys: Vec<String> = inner
            .index
            .map
            .keys()
            .filter(|key| prefix.is_none_or(|prefix| key.starts_with(prefix)))
            .map(|key| key.as_ref().to_string())
            .collect();
        keys.sort_unstable();
        Ok(keys)
    }

    /// 当前键数量。
    pub fn len(&self) -> Result<u64, KVError> {
        let guard = self.inner.read();
        let inner = guard.as_ref().ok_or(KVError::Closed)?;
        inner.check()?;
        Ok(inner.index.len() as u64)
    }

    /// 是否为空。
    pub fn is_empty(&self) -> Result<bool, KVError> {
        Ok(self.len()? == 0)
    }

    /// 统计快照。
    pub fn stats(&self) -> Result<Stats, KVError> {
        let guard = self.inner.read();
        let inner = guard.as_ref().ok_or(KVError::Closed)?;
        inner.check()?;
        Ok(stats_of(&self.options, inner))
    }

    /// 把映射脏页与文件元数据刷到持久介质。
    ///
    /// 默认写入不刷盘；进程崩溃后通常仍可读取，但掉电可能丢失从某个未持久
    /// 帧开始的后缀。调用方应在关键事务完成、退到后台或关闭前选择检查点
    /// 调用本方法，而不是为每次小写入都刷盘。失败表示本次持久化未确认。
    pub fn flush(&self) -> Result<(), KVError> {
        let mut guard = self.inner.write();
        let inner = guard.as_mut().ok_or(KVError::Closed)?;
        inner.check()?;
        inner.backend.sync()?;
        inner.flushes += 1;
        Ok(())
    }

    /// 把活记录重写到新文件并原子替换，回收已覆盖、删除或清空的记录。
    ///
    /// 本方法始终执行压缩，适合宿主在空闲、后台或维护节点选择。它持有写锁，
    /// 期间读写会等待，代价与活数据量有关；不要在消息热路径频繁调用。自动
    /// 压缩由 [`StoreOptions::compact_at_percent`] 和
    /// [`StoreOptions::compact_key_versions`] 控制。压缩失败保留原文件和全部
    /// 已提交写入，可在稍后的空闲节点重试。
    pub fn compact(&self) -> Result<(), KVError> {
        let mut guard = self.inner.write();
        let inner = guard.as_mut().ok_or(KVError::Closed)?;
        inner.check()?;
        self.compact_locked(inner, &|_| true)
    }

    /// 关闭存储：刷盘并释放映射。此后所有操作返回 `Closed`。
    ///
    /// 同时释放在进程内对该路径的占用，允许随后重新打开；即使刷盘失败，
    /// 存储也已关闭，占用同样会释放。
    pub fn close(&self) -> Result<(), KVError> {
        let mut guard = self.inner.write();
        let Some(mut inner) = guard.take() else {
            return Ok(());
        };
        let result = inner.backend.sync();
        drop(inner);
        self.registration.lock().take();
        result
    }

    fn validate_key(&self, key: &str) -> Result<(), KVError> {
        if key.len() > self.options.max_key_bytes {
            return Err(KVError::InvalidKey {
                reason: format!(
                    "key is {} bytes, max {}",
                    key.len(),
                    self.options.max_key_bytes
                ),
            });
        }
        Ok(())
    }

    fn validate_value(&self, value: &[u8]) -> Result<(), KVError> {
        if value.len() > self.options.max_value_bytes {
            return Err(KVError::ValueTooLarge {
                size: value.len(),
                max_bytes: self.options.max_value_bytes,
            });
        }
        Ok(())
    }

    fn validate_mutations(&self, mutations: &[Mutation<'_>]) -> Result<(), KVError> {
        let mut payload = 0usize;
        for mutation in mutations {
            match mutation {
                Mutation::Set { key, value } => {
                    self.validate_key(key)?;
                    self.validate_value(value)?;
                    payload = payload
                        .checked_add(RECORD_HEADER_LEN + key.len() + value.len())
                        .ok_or_else(|| batch_too_large(self.options.max_frame_bytes))?;
                }
                Mutation::Remove { key } => {
                    self.validate_key(key)?;
                    payload = payload
                        .checked_add(RECORD_HEADER_LEN + key.len())
                        .ok_or_else(|| batch_too_large(self.options.max_frame_bytes))?;
                }
                Mutation::Clear => {
                    payload = payload
                        .checked_add(RECORD_HEADER_LEN)
                        .ok_or_else(|| batch_too_large(self.options.max_frame_bytes))?;
                }
            }
        }
        let total = payload
            .checked_add(FRAME_HEADER_LEN)
            .ok_or_else(|| batch_too_large(self.options.max_frame_bytes))?;
        if total > self.options.max_frame_bytes {
            return Err(KVError::BatchTooLarge {
                size: total,
                max_bytes: self.options.max_frame_bytes,
            });
        }
        Ok(())
    }

    fn append_frame(
        &self,
        inner: &mut Inner,
        frame: &[u8],
        count: u32,
    ) -> Result<(), KVError> {
        let frame_len = frame.len() as u64;
        let mut needed = inner
            .valid_end
            .checked_add(frame_len)
            .ok_or(KVError::Full {
                max_bytes: self.options.max_bytes,
            })?;

        if needed > self.options.max_bytes {
            if !self.compact_can_fit(inner, frame_len) {
                return Err(KVError::Full {
                    max_bytes: self.options.max_bytes,
                });
            }
            self.compact_locked(inner, &|_| true)?;
            needed = inner
                .valid_end
                .checked_add(frame_len)
                .ok_or(KVError::Full {
                    max_bytes: self.options.max_bytes,
                })?;
            if needed > self.options.max_bytes {
                return Err(KVError::Full {
                    max_bytes: self.options.max_bytes,
                });
            }
        }

        let offset = inner.valid_end;
        inner.backend.append(offset, frame)?;
        inner.valid_end = needed;

        if let Err(error) = apply_payload(
            &mut inner.index,
            &frame[FRAME_HEADER_LEN..],
            count,
            offset + FRAME_HEADER_LEN as u64,
            false,
        ) {
            // 帧已经写入但索引没能更新：继续服务会读到错误数据，直接毒化。
            inner.poisoned = Some(error.to_string());
            return Err(error);
        }
        inner.writes += 1;
        inner.max_key_versions = inner.index.max_versions;
        if self.should_compact(inner) {
            // 自动回收失败不能否认已经完成的写入；业务可在空闲时重试 compact。
            if let Err(error) = self.compact_locked(inner, &|_| true) {
                log::warn!("n2x-kv: automatic compaction failed: {error}");
            }
        }
        Ok(())
    }

    fn should_compact_on_open(&self) -> bool {
        let guard = self.inner.read();
        let Some(inner) = guard.as_ref() else {
            return false;
        };
        inner.poisoned.is_none() && self.should_compact(inner)
    }

    /// 比例阈值只在确实有收益时触发，热点版本阈值不受 1 MiB 下限约束。
    fn should_compact(&self, inner: &Inner) -> bool {
        let data = inner.valid_end.saturating_sub(FILE_HEADER_LEN);
        let threshold = self
            .options
            .max_bytes
            .saturating_mul(self.options.compact_at_percent as u64)
            / 100;
        let useful_ratio = data >= MIN_COMPACT_BYTES.min(threshold)
            && inner.valid_end >= threshold
            && self.compacted_bytes(inner) < threshold;
        let hot_key = self.options.compact_key_versions != 0
            && inner.max_key_versions >= self.options.compact_key_versions;
        useful_ratio || hot_key
    }

    /// 压缩时的组包上限。估算与真正组包必须共用它；用固定 256 KiB 估算会在
    /// `max_frame_bytes` 偏小时低估帧数，进而批准放不下的压缩。
    fn compact_frame_limit(&self) -> usize {
        COMPACT_FRAME_TARGET
            .min(
                self.options
                    .max_frame_bytes
                    .saturating_sub(FRAME_HEADER_LEN),
            )
            .max(1)
    }

    fn compacted_bytes(&self, inner: &Inner) -> u64 {
        // 与实际压缩共用组包规则，避免大小不均的记录让收益和容量预判失真。
        packed_file_bytes(
            inner
                .index
                .map
                .values()
                .map(|entry| entry.record_bytes as usize),
            self.compact_frame_limit(),
        )
    }

    fn compact_can_fit(&self, inner: &Inner, frame_len: u64) -> bool {
        self.compacted_bytes(inner)
            .checked_add(frame_len)
            .is_some_and(|needed| needed <= self.options.max_bytes)
    }

    fn compact_if_large(&self) -> Result<bool, KVError> {
        let mut guard = self.inner.write();
        let inner = guard.as_mut().ok_or(KVError::Closed)?;
        inner.check()?;
        if !self.should_compact(inner) {
            return Ok(false);
        }
        self.compact_locked(inner, &|_| true)?;
        Ok(true)
    }

    /// 满容量时把一批只减不增的变更直接实现为一次压缩。
    ///
    /// `Clear` 之后只剩删除，最终状态就是“清空”；否则是“保留未被删除的键”。
    /// 与“追加一帧再回收”的结果相同，但不需要任何空闲空间。
    fn reclaim_locked(
        &self,
        inner: &mut Inner,
        mutations: &[Mutation<'_>],
    ) -> Result<(), KVError> {
        let mut cleared = false;
        let mut removed: Vec<&str> = Vec::new();
        for mutation in mutations {
            match mutation {
                Mutation::Remove { key } => removed.push(key),
                Mutation::Clear => cleared = true,
                Mutation::Set { key, .. } => {
                    return Err(KVError::Corrupt {
                        reason: format!("reclaiming compaction received a set for key {key:?}"),
                    });
                }
            }
        }
        if cleared {
            return self.compact_locked(inner, &|_| false);
        }
        self.compact_locked(inner, &|candidate| !removed.contains(&candidate))
    }

    /// 调用方必须持有写锁。
    ///
    /// `keep` 决定哪些键进入新文件；普通压缩保留全部，满容量时的 `remove` / `clear`
    /// 借它完成删除与清空，从而不必先追加一帧。
    fn compact_locked(
        &self,
        inner: &mut Inner,
        keep: &dyn Fn(&str) -> bool,
    ) -> Result<(), KVError> {
        let mut entries: Vec<_> = inner
            .index
            .map
            .iter()
            .filter(|(key, _)| keep(key))
            .collect();
        let mut limit = self.compact_frame_limit();
        // 历史文件已超限时允许保留原有数据，但压缩不能把一个未超限的库扩成超限。
        let capacity = self.options.max_bytes.max(inner.valid_end);
        let mut packed_bytes = packed_file_bytes(
            entries.iter().map(|(_, entry)| entry.record_bytes as usize),
            limit,
        );
        if packed_bytes > capacity {
            // 目标帧拆分或哈希顺序可能增加帧数。按日志顺序保留活记录，并恢复到
            // 完整帧上限；配置未缩小时，不会比来源日志需要更多帧。
            entries.sort_unstable_by_key(|(_, entry)| entry.value_offset);
            limit = self.options.max_frame_bytes - FRAME_HEADER_LEN;
            packed_bytes = packed_file_bytes(
                entries.iter().map(|(_, entry)| entry.record_bytes as usize),
                limit,
            );
        }
        if packed_bytes > capacity {
            return Err(KVError::Full {
                max_bytes: self.options.max_bytes,
            });
        }
        let tmp_path = compact_path(&self.path);
        match std::fs::remove_file(&tmp_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }

        let mut tmp_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&tmp_path)?;
        tmp_file.set_len(FILE_HEADER_LEN)?;
        tmp_file.seek(SeekFrom::Start(0))?;
        tmp_file.write_all(&file_header())?;

        let mut out_offset = FILE_HEADER_LEN;
        let mut new_index = Index::new();
        let mut frame = FrameBuf::new();
        let mut pending: Vec<PendingEntry> = Vec::new();

        {
            let backend = &inner.backend;
            let mut value_buf: Vec<u8> = Vec::new();
            for (key, entry) in entries {
                value_buf.clear();
                value_buf.resize(entry.value_len as usize, 0);
                backend.read(entry.value_offset, value_buf.len(), &mut value_buf)?;
                let value: &[u8] = &value_buf;
                let record_payload = RECORD_HEADER_LEN + key.len() + value.len();
                if !frame.is_empty() && frame.payload_len() + record_payload > limit {
                    flush_compact_frame(
                        &mut tmp_file,
                        &mut frame,
                        &mut pending,
                        &mut out_offset,
                        &mut new_index,
                    )?;
                }
                let payload_pos = frame.payload_len();
                frame.push_set(key, value)?;
                pending.push(PendingEntry {
                    key: key.clone(),
                    payload_pos: payload_pos as u32,
                    value_len: value.len() as u32,
                });
                if frame.payload_len() >= limit {
                    flush_compact_frame(
                        &mut tmp_file,
                        &mut frame,
                        &mut pending,
                        &mut out_offset,
                        &mut new_index,
                    )?;
                }
            }
        }
        flush_compact_frame(
            &mut tmp_file,
            &mut frame,
            &mut pending,
            &mut out_offset,
            &mut new_index,
        )?;
        tmp_file.sync_all()?;

        // 正式发布前再校验实际大小，失败保留原库。
        if out_offset > capacity {
            drop(tmp_file);
            let _ = std::fs::remove_file(&tmp_path);
            return Err(KVError::Full {
                max_bytes: self.options.max_bytes,
            });
        }
        // 宿主调小上限后仍保留历史数据；普通写入受限，允许删除与清空。
        if out_offset > self.options.max_bytes {
            log::warn!(
                "n2x-kv: compacted {} down to {} bytes, which still exceeds max_bytes {}; \
                 only remove/clear/compact can proceed until it shrinks",
                self.path.display(),
                out_offset,
                self.options.max_bytes
            );
        }

        // 先在临时文件上构建新后端，再 rename；这样 rename 之后不需要重新
        // 打开路径，也就不存在“rename 成功但 reopen 失败”的窗口。
        let (new_backend, new_degraded) = match open_backend(tmp_file, &self.options) {
            Ok(backend) => backend,
            Err(error) => {
                let _ = std::fs::remove_file(&tmp_path);
                return Err(error);
            }
        };
        if let Err(error) = std::fs::rename(&tmp_path, &self.path) {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(error.into());
        }
        sync_parent_dir(&self.path);

        inner.backend = new_backend;
        inner.valid_end = out_offset;
        inner.degraded = new_degraded;
        inner.max_key_versions = new_index.max_versions;
        inner.index = new_index;
        inner.compactions += 1;
        Ok(())
    }
}

struct PendingEntry {
    key: Box<str>,
    payload_pos: u32,
    value_len: u32,
}

/// 按不可拆分的记录组包，包含每帧未用满的空间对帧数量的影响。
fn packed_file_bytes(records: impl Iterator<Item = usize>, limit: usize) -> u64 {
    let mut bytes = FILE_HEADER_LEN;
    let mut payload = 0usize;
    for record in records {
        if payload != 0 && payload.saturating_add(record) > limit {
            bytes = bytes.saturating_add(FRAME_HEADER_LEN as u64);
            payload = 0;
        }
        bytes = bytes.saturating_add(record as u64);
        payload = payload.saturating_add(record);
        if payload >= limit {
            bytes = bytes.saturating_add(FRAME_HEADER_LEN as u64);
            payload = 0;
        }
    }
    if payload != 0 {
        bytes = bytes.saturating_add(FRAME_HEADER_LEN as u64);
    }
    bytes
}

fn flush_compact_frame(
    file: &mut File,
    frame: &mut FrameBuf,
    pending: &mut Vec<PendingEntry>,
    out_offset: &mut u64,
    index: &mut Index,
) -> Result<(), KVError> {
    if frame.is_empty() {
        return Ok(());
    }
    let frame_start = *out_offset;
    let (bytes, _count) = std::mem::replace(frame, FrameBuf::new()).finish();
    file.seek(SeekFrom::Start(frame_start))?;
    file.write_all(&bytes)?;

    let payload_base = frame_start + FRAME_HEADER_LEN as u64;
    for entry in pending.drain(..) {
        let key_len = entry.key.len();
        let value_offset = payload_base
            .checked_add(entry.payload_pos as u64)
            .and_then(|offset| offset.checked_add(RECORD_HEADER_LEN as u64))
            .and_then(|offset| offset.checked_add(key_len as u64))
            .ok_or_else(|| KVError::Corrupt {
                reason: "compacted value offset overflow".into(),
            })?;
        let record_bytes = u32::try_from(RECORD_HEADER_LEN + key_len + entry.value_len as usize)
            .map_err(|_| KVError::BatchTooLarge {
                size: RECORD_HEADER_LEN + key_len + entry.value_len as usize,
                max_bytes: u32::MAX as usize,
            })?;
        index.set_compacted(entry.key, value_offset, entry.value_len, record_bytes);
    }
    *out_offset = frame_start + bytes.len() as u64;
    Ok(())
}

/// 一批变更是否只减少活数据（不含任何 `Set`）。
fn shrinks_only(mutations: &[Mutation<'_>]) -> bool {
    mutations
        .iter()
        .all(|mutation| !matches!(mutation, Mutation::Set { .. }))
}

fn apply_payload(
    index: &mut Index,
    payload: &[u8],
    count: u32,
    payload_base: u64,
    validate_first: bool,
) -> Result<(), KVError> {
    // 恢复必须整帧生效，不能在解析失败后留下指向已截断数据的索引。
    if validate_first {
        for_each_record(payload, count, |_| Ok(()))?;
    }
    for_each_record(payload, count, |record| {
        match record.kind {
            RECORD_SET => {
                let value_offset = payload_base
                    .checked_add(record.value_offset as u64)
                    .ok_or_else(|| KVError::Corrupt {
                        reason: "value offset overflow".into(),
                    })?;
                let value_len =
                    u32::try_from(record.value.len()).map_err(|_| KVError::Corrupt {
                        reason: "value length does not fit".into(),
                    })?;
                let record_bytes =
                    u32::try_from(record.record_len).map_err(|_| KVError::Corrupt {
                        reason: "record length does not fit".into(),
                    })?;
                index.set(record.key, value_offset, value_len, record_bytes);
            }
            RECORD_REMOVE => {
                index.remove(record.key);
            }
            RECORD_CLEAR => {
                index.clear();
            }
            _ => {
                return Err(KVError::Corrupt {
                    reason: "unknown record kind".into(),
                });
            }
        }
        Ok(())
    })
}

fn recover(backend: &dyn Backend) -> Result<(Index, u64, bool), KVError> {
    let file_len = backend.len();
    if file_len < FILE_HEADER_LEN {
        return Err(KVError::Corrupt {
            reason: "file is shorter than its header".into(),
        });
    }
    let mut index = Index::new();
    let mut offset = FILE_HEADER_LEN;
    let mut dirty_tail = false;
    let mut header = [0u8; FRAME_HEADER_LEN];
    let mut payload_buf: Vec<u8> = Vec::new();

    while offset < file_len {
        let remaining = file_len - offset;
        if remaining < FRAME_HEADER_LEN as u64 {
            dirty_tail = true;
            break;
        }
        backend.read(offset, FRAME_HEADER_LEN, &mut header)?;
        match decode_frame_header(&header) {
            FrameHeader::End => {
                // 全零帧头只说明此处是预分配尾部；若其后还有非零字节，那就是
                // 介质上的零洞或外部改写，后续数据不可达，必须显式告警并回收。
                if remaining > FRAME_HEADER_LEN as u64 && !tail_is_zero(backend, offset, file_len)?
                {
                    log::warn!(
                        "n2x-kv: zero frame header at offset {offset} but data follows; dropping tail"
                    );
                    dirty_tail = true;
                }
                break;
            }
            FrameHeader::Invalid => {
                dirty_tail = true;
                break;
            }
            FrameHeader::Frame {
                payload_len,
                count,
                crc,
            } => {
                if payload_len as u64 > HARD_MAX_FRAME_BYTES {
                    log::warn!("n2x-kv: frame at offset {offset} is too large; dropping tail");
                    dirty_tail = true;
                    break;
                }
                let total = FRAME_HEADER_LEN as u64 + payload_len as u64;
                if total > remaining {
                    dirty_tail = true;
                    break;
                }
                payload_buf.clear();
                payload_buf.resize(payload_len as usize, 0);
                let payload_base = offset + FRAME_HEADER_LEN as u64;
                backend.read(payload_base, payload_len as usize, &mut payload_buf)?;
                if crc32fast::hash(&payload_buf) != crc {
                    log::warn!("n2x-kv: frame at offset {offset} failed its checksum");
                    dirty_tail = true;
                    break;
                }
                if let Err(error) =
                    apply_payload(&mut index, &payload_buf, count, payload_base, true)
                {
                    log::warn!("n2x-kv: frame at offset {offset} is unreadable: {error}");
                    dirty_tail = true;
                    break;
                }
                offset += total;
            }
        }
    }
    Ok((index, offset, dirty_tail))
}

fn stats_of(options: &StoreOptions, inner: &Inner) -> Stats {
    let data_bytes = inner.valid_end.saturating_sub(FILE_HEADER_LEN);
    Stats {
        entries: inner.index.len() as u64,
        live_bytes: inner.index.live_payload_bytes,
        dead_bytes: data_bytes.saturating_sub(inner.index.live_record_bytes),
        data_bytes,
        file_bytes: inner.backend.len(),
        max_bytes: options.max_bytes,
        backend: inner.backend.kind(),
        degraded: inner.degraded,
        compactions: inner.compactions,
    }
}

fn prepare_file(file: &mut File, initial_bytes: u64) -> Result<(), KVError> {
    let len = file.metadata()?.len();
    if len == 0 {
        return initialize_file(file, initial_bytes);
    }

    if len < FILE_HEADER_LEN {
        let mut bytes = vec![0u8; len as usize];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut bytes)?;
        if bytes.iter().all(|byte| *byte == 0) {
            return initialize_file(file, initial_bytes);
        }
        return Err(KVError::Corrupt {
            reason: format!("file header is truncated ({len} bytes)"),
        });
    }

    let mut header = [0u8; FILE_HEADER_LEN as usize];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut header)?;
    if header.iter().all(|byte| *byte == 0) {
        // 全零头有两种来源：初始化中途崩溃（整文件都是零，可以安全重建），
        // 或头部被外部清零/介质零洞（日志还在）。后者当成新库会静默截断旧数据。
        if len > FILE_HEADER_LEN && !file_tail_is_zero(file, FILE_HEADER_LEN, len)? {
            return Err(KVError::Corrupt {
                reason: "file header is zeroed but log data follows; refusing to reinitialize"
                    .into(),
            });
        }
        return initialize_file(file, initial_bytes);
    }
    validate_file_header(&header)
}

/// `[start, end)` 是否全为零。用于区分“预分配尾部”与“零洞后的残留数据”。
///
/// 预分配尾部通常有整兆字节的零，这里按 8 字节字比较再退化为逐字节，避免
/// 每次打开都为一段正常的零尾付出逐字节扫描的代价。
fn tail_is_zero(backend: &dyn Backend, start: u64, end: u64) -> Result<bool, KVError> {
    const CHUNK: usize = 64 * 1024;
    let mut buf = vec![0u8; CHUNK];
    let mut offset = start;
    while offset < end {
        // 剩余长度超出 usize 时按 CHUNK 读取：分块大小与语义都不变。
        let len = usize::try_from(end - offset).map_or(CHUNK, |rest| rest.min(CHUNK));
        backend.read(offset, len, &mut buf[..len])?;
        if chunk_has_non_zero(&buf[..len]) {
            return Ok(false);
        }
        offset += len as u64;
    }
    Ok(true)
}

fn file_tail_is_zero(file: &mut File, start: u64, end: u64) -> Result<bool, KVError> {
    const CHUNK: usize = 64 * 1024;
    let mut buf = vec![0u8; CHUNK];
    let mut offset = start;
    while offset < end {
        let len = usize::try_from(end - offset).map_or(CHUNK, |rest| rest.min(CHUNK));
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut buf[..len])?;
        if chunk_has_non_zero(&buf[..len]) {
            return Ok(false);
        }
        offset += len as u64;
    }
    Ok(true)
}

fn chunk_has_non_zero(bytes: &[u8]) -> bool {
    let (words, tail) = bytes.as_chunks::<8>();
    words.iter().any(|word| *word != [0u8; 8]) || tail.iter().any(|byte| *byte != 0)
}

fn initialize_file(file: &mut File, initial_bytes: u64) -> Result<(), KVError> {
    let target = initial_bytes.max(FILE_HEADER_LEN);
    file.set_len(target)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&file_header())?;
    file.sync_all()?;
    Ok(())
}

fn compact_path(path: &Path) -> PathBuf {
    let mut path = path.as_os_str().to_owned();
    path.push(".compact");
    PathBuf::from(path)
}

/// 压缩中断会留下 `<path>.compact`。它不影响正确性（正式文件始终完整），
/// 但会占一份磁盘；下一次 `compact` 开头会删掉它。这里只报告，不主动删除：
/// 没有跨进程锁时，删除可能撞上另一个仍在写临时文件的实例。
fn warn_stale_compact_file(path: &Path) {
    let tmp = compact_path(path);
    if let Ok(metadata) = std::fs::metadata(&tmp) {
        log::warn!(
            "n2x-kv: stale compaction file {} ({} bytes) from an interrupted compaction",
            tmp.display(),
            metadata.len()
        );
    }
}

fn sync_parent_dir(path: &Path) {
    #[cfg(unix)]
    if let Some(parent) = path.parent()
        && let Ok(dir) = File::open(parent)
    {
        let _ = dir.sync_all();
    }
    #[cfg(not(unix))]
    let _ = path;
}

fn invalid_options(reason: &str) -> KVError {
    KVError::InvalidOptions {
        reason: reason.into(),
    }
}

fn batch_too_large(max_bytes: usize) -> KVError {
    KVError::BatchTooLarge {
        size: usize::MAX,
        max_bytes,
    }
}

const _: () = {
    assert!(MIN_COMPACT_BYTES > FILE_HEADER_LEN);
};

impl Stats {
    /// 内存存储的统计构造；文件字段全部为零。
    pub(crate) fn memory(entries: usize, live_bytes: usize, max_bytes: usize) -> Self {
        Self {
            entries: entries as u64,
            live_bytes: live_bytes as u64,
            dead_bytes: 0,
            data_bytes: 0,
            file_bytes: 0,
            max_bytes: max_bytes as u64,
            backend: BackendKind::Memory,
            degraded: false,
            compactions: 0,
        }
    }
}
