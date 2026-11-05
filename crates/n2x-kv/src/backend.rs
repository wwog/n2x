//! 文件后端：mmap 主路径与可自动降级的缓冲路径。
//!
//! 两条路径暴露同一组操作，`Store` 只持有一个 `Box<dyn Backend>`。
//! 选择在打开时完成：mmap 失败就整体切到 buffered，不在写入中途混用。

use std::fs::File;
use std::io::{Seek, SeekFrom, Write};

use memmap2::MmapMut;

use crate::KVError;
use crate::format::{FILE_HEADER_LEN, FRAME_HEADER_LEN};
use crate::store::StoreOptions;

/// 实际使用的后端。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BackendKind {
    /// 内存映射文件。
    #[default]
    Mmap,
    /// 文件 IO（定位读，无内存副本）。
    Buffered,
    /// 纯内存（仅 `MemoryStore` 使用）。
    Memory,
}

pub(crate) trait Backend: Send + Sync {
    fn len(&self) -> u64;
    /// 从 `offset` 读取 `len` 字节写入 `buf`；要求 `buf.len() == len`。
    /// 拷贝语义而不是借用语义：调用方不持有后端内部内存的引用。
    fn read(&self, offset: u64, len: usize, buf: &mut [u8]) -> Result<(), KVError>;
    fn append(&mut self, offset: u64, bytes: &[u8]) -> Result<(), KVError>;
    fn truncate(&mut self, len: u64) -> Result<(), KVError>;
    fn sync(&mut self) -> Result<(), KVError>;
    fn kind(&self) -> BackendKind;
}

/// mmap 后端。读写都直接落在映射页上，`flush` 时才 msync + fsync。
pub(crate) struct Mapped {
    file: File,
    map: MmapMut,
    initial_bytes: u64,
    max_bytes: u64,
}

impl Mapped {
    pub(crate) fn new(file: File, options: &StoreOptions) -> Result<Self, KVError> {
        let len = file.metadata()?.len();
        if len == 0 {
            return Err(KVError::Backend {
                reason: "cannot mmap an empty file".into(),
            });
        }
        // SAFETY: 文件由本进程独占打开；本 crate 的写入都经过 Store 的读写锁，
        // 不会与映射外的并发写入竞争。
        let map = unsafe { MmapMut::map_mut(&file) }.map_err(|error| KVError::Backend {
            reason: format!("cannot mmap {len} bytes: {error}"),
        })?;
        probe_mapping(&map)?;
        Ok(Self {
            file,
            map,
            initial_bytes: options.initial_bytes,
            max_bytes: options.max_bytes,
        })
    }

    fn grow(&mut self, needed: u64) -> Result<(), KVError> {
        let current = self.map.len() as u64;
        let target = growth_target(current, needed, self.initial_bytes, self.max_bytes);
        if target < needed {
            return Err(KVError::Full {
                max_bytes: self.max_bytes,
            });
        }
        if target > current {
            self.file.set_len(target)?;
            // SAFETY: 持有写锁，且旧映射在赋值后立即释放；没有外部写入者。
            let new_map = unsafe { MmapMut::map_mut(&self.file)? };
            self.map = new_map;
        }
        Ok(())
    }
}

impl Backend for Mapped {
    fn len(&self) -> u64 {
        self.map.len() as u64
    }

    fn read(&self, offset: u64, len: usize, buf: &mut [u8]) -> Result<(), KVError> {
        debug_assert_eq!(buf.len(), len);
        let start = usize::try_from(offset).map_err(|_| out_of_range(offset, len))?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| out_of_range(offset, len))?;
        let bytes = self
            .map
            .get(start..end)
            .ok_or_else(|| out_of_range(offset, len))?;
        buf.copy_from_slice(bytes);
        Ok(())
    }

    fn append(&mut self, offset: u64, bytes: &[u8]) -> Result<(), KVError> {
        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or(KVError::Full {
                max_bytes: self.max_bytes,
            })?;
        if end > self.map.len() as u64 {
            self.grow(end)?;
        }
        let start = usize::try_from(offset).map_err(|_| out_of_range(offset, bytes.len()))?;
        let end = start
            .checked_add(bytes.len())
            .ok_or_else(|| out_of_range(offset, bytes.len()))?;
        if end > self.map.len() {
            return Err(KVError::Full {
                max_bytes: self.max_bytes,
            });
        }
        self.map[start..end].copy_from_slice(bytes);
        Ok(())
    }

    fn truncate(&mut self, len: u64) -> Result<(), KVError> {
        if len < FILE_HEADER_LEN {
            return Err(KVError::Backend {
                reason: format!("refusing to truncate below the file header ({len} bytes)"),
            });
        }
        // 缩小映射文件前必须先释放旧映射，否则访问被截断的页会触发 SIGBUS。
        let placeholder = MmapMut::map_anon(1)?;
        let old = std::mem::replace(&mut self.map, placeholder);
        drop(old);
        self.file.set_len(len)?;
        // SAFETY: 旧映射已释放，文件由本进程独占。
        self.map = unsafe { MmapMut::map_mut(&self.file)? };
        Ok(())
    }

    fn sync(&mut self) -> Result<(), KVError> {
        self.map.flush()?;
        self.file.sync_all()?;
        Ok(())
    }

    fn kind(&self) -> BackendKind {
        BackendKind::Mmap
    }
}

/// 文件 IO 后端。读按 offset 直接走文件，写按 offset 落文件，不保留内存副本。
///
/// 用于 mmap 不可用、被策略禁用，或需要确定行为的测试。内存占用 O(1)；
/// 代价是每次读一个系统调用，仅作降级路径。
pub(crate) struct Buffered {
    file: File,
    /// 与文件当前分配长度一致；`len()` 由此返回，避免每次读元数据。
    len: u64,
    initial_bytes: u64,
    max_bytes: u64,
}

impl Buffered {
    pub(crate) fn new(mut file: File, options: &StoreOptions) -> Result<Self, KVError> {
        let len = file.metadata()?.len();
        file.seek(SeekFrom::Start(0))?;
        Ok(Self {
            file,
            len,
            initial_bytes: options.initial_bytes,
            max_bytes: options.max_bytes,
        })
    }

    fn ensure_len(&mut self, needed: u64) -> Result<(), KVError> {
        if needed <= self.len {
            return Ok(());
        }
        let target = growth_target(self.len, needed, self.initial_bytes, self.max_bytes);
        if target < needed {
            return Err(KVError::Full {
                max_bytes: self.max_bytes,
            });
        }
        if let Err(error) = self.file.set_len(target) {
            return Err(error.into());
        }
        self.len = target;
        Ok(())
    }
}

impl Backend for Buffered {
    fn len(&self) -> u64 {
        self.len
    }

    fn read(&self, offset: u64, len: usize, buf: &mut [u8]) -> Result<(), KVError> {
        debug_assert_eq!(buf.len(), len);
        // 读锁允许多个读者并发，必须用定位读，不能动共享的 fd 位置游标。
        match read_exact_at(&self.file, buf, offset) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                Err(out_of_range(offset, len))
            }
            Err(error) => Err(error.into()),
        }
    }

    fn append(&mut self, offset: u64, bytes: &[u8]) -> Result<(), KVError> {
        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or(KVError::Full {
                max_bytes: self.max_bytes,
            })?;
        self.ensure_len(end)?;
        let mut handle = &self.file;
        handle.seek(SeekFrom::Start(offset))?;
        if let Err(error) = handle.write_all(bytes) {
            // 保持后端停在调用方认为的逻辑末尾，避免留下半帧。
            let _ = self.file.set_len(offset);
            self.len = offset;
            return Err(error.into());
        }
        Ok(())
    }

    fn truncate(&mut self, len: u64) -> Result<(), KVError> {
        if len < FILE_HEADER_LEN {
            return Err(KVError::Backend {
                reason: format!("refusing to truncate below the file header ({len} bytes)"),
            });
        }
        self.file.set_len(len)?;
        self.len = len;
        Ok(())
    }

    fn sync(&mut self) -> Result<(), KVError> {
        self.file.sync_all()?;
        Ok(())
    }

    fn kind(&self) -> BackendKind {
        BackendKind::Buffered
    }
}

/// 定位读：按 offset 读取且不碰共享的 fd 位置游标，读锁下的并发读者互不干扰。
/// Unix 的 `read_exact_at` 保证读满；Windows 的 `seek_read` 允许短读，循环补齐。
#[cfg(unix)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

#[cfg(windows)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut done = 0usize;
    while done < buf.len() {
        match file.seek_read(&mut buf[done..], offset + done as u64) {
            Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => done += n,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// 映射可用性校验：映射成功不等于页面可访问。
///
/// 主动读首尾字节触发缺页，把 open 阶段能暴露的权限、外部截断等问题变成
/// `Backend` 错误，`Auto` 才有机会降级。运行期文件被外部截断、磁盘在缺页时
/// 耗尽或 iOS 文件保护锁住页面会触发 SIGBUS；那是进程信号，不是 `Result`
/// 能捕获的错误，只能由调用方避免外部截断并做业务级容量管理。
fn probe_mapping(map: &MmapMut) -> Result<(), KVError> {
    if map.is_empty() {
        return Err(KVError::Backend {
            reason: "mmap returned an empty mapping".into(),
        });
    }
    let first = map[0];
    let last = map[map.len() - 1];
    std::hint::black_box((first, last));
    Ok(())
}

/// 几何增长，带初始容量下限和硬上限。
fn growth_target(current: u64, needed: u64, initial: u64, max: u64) -> u64 {
    let mut target = current.max(initial).min(max);
    while target < needed && target < max {
        target = target.saturating_mul(2).min(max);
        if target == 0 {
            break;
        }
    }
    target.max(needed).min(max)
}

fn out_of_range(offset: u64, len: usize) -> KVError {
    KVError::Corrupt {
        reason: format!("read range offset={offset} len={len} is outside the file"),
    }
}

/// 打开后端；`Auto` 在 mmap 失败时自动降级。
pub(crate) fn open_backend(
    file: File,
    options: &StoreOptions,
) -> Result<(Box<dyn Backend>, bool), KVError> {
    match options.backend {
        crate::BackendPreference::Buffered => Ok((Box::new(Buffered::new(file, options)?), false)),
        crate::BackendPreference::Mmap => Ok((Box::new(Mapped::new(file, options)?), false)),
        crate::BackendPreference::Auto => {
            let probe = file.try_clone()?;
            match Mapped::new(probe, options) {
                Ok(mapped) => Ok((Box::new(mapped), false)),
                Err(error) => {
                    log::warn!(
                        "n2x-kv: mmap unavailable ({error}); falling back to buffered backend"
                    );
                    Ok((Box::new(Buffered::new(file, options)?), true))
                }
            }
        }
    }
}

const _: () = {
    // 帧头长度必须能放进文件头之后的第一个帧里。
    assert!(FRAME_HEADER_LEN < FILE_HEADER_LEN as usize);
};

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use tempfile::tempdir;

    use super::*;
    use crate::BackendPreference;

    fn default_options() -> StoreOptions {
        StoreOptions::default()
    }

    fn set_readonly(path: &std::path::Path, readonly: bool) {
        let mut permissions = std::fs::metadata(path).expect("metadata").permissions();
        permissions.set_readonly(readonly);
        std::fs::set_permissions(path, permissions).expect("set permissions");
    }

    #[test]
    fn growth_is_geometric_and_capped() {
        assert_eq!(growth_target(64, 100, 1024, 4096), 1024);
        assert_eq!(growth_target(1024, 1100, 1024, 4096), 2048);
        assert_eq!(growth_target(2048, 4096, 1024, 4096), 4096);
        assert_eq!(growth_target(4096, 5000, 1024, 4096), 4096);
        assert_eq!(growth_target(0, 64, 64, 128), 64);
    }

    #[test]
    fn mmap_rejects_an_empty_file() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("empty.kv");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .expect("create");
        assert!(matches!(
            Mapped::new(file, &default_options()),
            Err(KVError::Backend { .. })
        ));
    }

    #[test]
    fn mmap_failure_is_reported_as_a_backend_error() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("readonly.kv");
        std::fs::write(&path, vec![0u8; 4096]).expect("write");
        set_readonly(&path, true);
        let file = File::open(&path).expect("open read-only");

        let Err(error) = Mapped::new(file, &default_options()) else {
            panic!("mmap must fail");
        };
        assert!(matches!(error, KVError::Backend { .. }));
        assert!(error.to_string().contains("cannot mmap"));

        set_readonly(&path, false);
    }

    #[test]
    fn auto_falls_back_when_mmap_is_unavailable() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("fallback.kv");
        std::fs::write(&path, vec![0u8; 4096]).expect("write");
        set_readonly(&path, true);
        let file = File::open(&path).expect("open read-only");

        let options = StoreOptions {
            backend: BackendPreference::Auto,
            ..default_options()
        };
        let (backend, degraded) = open_backend(file, &options).expect("fallback");
        assert!(degraded);
        assert_eq!(backend.kind(), BackendKind::Buffered);
        drop(backend);

        set_readonly(&path, false);
    }
}
