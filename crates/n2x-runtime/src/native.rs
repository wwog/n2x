use crate::workspace::storage;
use crate::*;
use n2x_core::*;
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    task::{Poll, Waker},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// n2x-kv 到标准工作区字节后端的适配。
pub struct KvBackend {
    store: n2x_kv::Store,
}
impl KvBackend {
    /// 打开持久状态目录，调用方需要在读写前持有 NativeLock。
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, N2xError> {
        fs::create_dir_all(&directory).map_err(storage)?;
        let store = n2x_kv::Store::open(directory.as_ref().join("state.kv"), Default::default())
            .map_err(storage)?;
        File::open(directory.as_ref())
            .and_then(|file| file.sync_all())
            .map_err(storage)?;
        Ok(Self { store })
    }
}
impl StateBackend for KvBackend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, N2xError> {
        self.store.get(key).map_err(storage)
    }
    fn keys(&self, prefix: &str) -> Result<Vec<String>, N2xError> {
        self.store.keys(Some(prefix)).map_err(storage)
    }
    fn write(
        &mut self,
        values: &BTreeMap<String, Vec<u8>>,
        removes: &BTreeSet<String>,
        durability: Durability,
    ) -> Result<(), N2xError> {
        let mutations: Vec<_> = values
            .iter()
            .map(|(key, value)| n2x_kv::Mutation::Set {
                key: key.as_str(),
                value: value.as_slice(),
            })
            .chain(
                removes
                    .iter()
                    .map(|key| n2x_kv::Mutation::Remove { key: key.as_str() }),
            )
            .collect();
        if !mutations.is_empty() {
            self.store.write(&mutations).map_err(storage)?;
        }
        if durability == Durability::Durable {
            self.store.flush().map_err(storage)?;
        }
        Ok(())
    }
}

/// 把被回收运行的材料写成按运行命名的 JSONL 文件；成功后才允许从 KV 删除。
pub struct NativeArchive {
    directory: PathBuf,
}
impl NativeArchive {
    /// 创建归档目录并确认目录项持久化。
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, N2xError> {
        let directory = directory.as_ref().to_owned();
        fs::create_dir_all(&directory).map_err(storage)?;
        File::open(&directory)
            .and_then(|file| file.sync_all())
            .map_err(storage)?;
        Ok(Self { directory })
    }
}
impl RunArchive for NativeArchive {
    fn append(&mut self, batch: &ArchiveBatch) -> Result<(), N2xError> {
        let path = self.directory.join(archive_name(&batch.run));
        if path.exists() {
            // 上一次归档已完成、但 KV 删除未确认；重试保持幂等。
            return Ok(());
        }
        let temp = self.directory.join(format!(
            ".pending-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        if let Err(error) = write_archive(&temp, batch) {
            let _ = fs::remove_file(&temp);
            return Err(error);
        }
        fs::rename(&temp, &path).map_err(storage)?;
        if let Err(error) = File::open(&self.directory).and_then(|file| file.sync_all()) {
            // 目录项未确认持久化时不要留下一个“看起来已完成”的归档。
            let _ = fs::remove_file(&path);
            return Err(storage(error));
        }
        Ok(())
    }
}

fn write_archive(path: &Path, batch: &ArchiveBatch) -> Result<(), N2xError> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(storage)?;
    let header = serde_json::json!({
        "format_version": 1,
        "run": &batch.run,
        "scope": &batch.scope,
        "current": &batch.current,
        "target": &batch.target,
        "plan": batch.plan.map(digest_hex),
        "retire_commit": &batch.retire_commit,
        "archived_at": SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64,
        "entries": batch.entries.len(),
    });
    write_json_line(&mut file, &header)?;
    for entry in &batch.entries {
        write_json_line(&mut file, entry)?;
    }
    file.sync_all().map_err(storage)?;
    Ok(())
}

fn write_json_line<T: serde::Serialize>(file: &mut File, value: &T) -> Result<(), N2xError> {
    serde_json::to_writer(&mut *file, value).map_err(storage)?;
    file.write_all(b"\n").map_err(storage)?;
    Ok(())
}

fn digest_hex(digest: Digest) -> String {
    digest.0.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn archive_name(run: &RunId) -> String {
    let digest = Sha256::digest(run.0.as_bytes());
    let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    let mut slug: String = run
        .0
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .take(48)
        .collect();
    if slug.is_empty() {
        slug.push_str("run");
    }
    format!("{slug}-{}.ndjson", &hex[..16])
}

/// 通过独立锁文件取得工作区进程级独占权；文件不会随 KV compact 替换。
pub struct NativeLock {
    path: PathBuf,
}
impl NativeLock {
    /// 创建锁描述；真正取锁发生在 acquire。
    pub fn new(directory: impl AsRef<Path>) -> Self {
        Self {
            path: directory.as_ref().join("workspace.lock"),
        }
    }

    /// 同步取得进程锁，可在打开 KV 之前使用。
    pub fn try_acquire(&mut self) -> Result<Box<dyn WorkspaceGuard>, N2xError> {
        fs::create_dir_all(self.path.parent().unwrap()).map_err(storage)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.path)
            .map_err(storage)?;
        file.try_lock().map_err(|error| {
            N2xError::new(
                "n2x.workspace.locked",
                ErrorKind::Conflict,
                ErrorOrigin::Workspace,
                error.to_string(),
            )
        })?;
        Ok(Box::new(FileGuard { _file: file }))
    }
}
struct FileGuard {
    _file: File,
}
impl WorkspaceGuard for FileGuard {}
impl WorkspaceLock for NativeLock {
    fn acquire(&mut self) -> PortResult<'_, Box<dyn WorkspaceGuard>> {
        Box::pin(async move { self.try_acquire() })
    }
}

impl Migrator {
    /// 原生组合入口：先独占目录，再打开 KV，默认使用系统时钟和标准状态工作区。
    pub fn open_native(
        directory: impl AsRef<Path>,
        manifests: Vec<Manifest>,
        registry: Registry,
    ) -> Result<Self, N2xError> {
        let mut lock = NativeLock::new(&directory);
        let guard = lock.try_acquire()?;
        let archive = NativeArchive::open(directory.as_ref().join("archive"))?;
        let workspace =
            StateWorkspace::new(KvBackend::open(&directory)?).with_archive(Box::new(archive));
        Ok(Self::new(
            manifests,
            registry,
            Box::new(workspace),
            Box::new(NativeClock),
            Box::new(lock),
        )
        .with_guard(guard))
    }
}

/// 原生系统时钟与非阻塞线程定时器，无需绑定具体异步执行器。
#[derive(Default)]
pub struct NativeClock;
impl Clock for NativeClock {
    fn now(&self) -> Timestamp {
        Timestamp(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(i64::MAX as u128) as i64,
        )
    }
    fn wake_at(&mut self, at: Timestamp) -> PortResult<'_, Timestamp> {
        let delay = Duration::from_millis(at.0.saturating_sub(self.now().0).max(0) as u64);
        let shared = Arc::new(Mutex::new((false, None::<Waker>)));
        let thread_shared = shared.clone();
        std::thread::spawn(move || {
            std::thread::sleep(delay);
            let mut state = thread_shared.lock().unwrap();
            state.0 = true;
            if let Some(waker) = state.1.take() {
                waker.wake();
            }
        });
        Box::pin(async move {
            std::future::poll_fn(move |cx| {
                let mut state = shared.lock().unwrap();
                if state.0 {
                    Poll::Ready(())
                } else {
                    state.1 = Some(cx.waker().clone());
                    Poll::Pending
                }
            })
            .await;
            Ok(NativeClock.now())
        })
    }
}

/// 本地持久 Blob 暂存服务，按内容摘要寻址。
pub struct NativeBlobStore {
    directory: PathBuf,
}
impl NativeBlobStore {
    /// 创建 spool 目录。
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, N2xError> {
        fs::create_dir_all(&directory).map_err(storage)?;
        Ok(Self {
            directory: directory.as_ref().to_owned(),
        })
    }
    fn path(&self, blob: &BlobRef) -> Result<PathBuf, N2xError> {
        if blob.id.0.len() != 64 || !blob.id.0.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(storage("invalid blob ID"));
        }
        Ok(self.directory.join(&blob.id.0))
    }
}
struct NativeBlobWriter {
    file: File,
    temp: PathBuf,
    directory: PathBuf,
    codec: String,
    version: u32,
}
impl Drop for NativeBlobWriter {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.temp);
    }
}
impl BlobWriter for NativeBlobWriter {
    fn append(&mut self, bytes: Vec<u8>) -> PortResult<'_, ()> {
        Box::pin(async move { self.file.write_all(&bytes).map_err(storage) })
    }
    fn finish(mut self: Box<Self>) -> PortResult<'static, BlobRef> {
        Box::pin(async move {
            self.file.sync_all().map_err(storage)?;
            self.file.seek(SeekFrom::Start(0)).map_err(storage)?;
            let (digest, length) = hash_file(&mut self.file)?;
            let id = digest
                .0
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            let destination = self.directory.join(&id);
            fs::rename(&self.temp, &destination).map_err(storage)?;
            File::open(&self.directory)
                .and_then(|file| file.sync_all())
                .map_err(storage)?;
            Ok(BlobRef {
                id: id.into(),
                codec: self.codec.clone(),
                version: self.version,
                bytes: length,
                digest,
            })
        })
    }
}
impl BlobStore for NativeBlobStore {
    fn create(&mut self, codec: String, version: u32) -> PortResult<'_, Box<dyn BlobWriter>> {
        Box::pin(async move {
            let temp = self.directory.join(format!(
                ".pending-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            ));
            let file = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&temp)
                .map_err(storage)?;
            Ok(Box::new(NativeBlobWriter {
                file,
                temp,
                directory: self.directory.clone(),
                codec,
                version,
            }) as Box<dyn BlobWriter>)
        })
    }
    fn read_range(&mut self, blob: BlobRef, offset: u64, limit: u32) -> PortResult<'_, Vec<u8>> {
        Box::pin(async move {
            let path = self.path(&blob)?;
            let mut file = File::open(path).map_err(storage)?;
            let (digest, length) = hash_file(&mut file)?;
            if length != blob.bytes || digest != blob.digest {
                return Err(storage("blob integrity mismatch"));
            }
            file.seek(SeekFrom::Start(offset.min(length)))
                .map_err(storage)?;
            let mut bytes = Vec::new();
            file.take(limit as u64)
                .read_to_end(&mut bytes)
                .map_err(storage)?;
            Ok(bytes)
        })
    }
    fn remove(&mut self, blob: BlobRef) -> PortResult<'_, ()> {
        Box::pin(async move {
            match fs::remove_file(self.path(&blob)?) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(storage(error)),
            }
        })
    }
}

fn hash_file(file: &mut File) -> Result<(n2x_core::Digest, u64), N2xError> {
    file.seek(SeekFrom::Start(0)).map_err(storage)?;
    let mut hash = Sha256::new();
    let mut length = 0;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(storage)?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
        length += read as u64;
    }
    Ok((n2x_core::Digest(hash.finalize().into()), length))
}
