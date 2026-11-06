//! 只读快照：不打开 [`Store`](crate::Store)，直接扫描日志文件。
//!
//! 查看工具、崩溃排查和离线导出都不应改变被查看的文件。`Store::open`
//! 会登记进程内路径、截断撕裂尾部，并可能在打开后触发压缩；快照路径
//! 则只用只读 mmap 读取，不写一个字节，也不参与进程内的路径占用。
//!
//! 扫描语义与恢复一致：逐帧校验 CRC 与记录，遇到第一个不完整或损坏的
//! 帧就停止，把其前的合法帧作为快照内容，并通过
//! [`Snapshot::tail_truncated`] 报告尾部被丢弃。因此正在被其它进程追加
//! 的文件也能读出一个一致的旧视图。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::KVError;
use crate::format::{
    FILE_HEADER_LEN, FRAME_HEADER_LEN, FrameHeader, HARD_MAX_FRAME_BYTES, RECORD_CLEAR,
    RECORD_REMOVE, RECORD_SET, decode_frame_header, for_each_record, validate_file_header,
};

/// 快照中的一个活键。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotEntry {
    /// 键。
    pub key: String,
    /// 最后一次写入的值。
    pub value: Vec<u8>,
    /// 值在文件中的绝对偏移，用于对照原始字节。
    pub value_offset: u64,
    /// 自该键上次删除/清空以来，日志中 `set` 记录的条数。
    ///
    /// 用于观察热点键的写入频次；压缩后历史被回收，重开文件时从 1 开始。
    pub updates: u32,
}

/// 一次只读扫描的结果。
#[derive(Clone, Debug)]
pub struct Snapshot {
    /// 被扫描的文件路径。
    pub path: PathBuf,
    /// 打开时的文件长度。
    pub file_bytes: u64,
    /// 最后一个完整帧的结束偏移；其后是被丢弃的尾部。
    pub valid_bytes: u64,
    /// 有效帧数量。
    pub frames: u64,
    /// 有效记录数量（`set` / `remove` / `clear` 都计入）。
    pub records: u64,
    /// 文件尾部是否无法解析（撕裂写、CRC 错或零洞后的残留数据）。
    pub tail_truncated: bool,
    /// 当前活键，按字典序排序。
    pub entries: Vec<SnapshotEntry>,
}

impl Snapshot {
    /// 按键查找快照中的条目。
    pub fn get(&self, key: &str) -> Option<&SnapshotEntry> {
        self.entries
            .binary_search_by(|entry| entry.key.as_str().cmp(key))
            .ok()
            .map(|index| &self.entries[index])
    }

    /// 活键的键与值字节总数（不含记录头与帧头）。
    pub fn live_bytes(&self) -> u64 {
        self.entries
            .iter()
            .map(|entry| (entry.key.len() + entry.value.len()) as u64)
            .sum()
    }
}

struct Live {
    value: Vec<u8>,
    value_offset: u64,
    updates: u32,
}

/// 扫描文件并返回当前活键。
///
/// 只读打开，不创建文件、不加进程内路径占用、不写回任何内容。文件为空、
/// 头部全零或魔数/版本不符时返回 [`KVError::Corrupt`]。
pub fn read_snapshot(path: impl AsRef<Path>) -> Result<Snapshot, KVError> {
    let path = path.as_ref();
    let file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    if len == 0 {
        return Err(KVError::Corrupt {
            reason: format!("{} is empty", path.display()),
        });
    }
    // SAFETY: 只读映射；扫描只解析映射长度以内的字节，从不写入。其它进程
    // 可能同时追加，但追加不会改变已映射前缀的内容，最坏情况是漏看新帧。
    let map = unsafe { memmap2::Mmap::map(&file) }.map_err(|error| KVError::Backend {
        reason: format!("cannot map {}: {error}", path.display()),
    })?;
    scan(path, &map)
}

fn scan(path: &Path, bytes: &[u8]) -> Result<Snapshot, KVError> {
    if bytes.len() >= FILE_HEADER_LEN as usize
        && bytes[..FILE_HEADER_LEN as usize]
            .iter()
            .all(|byte| *byte == 0)
    {
        return Err(KVError::Corrupt {
            reason: format!(
                "{} has a zeroed header (the store was never initialized)",
                path.display()
            ),
        });
    }
    validate_file_header(bytes)?;

    let mut live: BTreeMap<String, Live> = BTreeMap::new();
    let mut offset = FILE_HEADER_LEN as usize;
    let mut frames = 0u64;
    let mut records = 0u64;
    let mut tail_truncated = false;

    while offset < bytes.len() {
        let remaining = bytes.len() - offset;
        let header = &bytes[offset..offset + FRAME_HEADER_LEN.min(remaining)];
        match decode_frame_header(header) {
            FrameHeader::End => {
                // 零帧头之后若还有非零字节，说明数据不可达（零洞或外部改写），
                // 与恢复路径一样将其标记为被丢弃的尾部。
                if remaining > FRAME_HEADER_LEN
                    && bytes[offset + FRAME_HEADER_LEN..]
                        .iter()
                        .any(|byte| *byte != 0)
                {
                    tail_truncated = true;
                }
                break;
            }
            FrameHeader::Invalid => {
                tail_truncated = true;
                break;
            }
            FrameHeader::Frame {
                payload_len,
                count,
                crc,
            } => {
                if payload_len as u64 > HARD_MAX_FRAME_BYTES {
                    tail_truncated = true;
                    break;
                }
                let payload_start = offset + FRAME_HEADER_LEN;
                // 先用 u64 计算，避免 32 位目标上长度相加溢出。
                let total = payload_start as u64 + payload_len as u64;
                if total > bytes.len() as u64 {
                    tail_truncated = true;
                    break;
                }
                let total = total as usize;
                let payload = &bytes[payload_start..total];
                if crc32fast::hash(payload) != crc {
                    tail_truncated = true;
                    break;
                }
                // 与恢复一致：先整体校验记录，再应用，避免半帧进入快照。
                for_each_record(payload, count, |_| Ok(()))?;
                apply(payload, count, payload_start as u64, &mut live)?;
                frames += 1;
                records += count as u64;
                offset = total;
            }
        }
    }

    Ok(Snapshot {
        path: path.to_path_buf(),
        file_bytes: bytes.len() as u64,
        valid_bytes: offset as u64,
        frames,
        records,
        tail_truncated,
        entries: live
            .into_iter()
            .map(|(key, entry)| SnapshotEntry {
                key,
                value: entry.value,
                value_offset: entry.value_offset,
                updates: entry.updates,
            })
            .collect(),
    })
}

fn apply(
    payload: &[u8],
    count: u32,
    payload_base: u64,
    live: &mut BTreeMap<String, Live>,
) -> Result<(), KVError> {
    // 记录已由 for_each_record 校验过，这里不会失败。
    for_each_record(payload, count, |record| {
        match record.kind {
            RECORD_SET => {
                let value_offset = payload_base + record.value_offset as u64;
                let entry = live.entry(record.key.to_owned()).or_insert_with(|| Live {
                    value: Vec::new(),
                    value_offset,
                    updates: 0,
                });
                entry.value.clear();
                entry.value.extend_from_slice(record.value);
                entry.value_offset = value_offset;
                entry.updates = entry.updates.saturating_add(1);
            }
            RECORD_REMOVE => {
                live.remove(record.key);
            }
            RECORD_CLEAR => {
                live.clear();
            }
            _ => unreachable!("for_each_record rejects unknown record kinds"),
        }
        Ok(())
    })
}
