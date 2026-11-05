//! 存储层错误。
//!
//! 本 crate 不依赖 `engine`，因此错误先在这里收敛；`engine` 再把宿主能处理
//! 的类别（配额、非法键值）映射成自己的 FFI 错误。

use std::fmt;

/// 存储操作失败。
#[derive(Debug, thiserror::Error)]
pub enum KVError {
    /// 存储已经关闭，不再接受读写。
    #[error("n2x-kv is closed")]
    Closed,

    /// 写入会超过配置的容量上限。
    #[error("n2x-kv quota exceeded: max {max_bytes} bytes")]
    Full { max_bytes: u64 },

    /// 单个键超过长度上限。
    #[error("invalid n2x-kv key: {reason}")]
    InvalidKey { reason: String },

    /// 单个值超过长度上限。
    #[error("n2x-kv value is {size} bytes, max {max_bytes}")]
    ValueTooLarge { size: usize, max_bytes: usize },

    /// 一次批量写入的帧超过上限。
    #[error("n2x-kv batch is {size} bytes, max {max_bytes}")]
    BatchTooLarge { size: usize, max_bytes: usize },

    /// 存储中的字节无法按请求的类型解码。
    ///
    /// 长度不符、非法 UTF-8、非法布尔字节或坏 JSON 都归到这里；它与
    /// “键不存在”区分开，避免类型写错被当成空值。
    #[error("cannot decode n2x-kv value for key {key:?}: {reason}")]
    Decode { key: String, reason: String },

    /// 值无法按请求的类型编码。
    #[error("cannot encode n2x-kv value for key {key:?}: {reason}")]
    Encode { key: String, reason: String },

    /// 同一路径在本进程内已被另一个 `Store` 打开。
    ///
    /// 追加日志的 `valid_end` 由每个实例独立维护，两个写入者会互相覆盖，
    /// 因此进程内只允许一个实例。跨进程仍没有文件锁。
    #[error("n2x-kv file is already open in this process: {path}")]
    AlreadyOpen { path: String },

    /// 文件内容无法按当前格式解析。
    #[error("n2x-kv data is corrupt: {reason}")]
    Corrupt { reason: String },

    /// 存储配置组合不合法。
    #[error("invalid n2x-kv options: {reason}")]
    InvalidOptions { reason: String },

    /// 底层后端无法继续工作。
    #[error("n2x-kv backend failure: {reason}")]
    Backend { reason: String },

    /// 文件或目录 IO 失败。
    #[error("n2x-kv io failure: {0}")]
    Io(#[from] std::io::Error),
}

impl KVError {
    /// 是否属于调用方可修正的输入错误。
    pub fn is_invalid_input(&self) -> bool {
        matches!(
            self,
            Self::InvalidKey { .. }
                | Self::ValueTooLarge { .. }
                | Self::BatchTooLarge { .. }
                | Self::Encode { .. }
        )
    }

    /// 是否属于容量不足。
    pub fn is_full(&self) -> bool {
        matches!(self, Self::Full { .. })
    }
}

impl fmt::Display for crate::BackendKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Mmap => "mmap",
            Self::Buffered => "buffered",
            Self::Memory => "memory",
        })
    }
}
