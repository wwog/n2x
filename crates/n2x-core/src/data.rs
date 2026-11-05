//! 带编码标识的数据载体，不强制经过 JSON 或数据库特定类型。

use std::collections::BTreeMap;

use crate::{BlobId, Digest};

/// 可持久化的不透明值；编码器应定义自己的稳定格式。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Encoded {
    /// 编码名称，例如 application/json 或应用自定义名称。
    pub codec: String,
    /// 编码格式修订号。
    pub version: u32,
    /// 编码后的内容。
    pub bytes: Vec<u8>,
}

/// 能够在进程重开后重新访问的内容引用，不是文件句柄或内存地址。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BlobRef {
    /// 暂存服务中的内容 ID。
    pub id: BlobId,
    /// 内容编码名称。
    pub codec: String,
    /// 内容编码修订号。
    pub version: u32,
    /// 内容长度，单位为字节。
    pub bytes: u64,
    /// 用于重新打开时验证内容。
    pub digest: Digest,
}

/// 内联小数据或外置大内容。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Payload {
    /// 内联编码数据。
    Inline(Encoded),
    /// 持久化的内容引用。
    Blob(BlobRef),
}

/// 管线中的一个逻辑条目；不要求它是一行或一个文档。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Envelope {
    /// 可选的稳定业务键；其编码由读取端和转换端约定。
    pub key: Option<Encoded>,
    /// 条目的数据或可流式打开的引用。
    pub payload: Payload,
    /// 可持久化的条目属性。
    pub attributes: BTreeMap<String, String>,
}
