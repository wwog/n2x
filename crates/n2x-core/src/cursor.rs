//! Reader 的不透明位置与源一致性契约。

use crate::{Encoded, Envelope, ResourceId};

/// 由 Reader 解释的恢复游标，不能被调度机当作通用 offset。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Cursor(pub Encoded);

/// 一次扫描的一致性方案；由适配器明确声明并执行。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ReadConsistency {
    /// 源快照；是否能重开由 Reader 能力声明。
    Snapshot,
    /// 调用方或适配器已经冻结源写入。
    Frozen,
    /// 在固定高水位边界内扫描，具体稳定性由适配器定义。
    HighWatermark,
    /// 明确接受可能遗漏或重复的尽力扫描，不可静默选用。
    BestEffort,
}

/// 恢复读取时需要验证的源状态。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SourceState {
    /// 实际读取的资源身份。
    pub resource: ResourceId,
    /// 实际采用的一致性方案，必须满足计划要求。
    pub consistency: ReadConsistency,
    /// 快照、高水位或冻结凭证；具体格式由 Reader 定义。
    pub token: Option<Encoded>,
}

/// Reader 返回的有界批次。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ReadBatch {
    /// 本批数据；空批次不等于扫描结束。
    pub items: Vec<Envelope>,
    /// 全部输出确认后才能采用的下一游标；未结束时必须存在且能够前进。
    pub next: Option<Cursor>,
    /// 是否到达本次扫描的确定终点。
    pub eof: bool,
    /// 本批所属的源视图，后续批次必须与之相容。
    pub source: SourceState,
}
