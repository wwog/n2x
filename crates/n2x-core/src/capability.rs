//! 针对具体调用声明的能力；不能用“数据库支持事务”代替操作级恢复契约。

use std::time::Duration;

use crate::{BatchLimits, Durability, ReadConsistency};

/// 相同 OperationId 和请求摘要被重复执行时的语义。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ReplaySafety {
    /// 不允许在结果不明时直接重放。
    Unsafe,
    /// 请求本身幂等，重放不会产生额外业务效果。
    Idempotent,
    /// 业务效果与去重凭证原子提交。
    Deduplicated {
        /// 凭证最短保留期；None 表示不自动过期。
        retention: Option<Duration>,
    },
}

/// 一次副作用的恢复保证，覆盖部分执行和重开进程。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RecoveryCapabilities {
    /// 相同请求的重放语义。
    pub replay: ReplaySafety,
    /// 是否能权威判定 Applied、NotApplied 或仍在执行。
    pub authoritative_lookup: bool,
    /// 返回 Applied 前业务效果达到的持久级别。
    pub durability: Durability,
}

/// 一个具体 StreamSpec 的读取能力。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ReaderCapabilities {
    /// 适配器可提供的一致性方案。
    pub consistency: Vec<ReadConsistency>,
    /// 游标和源视图是否能在进程重开后恢复；过期仍必须报错。
    pub resumable: bool,
}

/// 一个具体 StreamSpec 的写入能力。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct WriterCapabilities {
    /// 操作级恢复保证；不得忽略多条输出部分成功的情况。
    pub recovery: RecoveryCapabilities,
    /// 单个写入单位允许的最大规模。
    pub limits: BatchLimits,
    /// 单位内所有条目是否原子提交；false 时仍须按恢复契约处理部分结果。
    pub atomic_unit: bool,
}

/// 编译计划时为每个步骤固化的已验证能力。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum StepContract {
    /// Task 的副作用保证。
    Task(RecoveryCapabilities),
    /// Transfer 的源和目标保证。
    Transfer {
        /// 源能力。
        reader: ReaderCapabilities,
        /// 目标能力。
        writer: WriterCapabilities,
    },
    /// 无副作用的只读检查。
    Check,
}
