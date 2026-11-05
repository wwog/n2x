//! 运行状态、进度和外部控制。

use std::time::Duration;

use crate::{
    Digest, Encoded, N2xError, OperationId, PlanSnapshot, Receipt, Revision, RunId, ScopeId,
    StepAddress, Timestamp,
};

/// 可加总的条目计数；1:N 转换时 written 可能大于 read。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Counters {
    /// 输入条目数。
    pub read: u64,
    /// 目标确认的输出条目数。
    pub written: u64,
    /// 有意过滤的输入条目数。
    pub filtered: u64,
    /// 持久隔离的输入条目数。
    pub rejected: u64,
    /// 已读取的数据字节数；由端口定义包含哪些内容。
    pub bytes_read: u64,
    /// 已写入的数据字节数。
    pub bytes_written: u64,
}

/// 等待原因，不使用固定睡眠阻塞调度机。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum WaitReason {
    /// 等待重试；运行时安排唤醒，重开后重新计算剩余等待。
    Retry {
        /// 要重试的操作。
        operation: OperationId,
        /// 本次至少等待的时长。
        delay: Duration,
    },
    /// 等待可靠通知队列容量。
    NotificationCapacity,
    /// 等待特定外部资源。
    Resource(String),
}

/// 标准运行状态；停止状态不表示已回滚外部数据。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RunStatus {
    /// 已准备计划。
    Ready,
    /// 正在执行。
    Running,
    /// 重开后正在验证现场与对账。
    Recovering,
    /// 等待明确条件。
    Waiting(WaitReason),
    /// 已在安全边界暂停。
    Paused,
    /// 无法自动安全继续，保留恢复现场。
    NeedsAttention,
    /// 所有步骤与最终版本推进都已持久确认。
    Completed,
    /// 执行停止；不隐含回滚，也不隐含没有部分效果。
    Failed,
    /// 已取消；若有未知副作用应先进入 NeedsAttention。
    Cancelled,
}

/// UI 使用的进度；observed 可以在恢复后回退，confirmed 来自持久检查点。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Progress {
    /// 当前进程观察到的累计工作量。
    pub observed: Counters,
    /// 已被工作区持久确认的累计工作量。
    pub confirmed: Counters,
    /// 当前步骤预计输入条目总量，未知时为空，不伪造百分比。
    pub estimated_input: Option<u64>,
}

/// 可查询或通过 watch 合并推送的运行快照。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RunSnapshot {
    /// 运行标识。
    pub run: RunId,
    /// 资源实例。
    pub scope: ScopeId,
    /// 不可变计划摘要。
    pub plan: Digest,
    /// 当前运行状态。
    pub status: RunStatus,
    /// 已确认版本，不冒充正在迁移的目标版本。
    pub current: Revision,
    /// 最终目标。
    pub target: Revision,
    /// 当前步骤，版本边提交期间可以为空。
    pub step: Option<StepAddress>,
    /// 进度。
    pub progress: Progress,
    /// 最近已持久化的关键事件序号；尚无事件时为 0。
    pub last_sequence: u64,
    /// 最近一次错误。
    pub last_error: Option<N2xError>,
}

/// 手工处理结果不明的操作；必须记录操作者提供的说明。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Resolution {
    /// 要处理的原操作。
    pub operation: OperationId,
    /// 显式判定，不自动根据错误类型猜测。
    pub decision: ResolutionDecision,
    /// 可审计说明。
    pub reason: String,
}

/// 人工对账结论；只有经过应用授权才可送入调度机。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ResolutionDecision {
    /// 已外部确认应用。
    Applied(Receipt),
    /// 已外部确认未应用且不会迟到提交。
    NotApplied,
}

/// 调用方控制；Observer 的回调返回值不能替代此通道。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Control {
    /// 在下一个安全提交边界暂停。
    Pause,
    /// 继续已暂停或满足条件的运行。
    Resume,
    /// 在安全边界取消，不自动删除源或目标。
    Cancel,
    /// 显式处理某个未知结果。
    Resolve(Resolution),
}

/// 运行结束后的汇总，允许拒绝数据的完成有独立结果。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RunOutcome {
    /// 全量完成。
    Completed,
    /// 已完成，但存在被策略允许的隔离记录。
    CompletedWithRejections,
    /// 停止并需要处理。
    NeedsAttention,
    /// 执行失败。
    Failed,
    /// 已取消。
    Cancelled,
}

/// 标准运行报告，不包含原生连接或任意业务对象。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RunReport {
    /// 最后运行快照。
    pub snapshot: RunSnapshot,
    /// 标准结果。
    pub outcome: RunOutcome,
    /// 首次开始时间。
    pub started_at: Timestamp,
    /// 此报告确认时刻。
    pub finished_at: Timestamp,
    /// 可选校验汇总。
    pub evidence: Option<Encoded>,
}

/// 初次启动引擎的完整输入；Scope 引导与锁由 Runtime 在此之前完成。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StartInput {
    /// 运行标识。
    pub run: RunId,
    /// 已校验的不可变计划。
    pub plan: PlanSnapshot,
    /// 外部时钟时间。
    pub now: Timestamp,
}
