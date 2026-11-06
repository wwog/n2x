//! 观察通知与持久关键事件；业务副作用必须声明为 Step。

use std::time::Duration;

use crate::{
    ManifestId, N2xError, OperationId, Progress, Resolution, Revision, RunId, RunOutcome,
    RunSnapshot, ScopeId, StepAddress, SubscriberId, Timestamp,
};

/// 标准通知事实；ProgressChanged 可以合并，关键事件应持久化后分发。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum NoticeKind {
    /// 首次运行。
    RunStarted,
    /// 开始恢复。
    RunRecovering,
    /// 开始步骤。
    StepStarted(StepAddress),
    /// 步骤已经确认完成。
    StepCompleted(StepAddress),
    /// 管线进度更新，可合并。
    ProgressChanged(Progress),
    /// 已安排安全重试。
    RetryScheduled {
        /// 对应副作用。
        operation: OperationId,
        /// 下一个尝试序号。
        attempt: u32,
        /// 等待时间。
        delay: Duration,
    },
    /// 需要对账。
    ReconciliationRequired(OperationId),
    /// 人工结论已持久保存。
    OperationResolved(Resolution),
    /// 迁移边及权威版本已提交。
    RevisionAdvanced {
        /// 完成的迁移边。
        manifest: ManifestId,
        /// 新版本。
        revision: Revision,
    },
    /// 已安全暂停。
    RunPaused,
    /// 已恢复执行。
    RunResumed,
    /// 已安全取消。
    RunCancelled,
    /// 运行与最终版本推进已确认完成。
    RunCompleted(RunOutcome),
    /// 执行失败。
    RunFailed(N2xError),
    /// 需要显式处理。
    RunNeedsAttention(N2xError),
}

/// 尚未持久化的观察事实；不能被解释为持久提交凭证。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Notice {
    /// 所属运行。
    pub run: RunId,
    /// 资源实例。
    pub scope: ScopeId,
    /// 外部提供的时间。
    pub at: Timestamp,
    /// 事实内容。
    pub kind: NoticeKind,
}

/// 已与 checkpoint 原子保存的关键事件，可按运行和 sequence 补读。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StoredNotice {
    /// 运行内严格递增，从 1 开始；与 run 共同构成去重 ID。
    pub sequence: u64,
    /// 事件内容。
    pub notice: Notice,
}

/// 事件补读的一页结果；历史过期必须显式返回 Gap。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum EventPage {
    /// sequence 大于请求 after 的有界事件页。
    Events {
        /// 按序排列的事件。
        items: Vec<StoredNotice>,
        /// 当前是否还有未读取事件。
        has_more: bool,
    },
    /// 请求位置已经早于保留历史，调用方应重新取得快照。
    Gap {
        /// 最早保留的事件序号；无保留事件时为下一可分配序号。
        oldest_sequence: u64,
        /// 当前权威视图。
        snapshot: Box<RunSnapshot>,
    },
}

/// 可靠订阅的起点或累计确认；表示该序号及此前事件均已处理。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NoticeAck {
    /// 订阅者身份。
    pub subscriber: SubscriberId,
    /// 所属运行。
    pub run: RunId,
    /// 已处理的连续事件前缀。
    pub through_sequence: u64,
}
