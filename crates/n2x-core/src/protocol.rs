//! Engine 与 Runtime 的 sans-IO 协议；同一命令必须匹配同类型回复。

use crate::{
    ActionCall, ApplyRequest, CheckResult, Control, N2xError, Notice, OperationFailure, ReadBatch,
    ReadRequest, Receipt, ReconcileRequest, ReconcileResult, RecoverySnapshot, StartInput,
    StoredNotice, Timestamp, TransformRequest, TransformResult, WorkspaceCommit, WorkspaceReceipt,
};

/// 一次分发的身份；重试更换 sequence/attempt，但保持 OperationId。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CommandId {
    /// 每次启动或恢复递增的执行代数，用来拒绝旧进程的迟到 Reply。
    ///
    /// 它只负责回复关联，不能代替外部资源的运行锁或 fencing。
    pub epoch: u64,
    /// 当前 epoch 内严格递增的命令序号。
    pub sequence: u64,
    /// 此次执行尝试，从 1 开始。
    pub attempt: u32,
}

/// 需要 Runtime 实际执行的工作。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum CommandKind {
    /// 读取一个有界批次。
    Read(ReadRequest),
    /// 执行无外部副作用的转换。
    Transform(TransformRequest),
    /// 执行已完成 Intent 持久化的副作用。
    Apply(ApplyRequest),
    /// 权威查询一个待确认操作。
    Reconcile(ReconcileRequest),
    /// 执行只读检查。
    Check(ActionCall),
    /// 原子保存恢复材料、确认状态、版本或通知。
    Commit(WorkspaceCommit),
    /// 请求 Runtime 重新提供当前时间。
    Clock,
    /// 由 Runtime 安排唤醒，核心不自行睡眠。
    WakeAt(Timestamp),
}

/// 带有回复关联 ID 的执行命令。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Command {
    /// 当前分发身份。
    pub id: CommandId,
    /// 命令内容。
    pub kind: CommandKind,
}

/// 与具体 CommandKind 一一对应的执行结果。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ReplyKind {
    /// Read 的结果。
    Read(Result<ReadBatch, N2xError>),
    /// Transform 的结果。
    Transformed(Result<TransformResult, N2xError>),
    /// Apply 的结果，失败必须明确提交确定性。
    Applied(Result<Receipt, OperationFailure>),
    /// Reconcile 的权威结果或查询错误。
    Reconciled(Result<ReconcileResult, N2xError>),
    /// Check 的结果，不满足条件使用标准错误。
    Checked(Result<CheckResult, N2xError>),
    /// 工作区持久化确认；失败不允许继续新的副作用。
    Committed(Result<WorkspaceReceipt, N2xError>),
    /// Clock 的结果。
    Time(Timestamp),
    /// WakeAt 的实际唤醒时间或定时器错误。
    Woken(Result<Timestamp, N2xError>),
}

/// 执行结果；必须关联原 Command，而非只使用当前 Step 推测归属。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Reply {
    /// 原命令 ID。
    pub command: CommandId,
    /// 对应结果。
    pub kind: ReplyKind,
}

/// Engine 的初次启动或重开入口。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum EngineStart {
    /// 创建新运行。
    New(Box<StartInput>),
    /// 恢复旧运行；Plan、资源绑定与模型版本必须先通过验证。
    Recover {
        /// 已保存的执行状态。
        snapshot: Box<RecoverySnapshot>,
        /// 已验证的不可变计划。
        plan: Box<crate::PlanSnapshot>,
        /// Runtime 提供的当前时间。
        now: Timestamp,
    },
}

/// 一次状态转换的输出；顺序模型最多有一条待回复命令。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Transition {
    /// 下一条命令；等待它的 Reply 前不得依赖其执行结果。
    pub command: Option<Command>,
    /// 可立即观察的临时通知，不具有持久事件语义。
    pub notices: Vec<Notice>,
    /// 已经收到提交确认的关键事件，Runtime 此时才允许分发。
    pub committed_notices: Vec<StoredNotice>,
}

/// 管线调度机契约；实现者在本协议之上构造纯状态机。
///
/// 方法返回的 Err 表示输入或协议无法接受；正常业务失败应推进运行状态、生成
/// 对应持久记录并通过 Transition 表达，而不是丢失恢复现场。
pub trait PipelineEngine {
    /// 初始化一次执行代；epoch 必须由 Runtime 在重开时更新。
    fn start(&mut self, start: EngineStart, epoch: u64) -> Result<Transition, N2xError>;

    /// 接受对应回复；拒绝未预期、类型错误或旧 epoch 的回复。
    fn accept(&mut self, reply: Reply) -> Result<Transition, N2xError>;

    /// 请求安全暂停、继续、取消或显式处理未知结果。
    fn control(&mut self, control: Control) -> Result<Transition, N2xError>;
}
