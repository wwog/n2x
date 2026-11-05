//! 工作区的语义记录和原子提交模型；不定义物理 KV key 或文件路径。

use crate::{
    ApplyRequest, CommitId, Counters, Cursor, Digest, Durability, Encoded, Intent, ManifestId,
    NoticeAck, PlanSnapshot, PortBinding, Receipt, RejectedRecord, Revision, RunId, RunReport,
    RunSnapshot, ScopeId, SourceState, StepAddress, StoredNotice, Timestamp,
};

/// 一条版本边的提交证明；与权威 Revision 原子保存。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RevisionCommit {
    /// 完成该边的运行。
    pub run: RunId,
    /// 不可变计划。
    pub plan: Digest,
    /// 已完成的迁移边。
    pub manifest: ManifestId,
    /// 原版本。
    pub from: Revision,
    /// 新版本。
    pub to: Revision,
}

/// 默认由 Workspace 保存的权威资源状态。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ScopeState {
    /// 资源实例。
    pub scope: ScopeId,
    /// 已确认版本，不包含尚未提交的目标。
    pub current: Revision,
    /// 尚未安全结束的运行，防止另一个新 Run 忽略此 Scope 的部分效果。
    ///
    /// 与首个 RecoverySnapshot 原子设置。仅在完成或显式核对后安全终止时清除；
    /// 失败、暂停和需要处理时保留，不因进程锁已释放而自动清除。
    pub active_run: Option<RunId>,
    /// 已确认的逻辑端口和资源身份。
    pub bindings: Vec<PortBinding>,
    /// 最近完成的版本边；首次引导时为空。
    pub last_revision_commit: Option<RevisionCommit>,
}

/// 顺序执行位置；索引从 0 开始，必须验证在 PlanSnapshot 范围内。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ExecutionPosition {
    /// 尚未完成的步骤。
    Step {
        /// route 索引。
        edge: u32,
        /// 此边内的步骤索引。
        step: u32,
    },
    /// 此边步骤全部完成，等待原子推进权威版本。
    AdvanceRevision {
        /// route 索引。
        edge: u32,
    },
    /// 路径与最终版本均已提交。
    Finished,
}

/// 一个已准备并冻结的输入批次；重开后不能重新生成不同的输出。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PreparedBatch {
    /// 此批全部确认后才能推进的游标。
    pub next_cursor: Option<Cursor>,
    /// 本批是否到达源终点。
    pub eof: bool,
    /// 本批输出拆分出的有界提交单位，顺序和 Operation ID 已固定。
    pub units: Vec<ApplyRequest>,
    /// 已确认的连续提交单位前缀，顺序与 units 对应。
    pub receipts: Vec<Receipt>,
    /// 允许隔离时必须持久保留的输入及错误。
    pub rejected: Vec<RejectedRecord>,
    /// 整批完成后计入 confirmed 的增量，不能提前累计。
    pub delta: Counters,
    /// 整批确认后采用的转换状态。
    pub next_transform_state: Option<Encoded>,
}

/// 当前 Transfer 的恢复材料。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TransferCheckpoint {
    /// 从 0 开始的输入批次序号，全批确认后递增。
    pub batch: u64,
    /// 已确认前缀之后的读取位置。
    pub cursor: Option<Cursor>,
    /// 首批读取后固定的源视图，即使 cursor 为空也不能丢弃。
    pub source: Option<SourceState>,
    /// 最近确认的转换状态。
    pub transform_state: Option<Encoded>,
    /// 当前冻结输出，尚未开始读取或全部确认后为空。
    pub prepared: Option<PreparedBatch>,
}

/// 当前步骤及确认进度，不要求捕获 Rust 执行栈。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Checkpoint {
    /// 下一项可执行工作。
    pub position: ExecutionPosition,
    /// Task/Check 为空，Transfer 必须与当前位置对应。
    pub transfer: Option<TransferCheckpoint>,
    /// 全运行已持久确认的计数。
    pub confirmed: Counters,
}

/// 以 RunId 存取的完整重开入口；PlanSnapshot 单独保存并按 digest 关联。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RecoverySnapshot {
    /// 恢复模型格式版本。
    pub format_version: u32,
    /// 已持久分配的执行代数；重开先递增并持久化，再分发新命令。
    pub epoch: u64,
    /// 当前可查询状态；持久快照中 observed 应与 confirmed 一致。
    pub run: RunSnapshot,
    /// 执行位置和当前批次材料。
    pub checkpoint: Checkpoint,
    /// 单在途执行模型下，最多一个尚未确认的外部操作。
    pub pending: Option<Intent>,
    /// 首次运行开始时刻。
    pub started_at: Timestamp,
}

/// 工作区一次原子提交中的语义变更。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum WorkspaceMutation {
    /// 只允许首次保存或同内容重放；不得覆盖成不同计划。
    SavePlan {
        /// 所属运行。
        run: RunId,
        /// 完整不可变计划。
        plan: Box<PlanSnapshot>,
    },
    /// 首次引导或推进权威版本。
    SaveScope(ScopeState),
    /// 原子更新进度与待确认操作。
    SaveRecovery(Box<RecoverySnapshot>),
    /// 保存关键事件；重复 run + sequence 必须是相同内容。
    AppendNotice(StoredNotice),
    /// 为一批数据保存永久隔离记录，再允许推进源游标。
    Quarantine {
        /// 所属运行。
        run: RunId,
        /// 所属步骤。
        step: StepAddress,
        /// 所属输入批次。
        batch: u64,
        /// 原记录或持久引用与错误。
        records: Vec<RejectedRecord>,
    },
    /// 保存最终或待处理报告。
    SaveReport(Box<RunReport>),
    /// 更新一个可靠订阅者的连续确认前缀。
    AckNotice(NoticeAck),
}

/// 单写者工作区原子事务；去重凭证与变更必须一同持久化。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct WorkspaceCommit {
    /// 全局唯一事务 ID，相同 ID 必须对应相同变更和请求摘要。
    pub id: CommitId,
    /// 除本字段外全部执行字段的规范摘要，阻止 ID 被复用。
    pub digest: Digest,
    /// 请求的持久级别，能力不足应在执行外部副作用前拒绝。
    pub durability: Durability,
    /// 同一事务的所有变更，禁止拆为多次可见写入。
    pub mutations: Vec<WorkspaceMutation>,
}

/// 工作区事务提交确认；只有级别满足请求才能继续依赖它的副作用。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct WorkspaceReceipt {
    /// 对应事务 ID。
    pub id: CommitId,
    /// 实际提交的内容摘要。
    pub digest: Digest,
    /// 实际达到的持久级别。
    pub durability: Durability,
}
