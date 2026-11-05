//! 读取、转换和副作用请求，以及可恢复的执行结果。

use crate::{
    ActionCall, BatchLimits, Cursor, Digest, Encoded, Envelope, N2xError, OperationId,
    ReadConsistency, RunId, SourceState, StepAddress, StreamSpec, Timestamp, TransformSpec,
};

/// 一次读取；恢复时 source 和 cursor 必须作为一对验证。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ReadRequest {
    /// 当前步骤。
    pub step: StepAddress,
    /// 数据集描述。
    pub stream: StreamSpec,
    /// None 表示从头开始。
    pub cursor: Option<Cursor>,
    /// 首次读取后固定的源视图；None 表示需要建立视图。
    pub source: Option<SourceState>,
    /// 请求的一致性方案。
    pub consistency: ReadConsistency,
    /// 有界读取限制。
    pub limits: BatchLimits,
}

/// 一次纯转换；转换实现只能读取显式输入及其 Blob，不可隐藏外部副作用。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TransformRequest {
    /// 当前步骤。
    pub step: StepAddress,
    /// 实现与参数。
    pub transform: TransformSpec,
    /// 待转换输入。
    pub items: Vec<Envelope>,
    /// 上一个已确认批次的转换状态。
    pub state: Option<Encoded>,
    /// 本次输出内联上限；超出时应暂存 Blob 或返回明确错误。
    pub limits: BatchLimits,
}

/// 必须持久化原始输入或持久引用后才能跳过的无效记录。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RejectedRecord {
    /// 原记录；不得引用即将失效的进程内数据。
    pub input: Envelope,
    /// 标准错误及来源。
    pub error: N2xError,
}

/// 一个输入批次的转换结果，支持 1:0、1:1、1:N。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TransformResult {
    /// 待写入输出，随后按 Writer 限制拆为提交单位。
    pub items: Vec<Envelope>,
    /// 有意过滤掉的输入条目数，不是输出条目数。
    pub filtered: u64,
    /// 无效输入与错误；是否允许隔离由管线策略决定。
    pub rejected: Vec<RejectedRecord>,
    /// 仅在整批确认后才能采用的转换状态。
    pub next_state: Option<Encoded>,
}

/// 一次副作用的身份，重试时所有字段保持不变。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct OperationContext {
    /// 全局唯一 Operation ID。
    pub id: OperationId,
    /// 所属运行。
    pub run: RunId,
    /// 所属步骤。
    pub step: StepAddress,
    /// 固定的计划摘要。
    pub plan: Digest,
    /// EffectRequest 的规范编码摘要。
    pub request_digest: Digest,
}

/// 一次可持久化并重放的副作用请求。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum EffectRequest {
    /// Task 操作；执行器必须遵守其声明的恢复契约。
    Task(ActionCall),
    /// 一个有界写入单位，不等于整个读取批次。
    Write {
        /// 输出数据集。
        target: StreamSpec,
        /// 完整请求或可持久重开的内容引用。
        items: Vec<Envelope>,
    },
}

/// 外部副作用的完整请求，必须先以 Intent 持久化。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ApplyRequest {
    /// 稳定身份与请求摘要。
    pub context: OperationContext,
    /// 操作内容，禁止在重试时替换。
    pub effect: EffectRequest,
}

/// 输出端确认某请求已应用的凭证。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Receipt {
    /// 已应用的 Operation ID。
    pub operation: OperationId,
    /// 必须匹配原请求，不能只比较 Operation ID。
    pub request_digest: Digest,
    /// 输出端可提供的持久提交信息或结果。
    pub proof: Option<Encoded>,
}

/// 错误发生时对业务效果的确定性判断。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ApplyState {
    /// 权威确认没有应用，也不会有迟到提交。
    NotApplied,
    /// 已应用；清理连接等后续错误不允许导致重复业务效果。
    Applied(Receipt),
    /// 可能全部或部分应用，必须根据恢复契约处理。
    Unknown,
}

/// 副作用失败，原因与提交确定性分开保存。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct OperationFailure {
    /// 标准错误。
    pub error: N2xError,
    /// 业务效果状态；超时通常无法直接判定为 NotApplied。
    pub apply_state: ApplyState,
}

/// 已持久化但尚未由工作区确认完成的外部操作。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Intent {
    /// 足以重新执行或权威对账的完整请求。
    pub request: ApplyRequest,
    /// Intent 第一次持久化的外部时间。
    pub created_at: Timestamp,
    /// 已开始的尝试次数；增加 attempt 不改变 Operation ID。
    pub attempts: u32,
    /// 最近一次失败，便于恢复决策和诊断。
    pub last_failure: Option<OperationFailure>,
}

/// 对原请求进行权威结果查询。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ReconcileRequest {
    /// 请求与资源绑定通过原计划验证，不能换目标端查询。
    pub request: ApplyRequest,
}

/// 对账结果；暂时查不到记录不能当成 NotApplied。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ReconcileResult {
    /// 已应用。
    Applied(Receipt),
    /// 权威确认没有应用且不会迟到提交。
    NotApplied,
    /// 原操作还在进行，禁止开始不安全的替代请求。
    InFlight,
    /// 无法得到权威结论。
    Unknown,
}

/// 只读校验的成功结果；校验失败使用 N2xError。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CheckResult {
    /// 可选的检查依据或汇总。
    pub evidence: Option<Encoded>,
}
