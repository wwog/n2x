//! 标准错误分类与诊断。错误原因和副作用是否提交是两条独立信息。

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use crate::{Encoded, OperationId, RunId, ScopeId, StepAddress};

/// 稳定机器错误码；适配器可使用自己的命名空间。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ErrorCode(pub String);

impl ErrorCode {
    /// 请求旧版本，但没有声明反向迁移路径。
    pub const ROLLBACK_UNSUPPORTED: &str = "n2x.version.rollback_unsupported";
    /// 恢复时发现计划或 Action 修订号发生变化。
    pub const PLAN_CHANGED: &str = "n2x.plan.changed";
    /// 多条路径之间无法唯一选择。
    pub const ROUTE_AMBIGUOUS: &str = "n2x.plan.route_ambiguous";
    /// 资源身份、快照或游标失效。
    pub const SOURCE_CHANGED: &str = "n2x.source.changed";
    /// 副作用结果无法确认。
    pub const OUTCOME_UNKNOWN: &str = "n2x.operation.outcome_unknown";
    /// 重试次数或总时间预算耗尽。
    pub const RETRY_EXHAUSTED: &str = "n2x.retry.exhausted";
    /// 工作区未达到所请求的持久级别。
    pub const PERSISTENCE_UNCONFIRMED: &str = "n2x.workspace.persistence_unconfirmed";
}

/// 标准分类；程序按 code/kind 分支，不解析 message。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ErrorKind {
    /// 声明、参数、路径或协议无效。
    InvalidPlan,
    /// 缺少所需功能或能力。
    Unsupported,
    /// 模型、计划或实现版本不相容。
    Incompatible,
    /// 所有权、版本或请求内容冲突。
    Conflict,
    /// 资源身份或源视图失效。
    SourceChanged,
    /// 数据无法转换或校验不通过。
    InvalidData,
    /// 暂时不可访问。
    Unavailable,
    /// 执行超时，不代表操作已撤销。
    Timeout,
    /// 容量或配额不足。
    Quota,
    /// 权限不足。
    Permission,
    /// 内容摘要或数据完整性不满足要求。
    Integrity,
    /// 持久化无法确认。
    Persistence,
    /// 实现内部错误。
    Internal,
}

/// 错误来源，用于定位和分类处理。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ErrorOrigin {
    /// 计划编译。
    Plan,
    /// 状态机。
    Engine,
    /// Runtime 驱动。
    Runtime,
    /// 读取端。
    Reader,
    /// 写入端。
    Writer,
    /// Task、Check 或 Transform 实现。
    Action,
    /// 持久状态或暂存服务。
    Workspace,
    /// 通知分发。
    Notification,
}

/// 错误位置；尚未进入某一级时对应字段为空。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ErrorLocation {
    /// 资源实例。
    pub scope: Option<ScopeId>,
    /// 运行。
    pub run: Option<RunId>,
    /// 步骤。
    pub step: Option<StepAddress>,
    /// 副作用 ID。
    pub operation: Option<OperationId>,
    /// 本次尝试序号，从 1 开始。
    pub attempt: Option<u32>,
    /// 可脱敏的记录键，不应直接暴露原始业务内容。
    pub record_key: Option<String>,
}

/// 适配器建议；不能覆盖安全重放限制或运行重试上限。
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RetryHint {
    /// 不建议自动重试。
    #[default]
    Never,
    /// 可根据默认退避策略尝试恢复。
    Transient,
    /// 建议至少等待指定时间。
    After(Duration),
}

/// 可持久化、可跨平台传递的错误。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct N2xError {
    /// 稳定机器码。
    pub code: ErrorCode,
    /// 标准分类。
    pub kind: ErrorKind,
    /// 来源层。
    pub origin: ErrorOrigin,
    /// 人类可读描述。
    pub message: String,
    /// 重试建议。
    pub retry_hint: RetryHint,
    /// 位置与可选诊断，装箱以保持常用 Result 的大小稳定。
    pub context: Box<ErrorContext>,
}

/// 可序列化的错误上下文，不存原生错误对象或连接句柄。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ErrorContext {
    /// 执行位置。
    pub location: ErrorLocation,
    /// 脱敏诊断和提供者原始错误码。
    pub details: BTreeMap<String, String>,
    /// 可选的结构化诊断。
    pub diagnostic: Option<Encoded>,
}

impl N2xError {
    /// 构造默认不重试、无额外上下文的标准错误。
    pub fn new(
        code: impl Into<String>,
        kind: ErrorKind,
        origin: ErrorOrigin,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code: ErrorCode(code.into()),
            kind,
            origin,
            message: message.into(),
            retry_hint: RetryHint::Never,
            context: Box::default(),
        }
    }
}

impl fmt::Display for N2xError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code.0, self.message)
    }
}

impl std::error::Error for N2xError {}
