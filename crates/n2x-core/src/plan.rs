//! 路径选择与不可变计划模型；编译与摘要见 compiler 和 codec 模块。

use crate::{
    ActionRef, Digest, ManifestId, PortId, ResourceId, Revision, RunId, RunPolicy, ScopeId, Step,
    StepContract,
};

/// 路径选择方式；不默认使用边数最少的路径。
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Route {
    /// 必须存在唯一合法路径，否则报告歧义或无路径。
    #[default]
    Unique,
    /// 明确指定按顺序经过的迁移边。
    Explicit(Vec<ManifestId>),
}

/// 启动请求；initial 只用于尚未初始化的 Scope。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RunRequest {
    /// 本次运行标识，恢复时必须沿用原 ID。
    pub run: RunId,
    /// 受管理资源实例。
    pub scope: ScopeId,
    /// 首次引导时经过验证的版本；已有 Scope 时以持久权威状态为准。
    pub initial: Option<Revision>,
    /// 目标状态。
    pub target: Revision,
    /// 版本路径选择。
    pub route: Route,
    /// 运行默认策略。
    pub policy: RunPolicy,
}

/// 逻辑端口到真实资源的稳定绑定，不保存打开的连接或凭证。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PortBinding {
    /// Manifest 使用的逻辑名称。
    pub port: PortId,
    /// 实际资源的稳定身份。
    pub resource: ResourceId,
    /// 端口实现与修订号；恢复时验证与快照相容。
    pub adapter: ActionRef,
}

/// 经过注册表解析和能力校验的步骤。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PlannedStep {
    /// 原始步骤声明。
    pub definition: Step,
    /// 与 kind 对应的能力；编译器必须拒绝不匹配的契约。
    pub contract: StepContract,
}

/// 选定路径中的一条边，不包含展示用 metadata。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PlannedManifest {
    /// 迁移边 ID。
    pub id: ManifestId,
    /// 起始版本。
    pub from: Revision,
    /// 目标版本。
    pub to: Revision,
    /// 已验证的顺序步骤。
    pub steps: Vec<PlannedStep>,
}

/// 开始任何外部副作用前必须持久化的不可变计划。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PlanSnapshot {
    /// 模型格式版本，初始为 MODEL_VERSION。
    pub format_version: u32,
    /// 资源实例。
    pub scope: ScopeId,
    /// 权威的初始版本。
    pub from: Revision,
    /// 最终目标版本。
    pub to: Revision,
    /// 连续、顺序的路径；from == to 时允许为空。
    pub route: Vec<PlannedManifest>,
    /// 按 port 排序，资源身份不可在恢复时静默改变。
    pub bindings: Vec<PortBinding>,
    /// 实际采用的默认策略。
    pub policy: RunPolicy,
    /// 除本字段以外所有执行字段的规范编码摘要。
    pub digest: Digest,
}
