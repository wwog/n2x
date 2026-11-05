//! 声明模型：版本边中的顺序管线，不按数据库产品定义 Mode。

use std::collections::BTreeMap;

use crate::{
    ActionId, BatchLimits, Encoded, InvalidDataPolicy, ManifestId, PortId, ReadConsistency,
    Revision, StepId, StepPolicy, StreamId,
};

/// 注册实现的稳定引用；修改行为时必须增加 revision。
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ActionRef {
    /// 注册表中的 ID。
    pub id: ActionId,
    /// 实现修订号，不是用户数据版本。
    pub revision: u32,
}

/// 一个注册操作的调用；只能通过声明的端口访问资源。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ActionCall {
    /// 操作实现。
    pub action: ActionRef,
    /// 声明依赖的逻辑端口，不包含连接凭证。
    pub ports: Vec<PortId>,
    /// 可持久化的调用参数。
    pub config: Option<Encoded>,
}

/// 数据集入口，由对应端口解释 stream 和参数。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StreamSpec {
    /// 逻辑端口。
    pub port: PortId,
    /// 表、集合、查询或条目流。
    pub stream: StreamId,
    /// 查询或写入配置，禁止放连接凭证。
    pub config: Option<Encoded>,
}

/// 纯数据转换的注册实现与固定配置。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TransformSpec {
    /// 转换实现，不可隐式访问外部 IO。
    pub action: ActionRef,
    /// 影响转换结果的配置。
    pub config: Option<Encoded>,
}

/// 标准读取、转换、提交管线。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TransferSpec {
    /// 源数据集。
    pub read: StreamSpec,
    /// None 表示直接传递原 Envelope。
    pub transform: Option<TransformSpec>,
    /// 目标数据集；第一版不隐式提供多目标原子写入。
    pub write: StreamSpec,
    /// 明确要求的源一致性方案。
    pub consistency: ReadConsistency,
    /// 每次读取的上限。
    pub read_limits: BatchLimits,
    /// 每个提交单位的上限；还必须满足 Writer 能力。
    pub write_limits: BatchLimits,
    /// 无效数据策略。
    pub invalid_data: InvalidDataPolicy,
}

/// 三种具有共同生命周期、不同执行契约的步骤。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum StepKind {
    /// 一次可追踪的副作用，例如建表、执行 SQL 或发布文件。
    Task(ActionCall),
    /// 有界批次的数据迁移或原地数据升级。
    Transfer(Box<TransferSpec>),
    /// 只读检查；不满足条件时阻止版本推进。
    Check(ActionCall),
}

/// 一条迁移边内的稳定步骤。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Step {
    /// 在所属 Manifest 内唯一，发布后不能随意复用。
    pub id: StepId,
    /// 步骤类型与参数。
    pub kind: StepKind,
    /// 通用策略覆盖。
    pub policy: StepPolicy,
}

/// 从一个 Revision 到另一个 Revision 的有向边。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Manifest {
    /// 版本图内的唯一 ID。
    pub id: ManifestId,
    /// 必须满足的起始状态。
    pub from: Revision,
    /// 所有步骤与边提交确认后的状态。
    pub to: Revision,
    /// 按顺序执行；只有全部完成后才能推进 Revision。
    pub steps: Vec<Step>,
    /// 描述信息，不得用来隐式控制执行。
    pub metadata: BTreeMap<String, String>,
}
