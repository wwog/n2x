//! 显式执行策略；策略不是适配器能力的替代品。

use std::num::NonZeroU32;
use std::time::Duration;

/// 一次恢复运行允许采用的自动处理方式。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RecoveryPolicy {
    /// 执行前要求每个副作用具备安全重放或权威对账能力。
    #[default]
    RequireAutomatic,
    /// 允许无法自动恢复的操作；结果不明时进入 NeedsAttention。
    AllowManual,
}

/// 工作区提交所请求或实际达到的持久级别。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Durability {
    /// 仅本进程可见；不能承诺重开恢复。
    Memory,
    /// 已达到后端声明的平台持久性屏障。
    #[default]
    Durable,
}

/// 有上限的重试策略；仅在副作用恢复语义允许时使用。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RetryPolicy {
    /// 包括首次执行在内的最大尝试次数。
    pub max_attempts: NonZeroU32,
    /// 第一次重试的基础等待时间。
    pub initial_delay: Duration,
    /// 指数退避的等待上限。
    pub max_delay: Duration,
    /// 从首次尝试开始的总时间预算。
    pub budget: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: NonZeroU32::new(3).expect("three is nonzero"),
            initial_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(10),
            budget: Duration::from_secs(60),
        }
    }
}

/// 数据处理的内存与提交边界；实际编码后的字节限制仍由端口校验。
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BatchLimits {
    /// 每个读取批次或写入单位的条目上限。
    pub max_items: NonZeroU32,
    /// 内联数据的字节上限，不包括外置 Blob 的全部内容。
    pub max_inline_bytes: u64,
}

impl Default for BatchLimits {
    fn default() -> Self {
        Self {
            max_items: NonZeroU32::new(500).expect("five hundred is nonzero"),
            max_inline_bytes: 4 * 1024 * 1024,
        }
    }
}

/// 无效记录的处理方式。
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum InvalidDataPolicy {
    /// 第一次遇到无效数据时停止。
    #[default]
    Stop,
    /// 保存原记录与错误后允许继续，超过运行累计上限时停止。
    Quarantine {
        /// 允许拒绝的最大条目数量，零表示不允许拒绝。
        max_records: u64,
    },
}

/// 整个计划的默认策略；需要参与计划摘要。
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RunPolicy {
    /// 默认重试设置。
    pub retry: RetryPolicy,
    /// 自动恢复要求。
    pub recovery: RecoveryPolicy,
    /// 工作区的持久级别要求。
    pub durability: Durability,
}

/// 单个步骤可覆盖的通用策略。
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StepPolicy {
    /// 未指定时继承 RunPolicy。
    pub retry: Option<RetryPolicy>,
    /// 未指定时继承 RunPolicy。
    pub recovery: Option<RecoveryPolicy>,
}
