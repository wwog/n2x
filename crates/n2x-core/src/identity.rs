//! 稳定标识；不根据版本名称推断大小关系或迁移方向。

use std::fmt;

macro_rules! id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        ///
        /// 计划校验阶段必须拒绝空标识及作用域内重复标识。
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
        #[cfg_attr(feature = "serde", serde(transparent))]
        pub struct $name(pub String);

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

id!(ScopeId, "受管理资源实例，例如 account/42。");
id!(Revision, "版本图中的状态节点，例如 wcdb/3 或 sqlite/1。");
id!(ManifestId, "版本图内唯一的迁移边标识。");
id!(StepId, "一条 Manifest 内唯一的步骤标识。");
id!(ActionId, "注册表内的实现标识，不包含闭包或运行时句柄。");
id!(PortId, "计划内资源入口的逻辑名称，例如 source 或 target。");
id!(ResourceId, "资源的稳定身份；不得包含密码等连接凭证。");
id!(StreamId, "由适配器解释的数据集、查询或文件条目流标识。");
id!(RunId, "一次运行的全局唯一标识；恢复时保持不变。");
id!(
    OperationId,
    "副作用的全局唯一标识；重试保持不变，不能用于不同请求。"
);
id!(CommitId, "工作区事务的全局唯一标识；重复提交必须去重。");
id!(BlobId, "暂存服务中可重新打开的内容标识。");
id!(SubscriberId, "可靠通知订阅者的稳定身份。");

/// SHA-256 摘要；规范编码与计算由实现层负责。
///
/// 不可用默认 Rust Hash 或任意 JSON 字节顺序替代规范摘要。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Digest(pub [u8; 32]);

/// 运行内的步骤地址，避免不同 Manifest 的同名 Step 冲突。
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StepAddress {
    /// 所属迁移边。
    pub manifest: ManifestId,
    /// 迁移边内的步骤。
    pub step: StepId,
}

/// 外部提供的 Unix 毫秒时间；核心不会自行读取时钟。
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Timestamp(pub i64);
