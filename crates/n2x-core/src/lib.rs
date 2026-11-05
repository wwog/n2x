//! 平台无关的迁移模型与 sans-IO 协议。
//!
//! 本 crate 不执行 IO、不获取系统时间、不选择异步运行时。它固定应用、调度机与
//! 执行端口之间的协议；计划编译、调度实现和持久化编码由上层实现。
//! 默认启用 `serde`，可通过 `default-features = false` 移除序列化依赖。

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod capability;
pub mod cursor;
pub mod data;
pub mod error;
pub mod identity;
pub mod manifest;
pub mod notice;
pub mod operation;
pub mod plan;
pub mod policy;
pub mod protocol;
pub mod run;
pub mod workspace;

pub use capability::*;
pub use cursor::*;
pub use data::*;
pub use error::*;
pub use identity::*;
pub use manifest::*;
pub use notice::*;
pub use operation::*;
pub use plan::*;
pub use policy::*;
pub use protocol::*;
pub use run::*;
pub use workspace::*;

/// 当前 PlanSnapshot 与 RecoverySnapshot 的模型格式版本。
///
/// 与用户数据的 Revision 和 Action 实现修订号相互独立。
pub const MODEL_VERSION: u32 = 1;
