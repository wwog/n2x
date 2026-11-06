//! n2x 的统一导出层。
//!
//! 提供模型、sans-IO 调度与 Migrator 组合入口，不附带具体业务适配器。
//! 默认启用 engine 和 native-kv；Web 可关闭默认功能并单独启用 engine。
//!
//! ```
//! use n2x::{Manifest, Revision};
//!
//! let migration = Manifest {
//!     id: "initialize".into(),
//!     from: Revision::from("empty"),
//!     to: Revision::from("v1"),
//!     steps: vec![],
//!     metadata: Default::default(),
//! };
//! assert_eq!(migration.to.as_ref(), "v1");
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub use n2x_core::*;

/// 执行端口、异步调用与工作区接口。
pub use n2x_runtime as runtime;
#[cfg(feature = "engine")]
pub use n2x_runtime::{Migrator, Registry, StateWorkspace};
