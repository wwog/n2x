//! n2x 的统一导出层。
//!
//! 当前提供已固定的模型与运行时契约。尚未提供 Runner、适配器或具体调度实现。
//! 默认启用 serde；`native-kv` 可导出本地 KV，Web 使用者无需链接文件存储。
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
