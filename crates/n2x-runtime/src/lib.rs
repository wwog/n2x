//! n2x 的运行时契约：连接 sans-IO Engine 与平台实现。
//!
//! 提供注册表、Migrator、状态工作区；native-kv 提供 KV、锁、时钟和 Blob 组合。
//! 异步接口不要求 Send，也不绑定 Tokio，允许 Web/WASM 的本地 Future。
//! 原生多线程驱动可以在组合层增加 Send 约束或使用专用工作线程。

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod ports;

pub use ports::*;
#[cfg(feature = "engine")]
mod registry;
#[cfg(feature = "engine")]
mod runner;
#[cfg(feature = "engine")]
mod workspace;
#[cfg(feature = "engine")]
pub use registry::*;
#[cfg(feature = "engine")]
pub use runner::*;
#[cfg(feature = "engine")]
pub use workspace::*;
#[cfg(feature = "native-kv")]
mod native;
#[cfg(feature = "native-kv")]
pub use native::*;

/// 可独立使用的本地 KV 后端，标准工作区通过 KvBackend 适配。
#[cfg(feature = "native-kv")]
pub use n2x_kv as kv;
