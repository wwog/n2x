//! n2x 的运行时契约：连接 sans-IO Engine 与平台实现。
//!
//! 本 crate 仅定义端口，不提供驱动、注册表、工作区 KV 映射或平台实现。
//! 异步接口不要求 Send，也不绑定 Tokio，允许 Web/WASM 的本地 Future。
//! 原生多线程驱动可以在组合层增加 Send 约束或使用专用工作线程。

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod ports;

pub use ports::*;

/// 可选的本地 KV 后端；启用不代表 Workspace 适配已经实现。
#[cfg(feature = "native-kv")]
pub use n2x_kv as kv;
