//! 进程内键值存储套件。
//!
//! 提供三种语义：
//!
//! - [`Store`]：文件后端，进程级、可持久化，对应 localn2x-kv 的位置；
//!   默认优先 mmap，映射不可用时自动降级为文件 IO（定位读，无内存副本）。
//! - [`MemoryStore`]：纯内存字节 KV，随所有者（例如一次登录 Session）销毁，
//!   对应 sessionn2x-kv 的位置。字节形态是为了能直接扩展到导出层。
//! - [`AnyStore`]：纯内存动态类型 KV，存 Rust 值而不编码，供内核内部使用。
//!
//!
//! # 类型化读写
//!
//! 核心 API 只认字节；[`TypedStore`] 在两种字节存储上提供 `String` / `i64` /
//! `u64` / `f64` / `bool` 的固定编码读写，编码是纯 API 约定、不改变落盘格式。
//! 开启 `serde` feature 后还有 `get_json` / `set_json`。
//!
//! [`AnyStore`] 是另一条路：结构体原样存入、原样取出，不需要 serde、不需要
//! `Clone`，也没有容量上限。它只在进程内有效，不能落盘、不进 FFI。
//!
//! # 性能模型
//!
//! 点读是一次哈希查找加一次值拷贝；点写是追加一帧日志、更新哈希索引，
//! 不解析 SQL、不遍历 B 树、不为每个键单独提交事务。默认不逐次 fsync，
//! 需要持久化到掉电级别时显式调用 [`Store::flush`]。

mod any;
mod backend;
mod error;
mod format;
mod index;
mod memory;
mod store;
mod typed;

pub use any::AnyStore;
pub use backend::BackendKind;
pub use error::KVError;
pub use memory::{MemoryOptions, MemoryStore};
pub use store::{BackendPreference, Mutation, Stats, Store, StoreOptions};
pub use typed::TypedStore;
