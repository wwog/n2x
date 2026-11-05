//! 进程内动态类型存储。
//!
//! [`MemoryStore`](crate::MemoryStore) 存字节，服务导出层（FFI 传 `Vec<u8>` /
//! `String`）；[`AnyStore`] 存 Rust 值，服务内核内部的状态对象与缓存——
//! 结构体不需要 serde、不需要 `Clone`、不需要手写编码。
//!
//! ```ignore
//! use n2x_kv::AnyStore;
//!
//! let state = AnyStore::new();
//! state.set("profile", Profile { id: 7 });
//!
//! let profile = state.get::<Profile>("profile").expect("profile");
//! let name = state.get_str("name").unwrap_or_default();
//! ```
//!
//! # 与 `MemoryStore` 的区别
//!
//! - 两套独立的键空间，互不覆盖、互不影响。
//! - 无容量上限、无键长限制：值可能大部分在堆上，浅层字节数说明不了实际
//!   占用，限额只会给出错误的安全感。需要约束时由调用方自己控制。
//! - 不实现 `Stats` / `flush` / `close`：没有文件、没有后端、没有生命周期
//!   状态，随持有者析构即可。
//! - 不进入 FFI：宿主侧没有 `Any` 的对应物，跨语言数据必须走字节 API 或
//!   JSON 字符串。
//!
//! # 类型约束
//!
//! 值类型必须 `Any + Send + Sync`，即 `'static` 且可跨线程共享。带借用的
//! 类型（`&'a str`、切片引用）不能存入，需改成自持有形态（`String`、
//! `Arc<str>`）。读取返回 `Arc<T>`，与写入共享同一份值，不要求 `T: Clone`。

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

/// 一个值与它的类型名。
///
/// `dyn Any` 不带运行时类型名，额外存一个 `&'static str`，类型不符时才能
/// 说清实际存的是什么。
struct AnyValue {
    value: Arc<dyn Any + Send + Sync>,
    type_name: &'static str,
}

impl AnyValue {
    fn new<T: Any + Send + Sync>(value: T) -> Self {
        Self {
            value: Arc::new(value),
            type_name: std::any::type_name::<T>(),
        }
    }

    fn downcast<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        Arc::clone(&self.value).downcast::<T>().ok()
    }
}

/// 动态类型 KV。
///
/// 所有方法都是同步的，失败只有“键不存在”和“类型不符”两种，因此泛型读取
/// 返回 `Option`。类型不符按未命中处理，用 [`AnyStore::type_of`] 可以区分
/// 这两种情况。
#[derive(Default)]
pub struct AnyStore {
    map: RwLock<HashMap<String, AnyValue>>,
}

impl AnyStore {
    /// 创建一个空存储。
    pub fn new() -> Self {
        Self::default()
    }

    /// 按类型读取一个值；键不存在或类型不符返回 `None`。
    ///
    /// 返回的 `Arc<T>` 与写入的值共享同一份数据，没有拷贝。
    pub fn get<T: Any + Send + Sync>(&self, key: &str) -> Option<Arc<T>> {
        self.map.read().get(key)?.downcast::<T>()
    }

    /// 写入一个值，覆盖同键的旧值（无论旧值是什么类型）。
    pub fn set<T: Any + Send + Sync>(&self, key: &str, value: T) {
        self.map
            .write()
            .insert(key.to_owned(), AnyValue::new(value));
    }

    /// 读取字符串，返回一份拷贝。
    ///
    /// 接受 [`String`]、`&'static str`；其他形态请用 [`AnyStore::get`]。
    pub fn get_str(&self, key: &str) -> Option<String> {
        let guard = self.map.read();
        let value = guard.get(key)?;
        value
            .downcast::<String>()
            .map(|value| value.as_str().to_owned())
            .or_else(|| {
                value
                    .downcast::<&'static str>()
                    .map(|value| (*value).to_owned())
            })
    }

    /// 写入字符串，等价于 `set(key, value.into())`。
    pub fn set_str(&self, key: &str, value: impl Into<String>) {
        self.set(key, value.into());
    }

    /// 读取 `u32`。
    pub fn get_u32(&self, key: &str) -> Option<u32> {
        self.copy::<u32>(key)
    }

    /// 读取 `u64`。
    pub fn get_u64(&self, key: &str) -> Option<u64> {
        self.copy::<u64>(key)
    }

    /// 读取 `i64`。
    pub fn get_i64(&self, key: &str) -> Option<i64> {
        self.copy::<i64>(key)
    }

    /// 读取 `f64`。
    pub fn get_f64(&self, key: &str) -> Option<f64> {
        self.copy::<f64>(key)
    }

    /// 读取 `bool`。
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.copy::<bool>(key)
    }

    /// 键是否存在，不关心值的类型。
    pub fn contains(&self, key: &str) -> bool {
        self.map.read().contains_key(key)
    }

    /// 键存在时返回值的类型名，用于排查类型不符。
    pub fn type_of(&self, key: &str) -> Option<&'static str> {
        self.map.read().get(key).map(|value| value.type_name)
    }

    /// 删除一个键，返回删除前是否存在。
    pub fn remove(&self, key: &str) -> bool {
        self.map.write().remove(key).is_some()
    }

    /// 清空全部键。
    pub fn clear(&self) {
        self.map.write().clear();
    }

    /// 当前键数量。
    pub fn len(&self) -> usize {
        self.map.read().len()
    }

    /// 是否为空。
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 返回按字典序排序的键。
    pub fn keys(&self) -> Vec<String> {
        let guard = self.map.read();
        let mut keys: Vec<String> = guard.keys().cloned().collect();
        keys.sort_unstable();
        keys
    }

    fn copy<T: Any + Send + Sync + Copy>(&self, key: &str) -> Option<T> {
        self.get::<T>(key).map(|value| *value)
    }
}

impl std::fmt::Debug for AnyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let guard = self.map.read();
        f.debug_struct("AnyStore")
            .field("entries", &guard.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    struct Profile {
        id: u64,
        name: String,
    }

    #[test]
    fn structs_roundtrip_without_clone_or_serde() {
        let store = AnyStore::new();
        store.set(
            "profile",
            Profile {
                id: 7,
                name: "ada".into(),
            },
        );

        let profile = store.get::<Profile>("profile").expect("profile");
        assert_eq!(profile.id, 7);
        assert_eq!(profile.name, "ada");
        assert!(store.get::<Profile>("missing").is_none());

        // 共享而不是拷贝：同一份值的两个句柄。
        let second = store.get::<Profile>("profile").expect("profile");
        assert_eq!(Arc::as_ptr(&profile), Arc::as_ptr(&second));
    }

    #[test]
    fn primitives_have_shortcuts() {
        let store = AnyStore::new();
        store.set_str("name", "ada");
        store.set("count", 3u32);
        store.set("total", u64::MAX);
        store.set("delta", -1i64);
        store.set("ratio", 0.5f64);
        store.set("flag", true);

        assert_eq!(store.get_str("name").as_deref(), Some("ada"));
        assert_eq!(store.get_u32("count"), Some(3));
        assert_eq!(store.get_u64("total"), Some(u64::MAX));
        assert_eq!(store.get_i64("delta"), Some(-1));
        assert_eq!(store.get_f64("ratio"), Some(0.5));
        assert_eq!(store.get_bool("flag"), Some(true));
        assert_eq!(store.get_u32("missing"), None);
    }

    #[test]
    fn get_str_accepts_static_str_values() {
        let store = AnyStore::new();
        store.set("static", "borrowed");
        assert_eq!(store.get_str("static").as_deref(), Some("borrowed"));
    }

    #[test]
    fn type_mismatch_reads_as_missing_and_is_visible_through_type_of() {
        let store = AnyStore::new();
        store.set("value", 42u32);

        assert!(store.get::<String>("value").is_none());
        assert_eq!(store.get_str("value"), None);
        assert!(store.get_u32("value").is_some());
        assert_eq!(store.type_of("value"), Some("u32"));
        assert_eq!(store.type_of("missing"), None);
    }

    #[test]
    fn overwrite_accepts_a_different_type() {
        let store = AnyStore::new();
        store.set("value", 1u32);
        store.set("value", "text");
        assert_eq!(store.type_of("value"), Some("&str"));
        assert_eq!(store.get_str("value").as_deref(), Some("text"));
        assert_eq!(store.get_u32("value"), None);
    }

    #[test]
    fn keys_remove_and_clear() {
        let store = AnyStore::new();
        store.set("b", 1u8);
        store.set("a", 2u8);
        assert_eq!(store.keys(), vec!["a", "b"]);
        assert_eq!(store.len(), 2);
        assert!(!store.is_empty());

        assert!(store.remove("a"));
        assert!(!store.remove("a"));
        assert!(!store.contains("a"));
        assert_eq!(store.len(), 1);

        store.clear();
        assert!(store.is_empty());
        assert!(store.keys().is_empty());
    }

    #[test]
    fn byte_store_keys_are_independent() {
        let bytes = crate::MemoryStore::new(crate::MemoryOptions::default());
        let any = AnyStore::new();

        bytes.set("k", b"raw").expect("set bytes");
        assert!(!any.contains("k"));

        any.set("k", 1u32);
        assert_eq!(bytes.get("k").unwrap(), Some(b"raw".to_vec()));
    }

    #[test]
    fn store_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AnyStore>();
    }
}
