//! 类型化门面：在字节 KV 之上提供固定编码的常用类型读写。
//!
//! [`Store`](crate::Store) 与 [`MemoryStore`](crate::MemoryStore) 的核心 API 只认
//! 字节；本模块是一层纯编码约定，不改变落盘格式，也不写类型标记。
//! `set_string(key, value)` 完全等价于 `set(key, value.as_bytes())`。
//!
//! # 编码
//!
//! | 类型 | 编码 |
//! | --- | --- |
//! | `String` | UTF-8 字节 |
//! | `i64` / `u64` | 8 字节大端 |
//! | `f64` | 8 字节大端 IEEE-754 |
//! | `bool` | 单字节 `0x00` / `0x01` |
//! | JSON（`serde` feature） | `serde_json` 输出的 UTF-8 字节 |
//!
//! 固定宽度的数字统一用大端，与平台字节序无关，也便于在十六进制转储里阅读。
//!
//! # 严格读取
//!
//! 键不存在返回 `None`；长度不符、非法 UTF-8、非法布尔字节或坏 JSON 返回
//! [`KVError::Decode`]。读取从不猜测类型，也不把坏数据当成缺失——否则
//! 一次类型写错会表现为“数据悄悄丢了”。

use crate::{MemoryStore, KVError, Store};

/// 类型化读写。
///
/// 由 [`Store`] 和 [`MemoryStore`] 共同实现。扩展方法需要先引入 trait：
///
/// ```ignore
/// use kv_n2x_kv::TypedStore;
///
/// store.set_string("theme", "dark")?;
/// let theme = store.get_string("theme")?;
/// ```
///
/// 每次调用只做一次编码/解码，底层仍是一次哈希查找加一次值拷贝。
pub trait TypedStore {
    /// 读取原始字节；与 [`Store::get`] / [`MemoryStore::get`] 等价。
    ///
    /// 自定义载荷（protobuf、密文、图片）走这里；常用类型走下面的方法。
    fn get_bytes(&self, key: &str) -> Result<Option<Vec<u8>>, KVError>;

    /// 写入原始字节；与 [`Store::set`] / [`MemoryStore::set`] 等价。
    fn set_bytes(&self, key: &str, value: &[u8]) -> Result<(), KVError>;

    /// 读取 UTF-8 字符串。
    fn get_string(&self, key: &str) -> Result<Option<String>, KVError> {
        match self.get_bytes(key)? {
            None => Ok(None),
            Some(bytes) => String::from_utf8(bytes)
                .map(Some)
                .map_err(|error| decode(key, format!("value is not valid UTF-8: {error}"))),
        }
    }

    /// 写入 UTF-8 字符串。空串是合法值，与“键不存在”不同。
    fn set_string(&self, key: &str, value: &str) -> Result<(), KVError> {
        self.set_bytes(key, value.as_bytes())
    }

    /// 读取 8 字节大端 `i64`。
    fn get_i64(&self, key: &str) -> Result<Option<i64>, KVError> {
        match self.get_bytes(key)? {
            None => Ok(None),
            Some(bytes) => Ok(Some(i64::from_be_bytes(fixed(bytes, key, "i64")?))),
        }
    }

    /// 写入 8 字节大端 `i64`。
    fn set_i64(&self, key: &str, value: i64) -> Result<(), KVError> {
        self.set_bytes(key, &value.to_be_bytes())
    }

    /// 读取 8 字节大端 `u64`。
    fn get_u64(&self, key: &str) -> Result<Option<u64>, KVError> {
        match self.get_bytes(key)? {
            None => Ok(None),
            Some(bytes) => Ok(Some(u64::from_be_bytes(fixed(bytes, key, "u64")?))),
        }
    }

    /// 写入 8 字节大端 `u64`。
    fn set_u64(&self, key: &str, value: u64) -> Result<(), KVError> {
        self.set_bytes(key, &value.to_be_bytes())
    }

    /// 读取 8 字节大端 IEEE-754 `f64`。
    fn get_f64(&self, key: &str) -> Result<Option<f64>, KVError> {
        match self.get_bytes(key)? {
            None => Ok(None),
            Some(bytes) => {
                let bits = u64::from_be_bytes(fixed(bytes, key, "f64")?);
                Ok(Some(f64::from_bits(bits)))
            }
        }
    }

    /// 写入 8 字节大端 IEEE-754 `f64`。
    fn set_f64(&self, key: &str, value: f64) -> Result<(), KVError> {
        self.set_bytes(key, &value.to_bits().to_be_bytes())
    }

    /// 读取单字节布尔。
    fn get_bool(&self, key: &str) -> Result<Option<bool>, KVError> {
        match self.get_bytes(key)? {
            None => Ok(None),
            Some(bytes) => match bytes.as_slice() {
                [0] => Ok(Some(false)),
                [1] => Ok(Some(true)),
                [value] => Err(decode(
                    key,
                    format!("expected 0 or 1 for bool, got {value}"),
                )),
                other => Err(decode(
                    key,
                    format!("expected a single byte for bool, got {} bytes", other.len()),
                )),
            },
        }
    }

    /// 写入单字节布尔。
    fn set_bool(&self, key: &str, value: bool) -> Result<(), KVError> {
        self.set_bytes(key, &[u8::from(value)])
    }

    /// 读取 JSON 值。仅在 `serde` feature 开启时可用。
    #[cfg(feature = "serde")]
    fn get_json<T>(&self, key: &str) -> Result<Option<T>, KVError>
    where
        T: serde::de::DeserializeOwned,
    {
        match self.get_bytes(key)? {
            None => Ok(None),
            Some(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|error| decode(key, format!("value is not valid JSON: {error}"))),
        }
    }

    /// 写入 JSON 值。仅在 `serde` feature 开启时可用。
    ///
    /// 宿主侧的 Structured Data 不经过这里：FFI 只传字符串，由宿主的原生
    /// 序列化器处理。
    #[cfg(feature = "serde")]
    fn set_json<T>(&self, key: &str, value: &T) -> Result<(), KVError>
    where
        T: serde::Serialize + ?Sized,
    {
        let bytes = serde_json::to_vec(value)
            .map_err(|error| encode(key, format!("value cannot be serialized as JSON: {error}")))?;
        self.set_bytes(key, &bytes)
    }
}

impl TypedStore for Store {
    fn get_bytes(&self, key: &str) -> Result<Option<Vec<u8>>, KVError> {
        Store::get(self, key)
    }

    fn set_bytes(&self, key: &str, value: &[u8]) -> Result<(), KVError> {
        Store::set(self, key, value)
    }
}

impl TypedStore for MemoryStore {
    fn get_bytes(&self, key: &str) -> Result<Option<Vec<u8>>, KVError> {
        MemoryStore::get(self, key)
    }

    fn set_bytes(&self, key: &str, value: &[u8]) -> Result<(), KVError> {
        MemoryStore::set(self, key, value)
    }
}

fn fixed<const N: usize>(bytes: Vec<u8>, key: &str, ty: &str) -> Result<[u8; N], KVError> {
    let size = bytes.len();
    bytes.try_into().map_err(|_| {
        decode(
            key,
            format!("expected {N} bytes for {ty}, got {size} bytes"),
        )
    })
}

fn decode(key: &str, reason: impl Into<String>) -> KVError {
    KVError::Decode {
        key: key.to_owned(),
        reason: reason.into(),
    }
}

#[cfg(feature = "serde")]
fn encode(key: &str, reason: impl Into<String>) -> KVError {
    KVError::Encode {
        key: key.to_owned(),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryOptions, StoreOptions};

    fn file_store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path().join("typed.kv"), StoreOptions::default())
            .expect("open file store");
        (dir, store)
    }

    fn check_roundtrip(store: &impl TypedStore) {
        assert_eq!(store.get_string("missing").unwrap(), None);
        assert_eq!(store.get_i64("missing").unwrap(), None);
        assert_eq!(store.get_bool("missing").unwrap(), None);

        store.set_string("text", "你好, n2x-kv").expect("set text");
        assert_eq!(
            store.get_string("text").unwrap().as_deref(),
            Some("你好, n2x-kv")
        );

        // 空串是值，不等于缺失。
        store.set_string("empty", "").expect("set empty");
        assert_eq!(store.get_string("empty").unwrap().as_deref(), Some(""));

        for value in [0, -1, 42, i64::MIN, i64::MAX] {
            store.set_i64("i", value).expect("set i64");
            assert_eq!(store.get_i64("i").unwrap(), Some(value));
        }

        for value in [0, 1, u64::MAX] {
            store.set_u64("u", value).expect("set u64");
            assert_eq!(store.get_u64("u").unwrap(), Some(value));
        }

        for value in [0.0, -0.0, 1.5, f64::MIN, f64::MAX, f64::INFINITY] {
            store.set_f64("f", value).expect("set f64");
            let read = store.get_f64("f").unwrap().expect("f64 should exist");
            assert_eq!(read.to_bits(), value.to_bits());
        }

        for value in [false, true] {
            store.set_bool("flag", value).expect("set bool");
            assert_eq!(store.get_bool("flag").unwrap(), Some(value));
        }

        // 编码约定是公开契约，逐字节固定住。
        store.set_i64("wire-i64", 1).expect("set wire i64");
        assert_eq!(
            store.get_bytes("wire-i64").unwrap(),
            Some(vec![0, 0, 0, 0, 0, 0, 0, 1])
        );
        store.set_bool("wire-bool", true).expect("set wire bool");
        assert_eq!(store.get_bytes("wire-bool").unwrap(), Some(vec![1]));
    }

    fn check_rejects_wrong_encoding(store: &impl TypedStore) {
        store.set_bytes("short", b"12").expect("set short");
        assert!(matches!(
            store.get_i64("short"),
            Err(KVError::Decode { .. })
        ));

        store
            .set_bytes("bad-utf8", &[0xff, 0xfe])
            .expect("set bytes");
        assert!(matches!(
            store.get_string("bad-utf8"),
            Err(KVError::Decode { .. })
        ));

        store.set_bytes("two-bytes", &[1, 2]).expect("set bytes");
        assert!(matches!(
            store.get_bool("two-bytes"),
            Err(KVError::Decode { .. })
        ));

        store.set_bytes("not-bool", &[2]).expect("set bytes");
        assert!(matches!(
            store.get_bool("not-bool"),
            Err(KVError::Decode { .. })
        ));
    }

    #[test]
    fn file_store_roundtrips_typed_values() {
        let (_dir, store) = file_store();
        check_roundtrip(&store);
        check_rejects_wrong_encoding(&store);
    }

    #[test]
    fn memory_store_roundtrips_typed_values() {
        let store = MemoryStore::new(MemoryOptions::default());
        check_roundtrip(&store);
        check_rejects_wrong_encoding(&store);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn json_roundtrips_and_rejects_bad_payloads() {
        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        struct Config {
            theme: String,
            size: u8,
        }

        let store = MemoryStore::new(MemoryOptions::default());
        let config = Config {
            theme: "dark".into(),
            size: 14,
        };
        store.set_json("config", &config).expect("set json");
        assert_eq!(store.get_json::<Config>("config").unwrap(), Some(config));
        assert_eq!(store.get_json::<Config>("missing").unwrap(), None);
        assert_eq!(
            store.get_bytes("config").unwrap(),
            Some(br#"{"theme":"dark","size":14}"#.to_vec())
        );

        store.set_bytes("broken", b"{").expect("set bytes");
        assert!(matches!(
            store.get_json::<Config>("broken"),
            Err(KVError::Decode { .. })
        ));
    }
}
