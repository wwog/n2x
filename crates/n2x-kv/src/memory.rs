//! 纯内存键值存储。
//!
//! 生命周期由持有者决定：Engine 把它挂到一个 Session 上，Session 回收时
//! 清空。没有文件、没有恢复、没有后台任务，所有操作只碰一个哈希表。

use std::collections::HashMap;

use parking_lot::RwLock;

use crate::KVError;
use crate::format::{FRAME_HEADER_LEN, RECORD_HEADER_LEN};
use crate::store::{Mutation, Stats};

/// 内存存储配置。
#[derive(Clone, Debug)]
pub struct MemoryOptions {
    /// 键 + 值的总字节上限。
    pub max_bytes: usize,
    /// 单键最大字节数。
    pub max_key_bytes: usize,
    /// 单值最大字节数。
    pub max_value_bytes: usize,
    /// 单次批量写入的最大字节数，含与文件格式一致的 20 字节帧头。
    ///
    /// 对应文件存储的 `max_frame_bytes`：两种存储使用相同的批量大小计量方式，
    /// 否则同一条批量写在内存（测试）里成功、在文件（真机）上失败。
    pub max_batch_bytes: usize,
}

impl Default for MemoryOptions {
    fn default() -> Self {
        Self {
            max_bytes: 32 * 1024 * 1024,
            max_key_bytes: 8 * 1024,
            max_value_bytes: 16 * 1024 * 1024,
            max_batch_bytes: 32 * 1024 * 1024,
        }
    }
}

struct MemoryInner {
    map: HashMap<Box<str>, Box<[u8]>>,
    bytes: usize,
}

/// 内存 KV。所有方法都是同步的；关闭后返回 [`KVError::Closed`]。
pub struct MemoryStore {
    options: MemoryOptions,
    inner: RwLock<Option<MemoryInner>>,
}

impl MemoryStore {
    /// 创建空的存储。
    pub fn new(options: MemoryOptions) -> Self {
        Self {
            options,
            inner: RwLock::new(Some(MemoryInner {
                map: HashMap::new(),
                bytes: 0,
            })),
        }
    }

    /// 读取一个键。
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, KVError> {
        let guard = self.inner.read();
        let inner = guard.as_ref().ok_or(KVError::Closed)?;
        Ok(inner.map.get(key).map(|value| value.to_vec()))
    }

    /// 是否存在该键。与 `get` 一样，超长键只会查不到，不当作输入错误。
    pub fn contains(&self, key: &str) -> Result<bool, KVError> {
        let guard = self.inner.read();
        let inner = guard.as_ref().ok_or(KVError::Closed)?;
        Ok(inner.map.contains_key(key))
    }

    /// 写入一个键值。
    pub fn set(&self, key: &str, value: &[u8]) -> Result<(), KVError> {
        self.validate_key(key)?;
        self.validate_value(value)?;
        let mut guard = self.inner.write();
        let inner = guard.as_mut().ok_or(KVError::Closed)?;
        let old = inner
            .map
            .get(key)
            .map(|old| key.len() + old.len())
            .unwrap_or(0);
        let next = inner
            .bytes
            .saturating_sub(old)
            .checked_add(key.len() + value.len())
            .ok_or(KVError::Full {
                max_bytes: self.options.max_bytes as u64,
            })?;
        if next > self.options.max_bytes {
            return Err(KVError::Full {
                max_bytes: self.options.max_bytes as u64,
            });
        }
        inner.map.insert(key.into(), value.into());
        inner.bytes = next;
        Ok(())
    }

    /// 删除一个键，返回删除前是否存在。
    pub fn remove(&self, key: &str) -> Result<bool, KVError> {
        self.validate_key(key)?;
        let mut guard = self.inner.write();
        let inner = guard.as_mut().ok_or(KVError::Closed)?;
        let Some(old) = inner.map.remove(key) else {
            return Ok(false);
        };
        inner.bytes = inner.bytes.saturating_sub(key.len() + old.len());
        Ok(true)
    }

    /// 原子批量写入。
    ///
    /// 先按“最后一个操作决定该键最终值”的语义预计算最终大小，再一次性
    /// 应用；容量不足时不做任何修改。
    pub fn write(&self, mutations: &[Mutation<'_>]) -> Result<(), KVError> {
        if mutations.is_empty() {
            return Ok(());
        }
        self.validate_mutations(mutations)?;

        let last_clear = mutations
            .iter()
            .rposition(|mutation| matches!(mutation, Mutation::Clear));
        let effective = last_clear.map_or(mutations, |index| &mutations[index + 1..]);

        let mut shadow: HashMap<&str, Option<&[u8]>> = HashMap::new();
        for mutation in effective {
            match mutation {
                Mutation::Set { key, value } => {
                    shadow.insert(key, Some(value));
                }
                Mutation::Remove { key } => {
                    shadow.insert(key, None);
                }
                Mutation::Clear => {
                    // rposition 之后不可能再出现 clear。
                    return Err(KVError::Corrupt {
                        reason: "clear after the last clear".into(),
                    });
                }
            }
        }

        let mut guard = self.inner.write();
        let inner = guard.as_mut().ok_or(KVError::Closed)?;

        let mut final_bytes = if last_clear.is_some() { 0 } else { inner.bytes };
        if last_clear.is_none() {
            for (key, value) in &shadow {
                if let Some(old) = inner.map.get(*key) {
                    final_bytes = final_bytes.saturating_sub(key.len() + old.len());
                }
                if let Some(value) = value {
                    final_bytes = final_bytes.saturating_add(key.len() + value.len());
                }
            }
        } else {
            for (key, value) in &shadow {
                if let Some(value) = value {
                    final_bytes = final_bytes.saturating_add(key.len() + value.len());
                }
            }
        }
        if final_bytes > self.options.max_bytes {
            return Err(KVError::Full {
                max_bytes: self.options.max_bytes as u64,
            });
        }

        if last_clear.is_some() {
            inner.map.clear();
            inner.bytes = 0;
        }
        for (key, value) in shadow {
            match value {
                Some(value) => {
                    if let Some(old) = inner.map.insert(key.into(), value.into()) {
                        inner.bytes = inner.bytes.saturating_sub(key.len() + old.len());
                    }
                    inner.bytes += key.len() + value.len();
                }
                None => {
                    if let Some(old) = inner.map.remove(key) {
                        inner.bytes = inner.bytes.saturating_sub(key.len() + old.len());
                    }
                }
            }
        }
        debug_assert_eq!(inner.bytes, final_bytes);
        Ok(())
    }

    /// 清空全部键。
    pub fn clear(&self) -> Result<(), KVError> {
        let mut guard = self.inner.write();
        let inner = guard.as_mut().ok_or(KVError::Closed)?;
        inner.map.clear();
        inner.bytes = 0;
        Ok(())
    }

    /// 返回按字典序排序的键。
    pub fn keys(&self, prefix: Option<&str>) -> Result<Vec<String>, KVError> {
        let guard = self.inner.read();
        let inner = guard.as_ref().ok_or(KVError::Closed)?;
        let mut keys: Vec<String> = inner
            .map
            .keys()
            .filter(|key| prefix.is_none_or(|prefix| key.starts_with(prefix)))
            .map(|key| key.as_ref().to_string())
            .collect();
        keys.sort_unstable();
        Ok(keys)
    }

    /// 当前键数量。
    pub fn len(&self) -> Result<u64, KVError> {
        let guard = self.inner.read();
        let inner = guard.as_ref().ok_or(KVError::Closed)?;
        Ok(inner.map.len() as u64)
    }

    /// 是否为空。
    pub fn is_empty(&self) -> Result<bool, KVError> {
        Ok(self.len()? == 0)
    }

    /// 统计快照。
    pub fn stats(&self) -> Result<Stats, KVError> {
        let guard = self.inner.read();
        let inner = guard.as_ref().ok_or(KVError::Closed)?;
        Ok(Stats::memory(
            inner.map.len(),
            inner.bytes,
            self.options.max_bytes,
        ))
    }

    /// 内存存储无需刷盘；保留该接口是为了与文件存储共用调用形状。
    pub fn flush(&self) -> Result<(), KVError> {
        let guard = self.inner.read();
        if guard.is_none() {
            return Err(KVError::Closed);
        }
        Ok(())
    }

    /// 关闭并立即释放全部值。
    pub fn close(&self) -> Result<(), KVError> {
        let mut guard = self.inner.write();
        guard.take();
        Ok(())
    }

    fn validate_key(&self, key: &str) -> Result<(), KVError> {
        if key.len() > self.options.max_key_bytes {
            return Err(KVError::InvalidKey {
                reason: format!(
                    "key is {} bytes, max {}",
                    key.len(),
                    self.options.max_key_bytes
                ),
            });
        }
        Ok(())
    }

    fn validate_value(&self, value: &[u8]) -> Result<(), KVError> {
        if value.len() > self.options.max_value_bytes {
            return Err(KVError::ValueTooLarge {
                size: value.len(),
                max_bytes: self.options.max_value_bytes,
            });
        }
        Ok(())
    }

    fn validate_mutations(&self, mutations: &[Mutation<'_>]) -> Result<(), KVError> {
        // 虽然不实际编码，仍包含文件格式的帧头与记录头，保持批量边界一致。
        let mut total = FRAME_HEADER_LEN;
        for mutation in mutations {
            match mutation {
                Mutation::Set { key, value } => {
                    self.validate_key(key)?;
                    self.validate_value(value)?;
                    total = total
                        .checked_add(RECORD_HEADER_LEN)
                        .and_then(|size| size.checked_add(key.len()))
                        .and_then(|size| size.checked_add(value.len()))
                        .ok_or_else(|| batch_too_large(self.options.max_batch_bytes))?;
                }
                Mutation::Remove { key } => {
                    self.validate_key(key)?;
                    total = total
                        .checked_add(RECORD_HEADER_LEN)
                        .and_then(|size| size.checked_add(key.len()))
                        .ok_or_else(|| batch_too_large(self.options.max_batch_bytes))?;
                }
                Mutation::Clear => {
                    total = total
                        .checked_add(RECORD_HEADER_LEN)
                        .ok_or_else(|| batch_too_large(self.options.max_batch_bytes))?;
                }
            }
        }
        if total > self.options.max_batch_bytes {
            return Err(KVError::BatchTooLarge {
                size: total,
                max_bytes: self.options.max_batch_bytes,
            });
        }
        Ok(())
    }
}

fn batch_too_large(max_bytes: usize) -> KVError {
    KVError::BatchTooLarge {
        size: usize::MAX,
        max_bytes,
    }
}

impl std::fmt::Debug for MemoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let guard = self.inner.read();
        let (entries, bytes) = guard
            .as_ref()
            .map(|inner| (inner.map.len(), inner.bytes))
            .unwrap_or((0, 0));
        f.debug_struct("MemoryStore")
            .field("entries", &entries)
            .field("bytes", &bytes)
            .field("options", &self.options)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> MemoryStore {
        MemoryStore::new(MemoryOptions {
            max_bytes: 16,
            ..MemoryOptions::default()
        })
    }

    #[test]
    fn set_get_remove_and_keys() {
        let store = store();
        store.set("b", b"2").expect("set b");
        store.set("a", b"1").expect("set a");
        store.set("a", b"11").expect("replace a");
        assert_eq!(store.get("a").unwrap(), Some(b"11".to_vec()));
        assert_eq!(store.get("missing").unwrap(), None);
        assert!(store.contains("b").unwrap());
        assert_eq!(store.keys(None).unwrap(), vec!["a", "b"]);
        assert_eq!(store.keys(Some("a")).unwrap(), vec!["a"]);
        assert!(store.remove("a").unwrap());
        assert!(!store.remove("a").unwrap());
        assert_eq!(store.len().unwrap(), 1);
        store.clear().expect("clear");
        assert!(store.is_empty().unwrap());
    }

    #[test]
    fn quota_failure_does_not_apply_any_part_of_a_batch() {
        let store = store();
        store.set("a", b"12345").expect("set a");
        let result = store.write(&[
            Mutation::Set {
                key: "a",
                value: b"x",
            },
            Mutation::Set {
                key: "b",
                value: b"0123456789abcdef",
            },
        ]);
        assert!(matches!(result, Err(KVError::Full { .. })));
        assert_eq!(store.get("a").unwrap(), Some(b"12345".to_vec()));
        assert_eq!(store.get("b").unwrap(), None);
    }

    #[test]
    fn batch_uses_last_operation_per_key() {
        let store = store();
        store
            .write(&[
                Mutation::Set {
                    key: "a",
                    value: b"1",
                },
                Mutation::Set {
                    key: "a",
                    value: b"22",
                },
                Mutation::Set {
                    key: "b",
                    value: b"3",
                },
                Mutation::Remove { key: "b" },
            ])
            .expect("batch");
        assert_eq!(store.get("a").unwrap(), Some(b"22".to_vec()));
        assert_eq!(store.get("b").unwrap(), None);
        assert_eq!(store.stats().unwrap().live_bytes, 3);
    }

    #[test]
    fn clear_inside_batch_discards_earlier_operations() {
        let store = store();
        store.set("old", b"value").expect("set old");
        store
            .write(&[
                Mutation::Set {
                    key: "discarded",
                    value: b"x",
                },
                Mutation::Clear,
                Mutation::Set {
                    key: "new",
                    value: b"y",
                },
            ])
            .expect("batch");
        assert_eq!(store.get("old").unwrap(), None);
        assert_eq!(store.get("discarded").unwrap(), None);
        assert_eq!(store.get("new").unwrap(), Some(b"y".to_vec()));
        assert_eq!(store.len().unwrap(), 1);
    }

    #[test]
    fn close_is_terminal_and_idempotent() {
        let store = store();
        store.set("a", b"1").expect("set");
        store.close().expect("close");
        store.close().expect("close again");
        assert!(matches!(store.get("a"), Err(KVError::Closed)));
        assert!(matches!(store.set("a", b"1"), Err(KVError::Closed)));
    }

    #[test]
    fn limits_are_enforced() {
        let store = MemoryStore::new(MemoryOptions {
            max_bytes: 32,
            max_key_bytes: 2,
            max_value_bytes: 2,
            ..MemoryOptions::default()
        });
        assert!(matches!(
            store.set("toolong", b"x"),
            Err(KVError::InvalidKey { .. })
        ));
        assert!(matches!(
            store.set("a", b"toolong"),
            Err(KVError::ValueTooLarge { .. })
        ));
    }
}
