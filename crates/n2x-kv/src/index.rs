//! 内存索引：键 → 值在日志文件中的位置。
//!
//! 索引只保存键与元数据，值仍留在 mmap 里。这样点读不需要额外缓存，
//! 内存占用与键数量相关，而不是与数据总量相关。

use std::collections::HashMap;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Entry {
    pub(crate) value_offset: u64,
    pub(crate) value_len: u32,
    /// `9 + key_len + value_len`，用于统计活数据量。
    pub(crate) record_bytes: u32,
}

#[derive(Default)]
pub(crate) struct Index {
    pub(crate) map: HashMap<Box<str>, Entry>,
    /// 所有活记录的格式字节数（含 9 字节记录头）。
    pub(crate) live_record_bytes: u64,
    /// 所有活记录的键 + 值字节数。
    pub(crate) live_payload_bytes: u64,
    /// 当前压缩周期的逐键历史记录数，包含已覆盖与删除的记录。
    versions: HashMap<Box<str>, u32>,
    pub(crate) max_versions: u32,
}

impl Index {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    pub(crate) fn get(&self, key: &str) -> Option<&Entry> {
        self.map.get(key)
    }

    pub(crate) fn contains(&self, key: &str) -> bool {
        self.map.contains_key(key)
    }

    pub(crate) fn set(&mut self, key: &str, value_offset: u64, value_len: u32, record_bytes: u32) {
        self.count_version(key);
        self.set_owned(key.into(), value_offset, value_len, record_bytes);
    }

    /// 压缩时按活记录重建索引，同时开启一个新的历史计数周期。
    pub(crate) fn set_compacted(
        &mut self,
        key: Box<str>,
        value_offset: u64,
        value_len: u32,
        record_bytes: u32,
    ) {
        self.max_versions = 1;
        self.versions.insert(key.clone(), 1);
        self.set_owned(key, value_offset, value_len, record_bytes);
    }

    fn count_version(&mut self, key: &str) {
        let count = self
            .versions
            .get(key)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        self.versions.insert(key.into(), count);
        self.max_versions = self.max_versions.max(count);
    }

    fn set_owned(&mut self, key: Box<str>, value_offset: u64, value_len: u32, record_bytes: u32) {
        if let Some(old) = self.map.insert(
            key,
            Entry {
                value_offset,
                value_len,
                record_bytes,
            },
        ) {
            self.live_record_bytes = self
                .live_record_bytes
                .saturating_sub(old.record_bytes as u64);
            self.live_payload_bytes = self
                .live_payload_bytes
                .saturating_sub(old.record_bytes.saturating_sub(9) as u64);
        }
        self.live_record_bytes += record_bytes as u64;
        self.live_payload_bytes += record_bytes.saturating_sub(9) as u64;
    }

    pub(crate) fn remove(&mut self, key: &str) -> bool {
        // 删除后不再保留该键的历史计数：被删键不应长期占用索引内存。
        // `max_versions` 是到下次压缩前的高水位，保留它可以让删除前积累的
        // 历史记录仍有机会触发回收。
        self.versions.remove(key);
        let Some(old) = self.map.remove(key) else {
            return false;
        };
        self.live_record_bytes = self
            .live_record_bytes
            .saturating_sub(old.record_bytes as u64);
        self.live_payload_bytes = self
            .live_payload_bytes
            .saturating_sub(old.record_bytes.saturating_sub(9) as u64);
        true
    }

    pub(crate) fn clear(&mut self) {
        self.map.clear();
        self.versions.clear();
        self.max_versions = 0;
        self.live_record_bytes = 0;
        self.live_payload_bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accounting_tracks_replace_remove_and_clear() {
        let mut index = Index::new();
        index.set("a", 64, 3, 9 + 1 + 3);
        assert_eq!(index.len(), 1);
        assert_eq!(index.live_record_bytes, 13);
        assert_eq!(index.live_payload_bytes, 4);

        index.set("a", 100, 10, 9 + 1 + 10);
        assert_eq!(index.len(), 1);
        assert_eq!(index.live_record_bytes, 20);
        assert_eq!(index.live_payload_bytes, 11);

        index.set("b", 200, 0, 9 + 1);
        assert_eq!(index.live_record_bytes, 30);
        assert_eq!(index.live_payload_bytes, 12);

        assert!(index.remove("a"));
        assert!(!index.remove("a"));
        assert_eq!(index.live_record_bytes, 10);
        assert_eq!(index.live_payload_bytes, 1);
        assert_eq!(index.max_versions, 2);

        index.clear();
        assert!(index.map.is_empty());
        assert_eq!(index.live_record_bytes, 0);
        assert_eq!(index.live_payload_bytes, 0);
        assert_eq!(index.max_versions, 0);
    }

    #[test]
    fn versions_track_replacements_and_are_dropped_on_remove() {
        let mut index = Index::new();
        index.set("hot", 64, 1, 13);
        index.set("hot", 80, 1, 13);
        assert_eq!(index.max_versions, 2);
        assert_eq!(index.versions.get("hot"), Some(&2));

        index.remove("hot");
        assert_eq!(index.len(), 0);
        assert!(!index.versions.contains_key("hot"));
        // 高水位保留到下一次压缩，删除前积累的历史仍可触发回收。
        assert_eq!(index.max_versions, 2);
        assert!(!index.remove("hot"));
        assert!(!index.versions.contains_key("hot"));

        let mut compacted = Index::new();
        compacted.set_compacted("hot".into(), 100, 1, 13);
        assert_eq!(compacted.max_versions, 1);
        compacted.set("hot", 120, 1, 13);
        assert_eq!(compacted.max_versions, 2);
    }
}
