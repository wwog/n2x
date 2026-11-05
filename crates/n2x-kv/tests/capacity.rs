//! 满容量与容量变更下的可用性测试。
//!
//! 追加日志的固有陷阱：`remove` / `clear` 本身也要写一帧，空间不足时若只尝试
//! 压缩（活数据没有可回收的死数据），库会永久只读。这里冻结“减压操作永远可用”
//! 这条契约。

use n2x_kv::{
    BackendPreference, KVError, MemoryOptions, MemoryStore, Mutation, Store, StoreOptions,
};

/// 小容量配置：4096 字节文件，单条 1980 字节记录形成一个 2010 字节的帧。
fn tiny() -> StoreOptions {
    StoreOptions {
        backend: BackendPreference::Buffered,
        max_bytes: 4096,
        initial_bytes: 4096,
        max_key_bytes: 64,
        max_value_bytes: 1990,
        max_frame_bytes: 2083,
        compact_at_percent: 100,
        compact_key_versions: 0,
    }
}

/// 两条 1980 字节记录后日志只剩 12 字节：任何 `set` / `remove` / `clear` 帧都放不下。
fn fill_nearly_full(path: &std::path::Path) -> Store {
    let store = Store::open(path, tiny()).expect("open");
    let value = vec![b'v'; 1980];
    store.set("a", &value).expect("fill a");
    store.set("b", &value).expect("fill b");

    // 64 字节头 + 2 * (20 帧头 + 9 记录头 + 1 键 + 1980 值) = 4084，剩 12 字节。
    let used = 64 + store.stats().expect("stats").data_bytes;
    let free = store.options().max_bytes - used;
    assert_eq!(free, 12, "expected exactly 12 free bytes, got {free}");
    // clear 帧需要 29 字节，remove 帧需要 30 字节——两者都放不下。
    assert!(free < 29);
    store
}

#[test]
fn remove_works_when_the_log_is_full() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = fill_nearly_full(&dir.path().join("full.kv"));

    store
        .remove("a")
        .expect("remove must reclaim space instead of failing");
    assert_eq!(store.get("a").expect("get"), None);
    assert!(store.get("b").expect("get").is_some());

    // 腾出空间后普通写入恢复。
    store.set("after", b"ok").expect("write after reclaiming");
    assert_eq!(store.get("after").expect("get"), Some(b"ok".to_vec()));
}

#[test]
fn clear_works_when_the_log_is_full() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = fill_nearly_full(&dir.path().join("full.kv"));

    store.clear().expect("clear on a full store");
    assert_eq!(store.len().expect("len"), 0);
    store.set("after", b"ok").expect("write after clear");
    assert_eq!(store.get("after").expect("get"), Some(b"ok".to_vec()));
}

#[test]
fn full_store_survives_reopen_and_is_writable_after_clear() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("full.kv");
    {
        let store = fill_nearly_full(&path);
        assert!(matches!(store.set("x", b"y"), Err(KVError::Full { .. })));
        store.clear().expect("clear while full");
        store.flush().expect("flush");
    }
    let store = Store::open(&path, tiny()).expect("reopen");
    assert_eq!(store.len().expect("len"), 0);
    store.set("after", b"ok").expect("write after reopen");
}

/// 20 条 500 字节记录（活数据 10600 字节），再用 4096 字节上限打开。
fn shrink_fixture(path: &std::path::Path) -> StoreOptions {
    let big = StoreOptions {
        max_bytes: 64 * 1024,
        initial_bytes: 64 * 1024,
        max_value_bytes: 500,
        max_frame_bytes: 1024,
        ..tiny()
    };
    {
        let store = Store::open(path, big.clone()).expect("open big");
        let value = vec![b'v'; 500];
        for index in 0..20 {
            store.set(&format!("k{index:03}"), &value).expect("fill");
        }
        assert_eq!(store.stats().expect("stats").entries, 20);
    }
    StoreOptions {
        max_bytes: 4096,
        initial_bytes: 4096,
        ..big
    }
}

#[test]
fn remove_reclaims_space_even_when_live_data_exceeds_max_bytes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("shrink.kv");
    let small = shrink_fixture(&path);

    let store = Store::open(&path, small).expect("reopen with smaller cap");
    assert!(matches!(store.set("z", b"1"), Err(KVError::Full { .. })));

    // 压缩与删除仍必须可用，否则超限数据永远无法收缩。
    store.compact().expect("compact oversized live data");
    assert!(store.remove("k000").expect("remove"));
    assert!(store.get("k000").expect("get").is_none());
    assert!(store.get("k001").expect("get").is_some());
}

#[test]
fn clear_works_when_live_data_exceeds_max_bytes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("shrink.kv");
    let small = shrink_fixture(&path);

    let store = Store::open(&path, small).expect("reopen with smaller cap");
    store.clear().expect("clear oversized store");
    assert_eq!(store.len().expect("len"), 0);
    store.set("after", b"ok").expect("write after clear");
}

/// 减压操作不能把其它键弄丢。
#[test]
fn reclaiming_operations_preserve_other_keys() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = fill_nearly_full(&dir.path().join("keep.kv"));

    let untouched = store.get("b").expect("get b");
    assert!(untouched.is_some());

    store.remove("a").expect("remove");
    assert_eq!(store.get("b").expect("get b"), untouched);
}

/// 满容量兜底走的是压缩，崩溃安全性继承 rename 语义：失败保留旧文件。
#[cfg(unix)]
#[test]
fn failed_reclaim_leaves_the_store_intact() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("tempdir");
    let store = fill_nearly_full(&dir.path().join("ro.kv"));
    let before = store.len().expect("len");

    // 目录不可写 -> 压缩无法创建 `<path>.compact`。
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o555))
        .expect("chmod dir");
    let result = store.clear();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755))
        .expect("restore dir");

    assert!(
        result.is_err(),
        "clear must report the failure, not fake success"
    );
    assert_eq!(
        store.len().expect("len"),
        before,
        "failed reclaim lost keys"
    );
    assert!(store.get("b").expect("get b").is_some());
}

#[test]
fn batch_limits_agree_between_memory_and_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let options = StoreOptions {
        max_bytes: 1024 * 1024,
        initial_bytes: 1024 * 1024,
        max_key_bytes: 64,
        max_value_bytes: 1000,
        max_frame_bytes: 2048,
        compact_at_percent: 100,
        compact_key_versions: 0,
        ..tiny()
    };
    let file = Store::open(dir.path().join("b.kv"), options).expect("open");
    let memory = MemoryStore::new(MemoryOptions {
        max_batch_bytes: 2048,
        ..MemoryOptions::default()
    });

    let value = vec![b'v'; 1000];
    let small_batch = [
        Mutation::Set {
            key: "a",
            value: &value,
        },
        Mutation::Set {
            key: "b",
            value: &value,
        },
    ];
    let big_batch = [
        Mutation::Set {
            key: "a",
            value: &value,
        },
        Mutation::Set {
            key: "b",
            value: &value,
        },
        Mutation::Set {
            key: "c",
            value: &value,
        },
    ];

    assert!(
        file.write(&small_batch).is_ok(),
        "file rejects a 2-record batch"
    );
    assert!(
        memory.write(&small_batch).is_ok(),
        "memory rejects a 2-record batch"
    );

    assert!(matches!(
        file.write(&big_batch),
        Err(KVError::BatchTooLarge { .. })
    ));
    assert!(matches!(
        memory.write(&big_batch),
        Err(KVError::BatchTooLarge { .. })
    ));
}

#[test]
fn batch_limits_agree_at_the_frame_header_boundary() {
    for backend in [BackendPreference::Buffered, BackendPreference::Mmap] {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = Store::open(
            dir.path().join("boundary.kv"),
            StoreOptions {
                backend,
                max_bytes: 4096,
                initial_bytes: 4096,
                max_key_bytes: 1,
                max_value_bytes: 50,
                max_frame_bytes: 100,
                compact_at_percent: 100,
                compact_key_versions: 0,
            },
        )
        .expect("open");
        let memory = MemoryStore::new(MemoryOptions {
            max_bytes: 4096,
            max_key_bytes: 1,
            max_value_bytes: 50,
            max_batch_bytes: 100,
        });

        for value_len in [30, 31, 35] {
            let value = vec![b'v'; value_len];
            let batch = [
                Mutation::Set {
                    key: "a",
                    value: &value,
                },
                Mutation::Set {
                    key: "b",
                    value: &value,
                },
            ];
            let frame_bytes = 20 + 2 * (9 + 1 + value_len);
            for result in [file.write(&batch), memory.write(&batch)] {
                if frame_bytes <= 100 {
                    result.expect("exactly one frame fits");
                } else {
                    assert!(matches!(
                        result,
                        Err(KVError::BatchTooLarge { size, max_bytes: 100 })
                            if size == frame_bytes
                    ));
                }
            }
            // 被拒绝的批量不能改变此前成功写入的值。
            assert_eq!(file.get("a").expect("file value"), Some(vec![b'v'; 30]));
            assert_eq!(memory.get("a").expect("memory value"), Some(vec![b'v'; 30]));
        }
    }
}

/// 压缩中断留下的 `<path>.compact` 不影响打开；下一次 `compact` 会清掉它。
#[test]
fn stale_compaction_file_does_not_block_open_and_is_reclaimed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("stale.kv");
    {
        let store = Store::open(&path, tiny()).expect("open");
        store.set("a", b"1").expect("set");
        store.flush().expect("flush");
    }

    let tmp = dir.path().join("stale.kv.compact");
    std::fs::write(&tmp, vec![b'g'; 4096]).expect("write stale tmp");

    let store = Store::open(&path, tiny()).expect("open with stale tmp");
    assert_eq!(store.get("a").expect("get"), Some(b"1".to_vec()));
    assert!(
        tmp.exists(),
        "open must not delete a file another process may own"
    );

    store.compact().expect("compact");
    assert!(!tmp.exists(), "compaction must remove the stale file");
}

/// 批量写里只含删除/清空时，满容量也必须完成——引擎按作用域清空正是这种形态。
#[test]
fn removal_only_batch_works_when_the_log_is_full() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = fill_nearly_full(&dir.path().join("batch.kv"));

    store
        .write(&[Mutation::Remove { key: "a" }, Mutation::Remove { key: "b" }])
        .expect("removal-only batch must not fail on a full store");
    assert_eq!(store.len().expect("len"), 0);
    store.set("after", b"ok").expect("write after reclaiming");
}

#[test]
fn clear_batch_works_when_the_log_is_full() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = fill_nearly_full(&dir.path().join("batch.kv"));

    store
        .write(&[Mutation::Clear])
        .expect("clear batch must not fail on a full store");
    assert_eq!(store.len().expect("len"), 0);
    store.set("after", b"ok").expect("write after clear");
}

/// 含 Set 的批量在满容量时仍必须失败，且不能只应用一半。
#[test]
fn mixed_batch_on_a_full_store_fails_atomically() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = fill_nearly_full(&dir.path().join("batch.kv"));

    let result = store.write(&[
        Mutation::Remove { key: "a" },
        Mutation::Set {
            key: "c",
            value: b"1",
        },
    ]);
    assert!(matches!(result, Err(KVError::Full { .. })));
    assert_eq!(store.get("a").expect("get"), Some(vec![b'v'; 1980]));
    assert_eq!(store.get("c").expect("get"), None);
}
