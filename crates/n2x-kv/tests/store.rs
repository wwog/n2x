use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;

use n2x_kv::{BackendPreference, Mutation, KVError, Store, StoreOptions};

fn options(backend: BackendPreference) -> StoreOptions {
    StoreOptions {
        backend,
        initial_bytes: 64 * 1024,
        ..StoreOptions::default()
    }
}

fn small_options(backend: BackendPreference, max_bytes: u64) -> StoreOptions {
    StoreOptions {
        backend,
        max_bytes,
        initial_bytes: max_bytes,
        max_key_bytes: 64,
        max_value_bytes: 1024,
        max_frame_bytes: 2048,
        compact_at_percent: 100,
        compact_key_versions: 0,
    }
}

fn append_bytes(path: &std::path::Path, bytes: &[u8]) {
    let mut file = OpenOptions::new()
        .append(true)
        .open(path)
        .expect("open for append");
    file.write_all(bytes).expect("append");
}

fn flip_last_byte(path: &std::path::Path) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open for edit");
    let len = file.metadata().expect("metadata").len();
    assert!(len > 0);
    file.seek(SeekFrom::Start(len - 1)).expect("seek");
    let mut byte = [0u8; 1];
    use std::io::Read;
    file.read_exact(&mut byte).expect("read");
    byte[0] ^= 0xFF;
    file.seek(SeekFrom::Start(len - 1)).expect("seek");
    file.write_all(&byte).expect("write");
}

#[test]
fn mmap_roundtrip_persists_and_reopens() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("local.kv");

    {
        let store = Store::open(&path, options(BackendPreference::Mmap)).expect("open");
        assert_eq!(store.stats().unwrap().backend, n2x_kv::BackendKind::Mmap);
        store.set("alpha", b"one").expect("set alpha");
        store.set("beta", b"two").expect("set beta");
        store.flush().expect("flush");
    }

    let store = Store::open(&path, options(BackendPreference::Mmap)).expect("reopen");
    assert_eq!(store.get("alpha").unwrap(), Some(b"one".to_vec()));
    assert_eq!(store.get("beta").unwrap(), Some(b"two".to_vec()));
    assert_eq!(store.get("missing").unwrap(), None);
    assert_eq!(store.keys(Some("a")).unwrap(), vec!["alpha"]);
    assert_eq!(store.len().unwrap(), 2);
}

#[test]
fn buffered_backend_matches_mmap_semantics() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path_mmap = dir.path().join("mmap.kv");
    let path_buffered = dir.path().join("buffered.kv");

    let mmap = Store::open(&path_mmap, options(BackendPreference::Mmap)).expect("mmap");
    let buffered =
        Store::open(&path_buffered, options(BackendPreference::Buffered)).expect("buffered");
    assert_eq!(
        buffered.stats().unwrap().backend,
        n2x_kv::BackendKind::Buffered
    );

    for (index, key) in ["c", "a", "b"].iter().enumerate() {
        let value = format!("value-{index}");
        mmap.set(key, value.as_bytes()).expect("mmap set");
        buffered.set(key, value.as_bytes()).expect("buffered set");
    }
    mmap.remove("a").expect("mmap remove");
    buffered.remove("a").expect("buffered remove");

    assert_eq!(mmap.keys(None).unwrap(), buffered.keys(None).unwrap());
    assert_eq!(mmap.len().unwrap(), buffered.len().unwrap());
    assert_eq!(mmap.get("b").unwrap(), buffered.get("b").unwrap());
    assert_eq!(mmap.get("a").unwrap(), buffered.get("a").unwrap());

    mmap.compact().expect("mmap compact");
    buffered.compact().expect("buffered compact");
    assert_eq!(mmap.keys(None).unwrap(), buffered.keys(None).unwrap());
    drop((mmap, buffered));

    let mmap = Store::open(&path_mmap, options(BackendPreference::Mmap)).expect("mmap reopen");
    let buffered =
        Store::open(&path_buffered, options(BackendPreference::Buffered)).expect("buffered reopen");
    assert_eq!(mmap.get("c").unwrap(), buffered.get("c").unwrap());
    assert_eq!(mmap.get("a").unwrap(), None);
    assert_eq!(buffered.get("a").unwrap(), None);
}

#[test]
fn torn_tail_is_dropped_and_store_remains_writable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("torn.kv");

    {
        let store = Store::open(&path, options(BackendPreference::Mmap)).expect("open");
        store.set("keep", b"value").expect("set");
        store.compact().expect("compact to shrink the file");
        store.flush().expect("flush");
    }
    append_bytes(&path, &[0xAB; 7]);

    let store = Store::open(&path, options(BackendPreference::Mmap)).expect("recover");
    assert_eq!(store.get("keep").unwrap(), Some(b"value".to_vec()));
    store
        .set("after", b"recovered")
        .expect("write after recovery");
    drop(store);

    let store = Store::open(&path, options(BackendPreference::Mmap)).expect("reopen");
    assert_eq!(store.get("keep").unwrap(), Some(b"value".to_vec()));
    assert_eq!(store.get("after").unwrap(), Some(b"recovered".to_vec()));
}

#[test]
fn corrupted_last_frame_is_dropped_as_a_whole() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("batch.kv");

    {
        let store = Store::open(&path, options(BackendPreference::Buffered)).expect("open");
        store
            .write(&[
                Mutation::Set {
                    key: "first",
                    value: b"111",
                },
                Mutation::Set {
                    key: "second",
                    value: b"222",
                },
            ])
            .expect("batch");
        store.compact().expect("compact");
        store.flush().expect("flush");
    }
    flip_last_byte(&path);

    let store = Store::open(&path, options(BackendPreference::Buffered)).expect("recover");
    assert_eq!(store.get("first").unwrap(), None);
    assert_eq!(store.get("second").unwrap(), None);
    assert_eq!(store.len().unwrap(), 0);
    store.set("later", b"ok").expect("write after torn batch");
}

#[test]
fn corrupt_record_count_does_not_leave_partial_index_entries() {
    for backend in [BackendPreference::Mmap, BackendPreference::Buffered] {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("count.kv");
        {
            let store = Store::open(&path, options(backend)).expect("open");
            store
                .write(&[
                    Mutation::Set {
                        key: "a",
                        value: b"AAA",
                    },
                    Mutation::Set {
                        key: "b",
                        value: b"BBB",
                    },
                    Mutation::Set {
                        key: "c",
                        value: b"CCC",
                    },
                ])
                .expect("batch");
            store.flush().expect("flush");
        }
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .expect("open raw");
            file.seek(SeekFrom::Start(64 + 12)).expect("seek count");
            file.write_all(&1u32.to_le_bytes()).expect("corrupt count");
        }

        let store = Store::open(&path, options(backend)).expect("recover");
        assert_eq!(store.len().unwrap(), 0);
        assert_eq!(store.keys(None).unwrap(), Vec::<String>::new());
        store.set("z", b"0123456789").expect("write after recovery");
        assert_eq!(store.get("a").unwrap(), None);
        assert_eq!(store.get("z").unwrap(), Some(b"0123456789".to_vec()));
    }
}

#[test]
fn compaction_reclaims_space_and_keeps_latest_values() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("compact.kv");
    let store = Store::open(
        &path,
        StoreOptions {
            compact_key_versions: 0,
            ..options(BackendPreference::Mmap)
        },
    )
    .expect("open");

    let payload = vec![b'x'; 1024];
    for _ in 0..200 {
        store.set("hot", &payload).expect("set");
    }
    let before = store.stats().unwrap();
    assert!(before.dead_bytes > 0);
    assert!(before.data_bytes > 64 * 1024);

    store.compact().expect("compact");
    let after = store.stats().unwrap();
    assert!(after.data_bytes < before.data_bytes);
    assert!(after.compactions >= 1);
    assert_eq!(store.get("hot").unwrap(), Some(payload.clone()));

    store.flush().expect("flush");
    drop(store);
    let store = Store::open(&path, options(BackendPreference::Mmap)).expect("reopen");
    assert_eq!(store.get("hot").unwrap(), Some(payload));
}

#[test]
fn hot_key_versions_trigger_automatic_compaction() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("hot.kv");
    let store = Store::open(
        &path,
        StoreOptions {
            backend: BackendPreference::Buffered,
            compact_key_versions: 100,
            ..options(BackendPreference::Buffered)
        },
    )
    .expect("open");

    for version in 0..100 {
        store
            .set("hot", format!("version-{version}").as_bytes())
            .expect("set hot key");
    }
    let stats = store.stats().unwrap();
    assert_eq!(stats.compactions, 1);
    assert!(stats.dead_bytes < 64);
    assert_eq!(store.get("hot").unwrap(), Some(b"version-99".to_vec()));

    drop(store);
    let store = Store::open(&path, options(BackendPreference::Buffered)).expect("reopen");
    assert_eq!(store.get("hot").unwrap(), Some(b"version-99".to_vec()));
}

#[test]
fn opening_reclaims_a_hot_key_that_reached_its_version_limit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("hot-reopen.kv");
    let mut options = StoreOptions {
        backend: BackendPreference::Buffered,
        compact_key_versions: 0,
        ..options(BackendPreference::Buffered)
    };
    {
        let store = Store::open(&path, options.clone()).expect("open");
        for version in 0..100 {
            store
                .set("hot", format!("version-{version}").as_bytes())
                .expect("set hot key");
        }
        assert_eq!(store.stats().unwrap().compactions, 0);
        store.flush().expect("flush");
    }

    options.compact_key_versions = 100;
    let store = Store::open(&path, options).expect("reopen");
    assert_eq!(store.stats().unwrap().compactions, 1);
    assert!(store.stats().unwrap().dead_bytes < 64);
    assert_eq!(store.get("hot").unwrap(), Some(b"version-99".to_vec()));
}

#[test]
fn capacity_percentage_triggers_only_when_compaction_has_headroom() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("percentage.kv");
    let store = Store::open(
        &path,
        StoreOptions {
            compact_at_percent: 75,
            compact_key_versions: 0,
            ..small_options(BackendPreference::Buffered, 4096)
        },
    )
    .expect("open");
    let value = vec![b'v'; 600];
    for key in ["a", "b", "c"] {
        store.set(key, &value).expect("fill below ratio threshold");
    }
    store.set("a", &value).expect("first replace");
    assert_eq!(store.stats().unwrap().compactions, 0);
    store
        .set("a", &value)
        .expect("cross ratio with reclaimable data");
    assert_eq!(store.stats().unwrap().compactions, 1);
    assert!(store.stats().unwrap().data_bytes < 2048);

    for key in ["d", "e", "f"] {
        store.set(key, &value).expect("fill with live data");
    }
    let before = store.stats().unwrap();
    for key in ["g", "h"] {
        assert!(matches!(
            store.set(key, &value),
            Err(KVError::Full { .. })
        ));
    }
    let after = store.stats().unwrap();
    assert_eq!(after.compactions, before.compactions);
    assert_eq!(store.get("f").unwrap(), Some(value));
}

#[test]
fn clear_is_durable_and_reclaims_space() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("clear.kv");
    {
        let store = Store::open(&path, options(BackendPreference::Mmap)).expect("open");
        store.set("a", b"1").expect("set");
        store.set("b", b"2").expect("set");
        store.clear().expect("clear");
        assert_eq!(store.len().unwrap(), 0);
        store.flush().expect("flush");
    }
    let store = Store::open(&path, options(BackendPreference::Mmap)).expect("reopen");
    assert_eq!(store.len().unwrap(), 0);
    assert_eq!(store.get("a").unwrap(), None);
}

#[test]
fn quota_rejects_the_write_without_losing_existing_keys() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("quota.kv");
    let store = Store::open(&path, small_options(BackendPreference::Buffered, 4096)).expect("open");
    let value = vec![b'v'; 1000];

    for key in ["a", "b", "c"] {
        store.set(key, &value).expect("fill");
    }
    let result = store.set("d", &value);
    assert!(matches!(result, Err(KVError::Full { .. })));
    for key in ["a", "b", "c"] {
        assert_eq!(store.get(key).unwrap(), Some(value.clone()));
    }

    store.remove("a").expect("remove");
    // 删除后活数据仍接近上限，下一次写入触发压缩后腾出空间。
    store
        .set("d", &value)
        .expect("write after reclaiming space");
}

#[test]
fn invalid_options_are_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("invalid.kv");
    let bad = StoreOptions {
        max_bytes: 128,
        initial_bytes: 1024,
        ..StoreOptions::default()
    };
    assert!(matches!(
        Store::open(&path, bad),
        Err(KVError::InvalidOptions { .. })
    ));
}

#[test]
fn invalid_compaction_threshold_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let bad = StoreOptions {
        compact_at_percent: 0,
        ..options(BackendPreference::Buffered)
    };
    assert!(matches!(
        Store::open(dir.path().join("invalid-percent.kv"), bad),
        Err(KVError::InvalidOptions { .. })
    ));
}

#[test]
fn garbage_header_is_reported_as_corrupt() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("garbage.kv");
    {
        let store = Store::open(&path, options(BackendPreference::Mmap)).expect("open");
        store.set("a", b"1").expect("set");
        store.flush().expect("flush");
    }
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open raw");
        file.seek(SeekFrom::Start(0)).expect("seek");
        file.write_all(b"BROKEN!!").expect("corrupt magic");
    }
    assert!(matches!(
        Store::open(&path, options(BackendPreference::Mmap)),
        Err(KVError::Corrupt { .. })
    ));
}

#[test]
fn concurrent_readers_and_writers_share_one_store() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("concurrent.kv");
    let store = Arc::new(Store::open(&path, options(BackendPreference::Mmap)).expect("open"));

    std::thread::scope(|scope| {
        for writer in 0..4 {
            let store = Arc::clone(&store);
            scope.spawn(move || {
                for index in 0..100 {
                    let key = format!("w{writer}-{index:03}");
                    let value = format!("value-{writer}-{index}");
                    store.set(&key, value.as_bytes()).expect("set");
                }
            });
        }
        for _ in 0..4 {
            let store = Arc::clone(&store);
            scope.spawn(move || {
                for writer in 0..4 {
                    for index in 0..100 {
                        let key = format!("w{writer}-{index:03}");
                        let value = store.get(&key).expect("get");
                        if let Some(value) = value {
                            assert!(value.starts_with(b"value-"));
                        }
                    }
                }
            });
        }
    });

    assert_eq!(store.len().unwrap(), 400);
    store.compact().expect("compact");
    assert_eq!(store.len().unwrap(), 400);
}

#[test]
fn opening_the_same_path_twice_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("single.kv");

    let first = Store::open(&path, options(BackendPreference::Buffered)).expect("first open");
    let error = Store::open(&path, options(BackendPreference::Buffered))
        .expect_err("second open must fail");
    assert!(matches!(error, KVError::AlreadyOpen { .. }));
    assert!(error.to_string().contains("single.kv"));

    drop(first);
    Store::open(&path, options(BackendPreference::Buffered)).expect("reopen after drop");
}

#[test]
fn close_releases_the_path_for_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("closed.kv");

    let first = Store::open(&path, options(BackendPreference::Buffered)).expect("first open");
    first.set("kept", b"value").expect("set");
    first.close().expect("close");

    // close 释放路径占用，即使旧对象还在作用域内也可以重新打开。
    let second = Store::open(&path, options(BackendPreference::Buffered)).expect("reopen");
    assert_eq!(second.get("kept").unwrap(), Some(b"value".to_vec()));
}

#[test]
fn full_live_data_reports_full_without_dropping_existing_keys() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("full-live.kv");
    let store = Store::open(&path, small_options(BackendPreference::Buffered, 4096)).expect("open");
    let value = vec![b'v'; 1000];

    for key in ["a", "b", "c"] {
        store.set(key, &value).expect("fill live data");
    }
    let before = store.stats().unwrap();
    assert!(matches!(
        store.set("d", &value),
        Err(KVError::Full { .. })
    ));
    // 没有可回收收益时不执行全量压缩。
    assert_eq!(store.stats().unwrap().compactions, before.compactions);
    for key in ["a", "b", "c"] {
        assert_eq!(store.get(key).unwrap(), Some(value.clone()));
    }
    assert_eq!(store.get("d").unwrap(), None);
}

#[test]
fn store_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Store>();
    assert_send_sync::<n2x_kv::MemoryStore>();
}

#[test]
fn reopening_a_store_with_a_compact_file_works() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("small.kv");
    {
        let store = Store::open(&path, options(BackendPreference::Mmap)).expect("open");
        store.set("only", b"value").expect("set");
        store.compact().expect("compact");
        store.close().expect("close");
        assert!(matches!(store.get("only"), Err(KVError::Closed)));
    }
    let file = File::open(&path).expect("open file");
    assert!(file.metadata().expect("metadata").len() >= 64);
    let store = Store::open(&path, options(BackendPreference::Mmap)).expect("reopen");
    assert_eq!(store.get("only").unwrap(), Some(b"value".to_vec()));
}
