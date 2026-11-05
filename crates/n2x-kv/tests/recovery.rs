//! 恢复判定测试：区分“正常预分配尾部”和“头部被清零 / 介质零洞”。
//!
//! 这两类损坏都会让文件看起来像“从头就终止”，但前者可以安全重建，后者必须
//! 报错或至少显式回收，绝不能当成新库静默截断已有数据。

use std::io::{Seek, SeekFrom, Write};

use n2x_kv::{BackendPreference, Store, StoreOptions};

fn options(backend: BackendPreference) -> StoreOptions {
    StoreOptions {
        backend,
        max_bytes: 1024 * 1024,
        initial_bytes: 64 * 1024,
        max_value_bytes: 4096,
        max_frame_bytes: 64 * 1024,
        ..StoreOptions::default()
    }
}

fn write_baseline(path: &std::path::Path, backend: BackendPreference) {
    let store = Store::open(path, options(backend)).expect("open");
    store.set("first", b"one").expect("set first");
    store.set("second", b"two").expect("set second");
    store.set("third", b"three").expect("set third");
    store.flush().expect("flush");
}

fn zero_range(path: &std::path::Path, start: u64, len: usize) {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open raw");
    file.seek(SeekFrom::Start(start)).expect("seek");
    file.write_all(&vec![0u8; len]).expect("zero");
}

#[test]
fn zeroed_header_on_populated_file_is_corrupt_and_preserved() {
    for backend in [BackendPreference::Mmap, BackendPreference::Buffered] {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("header.kv");
        write_baseline(&path, backend);
        let before = std::fs::metadata(&path).expect("meta").len();

        zero_range(&path, 0, 64);

        let error = Store::open(&path, options(backend))
            .expect_err("a zeroed header over existing data must not be treated as a new file");
        println!("{backend:?}: {error}");
        assert!(
            error.to_string().contains("zeroed"),
            "unexpected error: {error}"
        );

        // 拒绝打开，且不修改原文件——数据还在，可以人工取证或走修复工具。
        assert_eq!(
            std::fs::metadata(&path).expect("meta").len(),
            before,
            "corrupt file must not be truncated"
        );
    }
}

#[test]
fn zeroed_header_on_empty_file_reinitializes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("empty.kv");

    // 初始化中途崩溃：set_len 成功、文件头没写进去，整文件全零。
    std::fs::write(&path, vec![0u8; 64 * 1024]).expect("write zeros");

    let store = Store::open(&path, options(BackendPreference::Buffered)).expect("reinitialize");
    assert_eq!(store.len().expect("len"), 0);
    store.set("after", b"ok").expect("write");
    assert_eq!(store.get("after").expect("get"), Some(b"ok".to_vec()));
}

#[test]
fn zero_hole_between_frames_is_reported_by_dropping_the_tail() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("hole.kv");
    write_baseline(&path, BackendPreference::Buffered);

    // 第一帧 = 20 帧头 + 9 记录头 + "first"(5) + "one"(3) = 37 字节。
    let second_frame = 64 + 37;
    let before = std::fs::metadata(&path).expect("meta").len();
    zero_range(&path, second_frame, 4);

    let store = Store::open(&path, options(BackendPreference::Buffered)).expect("recover");
    // 零洞之后的数据不可达：这是顺序日志的固有代价，但必须真被回收，
    // 而不是留在文件里等下一次写入悄悄覆盖。
    assert_eq!(store.keys(None).expect("keys"), vec!["first"]);
    let after = std::fs::metadata(&path).expect("meta").len();
    assert!(
        after < before,
        "tail after the zero hole must be truncated ({before} -> {after})"
    );
    store.set("after", b"ok").expect("write after recovery");
}

#[test]
fn zero_hole_at_the_very_end_does_not_truncate_valid_data() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("tail.kv");
    write_baseline(&path, BackendPreference::Buffered);

    // 正常预分配尾部：最后一帧之后全是零，必须原样保留全部键。
    let store = Store::open(&path, options(BackendPreference::Buffered)).expect("reopen");
    assert_eq!(
        store.keys(None).expect("keys"),
        vec!["first", "second", "third"]
    );
}

/// 压缩估算必须与实际组包使用同一个帧上限，否则会批准放不下的压缩，
/// 把一次失败写入变成一次无收益的全量重写。
///
/// 参数说明：组包上限被 `max_frame_bytes` 压到 2030，而一条记录是 2023 字节，
/// 所以压缩后每条记录独占一帧。30 条记录的真实压缩产物是
/// `64 + 30*2003 + 30*20 = 60664`；若估算按固定 256 KiB 分帧会算成 1 帧，
/// 从而在 `max_bytes = 62500` 时批准一次最终仍返回 `Full` 的压缩。
#[test]
fn compaction_estimate_matches_actual_packing() {
    let options = StoreOptions {
        backend: BackendPreference::Buffered,
        max_bytes: 62_500,
        initial_bytes: 62_500,
        max_key_bytes: 8,
        max_value_bytes: 1990,
        max_frame_bytes: 2050,
        compact_at_percent: 100,
        compact_key_versions: 0,
    };
    let dir = tempfile::tempdir().expect("tempdir");
    let store = Store::open(dir.path().join("estimate.kv"), options).expect("open");

    let value = vec![b'v'; 1990];
    for index in 0..30 {
        store.set(&format!("k{index:02}"), &value).expect("live");
    }

    let before = store.stats().expect("stats").compactions;
    // 新帧 2022 字节：日志 60754 + 2022 > 62500，走压缩判定。
    let result = store.set("new", &value);
    let after = store.stats().expect("stats");

    assert!(
        matches!(result, Err(n2x_kv::KVError::Full { .. })),
        "the write cannot fit even after compaction, got {result:?}"
    );
    assert_eq!(
        after.compactions, before,
        "a rejected write must be rejected before rewriting the whole file"
    );
    // 已有数据不受影响。
    assert_eq!(store.len().expect("len"), 30);
    assert_eq!(store.get("k00").expect("get").map(|v| v.len()), Some(1990));
}

#[test]
fn compaction_estimate_accounts_for_unused_space_in_each_frame() {
    for backend in [BackendPreference::Buffered, BackendPreference::Mmap] {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(
            dir.path().join("fragmented.kv"),
            StoreOptions {
                backend,
                max_bytes: 2634,
                initial_bytes: 2634,
                max_key_bytes: 8,
                max_value_bytes: 600,
                max_frame_bytes: 1024,
                compact_at_percent: 100,
                compact_key_versions: 0,
            },
        )
        .expect("open");
        store.set("k0", b"x").expect("old version");
        let value = vec![b'v'; 589];
        for index in 0..4 {
            store.set(&format!("k{index}"), &value).expect("live");
        }

        // 每条活记录 600 字节，1004 字节的 payload 上限只能每帧放一条。
        // 不能按 ceil(2400 / 1004) 估成三帧后，先压缩再发现新写入放不下。
        let before = store.stats().expect("stats");
        assert!(matches!(
            store.set("new", &[b'n'; 68]),
            Err(n2x_kv::KVError::Full { .. })
        ));
        let after = store.stats().expect("stats");
        assert_eq!(after.compactions, before.compactions);
        assert_eq!(after.data_bytes, before.data_bytes);
        assert_eq!(store.get("k0").expect("get"), Some(value));
    }
}

#[test]
fn compaction_keeps_a_full_store_within_its_capacity() {
    use n2x_kv::Mutation;

    for backend in [BackendPreference::Buffered, BackendPreference::Mmap] {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("full-batch.kv");
        let options = StoreOptions {
            backend,
            max_bytes: 280_084,
            initial_bytes: 280_084,
            max_key_bytes: 1,
            max_value_bytes: 139_990,
            max_frame_bytes: 280_020,
            compact_at_percent: 100,
            compact_key_versions: 0,
        };
        let value = vec![b'v'; 139_990];
        let store = Store::open(&path, options.clone()).expect("open");
        store
            .write(&[
                Mutation::Set {
                    key: "a",
                    value: &value,
                },
                Mutation::Set {
                    key: "b",
                    value: &value,
                },
            ])
            .expect("fill in one frame");

        // 拆成两个 256 KiB 目标帧会多出一个帧头。空间不足时仍能使用合法的大帧。
        store.compact().expect("compact at capacity");
        assert!(store.stats().expect("stats").file_bytes <= options.max_bytes);
        assert_eq!(store.get("a").expect("get a"), Some(value.clone()));
        assert_eq!(store.get("b").expect("get b"), Some(value.clone()));
        store.close().expect("close");

        let store = Store::open(&path, options).expect("reopen");
        assert_eq!(store.get("a").expect("get a"), Some(value.clone()));
        assert_eq!(store.get("b").expect("get b"), Some(value));
        store.remove("a").expect("remove at capacity");
        assert!(store.get("a").expect("get removed").is_none());
    }
}

/// 活数据本身超过上限时，压缩必须保留全部数据并可用，只是拒绝新写入。
#[test]
fn compacting_oversized_data_preserves_every_key() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("oversized.kv");

    let big = StoreOptions {
        backend: BackendPreference::Buffered,
        max_bytes: 256 * 1024,
        initial_bytes: 256 * 1024,
        max_key_bytes: 64,
        max_value_bytes: 1500,
        max_frame_bytes: 2048,
        compact_at_percent: 100,
        compact_key_versions: 0,
    };
    {
        let store = Store::open(&path, big.clone()).expect("open");
        let value = vec![b'v'; 1500];
        for index in 0..100 {
            store.set(&format!("k{index:03}"), &value).expect("fill");
        }
    }

    let small = StoreOptions {
        max_bytes: 64 * 1024,
        initial_bytes: 64 * 1024,
        ..big
    };
    let store = Store::open(&path, small).expect("reopen");
    let before = store.len().expect("len");
    store.compact().expect("compact");
    assert_eq!(store.len().expect("len"), before, "compaction lost keys");
    for index in 0..100 {
        assert!(
            store.get(&format!("k{index:03}")).expect("get").is_some(),
            "key k{index:03} disappeared"
        );
    }
}
