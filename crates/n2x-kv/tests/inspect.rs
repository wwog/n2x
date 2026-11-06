use std::fs::OpenOptions;
use std::io::Write;

use n2x_kv::{KVError, Mutation, Store, StoreOptions, read_snapshot};

fn temp_file(dir: &tempfile::TempDir) -> std::path::PathBuf {
    dir.path().join("local.kv")
}

#[test]
fn snapshot_reports_live_entries_in_key_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = temp_file(&dir);
    {
        let store = Store::open(&path, StoreOptions::default()).expect("open");
        store.set("theme", b"dark").expect("set");
        store.set("theme", b"light").expect("overwrite");
        store
            .write(&[
                Mutation::Set {
                    key: "count",
                    value: b"42",
                },
                Mutation::Set {
                    key: "temp",
                    value: b"x",
                },
            ])
            .expect("batch");
        store.remove("temp").expect("remove");
        store.set("config", br#"{"size":14}"#).expect("set json");
    }

    let snapshot = read_snapshot(&path).expect("snapshot");
    let keys: Vec<&str> = snapshot
        .entries
        .iter()
        .map(|entry| entry.key.as_str())
        .collect();
    assert_eq!(keys, ["config", "count", "theme"]);

    assert_eq!(snapshot.get("theme").expect("theme").value, b"light");
    assert_eq!(snapshot.get("theme").expect("theme").updates, 2);
    assert_eq!(snapshot.get("count").expect("count").value, b"42");
    assert!(snapshot.get("temp").is_none());
    assert!(!snapshot.tail_truncated);
    assert!(snapshot.frames >= 4);
    assert!(snapshot.records >= 6);
    assert!(snapshot.valid_bytes <= snapshot.file_bytes);
}

#[test]
fn snapshot_reflects_clear() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = temp_file(&dir);
    {
        let store = Store::open(&path, StoreOptions::default()).expect("open");
        store.set("a", b"1").expect("set");
        store.clear().expect("clear");
        store.set("b", b"2").expect("set");
    }

    let snapshot = read_snapshot(&path).expect("snapshot");
    assert_eq!(snapshot.entries.len(), 1);
    assert_eq!(snapshot.entries[0].key, "b");
    assert_eq!(snapshot.entries[0].updates, 1);
}

#[test]
fn snapshot_is_read_only_and_keeps_valid_prefix_of_torn_tail() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = temp_file(&dir);
    {
        let store = Store::open(&path, StoreOptions::default()).expect("open");
        store.set("kept", b"value").expect("set");
    }
    let before = std::fs::metadata(&path).expect("metadata").len();

    // 模拟写到一半的帧头：非零但不是合法 magic。
    let mut file = OpenOptions::new().append(true).open(&path).expect("append");
    file.write_all(&[0xAB; 7]).expect("torn header");
    drop(file);

    let snapshot = read_snapshot(&path).expect("snapshot");
    assert_eq!(snapshot.get("kept").expect("kept").value, b"value");
    assert!(snapshot.tail_truncated);
    assert!(snapshot.valid_bytes > 64 && snapshot.valid_bytes < before);
    assert_eq!(
        std::fs::metadata(&path).expect("metadata").len(),
        before + 7,
        "read_snapshot must not write or truncate"
    );
}

#[test]
fn snapshot_rejects_empty_and_zeroed_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    let empty = dir.path().join("empty.kv");
    std::fs::write(&empty, b"").expect("write");
    assert!(matches!(
        read_snapshot(&empty),
        Err(KVError::Corrupt { .. })
    ));

    let zeroed = dir.path().join("zeroed.kv");
    std::fs::write(&zeroed, vec![0u8; 4096]).expect("write");
    assert!(matches!(
        read_snapshot(&zeroed),
        Err(KVError::Corrupt { .. })
    ));
}
