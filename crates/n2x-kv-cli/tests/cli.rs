use std::path::Path;
use std::process::{Command, Output};

use n2x_kv::{Mutation, Store, StoreOptions};

fn seed(path: &Path) {
    let store = Store::open(path, StoreOptions::default()).expect("open");
    store.set("theme", b"dark").expect("set");
    store.set("count", b"42").expect("set");
    store
        .set("config", br#"{"size":14,"flags":["a","b"]}"#)
        .expect("set");
    store.set("empty", b"").expect("set");
    store.set("binary", &[0x00, 0xFF, 0x10]).expect("set");
    store.set("temp", b"x").expect("set");
    store.remove("temp").expect("remove");
    store
        .write(&[
            Mutation::Set {
                key: "theme",
                value: b"light",
            },
            Mutation::Set {
                key: "batch",
                value: b"1",
            },
        ])
        .expect("batch");
}

fn run(path: &Path, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_n2x-kv"));
    command.arg(path).args(args);
    command.output().expect("run n2x-kv")
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("utf-8 stdout")
}

#[test]
fn table_shows_columns_and_live_values() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("local.kv");
    seed(&path);

    let output = run(&path, &["--stats"]);
    assert!(output.status.success());
    let text = stdout(&output);
    assert!(text.contains("KEY"));
    assert!(text.contains("TYPE"));
    assert!(text.contains("VALUE"));
    assert!(text.contains("light"));
    assert!(text.contains("json"));
    assert!(text.contains("binary"));
    assert!(!text.contains("temp"), "removed key must not appear");

    let stats = String::from_utf8(output.stderr).expect("utf-8 stderr");
    assert!(stats.contains("entries:  6"));
    assert!(stats.contains("tail:     clean"));
}

#[test]
fn kv_export_is_one_line_per_entry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("local.kv");
    seed(&path);

    let output = run(&path, &["--format", "kv"]);
    assert!(output.status.success());
    let text = stdout(&output);
    assert!(text.contains("theme = light"));
    assert!(text.contains("count = 42"));
    assert!(text.contains("config = {\"size\":14,\"flags\":[\"a\",\"b\"]}"));
    assert!(text.contains("binary = 0x00ff10"));
    assert_eq!(text.lines().count(), 6);
}

#[test]
fn json_export_embeds_structured_values() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("local.kv");
    seed(&path);

    let output = run(&path, &["--format", "json"]);
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).expect("json");
    let object = value.as_object().expect("object");
    assert_eq!(object["theme"], serde_json::json!("light"));
    // 标量保持字符串，只有对象/数组被嵌入。
    assert_eq!(object["count"], serde_json::json!("42"));
    assert_eq!(
        object["config"],
        serde_json::json!({"size": 14, "flags": ["a", "b"]})
    );
    assert_eq!(object["empty"], serde_json::json!(""));
    assert_eq!(
        object["binary"],
        serde_json::json!({"$binary_hex": "00ff10"})
    );
}

#[test]
fn prefix_filters_and_missing_exact_key_fails() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("local.kv");
    seed(&path);

    let output = run(&path, &["--format", "kv", "--prefix", "c"]);
    assert!(output.status.success());
    let text = stdout(&output);
    assert!(text.contains("count = 42"));
    assert!(text.contains("config ="));
    assert!(!text.contains("theme"));

    let output = run(&path, &["--key", "nope"]);
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("not found"));
}

#[test]
fn table_full_does_not_truncate() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("local.kv");
    let long = "x".repeat(120);
    {
        let store = Store::open(&path, StoreOptions::default()).expect("open");
        store.set("long", long.as_bytes()).expect("set");
    }

    let truncated = stdout(&run(&path, &[]));
    assert!(truncated.contains('…'));

    let full = stdout(&run(&path, &["--full"]));
    assert!(full.contains(&long));
}
