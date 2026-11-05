//! 点读 / 点写微基准。
//!
//! 用法：`cargo run --release -p n2x-kv --example bench`
//!
//! 这不是正式 benchmark 框架，只用于在目标设备上快速确认热路径量级：
//! 每个键 256 字节、10 万次写、10 万次读，默认不 fsync。

use std::hint::black_box;
use std::time::Instant;

use n2x_kv::{BackendPreference, Store, StoreOptions};

fn main() {
    let count: usize = std::env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(100_000);
    let value = vec![b'x'; 256];
    let keys: Vec<String> = (0..count).map(|index| format!("key-{index:08}")).collect();

    let dir = std::env::temp_dir().join(format!("chat-n2x-kv-bench-{}", std::process::id()));
    let path = dir.join("bench.kv");
    let _ = std::fs::remove_dir_all(&dir);

    let options = StoreOptions {
        max_bytes: 256 * 1024 * 1024,
        initial_bytes: 16 * 1024 * 1024,
        max_value_bytes: 64 * 1024,
        max_frame_bytes: 64 * 1024 * 1024,
        backend: BackendPreference::Auto,
        ..StoreOptions::default()
    };
    let store = Store::open(&path, options).expect("open store");
    let stats = store.stats().expect("stats");
    println!("backend={} keys={} value=256B", stats.backend, count);

    let started = Instant::now();
    for key in &keys {
        store.set(key, &value).expect("set");
    }
    let write_elapsed = started.elapsed();

    let started = Instant::now();
    let mut checksum = 0u64;
    for key in &keys {
        let read = store.get(key).expect("get").expect("value");
        checksum = checksum.wrapping_add(read[0] as u64);
    }
    let read_elapsed = started.elapsed();
    black_box(checksum);

    report("set", write_elapsed, count);
    report("get", read_elapsed, count);

    let started = Instant::now();
    store.compact().expect("compact");
    let compact_elapsed = started.elapsed();
    let stats = store.stats().expect("stats");
    println!(
        "compact={:?} entries={} data={}B dead={}B file={}B",
        compact_elapsed, stats.entries, stats.data_bytes, stats.dead_bytes, stats.file_bytes
    );

    store.flush().expect("flush");
    drop(store);
    let _ = std::fs::remove_dir_all(&dir);
}

fn report(label: &str, elapsed: std::time::Duration, count: usize) {
    let per_op = elapsed.as_secs_f64() / count as f64;
    println!(
        "{label}: {:.3?} total, {:.0} ns/op, {:.0} ops/s",
        elapsed,
        per_op * 1e9,
        count as f64 / elapsed.as_secs_f64()
    );
}
