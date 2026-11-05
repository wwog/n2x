//! 崩溃回归：在写入与压缩过程中反复 `SIGKILL` 子进程，校验重开后数据自洽。
//!
//! 默认忽略（需要派生进程并真实杀进程，耗时且依赖 Unix 信号）。运行方式：
//!
//! ```sh
//! cargo test -p n2x-kv --release --test crash -- --ignored --nocapture
//! ```
//!
//! 每个用例拉起一个写子进程，随机等待后 `kill -9`，再重开校验：
//! 索引里列出的每个键都必须可读，人工写入的键值必须自洽，恢复后必须能继续写。

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use n2x_kv::{BackendPreference, Mutation, Store, StoreOptions};

/// 子进程模式的环境变量：由父测试通过 `Command` 注入。
const CHILD_MODE: &str = "KV_n2x-kv_CRASH_CHILD";
const CHILD_PATH: &str = "KV_n2x-kv_CRASH_PATH";
const CHILD_BACKEND: &str = "KV_n2x-kv_CRASH_BACKEND";

fn options(backend: BackendPreference) -> StoreOptions {
    StoreOptions {
        backend,
        initial_bytes: 1024 * 1024,
        ..StoreOptions::default()
    }
}

/// 子进程：按模式持续写入或持续压缩，直到被父进程杀掉。
fn child_main() -> ! {
    let path = std::env::var(CHILD_PATH).expect("child path");
    let backend = match std::env::var(CHILD_BACKEND).as_deref() {
        Ok("buffered") => BackendPreference::Buffered,
        Ok("mmap") => BackendPreference::Mmap,
        _ => BackendPreference::Auto,
    };
    let mode = std::env::var(CHILD_MODE).expect("child mode");
    let store = Store::open(&path, options(backend)).expect("child open");

    match mode.as_str() {
        "write" => {
            let mut index = 0u64;
            loop {
                store
                    .set(
                        &format!("k{index:010}"),
                        format!("value-{index}").as_bytes(),
                    )
                    .expect("child set");
                if index % 64 == 63 {
                    store
                        .write(&[
                            Mutation::Set {
                                key: "batch-a",
                                value: format!("a-{index}").as_bytes(),
                            },
                            Mutation::Set {
                                key: "batch-b",
                                value: format!("b-{index}").as_bytes(),
                            },
                        ])
                        .expect("child batch");
                }
                index += 1;
            }
        }
        "compact" => {
            let bulk = vec![b'b'; 512];
            let mut round = 0u64;
            loop {
                store
                    .set(&format!("bulk-{:07}", round % 20_000), &bulk)
                    .expect("child bulk");
                store
                    .set("hot", format!("round-{round}").as_bytes())
                    .expect("child hot");
                if round % 32 == 31 {
                    store.compact().expect("child compact");
                }
                round += 1;
            }
        }
        other => panic!("unknown child mode {other}"),
    }
}

/// 父进程：派生自身为子进程，随机等待后 `kill -9`。
fn run_case(dir: &Path, mode: &str, backend: &str) {
    let path: PathBuf = dir.join(format!("{mode}-{backend}.kv"));
    let exe = std::env::current_exe().expect("current exe");

    for round in 0u32..3 {
        let mut child = Command::new(&exe)
            .arg("--exact")
            .arg("child_main_is_not_run_directly")
            .arg("--ignored")
            .arg("--nocapture")
            .env(CHILD_MODE, mode)
            .env(CHILD_PATH, &path)
            .env(CHILD_BACKEND, backend)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn child");

        // 让子进程先进入稳定循环，再打断；轮次不同等待时间不同，覆盖不同位置。
        std::thread::sleep(Duration::from_millis(120 + u64::from(round) * 70));
        child.kill().expect("kill child");
        let _ = child.wait();

        verify(&path, backend, mode, round);
    }
}

/// 重开后校验：列出的键可读、写入过的值自洽、恢复后仍可写。
fn verify(path: &Path, backend_name: &str, mode: &str, round: u32) {
    let backend = match backend_name {
        "buffered" => BackendPreference::Buffered,
        _ => BackendPreference::Mmap,
    };
    let store = Store::open(path, options(backend)).unwrap_or_else(|error| {
        panic!("{mode}/{backend_name} round {round}: reopen failed: {error}")
    });

    let keys = store.keys(None).expect("keys");
    for key in &keys {
        let value = store.get(key).unwrap_or_else(|error| {
            panic!("{mode}/{backend_name} round {round}: {key} unreadable: {error}")
        });
        assert!(
            value.is_some(),
            "{mode}/{backend_name} round {round}: {key} listed but missing"
        );
    }

    for key in &keys {
        if let Some(rest) = key.strip_prefix('k')
            && let Ok(number) = rest.parse::<u64>()
        {
            assert_eq!(
                store.get(key).expect("get"),
                Some(format!("value-{number}").into_bytes()),
                "{mode}/{backend_name} round {round}: value for {key} is inconsistent"
            );
        }
        if key == "batch-a" || key == "batch-b" {
            assert!(
                store
                    .get(key)
                    .expect("get")
                    .is_some_and(|value| !value.is_empty()),
                "{mode}/{backend_name} round {round}: {key} is empty"
            );
        }
    }

    store
        .set("after-recovery", b"ok")
        .expect("write after recovery");
    store.flush().expect("flush after recovery");
    println!(
        "{mode}/{backend_name} round {round}: recovered {} keys",
        keys.len()
    );
}

/// 该测试本体在父进程中为空；子进程通过环境变量在 `run_case` 启动的进程里执行。
/// 单独的 `#[ignore]` 用例保证 `cargo test -- --ignored` 能发现它。
#[test]
#[ignore = "spawns child processes and kills them with SIGKILL"]
fn child_main_is_not_run_directly() {
    if std::env::var(CHILD_MODE).is_ok() {
        child_main();
    }
}

#[test]
#[ignore = "spawns child processes and kills them with SIGKILL"]
fn killed_writer_recovers() {
    let dir = tempfile::tempdir().expect("tempdir");
    for backend in ["buffered", "mmap"] {
        run_case(dir.path(), "write", backend);
    }
}

#[test]
#[ignore = "spawns child processes and kills them with SIGKILL"]
fn killed_compactor_recovers() {
    let dir = tempfile::tempdir().expect("tempdir");
    for backend in ["buffered", "mmap"] {
        run_case(dir.path(), "compact", backend);
    }
}
