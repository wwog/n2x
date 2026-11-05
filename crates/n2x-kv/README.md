# n2x-kv

English | [简体中文](./docs/README.zh.md)

An in-process key-value store offering three semantics behind one API: an **append-only log on a memory-mapped file**, an **in-memory byte store**, and an **in-memory dynamically typed store**.

A point read is one hash lookup plus one value copy. A point write appends one frame to the log and updates the index. Writes are not fsynced by default; call `flush` explicitly when you need power-loss durability.

This crate is the foundation layer: it does not depend on `n2x-core`, knows nothing about sessions, and performs no FFI.

## The three stores

| | `Store` | `MemoryStore` | `AnyStore` |
| --- | --- | --- | --- |
| Carrier | File (mmap, falling back to file I/O) | Memory | Memory |
| Persistence | Yes | No | No |
| Value type | `&[u8]` | `&[u8]` | Any `Any + Send + Sync` |
| Capacity limit | `max_bytes` | `max_bytes` | None |
| Close semantics | `close()` → `Closed` | `close()` → `Closed` | Dropped with its owner |
| Typical use | State and caches that outlive a launch | Session-scoped scratch data, export/FFI payloads | Kernel-internal state objects |

The three key spaces are independent: `"k"` in `AnyStore` and `"k"` in `MemoryStore` never affect each other.

The byte stores speak bytes only. `TypedStore` is a **pure encoding convention** layered on top (`String` / `i64` / `u64` / `f64` / `bool`, plus optional JSON); it does not change the on-disk format and writes no type tags. `set_string(k, v)` is therefore exactly equivalent to `set(k, v.as_bytes())`.

## Installation

```toml
[dependencies]
n2x-kv = "0.1"
```

For JSON access:

```toml
n2x-kv = { version = "0.1", features = ["serde"] }
serde = { version = "1", features = ["derive"] } # needed by the examples
```

The `serde` feature only enables `TypedStore::get_json` / `TypedStore::set_json`. The base store has no serialization dependency, so `serde` is not pulled in transitively for you.

## Getting started

### File store

```rust
use n2x_kv::{Mutation, Store, StoreOptions};

fn main() -> Result<(), n2x_kv::KVError> {
    let store = Store::open("/tmp/demo.kv", StoreOptions::default())?;

    store.set("theme", b"dark")?;
    assert_eq!(store.get("theme")?, Some(b"dark".to_vec()));

    // Applied within a single frame: recovery never sees half of it
    store.write(&[
        Mutation::Set { key: "a", value: b"1" },
        Mutation::Set { key: "b", value: b"2" },
    ])?;

    assert_eq!(
        store.keys(None)?,
        vec!["a".to_string(), "b".to_string(), "theme".to_string()]
    );
    assert_eq!(store.keys(Some("t"))?, vec!["theme".to_string()]);

    store.flush()?;   // durability checkpoint: msync + fsync
    store.compact()?; // rewrite live records into a new file, reclaiming dead data
    store.close()?;   // sync and release the in-process claim on the path
    Ok(())
}
```

Missing parent directories are created automatically. A path can be opened only once per process; a second open returns `KVError::AlreadyOpen`. After `close()` or drop it can be opened again.

### In-memory store

```rust
use n2x_kv::{MemoryOptions, MemoryStore};

let store = MemoryStore::new(MemoryOptions::default());
store.set("session", b"opaque-token")?;
assert_eq!(store.get("session")?, Some(b"opaque-token".to_vec()));
# Ok::<(), n2x_kv::KVError>(())
```

### Typed access

```rust
use n2x_kv::{MemoryOptions, MemoryStore, TypedStore};

let store = MemoryStore::new(MemoryOptions::default());

store.set_string("theme", "dark")?;
store.set_i64("launch-count", 42)?;
store.set_bool("first-run", false)?;
store.set_f64("scale", 1.5)?;

assert_eq!(store.get_string("theme")?.as_deref(), Some("dark"));
assert_eq!(store.get_i64("launch-count")?, Some(42));
assert_eq!(store.get_bool("first-run")?, Some(false));
# Ok::<(), n2x_kv::KVError>(())
```

The encoding is a public contract:

| Type | Encoding |
| --- | --- |
| `String` | UTF-8 bytes |
| `i64` / `u64` | 8 bytes, big-endian |
| `f64` | 8 bytes, big-endian IEEE-754 |
| `bool` | A single byte, `0x00` / `0x01` |

Reads are strict: a missing key returns `None`, while a wrong length, invalid UTF-8, an invalid bool byte, or malformed JSON returns `KVError::Decode`. Types are never guessed and bad data is never treated as absent — otherwise one wrong write would look like "the data silently disappeared".

With the `serde` feature:

```rust
use n2x_kv::{MemoryOptions, MemoryStore, TypedStore};

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
struct Config {
    theme: String,
    size: u8,
}

let store = MemoryStore::new(MemoryOptions::default());
store.set_json("config", &Config { theme: "dark".into(), size: 14 })?;

let config: Option<Config> = store.get_json("config")?;
assert_eq!(config, Some(Config { theme: "dark".into(), size: 14 }));
# Ok::<(), n2x_kv::KVError>(())
```

### Dynamically typed store

```rust
use std::sync::Arc;

use n2x_kv::AnyStore;

struct Profile {
    id: u64,
    name: String,
}

let state = AnyStore::new();
state.set("profile", Profile { id: 7, name: "ada".into() });

// Shares the same value; `Profile: Clone` is not required
let profile: Arc<Profile> = state.get("profile").expect("profile");
assert_eq!(profile.id, 7);

// A type mismatch reads as a miss; `type_of` distinguishes it from absence
assert!(state.get::<String>("profile").is_none());
assert!(state.type_of("profile").is_some());
```

`AnyStore` needs no serde, no `Clone`, and has no capacity limit, at the cost of being process-local: it cannot be persisted and does not cross FFI. Values must be `Any + Send + Sync`; borrowed types (`&'a str`, slice references) cannot be stored and must be made self-owning first.

## API overview

### `Store` (file)

| Method | Description |
| --- | --- |
| `open(path, options)` | Open or create; scans the log to rebuild the index, truncating a torn tail |
| `get` / `contains` | Read; a missing key returns `None`, an empty value returns `Some(vec![])` |
| `set` / `remove` / `write` / `clear` | Write; `write` is the atomic batch form |
| `keys(prefix)` | Keys in lexicographic order; `None` or an empty prefix returns all |
| `len` / `is_empty` / `stats` | Size and statistics snapshot |
| `flush` | Durability checkpoint |
| `compact` | Compact now, reclaiming dead data |
| `close` | Sync, unmap, and release the path claim |

### `MemoryStore` (in-memory bytes)

`new`, `get`, `contains`, `set`, `remove`, `write`, `clear`, `keys`, `len`, `is_empty`, `stats`, `close`. `flush` is a no-op kept so the call shape matches the file store.

### `TypedStore` (encoding facade)

`get_bytes` / `set_bytes` plus paired accessors for `String`, `i64`, `u64`, `f64`, and `bool`; `get_json` / `set_json` with the `serde` feature. Implemented by both `Store` and `MemoryStore`.

### `AnyStore` (in-memory dynamic types)

`get<T>`, `set<T>`, `get_str` / `set_str`, `get_u32`, `get_u64`, `get_i64`, `get_f64`, `get_bool`, `contains`, `type_of`, `remove`, `clear`, `len`, `is_empty`, `keys`.

## Configuration

### `StoreOptions`

| Field | Default | Description |
| --- | --- | --- |
| `max_bytes` | 64 MiB | File length limit; **not** a quota semantic |
| `initial_bytes` | 1 MiB | Lower bound for the length on create or grow |
| `max_key_bytes` | 8 KiB | Per-key limit |
| `max_value_bytes` | 16 MiB | Per-value limit |
| `max_frame_bytes` | 32 MiB | Per-frame limit, also the batch write limit |
| `compact_at_percent` | `80` | Percentage of `max_bytes` at which automatic compaction becomes allowed; `100` disables this trigger |
| `compact_key_versions` | `128` | Historical records per key that trigger compaction; `0` disables |
| `backend` | `Auto` | `Auto` / `Mmap` / `Buffered` |

`Store::open` calls `StoreOptions::validate()` first, so an inconsistent combination returns `InvalidOptions` immediately instead of failing on the first write.

The three `backend` values:

- `Auto`: prefer mmap; if mapping fails or the first or last page is inaccessible, switch the whole store to buffered and expose the fact via `stats().degraded`
- `Mmap`: require mmap; opening fails if it is unavailable
- `Buffered`: positional file I/O with O(1) memory use, at the cost of one syscall per read

### `MemoryOptions`

| Field | Default | Description |
| --- | --- | --- |
| `max_bytes` | 32 MiB | Total key + value byte limit |
| `max_key_bytes` | 8 KiB | Per-key limit |
| `max_value_bytes` | 16 MiB | Per-value limit |
| `max_batch_bytes` | 32 MiB | Per-batch limit, counted the same way as the file store's `max_frame_bytes` |

Batch accounting deliberately includes the 20-byte frame header. Otherwise the same batch would succeed in memory (tests) and fail on a file (a real device).

## Semantics and guarantees

### Durability

Writes are **not** flushed by default. `flush()` performs `msync` + `fsync` and is a checkpoint the caller chooses explicitly.

- Process crash (e.g. `SIGKILL`): data written before the crash is normally still readable
- Power loss: the suffix starting at some unflushed frame may be lost
- `flush()` returning `Err` means this particular persistence attempt was not confirmed

Do not put `flush` behind every small write — that is SQLite's `synchronous=FULL` mode. Call it at a few checkpoints: when a critical transaction completes, when moving to the background, and before closing.

### Atomicity

- `write(&[...])` encodes the whole batch into a single frame carrying a CRC32. Recovery accepts only complete, checksum-valid frames, so a batch is **all-or-nothing**
- `set`, `remove`, and `clear` are single-record frames
- `clear` appends one atomic frame, then makes a best-effort attempt to compact and reclaim space

### Capacity and behavior when full

`max_bytes` is a file length limit that includes dead data. As it is approached:

- A normal write tries compaction first and returns `Full` only if the frame still does not fit
- When not even a remove frame fits, `remove` **compacts instead and drops the key** — the same effect, and it always frees space
- A batch containing only `Remove` / `Clear` succeeds even at full capacity, with the same semantics as "append a frame, then reclaim"
- A mixed batch containing `Set` fails atomically at full capacity and never applies halfway

A full store can therefore still rescue itself through deletes and clears instead of becoming permanently read-only.

### Compaction

Compaction rewrites live records into `<path>.compact`, syncs it, atomically `rename`s it over the real file, and syncs the parent directory. The new backend is built before the rename, so there is no window where "rename succeeded but reopen failed".

Triggers:

1. On open, if a threshold has already been crossed
2. After each write, if `compact_at_percent` or `compact_key_versions` fires
3. An explicit `compact()` call

Ratio-based triggering additionally requires the compaction to be worthwhile (the packed size is below the threshold) and the data to be at least `min(1 MiB, threshold)` in size, so small stores are not rewritten repeatedly. A failed compaction keeps the original file and every committed write; retry later at an idle point.

An interrupted compaction leaves a `.compact` file behind. It does not affect correctness; the next `compact` deletes it at the start, and opening merely logs one `log::warn`.

### Concurrency

`Store`, `MemoryStore`, and `AnyStore` are all `Send + Sync`:

- Reads take a read lock, so multiple readers run concurrently
- Writes take a write lock and are serialized; inside the lock they are sequential appends
- `valid_end` advances only after the index update succeeds; a failed index update poisons the instance so later operations return `Corrupt` instead of serving a possibly wrong index

Writers block readers. This is a single in-process `RwLock`, not MVCC, so it suits read-heavy workloads.

### Recovery

Opening scans the log sequentially:

| Encountered | Handling |
| --- | --- |
| A complete frame with a valid CRC | Applied; `valid_end` advances |
| Torn tail frame, out-of-range length, CRC mismatch, unknown record kind | The frame and everything after it are dropped; the file is truncated to `valid_end` |
| All-zero frame header with no data after it | Normal preallocated tail; scanning stops |
| All-zero frame header with non-zero data after it | Zero hole or external rewrite; warn and drop the tail |
| File header magic / version / header length mismatch | `Corrupt`; the file is left untouched |
| File header zeroed but log data follows | `Corrupt`; reinitialization is refused |

An all-zero file (including one left by a crash mid-initialization) is safely reinitialized. A zeroed header with data behind it is never mistaken for an empty store and silently truncated.

### Keys and values

- An over-long key returns `InvalidKey` on write; in `get` / `contains` it simply reads as missing and is not treated as an input error
- An empty value is a legal value, strictly distinguished from a missing key
- Keys are `&str`, compared and sorted by bytes

## File format

```text
file header (64B)
  magic[8] = "CHATKV01"
  version u32 = 1
  header_len u32 = 64
  reserved[48]

frame (20B header + payload)
  magic u32 = "KVF1"
  flags u32
  payload_len u32
  record_count u32
  crc32 u32        IEEE CRC-32 over the payload only
  payload

payload record
  kind u8          1 = set, 2 = remove, 3 = clear
  key_len u32
  value_len u32
  key bytes
  value bytes
```

The index is not persisted; it is rebuilt from the log on every open. Values never enter heap memory: the index stores only `(value_offset, value_len)` and reads copy straight out of the mapped page, so resident memory scales with the number of keys rather than the total data size.

## Errors

`KVError` is a `thiserror` enum; the `Io` variant converts automatically from `std::io::Error`:

| Variant | Meaning |
| --- | --- |
| `Full` | The write would exceed the capacity limit |
| `InvalidKey` / `ValueTooLarge` / `BatchTooLarge` | Input over a limit |
| `Decode` / `Encode` | Encoding or decoding failed in typed access |
| `AlreadyOpen` | The same path is already open in another instance in this process |
| `Corrupt` | The file cannot be parsed in the current format, or the instance is poisoned |
| `InvalidOptions` | The option combination is invalid |
| `Backend` | The underlying backend cannot continue |
| `Closed` | Already closed |
| `Io` | Filesystem error |

`KVError::is_invalid_input()` and `KVError::is_full()` let upper layers (an FFI boundary, for example) separate correctable input errors from capacity errors.

## Performance

Measure the hot path on your own target device rather than copying numbers from elsewhere. The repo ships a micro-benchmark:

```sh
cargo run --release -p n2x-kv --example bench
cargo run --release -p n2x-kv --example bench -- 1000000
```

It defaults to 100k keys with 256-byte values, then runs point writes, point reads, and one compaction, without fsync. Check `stats().backend` to confirm whether you are actually on mmap or have fallen back to buffered — the fallback path costs a syscall per read, which is a different order of magnitude.

## Non-goals

- **No cross-process file locking.** Multiple processes opening the same file each maintain their own `valid_end` and overwrite each other. Add locking in the host layer if you need it
- **No range scans.** `keys()` collects and sorts everything, `O(n log n)`; there is no cursor, no `range`, and no prefix iterator
- **No transactions, MVCC, or rollback.** Writes are appends and old versions are reclaimed during compaction; "undo" means writing the old value again or restoring from a backup
- **No background thread.** All compaction runs on the calling thread while holding the write lock
- **`max_bytes` does not protect against power loss**; it only bounds the file length
- On the mmap path, externally truncating the file makes accesses to the truncated pages raise `SIGBUS`. That is a process signal, not an error `Result` can capture; only the caller can guarantee no external truncation and manage capacity

## Testing

```sh
cargo test -p n2x-kv
cargo test -p n2x-kv --features serde
```

The crash-recovery cases fork a child process and `SIGKILL` it, so they are `#[ignore]`d by default:

```sh
cargo test -p n2x-kv --release --test crash -- --ignored --nocapture
```

Coverage includes: mmap and buffered semantic equivalence, torn-tail and zero-hole recovery, whole-frame discard of corrupt frames, deletes and clears on a full store, compaction estimation matching actual packing, recovery after an interrupted compaction, duplicate opens of the same path, and concurrent reads and writes.

## License

MIT OR Apache-2.0. See [LICENSE-MIT](../../../LICENSE-MIT) and [LICENSE-APACHE](../../../LICENSE-APACHE) at the repository root.
