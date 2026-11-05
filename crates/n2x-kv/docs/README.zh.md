# n2x-kv

[English](./README.md) | 简体中文

进程内键值存储，三种语义一套 API：**mmap 追加日志文件存储**、**纯内存字节存储**、**纯内存动态类型存储**。

点读是一次哈希查找加一次值拷贝；点写是追加一帧日志、更新索引。默认不逐次 fsync，需要掉电级持久化时显式调用 `flush`。


## 三种存储

| | `Store` | `MemoryStore` | `AnyStore` |
| --- | --- | --- | --- |
| 载体 | 文件（mmap，失败降级为文件 IO） | 内存 | 内存 |
| 持久化 | 是 | 否 | 否 |
| 值类型 | `&[u8]` | `&[u8]` | 任意 `Any + Send + Sync` |
| 容量上限 | `max_bytes` | `max_bytes` | 无 |
| 关闭语义 | `close()` → `Closed` | `close()` → `Closed` | 随持有者析构 |
| 典型用途 | 跨启动的状态、缓存 | 会话级临时数据、导出/FFI 载荷 | 内核内部状态对象 |

三者的键空间彼此独立：`AnyStore` 里的 `"k"` 和 `MemoryStore` 里的 `"k"` 互不影响。

字节存储的核心 API 只认字节；`TypedStore` 是在它之上的一层**纯编码约定**（`String` / `i64` / `u64` / `f64` / `bool`，可选 JSON），不改变落盘格式，也不写类型标记。所以 `set_string(k, v)` 完全等价于 `set(k, v.as_bytes())`。

## 安装

```toml
[dependencies]
n2x-kv = "0.1"
```

开启 JSON 读写：

```toml
n2x-kv = { version = "0.1", features = ["serde"] }
serde = { version = "1", features = ["derive"] } # 示例里要用 derive
```

`serde` feature 只影响 `TypedStore::get_json` / `TypedStore::set_json`；基础存储本身不依赖序列化库，所以 `serde` 不会作为传递依赖帮你引入。

## 快速开始

### 文件存储

```rust
use n2x_kv::{Mutation, Store, StoreOptions};

fn main() -> Result<(), n2x_kv::KVError> {
    let store = Store::open("/tmp/demo.kv", StoreOptions::default())?;

    store.set("theme", b"dark")?;
    assert_eq!(store.get("theme")?, Some(b"dark".to_vec()));

    // 同一帧内生效：恢复时不会只看到一半
    store.write(&[
        Mutation::Set { key: "a", value: b"1" },
        Mutation::Set { key: "b", value: b"2" },
    ])?;

    assert_eq!(
        store.keys(None)?,
        vec!["a".to_string(), "b".to_string(), "theme".to_string()]
    );
    assert_eq!(store.keys(Some("t"))?, vec!["theme".to_string()]);

    store.flush()?;   // 持久化检查点：msync + fsync
    store.compact()?; // 把活记录重写进新文件，回收死数据
    store.close()?;   // 刷盘并释放该路径的进程内占用
    Ok(())
}
```

目录不存在时会自动创建。同一路径在本进程内只能打开一次，重复打开返回 `KVError::AlreadyOpen`；`close()` 或析构后可以重新打开。

### 内存存储

```rust
use n2x_kv::{MemoryOptions, MemoryStore};

let store = MemoryStore::new(MemoryOptions::default());
store.set("session", b"opaque-token")?;
assert_eq!(store.get("session")?, Some(b"opaque-token".to_vec()));
# Ok::<(), n2x_kv::KVError>(())
```

### 类型化读写

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

编码是公开契约：

| 类型 | 编码 |
| --- | --- |
| `String` | UTF-8 字节 |
| `i64` / `u64` | 8 字节大端 |
| `f64` | 8 字节大端 IEEE-754 |
| `bool` | 单字节 `0x00` / `0x01` |

读取是严格的：键不存在返回 `None`；长度不符、非法 UTF-8、非法布尔字节、坏 JSON 一律返回 `KVError::Decode`。不会猜类型，也不会把坏数据当成缺失——否则一次类型写错会表现为「数据悄悄丢了」。

开启 `serde` feature 后：

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

### 动态类型存储

```rust
use std::sync::Arc;

use n2x_kv::AnyStore;

struct Profile {
    id: u64,
    name: String,
}

let state = AnyStore::new();
state.set("profile", Profile { id: 7, name: "ada".into() });

// 共享同一份值，不要求 `Profile: Clone`
let profile: Arc<Profile> = state.get("profile").expect("profile");
assert_eq!(profile.id, 7);

// 类型不符按未命中处理，`type_of` 可以区分「没有」和「类型不对」
assert!(state.get::<String>("profile").is_none());
assert!(state.type_of("profile").is_some());
```

`AnyStore` 的结构体不需要 serde、不需要 `Clone`、没有容量上限，代价是只在进程内有效：不能落盘、不进 FFI。值类型必须 `Any + Send + Sync`，带借用的类型（`&'a str`、切片引用）存不进去，需要改成自持有形态。

## API 概览

### `Store`（文件）

| 方法 | 说明 |
| --- | --- |
| `open(path, options)` | 打开或创建；扫描日志重建索引，撕裂的尾帧会被截断 |
| `get` / `contains` | 读；不存在返回 `None`，空值返回 `Some(vec![])` |
| `set` / `remove` / `write` / `clear` | 写；`write` 是原子批量 |
| `keys(prefix)` | 按字典序排序的键；`prefix` 为 `None` 或空串返回全部 |
| `len` / `is_empty` / `stats` | 规模与统计快照 |
| `flush` | 持久化检查点 |
| `compact` | 立即压缩，回收死数据 |
| `close` | 刷盘、释放映射与路径占用 |

### `MemoryStore`（内存字节）

`new`、`get`、`contains`、`set`、`remove`、`write`、`clear`、`keys`、`len`、`is_empty`、`stats`、`close`。`flush` 是空操作，保留它是为了与文件存储共用调用形状。

### `TypedStore`（编码门面）

`get_bytes` / `set_bytes` 以及 `String`、`i64`、`u64`、`f64`、`bool` 的成对读写，开启 `serde` 后有 `get_json` / `set_json`。`Store` 和 `MemoryStore` 都实现了它。

### `AnyStore`（内存动态类型）

`get<T>`、`set<T>`、`get_str` / `set_str`、`get_u32`、`get_u64`、`get_i64`、`get_f64`、`get_bool`、`contains`、`type_of`、`remove`、`clear`、`len`、`is_empty`、`keys`。

## 配置

### `StoreOptions`

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `max_bytes` | 64 MiB | 文件长度上限，**不是**配额语义 |
| `initial_bytes` | 1 MiB | 新建或增容时的长度下限 |
| `max_key_bytes` | 8 KiB | 单键上限 |
| `max_value_bytes` | 16 MiB | 单值上限 |
| `max_frame_bytes` | 32 MiB | 单帧上限，也是批量写的上限 |
| `compact_at_percent` | `80` | 日志达到 `max_bytes` 的百分比后允许自动压缩；`100` 关闭该触发器 |
| `compact_key_versions` | `128` | 单键累计多少条历史记录后触发压缩；`0` 关闭 |
| `backend` | `Auto` | `Auto` / `Mmap` / `Buffered` |

`Store::open` 会先调用 `StoreOptions::validate()`，配置组合不自洽时直接返回 `InvalidOptions`，不会等到写下第一条记录才失败。

`backend` 的三个取值：

- `Auto`：优先 mmap；映射失败或首尾页不可访问时整体降级为 buffered，并通过 `stats().degraded` 暴露降级事实
- `Mmap`：强制 mmap，不可用时打开失败
- `Buffered`：定位读写的文件 IO，内存占用 O(1)，每次读一个系统调用

### `MemoryOptions`

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `max_bytes` | 32 MiB | 键 + 值总字节上限 |
| `max_key_bytes` | 8 KiB | 单键上限 |
| `max_value_bytes` | 16 MiB | 单值上限 |
| `max_batch_bytes` | 32 MiB | 单次批量上限，计数方式与文件存储的 `max_frame_bytes` 一致 |

批量的计量口径刻意包含 20 字节帧头，否则同一条批量写会在内存（测试）里成功、在文件（真机）上失败。

## 语义与保证

### 持久化

写入默认**不刷盘**。`flush()` 执行 `msync` + `fsync`，是调用方显式选择的检查点。

- 进程崩溃（如 `SIGKILL`）：通常仍可读到崩溃前已写入的数据
- 掉电：可能丢失从某个未持久化帧开始的后缀
- `flush()` 返回 `Err` 表示本次持久化未确认

不要把 `flush` 挂在每条小写入后面，那是 SQLite 的 `synchronous=FULL` 模式；按关键事务完成、退到后台、关闭前这几个检查点调用。

### 原子性

- `write(&[...])` 把整批记录编码进同一帧，帧带 CRC32。恢复时只接受完整且校验通过的帧，因此批量写是**全有或全无**
- `set` / `remove` / `clear` 都是只有一条记录的帧
- `clear` 先追加一个原子帧，然后尽力压缩回收空间

### 容量与满容量行为

`max_bytes` 是文件长度上限，统计里包含死数据。接近上限时：

- 普通写入会先尝试压缩；压缩后仍放不下才返回 `Full`
- `remove` 在连 remove 帧都放不下时，**改为压缩并丢弃该键**——效果相同且必然腾出空间
- 只含 `Remove` / `Clear` 的批量写，即使满容量也能成功，语义与「追加一帧再回收」一致
- 含 `Set` 的混合批量在满容量时原子失败，不会只应用一半

这样满容量的库至少还能靠删除和清空自救，不会永久只读。

### 压缩

压缩把活记录重写到 `<path>.compact`，`sync` 后原子 `rename` 覆盖正式文件，并同步父目录。新后端在 `rename` 之前就已构建完成，所以不存在「rename 成功但 reopen 失败」的窗口。

触发点：

1. 打开时，如果阈值已经越线
2. 每次写入后，如果 `compact_at_percent` 或 `compact_key_versions` 触发
3. 显式调用 `compact()`

按比例触发还要求压缩确有收益（打包后的字节数低于阈值）且数据量不小于 `min(1 MiB, 阈值)`，避免小库反复重写。压缩失败保留原文件和全部已提交写入，可以在稍后的空闲节点重试。

压缩中断会留下 `.compact` 文件，它不影响正确性；下一次 `compact` 开头会删掉它，打开时只记一条 `log::warn`。

### 并发

`Store`、`MemoryStore`、`AnyStore` 都是 `Send + Sync`：

- 读走读锁，多个读者并发
- 写走写锁，串行；锁内是顺序追加
- 索引更新成功后才推进 `valid_end`；索引更新失败会把实例置为毒化状态，后续操作返回 `Corrupt`，而不是继续服务可能错误的索引

写入期间会阻塞读取，这是一个进程内的 `RwLock`，不是 MVCC。适合读多写少的工作负载。

### 恢复

打开时顺序扫描日志：

| 遇到 | 处理 |
| --- | --- |
| 完整且 CRC 正确的帧 | 应用，推进 `valid_end` |
| 撕裂的尾帧、长度越界、CRC 不符、未知记录类型 | 丢弃该帧及之后全部内容，截断到 `valid_end` |
| 全零帧头，其后无数据 | 正常的预分配尾部，停止扫描 |
| 全零帧头，其后仍有非零字节 | 零洞或外部改写，告警并丢弃尾部 |
| 文件头 magic / 版本 / 头长不符 | `Corrupt`，不碰文件 |
| 文件头被清零但日志还在 | `Corrupt`，拒绝重新初始化 |

全零文件（含初始化中途崩溃留下的）会安全地重建；全零头 + 有数据的情况不会被误判成空库然后悄悄截断旧数据。

### 键与值

- 超长键在写入时返回 `InvalidKey`；在 `get` / `contains` 里只表现为查不到，不当作输入错误
- 空值是合法值，与「键不存在」严格区分
- 键是 `&str`，按字节比较和排序

## 文件格式

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
  crc32 u32        IEEE CRC-32，只覆盖 payload
  payload

payload record
  kind u8          1 = set, 2 = remove, 3 = clear
  key_len u32
  value_len u32
  key bytes
  value bytes
```

索引不落盘，每次打开从日志重建。值不进堆内存：索引只记录 `(value_offset, value_len)`，读取时直接从映射页拷出去，所以常驻内存与键数量相关，而不是与数据总量相关。

## 错误

`KVError` 是 `thiserror` 枚举，`Io` 变体会从 `std::io::Error` 自动转换：

| 变体 | 含义 |
| --- | --- |
| `Full` | 写入会超过容量上限 |
| `InvalidKey` / `ValueTooLarge` / `BatchTooLarge` | 输入超限 |
| `Decode` / `Encode` | 类型化读写时编解码失败 |
| `AlreadyOpen` | 同一路径已被本进程的另一个实例打开 |
| `Corrupt` | 文件内容无法按当前格式解析，或实例已毒化 |
| `InvalidOptions` | 配置组合不合法 |
| `Backend` | 底层后端无法继续工作 |
| `Closed` | 已关闭 |
| `Io` | 文件系统错误 |

`KVError::is_invalid_input()` 与 `KVError::is_full()` 用于在上层（例如 FFI 边界）分流可修正的输入错误与容量错误。

## 性能

热路径的量级应该在目标设备上实测，不要照搬别处的数字。仓库带了一个微基准：

```sh
cargo run --release -p n2x-kv --example bench
cargo run --release -p n2x-kv --example bench -- 1000000
```

默认 10 万键、256 字节值，依次做点写、点读、一次压缩，默认不 fsync。用 `stats().backend` 确认实际走的是 mmap 还是降级到了 buffered——降级路径每次读都是一个系统调用，量级差很多。

## 明确的非目标

- **没有跨进程文件锁**。多个进程打开同一个文件会各自维护 `valid_end` 并互相覆盖。需要多进程时请在宿主层加锁
- **没有范围扫描**。`keys()` 是全量收集后排序，`O(n log n)`；没有游标、没有 `range`、没有前缀迭代器
- **没有事务、MVCC 或回滚**。写入即追加，旧版本在压缩时回收；「撤销」要么再写一次旧值，要么用备份
- **没有后台线程**。压缩全部在调用线程完成，并持有写锁
- **`max_bytes` 不能防止掉电丢失**，它只限制文件长度
- mmap 路径下如果文件被外部截断，访问被截断的页会触发 `SIGBUS`。那是进程信号，不是 `Result` 能捕获的错误，只能由调用方保证不外部截断并做容量管理

## 测试

```sh
cargo test -p n2x-kv
cargo test -p n2x-kv --features serde
```

崩溃恢复用例会 fork 子进程并 `SIGKILL`，默认标记为 `#[ignore]`：

```sh
cargo test -p n2x-kv --release --test crash -- --ignored --nocapture
```

覆盖范围包括：mmap 与 buffered 的语义一致性、撕裂尾帧与零洞恢复、损坏帧整帧丢弃、满容量下的删除与清空、压缩估算与实际组包一致、压缩中断后的恢复、同路径重复打开、并发读写。

## License

MIT OR Apache-2.0，见仓库根目录的 [LICENSE-MIT](../../../LICENSE-MIT) 与 [LICENSE-APACHE](../../../LICENSE-APACHE)。
