# n2x

**N-to-X Data Migration Framework**

用同一套标准管线描述存储迁移、结构升级和数据转换。核心采用 sans-IO，所有数据库、
文件和平台交互交给运行时端口实现。

当前已固定模型、错误与通知协议、恢复快照和执行端口。计划编译器、调度器、Runner、
Workspace KV 适配和数据库适配器由后续实现。现有 `n2x-kv` 是可独立使用的存储实现。

## 结构

```text
crates/
  n2x-core/       声明、计划、数据、错误、通知、恢复及 sans-IO 协议
  n2x-runtime/    Reader、Writer、Action、Workspace、Blob、Clock、Lock 契约
  n2x-kv/         本地持久 KV 与内存存储
  n2x/            统一模型导出、runtime 入口、可选本地 KV
docs/
  architecture.zh-CN.md    分层、执行边界与设计决策
  model.zh-CN.md           已落地类型、字段关系和必须维持的不变量
  implementation.zh-CN.md  后续实现顺序与验收清单
```

依赖方向为 `n2x → n2x-runtime → n2x-core`。Runtime 的 `native-kv` feature 才引入本地 KV。
核心不依赖文件系统、数据库客户端或异步运行时。原 `n2x-scheduler` 已被 `n2x-runtime` 替代。

## 模型

```text
Scope: account/42

wcdb/3 ── Manifest ──> sqlite/1 ── Manifest ──> sqlite/2
              |
              +── Task → Transfer → Task → Check
```

- `Task`：一次可追踪的副作用，例如建表、执行 SQL、发布目录。
- `Transfer`：读取有界批次、纯转换、分单位提交、确认后推进游标。
- `Check`：只读验证，失败阻止版本推进。

版本边内顺序执行，整条边确认后才推进 Revision。`after` 是显式插入的 Task。
行为通过 `ActionRef { id, revision }` 注册，Manifest 不保存闭包或连接句柄。

```rust
use n2x::{ActionCall, ActionRef, Manifest, Step, StepKind};

let migration = Manifest {
    id: "create-store".into(),
    from: "empty".into(),
    to: "v1".into(),
    steps: vec![Step {
        id: "create".into(),
        kind: StepKind::Task(ActionCall {
            action: ActionRef { id: "store.create".into(), revision: 1 },
            ports: vec!["target".into()],
            config: None,
        }),
        policy: Default::default(),
    }],
    metadata: Default::default(),
};
```

完整声明见 [`manifest.rs`](crates/n2x-core/examples/manifest.rs)，示例不执行数据库操作。

## 使用与检查

通过 `n2x` 导入模型，通过 `n2x::runtime` 实现端口。默认启用可选的 `serde`；
序列化支持不等于已经定义跨语言持久化编码标准。

```toml
[dependencies]
n2x = { path = "path/to/n2x/crates/n2x" }
# 需要本地 KV 时启用 features = ["native-kv"]。
# 无需序列化时设置 default-features = false。
```

```sh
cargo run -p n2x-core --example manifest
cargo test --workspace --all-features
cargo check -p n2x --no-default-features
cargo doc --workspace --all-features --no-deps
```

## 文档入口

- [架构与边界](docs/architecture.zh-CN.md)
- [模型契约](docs/model.zh-CN.md)
- [实现清单](docs/implementation.zh-CN.md)
- [KV 使用说明](crates/n2x-kv/docs/README.zh.md)
