# n2x

**N-to-X Data Migration Framework**

用同一套版本图和管线调度机处理跨存储迁移、数据转换、结构升级与文件搬迁。
核心采用 sans-IO；数据库、文件与平台交互由应用注册的端口实现。

已实现计划编译、顺序调度、持久恢复、标准错误、通知补读、可靠订阅，以及基于
`n2x-kv` 的原生工作区。库不附带 SQLite、MongoDB、文件搬迁等业务适配器。
具体 JSON / SQLite / 目录实现只属于集成测试项目。

## 先运行网页测试

```sh
cargo run -p n2x-migration-lab
```

服务监听 <http://127.0.0.1:8427> 并打开浏览器，所有生成资源保存在
[`tests/migration-lab/data/`](tests/migration-lab/data/)。可以逐步骤、逐命令执行，
也可以快捷到 v1、v2、v3。重开执行器保留已有数据与恢复记录。

| 版本边 | 同一管线中的行为 |
| --- | --- |
| v0 → v1 | 建表、JSON → SQL、目录 a → b、清理已验证源目录、检查 |
| v1 → v2 | 准备新表、first_name + last_name → name、发布模型、检查 |
| v2 → v3 | 添加 idx_users_name 索引、检查；行数据保持一致 |

直接请求 v3 会自动经过 v1 和 v2。没有声明反向边时，请求旧版本返回
`n2x.version.rollback_unsupported`。页面可注入一次真实 SQL 事务回滚，观察错误、
停留版本和继续执行。[测试服务说明](tests/migration-lab/README.md) 包含目录和 API。

## 分层与入口

```text
crates/
  n2x-core/       模型、计划编译器、规范摘要、sans-IO Engine
  n2x-runtime/    端口契约、Registry、Migrator、StateWorkspace、原生基础设施
  n2x-kv/         可独立使用的持久 KV 与内存存储
  n2x-kv-cli/     KV 文件查看器（表格 / key = value / JSON 导出）
  n2x/            统一导出层
```

依赖方向为 `n2x → n2x-runtime → n2x-core`，Runtime 的 `native-kv` 才引入本地 KV。
核心不读取文件、不连接数据库、不取得系统时间，也不依赖异步执行器。

- `Manifest`：一条版本边；同一版本中的多个迁移项目成为顺序 `Step`。
- `Task`：一次有恢复契约的副作用，DDL、发布、清理都显式声明。
- `Transfer`：读批次 → 纯转换 → 分单位写入 → 整批确认后推进游标。
- `Check`：只读校验，失败阻止版本推进。

`ActionRef { id, revision }` 引用注册行为，声明中不保存闭包或连接。
[`manifest.rs`](crates/n2x-core/examples/manifest.rs) 展示声明；
[`migration-lab/src/lib.rs`](tests/migration-lab/src/lib.rs) 展示完整接入。

原生应用使用 `Migrator::open_native(state_dir, manifests, registry)`，它先取得
独占锁再打开 KV。`start` 编译或恢复，`tick` 执行一条命令，`run` 连续执行；
`snapshot / report / events_after` 查询，`control` 暂停、继续、取消或人工核对。
只有整条版本边确认完成，权威版本才推进。失败与取消不会自动撤销已完成的业务效果。

工作区默认每个运行保留最近 128 条提交收据，事件每个运行 1000 条、整个工作区
20000 条。已完成运行在读取或导出报告后调用 `retire_run(CommitId, RunId, keep_report)`
回收计划、恢复快照、事件、隔离记录和收据；报告默认保留为 RunId 墓碑。
`open_native` 默认在删除前把同一批材料写成 `state_dir/archive/*.ndjson`，归档失败则
保留 KV 现场。

## 功能开关

```toml
[dependencies]
n2x = { path = "path/to/n2x/crates/n2x" } # 默认 engine + native-kv
# 平台自接 IO（例如 Web），仅启用通用调度与运行时：
# n2x = { path = "...", default-features = false, features = ["engine"] }
# 仅模型：default-features = false；serde 可单独启用。
```

WASM 应用自行实现 Workspace、Clock、Lock 和业务端口，异步接口不强制 Send。
当前不承诺 no_std，也不内置浏览器存储适配器。

## 检查与文档

```sh
cargo test --workspace --all-features
cargo clippy -p n2x-core -p n2x-runtime -p n2x -p n2x-migration-lab --all-features --all-targets -- -D warnings
cargo check -p n2x --no-default-features
cargo check -p n2x --no-default-features --features engine
cargo doc --workspace --all-features --no-deps
```

- [官网（静态）](site/index.html)
- [架构与设计边界](docs/architecture.zh-CN.md)
- [模型契约](docs/model.zh-CN.md)
- [实现与接入说明](docs/implementation.zh-CN.md)
- [网页集成测试](tests/migration-lab/README.md)
- [KV 使用说明](crates/n2x-kv/docs/README.zh.md)
- [KV 文件查看器](crates/n2x-kv-cli/README.md)
