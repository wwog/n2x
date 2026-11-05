# n2x

**n2x — N-to-X Data Migration Framework**

n2x 是一个 Rust 编写的通用数据迁移框架，用统一抽象描述「从任意源 N 到任意目标 X」的迁移。
首个落地场景：iOS 原生逻辑迁移到 Rust 时，WCDB（SQLCipher）→ Rust 内建 SQLite 的 schema 迁移，以及附件目录的文件迁移。

## 设计原则

1. **配置即计划**：迁移由 `{from, to, mode, lifecycle}` 四元组描述，Manifest 可序列化、可审计。
2. **Sans-IO 调度**：核心调度器不执行 I/O、不读时钟、不依赖 async runtime，只消费 `Command`、产出 `Effect`。
3. **可中断可恢复**：上层传入缓存目录，框架通过 KV 记录 Plan、游标与已完成单元；恢复不重新规划。
4. **Adapter 下沉**：WCDB、SQLite、Dir 都是 adapter；`n2x-scheduler` 只依赖 `n2x-core`。
5. **幂等优先**：目标写入与 checkpoint 提交遵循 at-least-once + 幂等契约；回滚靠备份，不靠反向转换。

## 架构文档

从 [docs/README.md](./docs/README.md) 开始。

| 文档 | 内容 |
| --- | --- |
| [01-architecture.md](./docs/01-architecture.md) | 分层、crate 划分、核心对象、不变量、设计决策 |
| [02-manifest.md](./docs/02-manifest.md) | `{from, to, mode, lifecycle}` 规格与校验规则 |
| [03-modes.md](./docs/03-modes.md) | 各 mode 语义、准入判据、扩展提案 |
| [04-scheduler-sans-io.md](./docs/04-scheduler-sans-io.md) | 调度器状态机、Effect/Command 协议、崩溃恢复 |
| [05-kv-store.md](./docs/05-kv-store.md) | 恢复 KV 的键空间、原子性、锁与损坏处理 |
| [06-ios-wcdb-to-sqlite.md](./docs/06-ios-wcdb-to-sqlite.md) | iOS 场景落地与验收 |
| [07-roadmap.md](./docs/07-roadmap.md) | 里程碑、crate 落地顺序、风险 |

## Members

```text
crates/
  n2x-core       纯数据模型与 trait（无 I/O）
  n2x-scheduler  sans-io 状态机
  n2x-kv         KV 后端（SQLite + WAL）
  n2x-sqlite     SQLite/WCDB 适配器
  n2x-fs         文件系统适配器
  n2x-driver     Effect 执行器（唯一做 I/O 的层）
  n2x-ios        iOS 桥接（feature gate）
  n2x-cli        本地演练与运维命令
```

Cargo workspace 当前只包含 `n2x-scheduler`；其余 crate 按 [07-roadmap.md](./docs/07-roadmap.md) §2 的顺序落地。
</content>
