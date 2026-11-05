# n2x

**n2x — N-to-X Data Migration Framework**

n2x 是一个 Rust 编写的通用数据迁移框架，用统一抽象描述「从任意源 N 到任意目标 X」的迁移。
首个落地场景：iOS 原生逻辑迁移到 Rust 时，WCDB → Rust 内建 SQLite 的 schema 迁移。

## 设计原则

1. **配置即计划**：迁移由 `{from, to, mode, lifecycle}` 四元组描述，Manifest 可序列化、可审计。
2. **Sans-IO 调度**：核心调度器不执行 I/O、不读时钟、不依赖 async runtime，只消费 `Command`、产出 `Effect`。
3. **可中断可恢复**：上层传入缓存目录，框架通过 KV 记录 Plan、游标与已完成单元；恢复不重新规划。
4. **幂等优先**：目标写入与 checkpoint 提交遵循 at-least-once + 幂等契约；回滚靠备份，不靠反向转换。
