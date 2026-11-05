# n2x

**n2x — N-to-X Data Migration Framework**

n2x 是一个 Rust 编写的通用数据迁移框架，用统一抽象描述「从任意源 N 到任意目标 X」的迁移。

## 设计原则

1. **配置即计划**：由 manifest 做迁移唯一依据。

