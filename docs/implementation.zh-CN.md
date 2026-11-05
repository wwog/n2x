# 后续实现清单

当前是可编译模型和端口，没有空壳 Runner 或 todo 实现。具体接口以
[模型契约](model.zh-CN.md) 和 rustdoc 为准。

## 1. 模块落点

| 落点 | 已有 | 后续工作 |
| --- | --- | --- |
| n2x-core::manifest | 声明模型 | 声明校验 |
| n2x-core::plan | 快照、路径和能力 | 路径搜索、注册解析、计划编译 |
| n2x-core::protocol | PipelineEngine、命令、结果 | 纯状态机 |
| n2x-core::workspace | 恢复和事务模型 | 由 Runtime 落盘 |
| n2x-runtime::ports | 执行端口 trait | 注册表、命令分发、平台实现 |
| n2x-runtime | feature 与依赖边界 | 工作区、通知、锁、驱动 |
| n2x-kv | 现有持久存储 | 通过 Runtime 适配 |
| n2x | 统一导出 | 最后实现 Builder、Migrator、RunHandle |

## 2. 计划编译器

执行副作用前完成：

- 校验 ID、重复、图、路径和显式 route 连续性；initial 不覆盖已有状态。
- 检查 ScopeState.active_run，防止新运行忽略旧运行的待确认或部分效果。
- 验证 Action 修订、资源身份、编码和配置，固定具体调用的能力。
- 检查源一致性、游标恢复、单位大小、同库读写条件和恢复策略。
- 检查重试参数、持久级别和去重保留期；确定允许的恢复期限。
- 决定规范编码及版本，计算 SHA-256，保存 PlanSnapshot。

第一版建议普通升级图无环。from == to 可使用空计划，但仍要验证资源和权威状态。

## 3. Engine

先实现顺序状态机：初始化 -> Task/Transfer/Check -> AdvanceRevision -> 下一边 -> 完成。
业务失败通过 Transition 保存运行状态；输入协议无效才返回方法 Err。

每次最多一条命令在途；Pause/Cancel 在安全边界实施。实现有限退避、预算、Clock/WakeAt
输入和持久 epoch。本进程耗时使用单调计时，Unix 时间用于日志和跨重开调度，防止时间
倒退制造无限重试。

## 4. Workspace / Native

先固定状态编码与保留键空间，再适配 n2x-kv。一个 Workspace 一个写者，加载和修改前
取得所有权，直到在途操作结束。使用独立锁文件，避免 KV compact 替换 inode 破坏锁。

```text
state_dir/
  workspace.lock
  state.kv
  spool/           恢复必需，不能随意删除

cache_dir/         可以清空
```

适配时必须处理：

- 一个 WorkspaceCommit 对应一个原子 write，去重凭证也在其中。
- Durable 请求调用 flush，成功后才能返回相应 WorkspaceReceipt。
- 单帧放不下时提前拒绝或外置内容，不能拆开语义事务。
- flush 失败后停止新副作用，沿用原 CommitId 重试或核对现场。
- 新文件、spool、重命名及必要目录项的持久化。
- 容量预算；不能为了腾空间删除 pending 或尚有引用的 Blob。
- 原子完成整批时保存隔离记录，再移除 PreparedBatch 暂存副本。
- 分别维护恢复日志、事件历史、隔离、spool、cache 的保留规则。
- 同步 KV IO 的平台驱动或工作线程，不把阻塞行为引入核心。

当前 KV 没有跨进程锁或分布式 CAS，不能用 get + set 模拟抢锁。

## 5. 端口 / Registry

注册 key 为 ActionRef 或 PortId，验证实际资源与计划匹配。Task/Check 的资源由组合层
注入，不能私自连接声明之外的资源。

推荐实现顺序：

1. 假 Reader/Writer/Task，精确模拟未知结果与中断。
2. 文件夹迁移：Blob 分块、单文件提交、清单校验。
3. SQLite：DDL、同事务业务写入与去重表、回填、校验。
4. WCDB：旧接口 Reader，复用 SQLite 输出与运行管理。
5. Web：IndexedDB Workspace、单线程 Future、跨上下文所有权。

无法原子提交多条输出时，Writer 可以只接受单条单位，或提供可恢复的部分结果收敛。
“支持事务”不能替代 OperationId 去重。发布、切换业务入口、清理旧数据都显式建模。

## 6. 通知 / SDK

先实现 snapshot/watch、关键事件补读，再按需实现可靠 outbox。UI 可以合并进度，
过期历史返回 Gap。可靠投递需要容量预算、背压、ACK 和重发去重。

Observer 不控制执行结果，必需行为用 Step，控制用 Control。SDK 最后组合初始化、
启动/恢复、默认策略、句柄和查询，让业务无需自建版本表、重试队列和 checkpoint。

## 7. 故障验收

| 中断窗口 / 条件 | 必须验证 |
| --- | --- |
| Intent 持久化前 | 未发送对应业务操作 |
| Intent 后、输出回复前 | 相同 ID 安全重放或权威对账 |
| 输出提交后、checkpoint 前 | 不重复产生业务效果 |
| 部分输出单位完成 | 不重新生成输出，不提前推进输入游标 |
| 全过滤或全隔离批次 | 无输出也能正确持久推进 |
| write 成功、flush 失败 | 不依赖未确认状态继续 |
| Workspace Reply 丢失 | 同 CommitId 不重复计数和事件 |
| 版本推进前后 | current 与完整边提交一致 |
| 通知发送或 ACK 前 | 可补读、去重重发 |
| 快照过期或资源换绑 | 明确停止，不沿用无效游标 |
| KV 满、Blob 丢失或摘要不符 | 不删除现场，不误报成功 |
| 旧 epoch Reply | 不推进当前运行 |
| 取消时仍有未知结果 | 先进入待处理，不误报安全取消 |

当前模型测试验证声明与序列化保真，不声称已验证上述运行时行为。
