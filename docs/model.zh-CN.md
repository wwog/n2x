# 模型契约

本文对应当前已编译的 Rust 类型。模型格式版本为 `n2x_core::MODEL_VERSION = 1`。
计划编译、调度及 KV 持久化已经实现；本文中的“必须”也是自定义端口的接入契约。

## 1. 标识与版本

标识均为独立 String newtype，支持 `"name".into()`，不同种类不能隐式混用。
构造类型不会验证内容，计划编译和工作区加载边界必须检查空值、重复和格式。

| 类型 | 唯一性 / 作用域 | 含义 |
| --- | --- | --- |
| `ScopeId` | Workspace 中的资源实例 | 账号、数据库实例或目录数据集 |
| `Revision` | 此资源使用的版本谱系 | 不透明节点，不按数字大小比较 |
| `ManifestId` | 同一版本图唯一 | 一条有向迁移边 |
| `StepId` | 同一 Manifest 内唯一 | 稳定步骤 |
| `StepAddress` | ManifestId + StepId | 运行内的步骤地址 |
| `ActionRef` | ActionId + revision | 注册实现与行为修订 |
| `PortId` | 计划内唯一 | source、target 等逻辑入口 |
| `ResourceId` | 可辨识的实际资源 | 不包含连接密码，不随重连改变 |
| `RunId` | 全局唯一 | 一次运行，恢复沿用 |
| `OperationId` | 全局唯一 | 一次外部业务效果，重试沿用 |
| `CommitId` | 全局唯一 | 一次 Workspace 原子事务，重试沿用 |
| `CommandId` | epoch + sequence + attempt | 一次命令分发，不能代替操作去重 ID |

三类版本互相独立：用户数据 `Revision`、注册实现 `ActionRef.revision`、模型编码
`format_version`。Action 行为改变必须提高 revision。

## 2. 声明与计划

`Manifest { id, from, to, steps, metadata }` 描述版本边。`Step { id, kind, policy }` 定义
顺序步骤，不提供 Mode、before/after 隐式回调或平台特定分支。

| StepKind | 执行契约 | 成功条件 |
| --- | --- | --- |
| `Task(ActionCall)` | 一次显式副作用 | Receipt 被工作区确认 |
| `Transfer(Box<TransferSpec>)` | 标准数据管线 | 源终点及全部输出、隔离、状态确认 |
| `Check(ActionCall)` | 无副作用的检查 | CheckResult 成功并保存执行位置 |

ActionCall 的 ports 是资源依赖声明。Runtime 注入的实际资源必须与 Plan 绑定匹配。
Transfer 通过两个 StreamSpec 声明输入与输出，配置由适配器解释。

RunRequest 指定 RunId、Scope、目标、路径和策略。initial 只用于首次引导，不能覆盖
已有权威状态。Route::Unique 要求唯一合法路径；多路径必须选择 Route::Explicit。

编译后 PlanSnapshot 固定实际 from/to、连续路径、步骤声明与 StepContract、端口绑定、
运行策略、模型格式和摘要。StepKind 与 StepContract 必须匹配。metadata 只做展示，
已从计划类型剔除，不参与执行。Plan 中其余字段均应参与摘要。

Digest 为 SHA-256。`codec` 将模型序列化为递归键排序、无空白的 JSON UTF-8，
Plan/Commit 排除自身 digest 字段；Encoded.bytes 保持不透明。Blob 按原始字节计算摘要。
跨语言实现须匹配 Rust serde 的枚举、整数、Duration 和字节数组表达。
不得把 Rust Hash 或带凭证的连接字符串当作摘要。恢复不相容即拒绝。

## 3. 数据、源视图与批次

Encoded { codec, version, bytes } 保留应用类型的真实编码。Envelope 包含可选键、
属性和 Payload::Inline / Blob，不强制经过 JSON。

BlobRef 必须在进程重开后有效，包含长度和摘要。返回引用前 BlobStore 必须完成
持久化。不得用内存地址、临时文件描述符或没有保留保证的 URL 当作恢复引用。

Reader 的 ReadBatch 同时返回 next、eof 和 SourceState：

- 空 items 不代表结束，只有 eof 表示确定终点。
- 未结束时必须返回能前进的 next；eof 时 next 可以为空。
- SourceState 记录资源身份、一致性方案和快照/高水位凭证。
- 后续读取验证原视图；快照过期不能换新快照后沿用旧游标。
- BestEffort 必须显式选择，last_id 本身不能证明完整导出。
- 源目标相同时，必须验证写入不会破坏扫描稳定性。

TransformRequest 包含显式输入、参数和上批确认状态。TransformResult 包含输出、
过滤数量、拒绝记录及下一状态，支持 1:0、1:1、1:N。输出通过 write_limits 拆成多个
提交单位，不要求与读取批次一一对应。转换不得隐藏业务 IO。

第一版每次只处理一个输入批次、一个提交单位。批次及展开输出必须有界，大内容进入
BlobStore。若仍超过允许规模，必须报错或选择更细输入，不能无限增长 Vec。

## 4. 外部效果与工作区事务

| 类型 | 提交系统 | 结果 |
| --- | --- | --- |
| ApplyRequest | Writer / Task 的外部资源 | Receipt / OperationFailure |
| WorkspaceCommit | 状态、恢复材料与事件 | WorkspaceReceipt / N2xError |

ApplyRequest 通过 OperationContext 固定 Run、Step、Plan 与请求摘要。Receipt 必须匹配
OperationId 和请求摘要。同 ID 不同请求必须拒绝；重试只改变 Command attempt。

```text
持久保存 Intent 和请求材料
    -> 等待工作区达到所需持久级别
    -> Apply
    -> 校验 Receipt
    -> 原子保存 checkpoint、进度、事件并清除 pending
    -> 等待持久确认
    -> 下一项工作
```

OperationFailure 另外保存 ApplyState：

- NotApplied：权威确认不会有迟到提交，可按策略重试。
- Applied：凭证已知，不能重复产生业务效果。
- Unknown：可能全部或部分成功，需要安全重放或权威对账。

ReconcileResult 还包含 InFlight。“暂时未查到记录”不能当作权威 NotApplied。
ReplaySafety 支持 Unsafe、Idempotent、Deduplicated。去重必须和业务效果原子保存，
保留期覆盖允许的恢复期限；普通事务不能独自消除未知提交窗口。

默认 RequireAutomatic，编译时拒绝缺少安全重放及权威对账的步骤。AllowManual 允许
进入 NeedsAttention，不允许直接重试未知结果。

WorkspaceCommit 的全部 mutations 和去重凭证必须原子保存。flush 失败不代表没执行。
重试沿用原 CommitId，或重开核对现场；确认前停止新业务副作用。
`owner` 把提交收据归入对应运行的有界队列；收据只服务于重试去重，可以被裁剪。
Durability::Memory 不得标注为持久可恢复运行。

## 5. 恢复快照

RecoverySnapshot 是重开入口：格式版本、epoch、RunSnapshot、Checkpoint、最多一个
pending Intent 和开始时间。PlanSnapshot 按 RunId 单独保存，通过 digest 关联。

重开先取得独占权，验证模型、计划与资源，递增持久 epoch 后才分发命令。epoch 只过滤
迟到 Reply，不能阻止另一个进程继续写数据库，也不代替运行锁。

Checkpoint 保存 ExecutionPosition 和 confirmed 计数：

```text
Step(edge, step) -> AdvanceRevision(edge) -> 下一边或 Finished
```

TransferCheckpoint 保存确认 cursor、SourceState、转换状态、批次序号和 PreparedBatch。
PreparedBatch 冻结当前输出请求和顺序，保存连续 Receipt 前缀、拒绝记录、计数增量及
待采用 next_cursor/next_state。

必须维持：

1. receipts.len() 不超过 units.len()，每个凭证匹配对应请求。
2. Transfer 的 pending 对应第一个未确认单位。
3. Intent 请求及 Blob 不可修改或提前回收。
4. 全部单位确认且隔离记录持久保存后，才同时采用下一游标、转换状态和 delta。
5. 部分输出确认只能推进 Receipt 前缀，不能推进输入 Cursor。
6. eof 批次确认后清除 TransferCheckpoint，再推进到下一 Step 或 AdvanceRevision。
7. Task Receipt 确认与位置前进原子保存，避免恢复重复执行已完成 Task。

WorkspaceMutation 定义存储语义，物理 KV key 由实现决定。大记录可以 Blob 外置，但
单次语义事务不能因帧大小限制拆成多次可见提交。

## 6. 版本权威

当前模型采用 Workspace 的 ScopeState 作为版本权威。全部步骤确认后，同事务保存
RevisionCommit、新 ScopeState、下一位置与 RevisionAdvanced 事件。

ScopeState.active_run 与首次运行快照原子设置。暂停、失败或结果不明时保留，后续启动应
恢复此 Run 或报告冲突；不能仅因进程锁已释放就开启新 Run。完成或显式核对后安全终止
时才清除，清除动作同样与最终运行状态原子保存。

A -> B -> C 在第二条边失败时，current 为 B，日志保留到 C 的部分执行。失败、暂停和
取消均不表示回滚。取消保留活动运行，可显式 Resume；存在未知操作时先
NeedsAttention，不能宣称安全取消。

SQLite user_version 等外部字段可作为 Task 更新并由 Check 验证。当前没有第二个独立
StateStore 权威源；未来扩展需补齐版本端与工作区之间的对账协议。

## 7. 错误、状态与通知

N2xError 含 code、kind、origin、message、retry_hint 和装箱 ErrorContext。Context 保存
位置、提供者错误码和诊断。程序只依赖 code/kind，不解析 message。

RetryHint 不覆盖 ApplyState、恢复能力、次数和预算。无效数据默认 Stop；Quarantine
必须先保存原输入与错误，报告 CompletedWithRejections。

RunStatus 区分 Ready、Running、Recovering、Waiting、Paused、NeedsAttention、Completed、
Failed、Cancelled。RunSnapshot 区分 observed 与 confirmed。持久快照只记录确认进度，
不能把 UI 曾显示的未确认数值当成恢复位置。

- Notice 是临时观察事实，可合并，没有持久事件语义。
- StoredNotice 是关键事件，原子保存并确认后才分发。
- 事件 ID 为 RunId + sequence，从 1 开始严格递增。
- EventPage::Gap 表示历史已过期，附当前快照。
- NoticeAck 按订阅者确认连续事件前缀，接收者需对重发去重。
- `subscribe` 注册累计起点，`acknowledge` 确认连续前缀，`unsubscribe` 释放保护。
- StateWorkspace 默认每个运行保留 1000 条事件、全工作区 20_000 条，可调；未确认事件
  不能被裁剪，满时返回 `n2x.notice.backpressure`，ACK 或回收旧运行后重试原 CommitId。
  起点过期的订阅注册会拒绝。
- 事件传输由调用方拉取，通知消费者去重；框架不内置网络推送或后台投递进程。

## 8. 异步与平台

Runtime 使用对象安全的 PortFuture，不强制 Send，不绑定 Tokio，允许 Web 单线程 Future。
原生多线程驱动可在组合层增加约束或使用专用工作线程。

关闭默认功能可仅编译模型；`serde` 可独立启用，`engine` 包含 serde。当前不承诺 no_std。
本地 KV 仅通过 native-kv 引入；Web 组合使用 default-features=false, features=["engine"]。
核心与端口不包含平台数据库依赖、路径格式或 JS 类型。

## 9. 运行边界

版本向后请求不会猜测逆变换；没有声明反向边时返回 `n2x.version.rollback_unsupported`。
`Route::Unique` 自动经过所需版本，显式捷径必须声明对应 Manifest 并选择路径。

有限自动重试针对副作用；读、转换、检查错误直接持久失败，修复后显式 Resume。
重开不会重置自动重试次数；显式 Resume 对权威 NotApplied 开启新的人工重试预算。
Unknown 不允许借 Resume 绕过安全恢复契约。

Engine 同时限制转换输出条数与内联字节（使用 read_limits 作为 TransformRequest.limits），
再按 write_limits 拆提交单位。适配器也须在生成结果时遵守界限。
工作区事务收据默认每个运行只保留最近 128 条，更早的收据由实现裁剪；重复执行只会重复
幂等 mutation。已完成运行可经 `RetireRun` 回收计划、恢复快照、事件、隔离记录和收据，
报告默认保留为 RunId 墓碑。实现可以在删除前把这些材料写入外部归档；归档失败应中止
回收并保留现场。活动、暂停、失败、取消或仍有订阅者的运行不会被回收。
Native KV 容量耗尽时返回错误并保留现场。
