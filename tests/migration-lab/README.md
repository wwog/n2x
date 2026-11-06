# Migration lab

一个真正使用 n2x 公共入口的本地网页测试服务。JSON、SQLite 与目录端口全部位于此测试
项目，不作为框架业务适配器发布。测试运行不访问项目外的业务数据。

## 启动

在仓库根目录执行：

```sh
cargo run -p n2x-migration-lab
# 不自动打开浏览器 / 更换端口：
cargo run -p n2x-migration-lab -- --no-open --port 8427
```

默认页面：<http://127.0.0.1:8427>，默认数据目录为本项目的 `data/`，已被 Git 忽略。
`--data /absolute/path` 可以指定独立测试目录。服务只监听 127.0.0.1，无生产部署功能。

## 版本和实际资源

| 边 | 行为 | 完成后 |
| --- | --- | --- |
| v0→v1 | 创建 users，导入 JSON，搬迁 a→b，核对后删除 a，校验 | users(id, first_name, last_name)，SQL user_version=1 |
| v1→v2 | 建 users_v2，分批合并姓名，发布新表并校验 | users(id, name)，SQL user_version=2 |
| v2→v3 | 创建姓名索引、校验 | idx_users_name，SQL user_version=3，行数据不变 |

v1 的 JSON 导入和目录搬迁是同一版本边内的不同 Step，全部完成后才提交 v1。
四条初始记录为 Ada Lovelace、Grace Hopper、Alan Turing、Linus Torvalds。
目录包含普通文件、嵌套文件与空目录。

```text
data/
  initialized             fixture 初始化标记
  users.json              初始数据，保留以供观察
  users.sqlite            真实数据库，含 _n2x_receipts 去重表
  a/                      初始目录；v1 校验后被删除
  b/                      迁移后的目录
  workspace/
    workspace.lock        跨进程独占锁
    state.kv              版本、计划、恢复、报告、事件
  observations.jsonl      每条命令后的版本与下一命令
  errors.jsonl            业务与请求错误
```

## 建议操作

1. 首次打开看到 v0。选 v1，点击“创建 / 恢复计划”。
2. “执行下一个步骤”可看到先建表、再分批导入、再搬目录。中途权威版本仍为 v0。
3. 完成后点击“快捷到 v2”，观察两列变为 name；点击 v3，观察新增索引且行数据一致。
4. v3 请求 v1，页面显示 `n2x.version.rollback_unsupported`，实际版本保持 v3。
5. 用“重置测试资源”回到 v0；直接“快捷到 v3”，观察日志包含 v1、v2、v3。
6. 再重置并快捷到 v1，注入下一次 v2 回滚，然后请求 v2。运行 Failed、权威仍 v1，
   users_v2 未插入数据；点击“继续失败运行”再执行，可以完成 v2。
7. 运行中“重开执行器 / 保留 KV”，再为原目标创建 / 恢复计划，观察断点继续。

重置只在用户明确点击并确认后删除指定测试数据目录。请勿将 `--data` 指向业务目录。
“继续失败运行”只创建继续命令，再用单步或快捷按钮执行；故障注入仅影响下一次 v2 写入。

直接请求 v3 是自动补齐路径，不是直接忽略 v1/v2。框架支持单独声明捷径边并使用
Route::Explicit 选择；本项目故意保留完整链路来检查每个项目不会遗漏。

## 本地 HTTP API

接口响应为 `{ ok, state }` 或 `{ ok: false, error, state }`。
业务步骤失败也会保存到 `state.run.last_error`、标准报告与 errors.jsonl。
所有操作串行执行，不提供外部服务或通知发送。

| 方法与路径 | 请求 | 用途 |
| --- | --- | --- |
| GET /api/state | 无 | 查询实际 SQL、目录、运行、事件、错误 |
| POST /api/start | {"target":"v1"} | 编译 / 恢复计划 |
| POST /api/tick | {} | 一条协议命令，包含持久化边界 |
| POST /api/step | {} | 到下一个步骤完成 / 版本提交 / 停止 |
| POST /api/run | {"target":"v3"} | 连续执行，补齐路径 |
| POST /api/resume | {} | 显式继续失败 / 暂停运行 |
| POST /api/fault | {} | 注入下一次 v2 写事务回滚 |
| POST /api/reopen | {} | 丢弃执行器、重新打开持久状态 |
| POST /api/reset | {} | 删除并重新生成测试资源 |

SQL 业务变更与 OperationId 凭证在同一 SQLite 事务提交；同步级别 FULL。
目录写入按内容幂等重放；清理前核对 a/b 内容。测试用 Reader 每批核对源摘要，在迁移
期间外部改动源会报错。工作区权威版本与 SQL user_version 分别展示，方便观察中间窗口。

## 自动测试

```sh
cargo test -p n2x-migration-lab
```

每个测试使用独立临时目录，避免改动网页的 data。覆盖逐版、直达 v3、反向请求、
事务回滚后继续、提交窗口重开、逐命令边界重开、暂停/取消和源视图变化。
逐命令边界测试会执行多轮真实持久 IO，耗时通常约一分钟。
