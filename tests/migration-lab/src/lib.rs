//! n2x 的可观察集成测试服务，具体业务端口仅属于测试项目。
pub mod fixtures;
use fixtures::*;
use n2x::runtime::*;
use n2x::*;
use serde_json::{Value, json};
use std::{
    cell::Cell,
    fs,
    io::Write,
    path::{Path, PathBuf},
    rc::Rc,
    time::{SystemTime, UNIX_EPOCH},
};

fn step(id: &str, kind: StepKind) -> Step {
    Step {
        id: id.into(),
        kind,
        policy: Default::default(),
    }
}
fn call(id: &str, ports: &[&str]) -> ActionCall {
    ActionCall {
        action: action(id),
        ports: ports.iter().map(|id| (*id).into()).collect(),
        config: None,
    }
}
fn transfer(source: &str, input: &str, target: &str, output: &str, transform: bool) -> StepKind {
    let limits = BatchLimits {
        max_items: std::num::NonZeroU32::new(2).unwrap(),
        ..Default::default()
    };
    StepKind::Transfer(Box::new(TransferSpec {
        read: StreamSpec {
            port: source.into(),
            stream: input.into(),
            config: None,
        },
        write: StreamSpec {
            port: target.into(),
            stream: output.into(),
            config: None,
        },
        transform: transform.then(|| TransformSpec {
            action: action("users.merge_names"),
            config: None,
        }),
        consistency: ReadConsistency::Frozen,
        read_limits: limits.clone(),
        write_limits: limits,
        invalid_data: Default::default(),
    }))
}
/// 数据导入、同版本目录迁移、模型变更、索引变更共享一张版本图。
pub fn manifests() -> Vec<Manifest> {
    vec![
        Manifest {
            id: "v1-import".into(),
            from: "v0".into(),
            to: "v1".into(),
            metadata: Default::default(),
            steps: vec![
                step(
                    "create-users",
                    StepKind::Task(call("sql.create_v1", &["sql"])),
                ),
                step(
                    "json-to-sql",
                    transfer("json", "users", "sql", "users", false),
                ),
                step(
                    "a-to-b",
                    transfer("files-a", "entries", "files-b", "entries", false),
                ),
                step(
                    "remove-a",
                    StepKind::Task(call("files.remove_source", &["files-a", "files-b"])),
                ),
                step(
                    "verify-files",
                    StepKind::Check(call("files.verify", &["files-b"])),
                ),
                step("mark-sql", StepKind::Task(call("sql.mark_v1", &["sql"]))),
                step(
                    "verify-users",
                    StepKind::Check(call("sql.verify_v1", &["sql"])),
                ),
            ],
        },
        Manifest {
            id: "v2-model".into(),
            from: "v1".into(),
            to: "v2".into(),
            metadata: Default::default(),
            steps: vec![
                step(
                    "prepare-model",
                    StepKind::Task(call("sql.prepare_v2", &["sql"])),
                ),
                step(
                    "merge-names",
                    transfer("sql", "users", "sql", "users_v2", true),
                ),
                step(
                    "publish-model",
                    StepKind::Task(call("sql.publish_v2", &["sql"])),
                ),
                step(
                    "verify-model",
                    StepKind::Check(call("sql.verify_v2", &["sql"])),
                ),
            ],
        },
        Manifest {
            id: "v3-index".into(),
            from: "v2".into(),
            to: "v3".into(),
            metadata: Default::default(),
            steps: vec![
                step(
                    "create-name-index",
                    StepKind::Task(call("sql.index_v3", &["sql"])),
                ),
                step(
                    "verify-index",
                    StepKind::Check(call("sql.verify_v3", &["sql"])),
                ),
            ],
        },
    ]
}

/// 浏览器和自动化测试共享的测试应用。
pub struct Lab {
    pub root: PathBuf,
    migrator: Migrator,
    pub fault: Rc<Cell<bool>>,
    pub errors: Vec<N2xError>,
    serial: u64,
}
impl Lab {
    /// 在指定数据目录打开或初始化，已有资源绝不自动重置。
    pub fn open(root: impl AsRef<Path>) -> Result<Self, N2xError> {
        let root = std::path::absolute(root).map_err(io)?;
        fs::create_dir_all(&root).map_err(io)?;
        if !root.join("initialized").exists() {
            seed(&root)?;
        }
        let errors = if root.join("errors.jsonl").exists() {
            fs::read_to_string(root.join("errors.jsonl"))
                .map_err(io)?
                .lines()
                .filter_map(|line| serde_json::from_str(line).ok())
                .collect()
        } else {
            vec![]
        };
        let fault = Rc::new(Cell::new(false));
        let migrator = make_migrator(&root, fault.clone())?;
        Ok(Self {
            root,
            migrator,
            fault,
            errors,
            serial: 0,
        })
    }
    /// 请求目标版本；自动补齐版本路径，活动运行优先恢复。
    pub async fn start(&mut self, target: &str) -> Result<Value, N2xError> {
        self.serial += 1;
        let run = format!(
            "lab-{}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            self.serial
        );
        let result = self
            .migrator
            .start(RunRequest {
                run: run.into(),
                scope: "demo".into(),
                initial: Some("v0".into()),
                target: target.into(),
                route: Route::Unique,
                policy: Default::default(),
            })
            .await;
        if let Err(error) = result {
            self.record_error(error.clone())?;
            return Err(error);
        }
        self.observe("start").await
    }
    /// 执行一条框架命令。
    pub async fn tick(&mut self) -> Result<Value, N2xError> {
        if let Err(error) = self.migrator.tick().await {
            self.record_error(error.clone())?;
            return Err(error);
        }
        if let Some(error) = self
            .migrator
            .snapshot()
            .filter(|state| matches!(state.status, RunStatus::Failed | RunStatus::NeedsAttention))
            .and_then(|state| state.last_error.clone())
            && self.errors.last() != Some(&error)
        {
            self.record_error(error)?;
        }
        self.observe("tick").await
    }
    /// 运行到下一个业务步骤完成、版本提交或停止，便于观察中间状态。
    pub async fn step(&mut self) -> Result<Value, N2xError> {
        for _ in 0..1000 {
            if self.migrator.pending_command().is_none() {
                break;
            }
            self.tick().await?;
            let notices = self.migrator.drain_notices();
            if notices.iter().any(|notice| {
                matches!(
                    notice.notice.kind,
                    NoticeKind::StepCompleted(_)
                        | NoticeKind::RevisionAdvanced { .. }
                        | NoticeKind::RunFailed(_)
                        | NoticeKind::RunNeedsAttention(_)
                        | NoticeKind::RunCompleted(_)
                )
            }) {
                break;
            }
        }
        self.state().await
    }
    /// 快捷执行至目标（保留失败报告，不跳过失败步骤）。
    pub async fn run_to(&mut self, target: &str) -> Result<Value, N2xError> {
        if self.migrator.pending_command().is_none() {
            self.start(target).await?;
        } else if self
            .migrator
            .snapshot()
            .is_none_or(|state| state.target.0 != target)
        {
            return Err(err(
                "lab.run.active",
                "请先完成或恢复当前目标，再请求其他版本",
            ));
        }
        for _ in 0..10000 {
            if self.migrator.pending_command().is_none() {
                break;
            }
            self.tick().await?;
        }
        self.state().await
    }
    /// 通过控制通道重试已修正的运行。
    pub async fn resume(&mut self) -> Result<Value, N2xError> {
        self.migrator.control(Control::Resume)?;
        self.observe("resume").await
    }
    /// 标准暂停与取消控制，测试页面和集成测试可共用。
    pub fn control(&mut self, control: Control) -> Result<(), N2xError> {
        self.migrator.control(control)
    }
    /// 丢弃当前内存执行器，下一次 start 从 KV 重开；不重置任何数据。
    pub fn reopen(self) -> Result<Self, N2xError> {
        let root = self.root.clone();
        drop(self);
        Self::open(root)
    }
    /// 状态包含真实 SQLite 和文件内容，以及框架的持久权威版本。
    pub async fn state(&mut self) -> Result<Value, N2xError> {
        let scope = self.migrator.scope("demo".into()).await?;
        let executor_loaded = self.migrator.snapshot().is_some();
        let snapshot = if let Some(snapshot) = self.migrator.snapshot() {
            Some(snapshot.clone())
        } else if let Some(active) = scope.as_ref().and_then(|scope| scope.active_run.clone()) {
            self.migrator.recovery(active).await?.map(|state| state.run)
        } else {
            None
        };
        let events = if let Some(snapshot) = &snapshot {
            Some(
                self.migrator
                    .events_after(snapshot.run.clone(), 0, 1000)
                    .await?,
            )
        } else {
            None
        };
        let connection = connect(&self.root)?;
        let schema: Vec<Value> = {
            let mut statement=connection.prepare("SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name").map_err(io)?;
            statement.query_map([],|row|Ok(json!({"type":row.get::<_,String>(0)?,"name":row.get::<_,String>(1)?,"sql":row.get::<_,Option<String>>(2)?}))).map_err(io)?.collect::<Result<_,_>>().map_err(io)?
        };
        let mut tables = serde_json::Map::new();
        for table in ["users", "users_v2"] {
            if schema
                .iter()
                .any(|item| item["name"] == table && item["type"] == "table")
            {
                let mut statement = connection
                    .prepare(&format!("SELECT * FROM {table} ORDER BY id"))
                    .map_err(io)?;
                let columns = statement
                    .column_names()
                    .into_iter()
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                let rows = statement
                    .query_map([], |row| {
                        let mut value = serde_json::Map::new();
                        for (index, name) in columns.iter().enumerate() {
                            let cell = match row.get_ref(index)? {
                                rusqlite::types::ValueRef::Integer(value) => json!(value),
                                rusqlite::types::ValueRef::Text(value) => {
                                    json!(String::from_utf8_lossy(value))
                                }
                                _ => Value::Null,
                            };
                            value.insert(name.clone(), cell);
                        }
                        Ok(Value::Object(value))
                    })
                    .map_err(io)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(io)?;
                tables.insert(table.into(), json!({"columns":columns,"rows":rows}));
            }
        }
        let sql_version: i64 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(io)?;
        Ok(
            json!({"data_dir":self.root,"version":scope.as_ref().map(|scope|scope.current.0.clone()).unwrap_or("v0".into()),"scope":scope,"run":snapshot,"executor_loaded":executor_loaded,"next_command":self.migrator.pending_command(),"events":events,"schema":schema,"tables":tables,"sql_version":sql_version,"files":{"a_exists":self.root.join("a").exists(),"a":files(&self.root.join("a"))?,"b":files(&self.root.join("b"))?},"errors":self.errors,"fault_armed":self.fault.get()}),
        )
    }
    fn record_error(&mut self, error: N2xError) -> Result<(), N2xError> {
        self.errors.push(error.clone());
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join("errors.jsonl"))
            .map_err(io)?;
        writeln!(file, "{}", serde_json::to_string(&error).map_err(io)?).map_err(io)
    }
    async fn observe(&mut self, event: &str) -> Result<Value, N2xError> {
        let state = self.state().await?;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join("observations.jsonl"))
            .map_err(io)?;
        writeln!(file,"{}",json!({"event":event,"version":state["version"],"run":state["run"],"next_command":state["next_command"]})).map_err(io)?;
        Ok(state)
    }
}

fn make_migrator(root: &Path, fault: Rc<Cell<bool>>) -> Result<Migrator, N2xError> {
    let mut registry = Registry::default();
    for (port, mode) in [("json", "json"), ("sql", "sql"), ("files-a", "files-in")] {
        registry.readers.insert(
            port.into(),
            Box::new(LabPort {
                root: root.to_owned(),
                port,
                mode,
                fault: fault.clone(),
            }),
        );
    }
    for (port, mode) in [("sql", "sql"), ("files-b", "files-out")] {
        registry.writers.insert(
            port.into(),
            Box::new(LabPort {
                root: root.to_owned(),
                port,
                mode,
                fault: fault.clone(),
            }),
        );
    }
    for id in [
        "sql.create_v1",
        "sql.mark_v1",
        "sql.prepare_v2",
        "sql.publish_v2",
        "sql.index_v3",
        "files.remove_source",
    ] {
        registry.tasks.insert(
            action(id),
            Box::new(LabAction {
                root: root.to_owned(),
                id,
            }),
        );
    }
    for id in [
        "sql.verify_v1",
        "sql.verify_v2",
        "sql.verify_v3",
        "files.verify",
    ] {
        registry.checks.insert(
            action(id),
            Box::new(LabAction {
                root: root.to_owned(),
                id,
            }),
        );
    }
    registry
        .transforms
        .insert(action("users.merge_names"), Box::new(MergeNames));
    Migrator::open_native(root.join("workspace"), manifests(), registry)
}
fn seed(root: &Path) -> Result<(), N2xError> {
    let users = json!([{"id":1,"first_name":"Ada","last_name":"Lovelace"},{"id":2,"first_name":"Grace","last_name":"Hopper"},{"id":3,"first_name":"Alan","last_name":"Turing"},{"id":4,"first_name":"Linus","last_name":"Torvalds"}]);
    fs::write(
        root.join("users.json"),
        serde_json::to_vec_pretty(&users).unwrap(),
    )
    .map_err(io)?;
    fs::create_dir_all(root.join("a/nested")).map_err(io)?;
    fs::create_dir_all(root.join("a/empty")).map_err(io)?;
    fs::write(root.join("a/readme.txt"), "migration fixture\n").map_err(io)?;
    fs::write(root.join("a/nested/note.txt"), "nested content\n").map_err(io)?;
    fs::write(root.join("initialized"), "n2x migration lab\n").map_err(io)
}
