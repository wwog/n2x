use n2x::runtime::*;
use n2x::*;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::{
    cell::Cell,
    fs,
    path::{Path, PathBuf},
    rc::Rc,
};

pub fn err(code: &str, message: impl ToString) -> N2xError {
    N2xError::new(
        code,
        ErrorKind::InvalidData,
        ErrorOrigin::Action,
        message.to_string(),
    )
}
pub fn io(error: impl std::fmt::Display) -> N2xError {
    err("lab.io", error.to_string())
}
pub fn action(id: &str) -> ActionRef {
    ActionRef {
        id: id.into(),
        revision: 1,
    }
}
pub fn recovery() -> RecoveryCapabilities {
    RecoveryCapabilities {
        replay: ReplaySafety::Deduplicated { retention: None },
        authoritative_lookup: true,
        durability: Durability::Durable,
    }
}
pub fn encode(value: &Value) -> Encoded {
    Encoded {
        codec: "application/json".into(),
        version: 1,
        bytes: serde_json::to_vec(value).unwrap(),
    }
}
pub fn envelope(value: Value) -> Envelope {
    Envelope {
        key: None,
        payload: Payload::Inline(encode(&value)),
        attributes: Default::default(),
    }
}
pub fn value(item: &Envelope) -> Result<Value, N2xError> {
    match &item.payload {
        Payload::Inline(value) => serde_json::from_slice(&value.bytes).map_err(io),
        _ => Err(err("lab.codec", "inline JSON expected")),
    }
}
pub fn connect(root: &Path) -> Result<Connection, N2xError> {
    let connection = Connection::open(root.join("users.sqlite")).map_err(io)?;
    connection.execute_batch("PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS _n2x_receipts (id TEXT PRIMARY KEY, digest TEXT NOT NULL);").map_err(io)?;
    Ok(connection)
}
fn proof(connection: &Connection, request: &ApplyRequest) -> Result<Option<Receipt>, N2xError> {
    let digest: Option<String> = connection
        .query_row(
            "SELECT digest FROM _n2x_receipts WHERE id=?1",
            [&request.context.id.0],
            |row| row.get(0),
        )
        .optional()
        .map_err(io)?;
    match digest {
        Some(digest) => {
            if digest != serde_json::to_string(&request.context.request_digest).unwrap() {
                return Err(err("lab.id_conflict", "operation ID reused"));
            }
            Ok(Some(receipt(request)))
        }
        None => Ok(None),
    }
}
fn receipt(request: &ApplyRequest) -> Receipt {
    Receipt {
        operation: request.context.id.clone(),
        request_digest: request.context.request_digest,
        proof: None,
    }
}
fn save_receipt(connection: &Connection, request: &ApplyRequest) -> Result<(), N2xError> {
    connection
        .execute(
            "INSERT INTO _n2x_receipts(id,digest) VALUES(?1,?2)",
            params![
                request.context.id.0,
                serde_json::to_string(&request.context.request_digest).unwrap()
            ],
        )
        .map_err(io)?;
    Ok(())
}

#[derive(Clone)]
pub struct LabPort {
    pub root: PathBuf,
    pub port: &'static str,
    pub mode: &'static str,
    pub fault: Rc<Cell<bool>>,
}
impl ResourcePort for LabPort {
    fn binding(&self) -> PortBinding {
        PortBinding {
            port: self.port.into(),
            resource: self
                .root
                .join(match self.mode {
                    "json" => "users.json",
                    "sql" => "users.sqlite",
                    "files-in" => "a",
                    _ => "b",
                })
                .to_string_lossy()
                .to_string()
                .into(),
            adapter: action(match self.mode {
                "sql" => "lab.sqlite",
                "json" => "lab.json",
                _ => "lab.files",
            }),
        }
    }
}

pub fn files(root: &Path) -> Result<Vec<Value>, N2xError> {
    fn visit(root: &Path, path: &Path, output: &mut Vec<Value>) -> Result<(), N2xError> {
        let mut entries = fs::read_dir(path)
            .map_err(io)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(io)?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
            let relative = path
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .to_string();
            let metadata = fs::symlink_metadata(&path).map_err(io)?;
            if metadata.file_type().is_symlink() {
                return Err(err("lab.symlink", "symlink fixture unsupported"));
            }
            if metadata.is_dir() {
                output.push(json!({"path":relative,"directory":true}));
                visit(root, &path, output)?;
            } else {
                output.push(json!({"path":relative,"bytes":fs::read(path).map_err(io)?}));
            }
        }
        Ok(())
    }
    let mut result = vec![];
    if root.exists() {
        visit(root, root, &mut result)?;
    }
    Ok(result)
}

impl Reader for LabPort {
    fn capabilities(&self, _: &StreamSpec) -> Result<ReaderCapabilities, N2xError> {
        Ok(ReaderCapabilities {
            consistency: vec![ReadConsistency::Frozen],
            resumable: true,
        })
    }
    fn read(&mut self, request: ReadRequest) -> PortResult<'_, ReadBatch> {
        Box::pin(async move {
            let values: Vec<Value> = match self.mode {
                "json" => {
                    serde_json::from_slice(&fs::read(self.root.join("users.json")).map_err(io)?)
                        .map_err(io)?
                }
                "files-in" => files(&self.root.join("a"))?,
                "sql" => {
                    let connection = connect(&self.root)?;
                    let mut statement = connection
                        .prepare("SELECT id, first_name, last_name FROM users ORDER BY id")
                        .map_err(io)?;
                    statement.query_map([],|row|Ok(json!({"id":row.get::<_,i64>(0)?,"first_name":row.get::<_,String>(1)?,"last_name":row.get::<_,String>(2)?}))).map_err(io)?.collect::<Result<Vec<_>,_>>().map_err(io)?
                }
                _ => return Err(err("lab.reader", "unsupported reader")),
            };
            let source = SourceState {
                resource: self.binding().resource,
                consistency: ReadConsistency::Frozen,
                token: Some(encode(
                    &serde_json::to_value(n2x::codec::digest(&values)?).unwrap(),
                )),
            };
            if request
                .source
                .as_ref()
                .is_some_and(|previous| previous != &source)
            {
                return Err(err(
                    ErrorCode::SOURCE_CHANGED,
                    "source fixture changed during scan",
                ));
            }
            let offset = request
                .cursor
                .map(|cursor| serde_json::from_slice::<usize>(&cursor.0.bytes).map_err(io))
                .transpose()?
                .unwrap_or(0);
            let end = (offset + request.limits.max_items.get() as usize).min(values.len());
            Ok(ReadBatch {
                items: values[offset..end].iter().cloned().map(envelope).collect(),
                next: Some(Cursor(encode(&json!(end)))),
                eof: end == values.len(),
                source,
            })
        })
    }
}

impl Writer for LabPort {
    fn capabilities(&self, _: &StreamSpec) -> Result<WriterCapabilities, N2xError> {
        Ok(WriterCapabilities {
            recovery: if self.mode == "sql" {
                recovery()
            } else {
                RecoveryCapabilities {
                    replay: ReplaySafety::Idempotent,
                    authoritative_lookup: false,
                    durability: Durability::Durable,
                }
            },
            limits: Default::default(),
            atomic_unit: self.mode == "sql",
        })
    }
    fn apply(
        &mut self,
        request: ApplyRequest,
    ) -> PortFuture<'_, Result<Receipt, OperationFailure>> {
        Box::pin(async move {
            let result = (|| -> Result<Receipt, N2xError> {
                let EffectRequest::Write { target, items } = &request.effect else {
                    return Err(err("lab.writer", "Write expected"));
                };
                if self.mode == "sql" {
                    let mut connection = connect(&self.root)?;
                    if let Some(receipt) = proof(&connection, &request)? {
                        return Ok(receipt);
                    }
                    let transaction = connection.transaction().map_err(io)?;
                    for item in items {
                        let value = value(item)?;
                        if target.stream.0 == "users_v2" {
                            transaction
                                .execute(
                                    "INSERT OR REPLACE INTO users_v2(id,name) VALUES(?1,?2)",
                                    params![value["id"].as_i64(), value["name"].as_str()],
                                )
                                .map_err(io)?;
                        } else {
                            transaction.execute("INSERT OR REPLACE INTO users(id,first_name,last_name) VALUES(?1,?2,?3)",params![value["id"].as_i64(),value["first_name"].as_str(),value["last_name"].as_str()]).map_err(io)?;
                        }
                    }
                    if target.stream.0 == "users_v2" && self.fault.replace(false) {
                        transaction.rollback().map_err(io)?;
                        return Err(err(
                            "lab.business.rolled_back",
                            "业务事务已回滚：模拟 v2 写入失败，版本保持 v1，修正后可恢复",
                        ));
                    }
                    save_receipt(&transaction, &request)?;
                    transaction.commit().map_err(io)?;
                    Ok(receipt(&request))
                } else {
                    let destination = self.root.join("b");
                    fs::create_dir_all(&destination).map_err(io)?;
                    for item in items {
                        let value = value(item)?;
                        let relative = value["path"]
                            .as_str()
                            .ok_or_else(|| err("lab.path", "missing path"))?;
                        if Path::new(relative)
                            .components()
                            .any(|part| !matches!(part, std::path::Component::Normal(_)))
                        {
                            return Err(err("lab.path", "invalid relative path"));
                        }
                        let path = destination.join(relative);
                        if value["directory"] == true {
                            fs::create_dir_all(&path).map_err(io)?;
                        } else {
                            fs::create_dir_all(path.parent().unwrap()).map_err(io)?;
                            let bytes: Vec<u8> =
                                serde_json::from_value(value["bytes"].clone()).map_err(io)?;
                            let temp = path.with_extension("n2x-tmp");
                            let file = fs::File::create(&temp).map_err(io)?;
                            use std::io::Write;
                            let mut file = file;
                            file.write_all(&bytes).map_err(io)?;
                            file.sync_all().map_err(io)?;
                            fs::rename(temp, &path).map_err(io)?;
                        }
                        fs::File::open(path.parent().unwrap())
                            .and_then(|file| file.sync_all())
                            .map_err(io)?;
                    }
                    Ok(receipt(&request))
                }
            })();
            // SQLite errors during commit can be unknown. The explicit business rollback is authoritative.
            result.map_err(|error| OperationFailure {
                apply_state: if error.code.0 == "lab.business.rolled_back" {
                    ApplyState::NotApplied
                } else {
                    ApplyState::Unknown
                },
                error,
            })
        })
    }
    fn reconcile(&mut self, request: ReconcileRequest) -> PortResult<'_, ReconcileResult> {
        Box::pin(async move {
            let connection = connect(&self.root)?;
            Ok(match proof(&connection, &request.request)? {
                Some(receipt) => ReconcileResult::Applied(receipt),
                None => ReconcileResult::NotApplied,
            })
        })
    }
}

pub struct LabAction {
    pub root: PathBuf,
    pub id: &'static str,
}
impl TaskAction for LabAction {
    fn action(&self) -> ActionRef {
        action(self.id)
    }
    fn capabilities(&self, _: &ActionCall) -> Result<RecoveryCapabilities, N2xError> {
        Ok(if self.id == "files.remove_source" {
            RecoveryCapabilities {
                replay: ReplaySafety::Idempotent,
                authoritative_lookup: false,
                durability: Durability::Durable,
            }
        } else {
            recovery()
        })
    }
    fn apply(
        &mut self,
        request: ApplyRequest,
    ) -> PortFuture<'_, Result<Receipt, OperationFailure>> {
        Box::pin(async move {
            let result = (|| -> Result<Receipt, N2xError> {
                if self.id == "files.remove_source" {
                    let source = self.root.join("a");
                    if source.exists() {
                        if files(&source)? != files(&self.root.join("b"))? {
                            return Err(err("lab.files.verify", "destination differs from source"));
                        }
                        fs::remove_dir_all(source).map_err(io)?;
                        fs::File::open(&self.root)
                            .and_then(|file| file.sync_all())
                            .map_err(io)?;
                    }
                    return Ok(receipt(&request));
                }
                let mut connection = connect(&self.root)?;
                if let Some(receipt) = proof(&connection, &request)? {
                    return Ok(receipt);
                }
                let transaction = connection.transaction().map_err(io)?;
                transaction.execute_batch(match self.id {
                "sql.create_v1"=>"CREATE TABLE users(id INTEGER PRIMARY KEY,first_name TEXT NOT NULL,last_name TEXT NOT NULL);",
                "sql.mark_v1"=>"PRAGMA user_version=1;",
                "sql.prepare_v2"=>"CREATE TABLE users_v2(id INTEGER PRIMARY KEY,name TEXT NOT NULL);",
                "sql.publish_v2"=>"DROP TABLE users; ALTER TABLE users_v2 RENAME TO users; PRAGMA user_version=2;",
                "sql.index_v3"=>"CREATE INDEX idx_users_name ON users(name); PRAGMA user_version=3;",
                _=>return Err(err("lab.action","unknown SQL task")),
            }).map_err(io)?;
                save_receipt(&transaction, &request)?;
                transaction.commit().map_err(io)?;
                Ok(receipt(&request))
            })();
            result.map_err(|error| OperationFailure {
                error,
                apply_state: ApplyState::Unknown,
            })
        })
    }
    fn reconcile(&mut self, request: ReconcileRequest) -> PortResult<'_, ReconcileResult> {
        Box::pin(async move {
            let connection = connect(&self.root)?;
            Ok(match proof(&connection, &request.request)? {
                Some(receipt) => ReconcileResult::Applied(receipt),
                None => ReconcileResult::NotApplied,
            })
        })
    }
}
impl CheckAction for LabAction {
    fn action(&self) -> ActionRef {
        action(self.id)
    }
    fn check(&mut self, _: ActionCall) -> PortResult<'_, CheckResult> {
        Box::pin(async move {
            if self.id == "files.verify" {
                if self.root.join("a").exists() || files(&self.root.join("b"))?.len() != 4 {
                    return Err(err("lab.files.verify", "directory migration incomplete"));
                }
            } else {
                let connection = connect(&self.root)?;
                let count: i64 = connection
                    .query_row("SELECT COUNT(*) FROM users", [], |row| row.get(0))
                    .map_err(io)?;
                if count != 4 {
                    return Err(err("lab.sql.verify", "expected four users"));
                }
                if self.id == "sql.verify_v2" {
                    let count:i64=connection.query_row("SELECT COUNT(*) FROM users WHERE name IN ('Ada Lovelace','Grace Hopper','Alan Turing','Linus Torvalds')",[],|row|row.get(0)).map_err(io)?;
                    if count != 4 {
                        return Err(err("lab.sql.verify", "merged names mismatch"));
                    }
                }
                if self.id == "sql.verify_v3" {
                    let count:i64=connection.query_row("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_users_name'",[],|row|row.get(0)).map_err(io)?;
                    if count != 1 {
                        return Err(err("lab.sql.verify", "index missing"));
                    }
                }
            }
            Ok(CheckResult {
                evidence: Some(encode(&json!({"passed":true}))),
            })
        })
    }
}

pub struct MergeNames;
impl TransformAction for MergeNames {
    fn action(&self) -> ActionRef {
        action("users.merge_names")
    }
    fn transform(&mut self, request: TransformRequest) -> PortResult<'_, TransformResult> {
        Box::pin(async move {
            let mut items = vec![];
            for item in request.items {
                let value = value(&item)?;
                items.push(envelope(json!({"id":value["id"],"name":format!("{} {}",value["first_name"].as_str().unwrap_or_default(),value["last_name"].as_str().unwrap_or_default()).trim()})));
            }
            Ok(TransformResult {
                items,
                filtered: 0,
                rejected: vec![],
                next_state: None,
            })
        })
    }
}
