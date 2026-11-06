use crate::*;
use n2x_core::*;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::{BTreeMap, BTreeSet};

const EVENT_PREFIX: &str = "n2x/v1/events/";
const EVENT_TOTAL_KEY: &str = "n2x/v1/meta/events";

/// Workspace 的字节后端：一次 commit 中的键值修改必须原子保存。
pub trait StateBackend {
    /// 读取一个值。
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, N2xError>;
    /// 列出指定前缀下的键，用于回收运行材料。
    fn keys(&self, prefix: &str) -> Result<Vec<String>, N2xError>;
    /// 原子提交 set 与 remove，并达到请求的持久级别。
    ///
    /// 实现必须先应用 `values` 再应用 `removes`，且两者同属一个可见提交。
    fn write(
        &mut self,
        values: &BTreeMap<String, Vec<u8>>,
        removes: &BTreeSet<String>,
        durability: Durability,
    ) -> Result<(), N2xError>;
}

/// 临时测试后端，只能提供 Memory 持久级别。
#[derive(Default)]
pub struct MemoryBackend {
    values: BTreeMap<String, Vec<u8>>,
}
impl StateBackend for MemoryBackend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, N2xError> {
        Ok(self.values.get(key).cloned())
    }
    fn keys(&self, prefix: &str) -> Result<Vec<String>, N2xError> {
        Ok(self
            .values
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect())
    }
    fn write(
        &mut self,
        values: &BTreeMap<String, Vec<u8>>,
        removes: &BTreeSet<String>,
        durability: Durability,
    ) -> Result<(), N2xError> {
        if durability != Durability::Memory {
            return Err(storage("memory backend cannot promise durability"));
        }
        self.values.extend(values.clone());
        for key in removes {
            self.values.remove(key);
        }
        Ok(())
    }
}

/// 被回收运行的一份完整归档；实现必须在返回前持久化。
#[derive(Clone, Debug, PartialEq)]
pub struct ArchiveBatch {
    /// 所属运行。
    pub run: RunId,
    /// 资源实例；没有恢复快照时为空。
    pub scope: Option<ScopeId>,
    /// 运行终止时的权威版本。
    pub current: Option<Revision>,
    /// 运行目标版本。
    pub target: Option<Revision>,
    /// 计划摘要。
    pub plan: Option<Digest>,
    /// 触发回收的提交 ID。
    pub retire_commit: CommitId,
    /// 被删除的键值。
    pub entries: Vec<ArchiveEntry>,
}

/// 一条归档记录；`value` 是工作区原始 JSON 值。
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ArchiveEntry {
    /// 逻辑类型：plan / recovery / report / events / quarantine / commit-log / commit。
    pub kind: String,
    /// 原始物理键。
    pub key: String,
    /// 解析后的 JSON 值。
    pub value: serde_json::Value,
}

/// 在从工作区删除运行材料前写入外部归档。
///
/// 实现必须先完成持久化再返回；返回错误时工作区不会删除键值。
/// 同一运行重复归档应幂等。
pub trait RunArchive {
    /// 追加一个运行的归档。
    fn append(&mut self, batch: &ArchiveBatch) -> Result<(), N2xError>;
}

/// 标准 Workspace 实现，封装版本、检查点、去重、通知、隔离与报告的键空间。
pub struct StateWorkspace<B> {
    backend: B,
    event_limit: usize,
    event_budget: usize,
    commit_retention: usize,
    archive: Option<Box<dyn RunArchive>>,
}

impl<B: StateBackend> StateWorkspace<B> {
    /// 组合后端；关键事件默认每个运行保留最近 1000 条。
    ///
    /// 事件总量默认 20_000 条，提交收据默认每个运行保留最近 128 条。
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            event_limit: 1000,
            event_budget: 20_000,
            commit_retention: 128,
            archive: None,
        }
    }
    /// 设置单个运行的事件历史保留上限，至少保留 1 条；不删除恢复和隔离材料。
    pub fn with_event_limit(mut self, limit: usize) -> Self {
        self.event_limit = limit.max(1);
        self
    }
    /// 设置整个工作区的事件历史预算；0 表示不启用总预算。
    ///
    /// 预算按“事件条数”计算，超限的提交返回 `n2x.notice.backpressure`。
    /// 应用应导出并 `retire_run` 已完成的运行来腾出预算。
    pub fn with_event_budget(mut self, limit: usize) -> Self {
        self.event_budget = limit;
        self
    }
    /// 设置每个运行保留的提交收据数量；0 表示不自动裁剪。
    ///
    /// 收据只服务于“已写入但未收到回复”的重试去重。更早的提交不会被引擎重放，
    /// 因此可以安全裁剪；被裁剪的提交再次执行也只会重复幂等 mutation。
    pub fn with_commit_retention(mut self, keep: usize) -> Self {
        self.commit_retention = keep;
        self
    }
    /// 配置运行材料归档；配置后 `RetireRun` 必须先成功写入归档，再删除 KV。
    ///
    /// 未配置时 `RetireRun` 直接删除，历史只保留在报告和事件里。
    pub fn with_archive(mut self, archive: Box<dyn RunArchive>) -> Self {
        self.archive = Some(archive);
        self
    }
    fn read<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, N2xError> {
        self.backend
            .get(key)?
            .map(|value| serde_json::from_slice(&value).map_err(storage))
            .transpose()
    }
    fn read_from<T: DeserializeOwned>(
        &self,
        values: &BTreeMap<String, Vec<u8>>,
        key: &str,
    ) -> Result<Option<T>, N2xError> {
        match values.get(key) {
            Some(value) => serde_json::from_slice(value).map(Some).map_err(storage),
            None => self.read(key),
        }
    }
    fn event_total(&self) -> Result<(u64, bool), N2xError> {
        if let Some(value) = self.backend.get(EVENT_TOTAL_KEY)? {
            let total: u64 = serde_json::from_slice(&value).map_err(storage)?;
            return Ok((total, true));
        }
        let mut total = 0u64;
        for key in self.backend.keys(EVENT_PREFIX)? {
            let list: Vec<StoredNotice> = self.read(&key)?.unwrap_or_default();
            total = total.saturating_add(list.len() as u64);
        }
        Ok((total, false))
    }
    fn commit_sync(&mut self, commit: WorkspaceCommit) -> Result<WorkspaceReceipt, N2xError> {
        if codec::commit_digest(&commit)? != commit.digest {
            return Err(storage("workspace commit digest mismatch"));
        }
        let commit_key = key("commit", &commit.id)?;
        if let Some(old) = self.read::<WorkspaceReceipt>(&commit_key)? {
            if old.digest != commit.digest {
                return Err(storage("CommitId reused for different content"));
            }
            // Repeat durability barrier: previous write may have succeeded but flush/Reply failed.
            self.backend
                .write(&BTreeMap::new(), &BTreeSet::new(), commit.durability)?;
            return Ok(old);
        }
        let mut values = BTreeMap::new();
        let mut removes = BTreeSet::new();
        let mut events: BTreeMap<RunId, Vec<StoredNotice>> = BTreeMap::new();
        let mut event_before: BTreeMap<RunId, usize> = BTreeMap::new();
        let mut subscriptions: BTreeMap<RunId, BTreeMap<SubscriberId, u64>> = BTreeMap::new();
        let mut retire: Vec<(RunId, bool)> = Vec::new();
        for mutation in commit.mutations {
            match mutation {
                WorkspaceMutation::SavePlan { run, plan } => {
                    if codec::plan_digest(&plan)? != plan.digest {
                        return Err(storage("invalid plan digest"));
                    }
                    let key = key("plan", &run)?;
                    if self
                        .read::<PlanSnapshot>(&key)?
                        .is_some_and(|old| old != *plan)
                    {
                        return Err(storage("immutable plan changed"));
                    }
                    put(&mut values, key, &plan)?;
                }
                WorkspaceMutation::SaveScope(scope) => {
                    put(&mut values, key("scope", &scope.scope)?, &scope)?;
                }
                WorkspaceMutation::SaveRecovery(snapshot) => {
                    put(&mut values, key("recovery", &snapshot.run.run)?, &snapshot)?;
                }
                WorkspaceMutation::SaveReport(report) => {
                    put(&mut values, key("report", &report.snapshot.run)?, &report)?;
                }
                WorkspaceMutation::AppendNotice(notice) => {
                    let run = notice.notice.run.clone();
                    if !events.contains_key(&run) {
                        let loaded: Vec<StoredNotice> =
                            self.read(&key("events", &run)?)?.unwrap_or_default();
                        event_before.insert(run.clone(), loaded.len());
                        events.insert(run.clone(), loaded);
                    }
                    let list = events.get_mut(&run).unwrap();
                    if let Some(old) = list.iter().find(|old| old.sequence == notice.sequence) {
                        if old != &notice {
                            return Err(storage("event ID reused"));
                        }
                    } else {
                        if list
                            .last()
                            .map_or(1, |last| last.sequence.saturating_add(1))
                            != notice.sequence
                        {
                            return Err(storage("event sequence gap"));
                        }
                        list.push(notice);
                    }
                }
                WorkspaceMutation::Quarantine {
                    run,
                    step,
                    batch,
                    records,
                } => {
                    put(
                        &mut values,
                        key("quarantine", &(run, step, batch))?,
                        &records,
                    )?;
                }
                mutation @ (WorkspaceMutation::AckNotice(_)
                | WorkspaceMutation::SubscribeNotice(_)
                | WorkspaceMutation::UnsubscribeNotice { .. }) => {
                    let (run, subscriber) = match &mutation {
                        WorkspaceMutation::AckNotice(ack)
                        | WorkspaceMutation::SubscribeNotice(ack) => {
                            (ack.run.clone(), ack.subscriber.clone())
                        }
                        WorkspaceMutation::UnsubscribeNotice { run, subscriber } => {
                            (run.clone(), subscriber.clone())
                        }
                        _ => unreachable!(),
                    };
                    if !subscriptions.contains_key(&run) {
                        subscriptions.insert(
                            run.clone(),
                            self.read(&key("subscribers", &run)?)?.unwrap_or_default(),
                        );
                    }
                    if !events.contains_key(&run) {
                        let loaded: Vec<StoredNotice> =
                            self.read(&key("events", &run)?)?.unwrap_or_default();
                        event_before.insert(run.clone(), loaded.len());
                        events.insert(run.clone(), loaded);
                    }
                    let list = &events[&run];
                    let subscribers = subscriptions.get_mut(&run).unwrap();
                    match mutation {
                        WorkspaceMutation::UnsubscribeNotice { .. } => {
                            subscribers.remove(&subscriber);
                        }
                        WorkspaceMutation::SubscribeNotice(ack)
                        | WorkspaceMutation::AckNotice(ack) => {
                            if subscriber.0.is_empty()
                                || ack.through_sequence
                                    > list.last().map_or(0, |event| event.sequence)
                                || list.first().is_some_and(|event| {
                                    ack.through_sequence.saturating_add(1) < event.sequence
                                })
                                || subscribers
                                    .get(&subscriber)
                                    .is_some_and(|old| *old > ack.through_sequence)
                            {
                                return Err(storage(
                                    "invalid or expired subscription/ACK position",
                                ));
                            }
                            subscribers.insert(subscriber, ack.through_sequence);
                        }
                        _ => unreachable!(),
                    }
                }
                WorkspaceMutation::RetireRun { run, keep_report } => {
                    retire.push((run, keep_report));
                }
            }
        }
        let mut total_delta = 0i128;
        for (run, mut list) in events {
            let subscribers: BTreeMap<SubscriberId, u64> = match subscriptions.get(&run) {
                Some(value) => value.clone(),
                None => self.read(&key("subscribers", &run)?)?.unwrap_or_default(),
            };
            if list.len() > self.event_limit {
                let remove = list.len() - self.event_limit;
                if subscribers
                    .values()
                    .min()
                    .is_some_and(|through| list[remove - 1].sequence > *through)
                {
                    return Err(N2xError::new(
                        "n2x.notice.backpressure",
                        ErrorKind::Quota,
                        ErrorOrigin::Notification,
                        "unacknowledged events fill retention capacity; ACK or unsubscribe before retrying the commit",
                    ));
                }
                list.drain(..remove);
            }
            let before = event_before.get(&run).copied().unwrap_or(list.len());
            total_delta += list.len() as i128 - before as i128;
            put(&mut values, key("events", &run)?, &list)?;
        }
        for (run, subscribers) in subscriptions {
            put(&mut values, key("subscribers", &run)?, &subscribers)?;
        }
        let mut skip_receipt = false;
        for (run, keep_report) in retire {
            let plan_key = key("plan", &run)?;
            let recovery_key = key("recovery", &run)?;
            let report_key = key("report", &run)?;
            let events_key = key("events", &run)?;
            let subscribers_key = key("subscribers", &run)?;
            let commit_log_key = key("commit-log", &run)?;

            let snapshot: Option<RecoverySnapshot> = self.read_from(&values, &recovery_key)?;
            if let Some(snapshot) = &snapshot {
                let scope: Option<ScopeState> =
                    self.read_from(&values, &key("scope", &snapshot.run.scope)?)?;
                let scope = scope.ok_or_else(|| storage("scope missing for retired run"))?;
                if scope.active_run.as_ref() == Some(&run) {
                    return Err(conflict(
                        "cannot retire an active run; finish or resolve it first",
                    ));
                }
            }
            let subscribers: BTreeMap<SubscriberId, u64> = self
                .read_from(&values, &subscribers_key)?
                .unwrap_or_default();
            if !subscribers.is_empty() {
                return Err(conflict(
                    "unsubscribe reliable consumers before retiring the run",
                ));
            }

            let plan: Option<PlanSnapshot> = self.read_from(&values, &plan_key)?;
            let report: Option<RunReport> = self.read_from(&values, &report_key)?;
            let events: Vec<StoredNotice> =
                self.read_from(&values, &events_key)?.unwrap_or_default();
            let log: Vec<CommitId> = self
                .read_from(&values, &commit_log_key)?
                .unwrap_or_default();
            let prefix = quarantine_prefix(&run)?;
            let mut quarantine_keys = self.backend.keys(&prefix)?;
            quarantine_keys.extend(
                values
                    .keys()
                    .filter(|key| key.starts_with(&prefix))
                    .cloned(),
            );
            quarantine_keys.sort();
            quarantine_keys.dedup();
            let mut quarantine = Vec::new();
            for quarantine_key in &quarantine_keys {
                let records: Option<Vec<RejectedRecord>> =
                    self.read_from(&values, quarantine_key)?;
                if let Some(records) = records {
                    quarantine.push((quarantine_key.clone(), records));
                }
            }
            let mut receipts = Vec::new();
            for id in &log {
                let receipt_key = key("commit", id)?;
                if let Some(receipt) = self.read_from::<WorkspaceReceipt>(&values, &receipt_key)? {
                    receipts.push((receipt_key, receipt));
                }
            }

            if let Some(archive) = &mut self.archive {
                let mut entries = Vec::new();
                if let Some(plan) = &plan {
                    entries.push(archive_entry("plan", &plan_key, plan)?);
                }
                if let Some(snapshot) = &snapshot {
                    entries.push(archive_entry("recovery", &recovery_key, snapshot)?);
                }
                if let Some(report) = &report {
                    entries.push(archive_entry("report", &report_key, report)?);
                }
                if !events.is_empty() {
                    entries.push(archive_entry("events", &events_key, &events)?);
                }
                for (quarantine_key, records) in &quarantine {
                    entries.push(archive_entry("quarantine", quarantine_key, records)?);
                }
                if !log.is_empty() {
                    entries.push(archive_entry("commit-log", &commit_log_key, &log)?);
                }
                for (receipt_key, receipt) in &receipts {
                    entries.push(archive_entry("commit", receipt_key, receipt)?);
                }
                archive.append(&ArchiveBatch {
                    run: run.clone(),
                    scope: snapshot
                        .as_ref()
                        .map(|snapshot| snapshot.run.scope.clone())
                        .or_else(|| plan.as_ref().map(|plan| plan.scope.clone())),
                    current: snapshot
                        .as_ref()
                        .map(|snapshot| snapshot.run.current.clone()),
                    target: snapshot
                        .as_ref()
                        .map(|snapshot| snapshot.run.target.clone()),
                    plan: plan
                        .as_ref()
                        .map(|plan| plan.digest)
                        .or_else(|| snapshot.as_ref().map(|snapshot| snapshot.run.plan)),
                    retire_commit: commit.id.clone(),
                    entries,
                })?;
            }

            total_delta -= events.len() as i128;
            for key in [plan_key, events_key, subscribers_key, commit_log_key] {
                values.remove(&key);
                removes.insert(key);
            }
            values.remove(&recovery_key);
            removes.insert(recovery_key);
            if !keep_report {
                values.remove(&report_key);
                removes.insert(report_key);
            }
            for quarantine_key in quarantine_keys {
                values.remove(&quarantine_key);
                removes.insert(quarantine_key);
            }
            for (receipt_key, _) in receipts {
                values.remove(&receipt_key);
                removes.insert(receipt_key);
            }
            if commit.owner.as_ref() == Some(&run) {
                // 回收提交本身不需要收据：重复执行只会重复幂等删除。
                skip_receipt = true;
            }
        }
        if self.event_budget > 0 && total_delta != 0 {
            let (total, existed) = self.event_total()?;
            if !existed {
                put(&mut values, EVENT_TOTAL_KEY.to_owned(), &total)?;
            }
            let next = total as i128 + total_delta;
            if next > self.event_budget as i128 {
                return Err(N2xError::new(
                    "n2x.notice.backpressure",
                    ErrorKind::Quota,
                    ErrorOrigin::Notification,
                    "global event history budget is full; retire finished runs before appending more events",
                ));
            }
            let next = next.max(0) as u64;
            put(&mut values, EVENT_TOTAL_KEY.to_owned(), &next)?;
        }
        let receipt = WorkspaceReceipt {
            id: commit.id.clone(),
            digest: commit.digest,
            durability: commit.durability,
        };
        if !skip_receipt {
            put(&mut values, commit_key, &receipt)?;
            if let Some(owner) = &commit.owner {
                let log_key = key("commit-log", owner)?;
                let mut log: Vec<CommitId> = self.read_from(&values, &log_key)?.unwrap_or_default();
                if !log.contains(&commit.id) {
                    log.push(commit.id.clone());
                }
                if self.commit_retention > 0 {
                    while log.len() > self.commit_retention {
                        let old = log.remove(0);
                        let key = key("commit", &old)?;
                        values.remove(&key);
                        removes.insert(key);
                    }
                }
                put(&mut values, log_key, &log)?;
            }
        }
        self.backend.write(&values, &removes, commit.durability)?;
        Ok(receipt)
    }
}

impl<B: StateBackend> Workspace for StateWorkspace<B> {
    fn scope(&mut self, scope: ScopeId) -> PortResult<'_, Option<ScopeState>> {
        Box::pin(async move { self.read(&key("scope", &scope)?) })
    }
    fn plan(&mut self, run: RunId) -> PortResult<'_, Option<PlanSnapshot>> {
        Box::pin(async move { self.read(&key("plan", &run)?) })
    }
    fn load(&mut self, run: RunId) -> PortResult<'_, Option<RecoverySnapshot>> {
        Box::pin(async move { self.read(&key("recovery", &run)?) })
    }
    fn report(&mut self, run: RunId) -> PortResult<'_, Option<RunReport>> {
        Box::pin(async move { self.read(&key("report", &run)?) })
    }
    fn commit(&mut self, commit: WorkspaceCommit) -> PortResult<'_, WorkspaceReceipt> {
        Box::pin(async move { self.commit_sync(commit) })
    }
    fn events_after(&mut self, run: RunId, after: u64, limit: u32) -> PortResult<'_, EventPage> {
        Box::pin(async move {
            let events: Vec<StoredNotice> = self.read(&key("events", &run)?)?.unwrap_or_default();
            if events
                .first()
                .is_some_and(|notice| after.saturating_add(1) < notice.sequence)
            {
                let snapshot = self
                    .read::<RecoverySnapshot>(&key("recovery", &run)?)?
                    .ok_or_else(|| storage("missing run snapshot"))?;
                return Ok(EventPage::Gap {
                    oldest_sequence: events[0].sequence,
                    snapshot: Box::new(snapshot.run),
                });
            }
            let remaining: Vec<_> = events
                .into_iter()
                .filter(|notice| notice.sequence > after)
                .collect();
            let has_more = remaining.len() > limit.max(1) as usize;
            Ok(EventPage::Events {
                items: remaining.into_iter().take(limit.max(1) as usize).collect(),
                has_more,
            })
        })
    }
}

fn key<T: Serialize>(namespace: &str, id: &T) -> Result<String, N2xError> {
    Ok(format!(
        "n2x/v1/{namespace}/{}",
        serde_json::to_string(id).map_err(storage)?
    ))
}
fn quarantine_prefix(run: &RunId) -> Result<String, N2xError> {
    Ok(format!(
        "n2x/v1/quarantine/[{},",
        serde_json::to_string(run).map_err(storage)?
    ))
}
fn put<T: Serialize>(
    values: &mut BTreeMap<String, Vec<u8>>,
    key: String,
    value: &T,
) -> Result<(), N2xError> {
    values.insert(key, serde_json::to_vec(value).map_err(storage)?);
    Ok(())
}
fn archive_entry<T: Serialize>(kind: &str, key: &str, value: &T) -> Result<ArchiveEntry, N2xError> {
    Ok(ArchiveEntry {
        kind: kind.to_owned(),
        key: key.to_owned(),
        value: serde_json::to_value(value).map_err(storage)?,
    })
}
pub(crate) fn storage(error: impl std::fmt::Display) -> N2xError {
    N2xError::new(
        ErrorCode::PERSISTENCE_UNCONFIRMED,
        ErrorKind::Persistence,
        ErrorOrigin::Workspace,
        error.to_string(),
    )
}
fn conflict(message: impl Into<String>) -> N2xError {
    N2xError::new(
        "n2x.workspace.conflict",
        ErrorKind::Conflict,
        ErrorOrigin::Workspace,
        message.into(),
    )
}
