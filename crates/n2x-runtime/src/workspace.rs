use crate::*;
use n2x_core::*;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::BTreeMap;

/// Workspace 的字节后端：一次 commit 中的键值修改必须原子保存。
pub trait StateBackend {
    /// 读取一个值。
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, N2xError>;
    /// 原子提交并达到请求的持久级别。
    fn write(
        &mut self,
        values: &BTreeMap<String, Vec<u8>>,
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
    fn write(
        &mut self,
        values: &BTreeMap<String, Vec<u8>>,
        durability: Durability,
    ) -> Result<(), N2xError> {
        if durability != Durability::Memory {
            return Err(storage("memory backend cannot promise durability"));
        }
        self.values.extend(values.clone());
        Ok(())
    }
}

/// 标准 Workspace 实现，封装版本、检查点、去重、通知、隔离与报告的键空间。
pub struct StateWorkspace<B> {
    backend: B,
    event_limit: usize,
}

impl<B: StateBackend> StateWorkspace<B> {
    /// 组合后端；关键事件默认保留最近 1000 条。
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            event_limit: 1000,
        }
    }
    /// 设置事件历史保留上限，至少保留 1 条；不删除恢复和隔离材料。
    pub fn with_event_limit(mut self, limit: usize) -> Self {
        self.event_limit = limit.max(1);
        self
    }
    fn read<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, N2xError> {
        self.backend
            .get(key)?
            .map(|value| serde_json::from_slice(&value).map_err(storage))
            .transpose()
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
            self.backend.write(&BTreeMap::new(), commit.durability)?;
            return Ok(old);
        }
        let mut values = BTreeMap::new();
        let mut events: BTreeMap<RunId, Vec<StoredNotice>> = BTreeMap::new();
        let mut subscriptions: BTreeMap<RunId, BTreeMap<SubscriberId, u64>> = BTreeMap::new();
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
                        events.insert(
                            run.clone(),
                            self.read(&key("events", &run)?)?.unwrap_or_default(),
                        );
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
                        events.insert(
                            run.clone(),
                            self.read(&key("events", &run)?)?.unwrap_or_default(),
                        );
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
            }
        }
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
            put(&mut values, key("events", &run)?, &list)?;
        }
        for (run, subscribers) in subscriptions {
            put(&mut values, key("subscribers", &run)?, &subscribers)?;
        }
        let receipt = WorkspaceReceipt {
            id: commit.id,
            digest: commit.digest,
            durability: commit.durability,
        };
        put(&mut values, commit_key, &receipt)?;
        self.backend.write(&values, commit.durability)?;
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
fn put<T: Serialize>(
    values: &mut BTreeMap<String, Vec<u8>>,
    key: String,
    value: &T,
) -> Result<(), N2xError> {
    values.insert(key, serde_json::to_vec(value).map_err(storage)?);
    Ok(())
}
pub(crate) fn storage(error: impl std::fmt::Display) -> N2xError {
    N2xError::new(
        ErrorCode::PERSISTENCE_UNCONFIRMED,
        ErrorKind::Persistence,
        ErrorOrigin::Workspace,
        error.to_string(),
    )
}
