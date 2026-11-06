#![cfg(feature = "native-kv")]
use n2x_core::*;
use n2x_runtime::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

fn block_on<F: Future>(future: F) -> F::Output {
    struct ThreadWake(std::thread::Thread);
    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::park(),
        }
    }
}
fn commit(id: &str, mutations: Vec<WorkspaceMutation>, durability: Durability) -> WorkspaceCommit {
    owned_commit(id, None, mutations, durability)
}
fn owned_commit(
    id: &str,
    owner: Option<&str>,
    mutations: Vec<WorkspaceMutation>,
    durability: Durability,
) -> WorkspaceCommit {
    let mut value = WorkspaceCommit {
        id: id.into(),
        digest: Digest([0; 32]),
        durability,
        owner: owner.map(RunId::from),
        mutations,
    };
    value.digest = codec::commit_digest(&value).unwrap();
    value
}
fn scope() -> ScopeState {
    ScopeState {
        scope: "scope".into(),
        current: "v0".into(),
        active_run: None,
        bindings: vec![],
        last_revision_commit: None,
    }
}

#[test]
fn commit_replay_is_idempotent_and_different_content_is_rejected() {
    block_on(async {
        let mut workspace = StateWorkspace::new(MemoryBackend::default());
        let first = commit(
            "c1",
            vec![WorkspaceMutation::SaveScope(scope())],
            Durability::Memory,
        );
        let receipt = workspace.commit(first.clone()).await.unwrap();
        assert_eq!(workspace.commit(first).await.unwrap(), receipt);
        let mut changed = scope();
        changed.current = "v1".into();
        assert!(
            workspace
                .commit(commit(
                    "c1",
                    vec![WorkspaceMutation::SaveScope(changed)],
                    Durability::Memory
                ))
                .await
                .is_err()
        );
        assert_eq!(
            workspace
                .scope("scope".into())
                .await
                .unwrap()
                .unwrap()
                .current,
            Revision::from("v0")
        );
    })
}

struct FlushOnce {
    values: BTreeMap<String, Vec<u8>>,
    failed: bool,
}
impl StateBackend for FlushOnce {
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
        _: Durability,
    ) -> Result<(), N2xError> {
        self.values.extend(values.clone());
        for key in removes {
            self.values.remove(key);
        }
        if !self.failed {
            self.failed = true;
            Err(N2xError::new(
                "test.flush",
                ErrorKind::Persistence,
                ErrorOrigin::Workspace,
                "simulated flush uncertainty",
            ))
        } else {
            Ok(())
        }
    }
}
#[test]
fn uncertain_flush_retries_original_commit_without_duplicate_events() {
    block_on(async {
        let mut workspace = StateWorkspace::new(FlushOnce {
            values: BTreeMap::new(),
            failed: false,
        });
        let original = commit(
            "c1",
            vec![WorkspaceMutation::AppendNotice(StoredNotice {
                sequence: 1,
                notice: Notice {
                    run: "r1".into(),
                    scope: "s1".into(),
                    at: Timestamp(0),
                    kind: NoticeKind::RunStarted,
                },
            })],
            Durability::Durable,
        );
        assert!(workspace.commit(original.clone()).await.is_err());
        workspace.commit(original).await.unwrap();
        let EventPage::Events { items, .. } =
            workspace.events_after("r1".into(), 0, 10).await.unwrap()
        else {
            panic!()
        };
        assert_eq!(items.len(), 1);
    })
}

#[test]
fn native_blob_roundtrip_ranges_and_integrity() {
    block_on(async {
        let directory = std::env::temp_dir().join(format!(
            "n2x-blob-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut store = NativeBlobStore::open(&directory).unwrap();
        let mut writer = store.create("binary".into(), 1).await.unwrap();
        writer.append(vec![1, 2, 3]).await.unwrap();
        writer.append(vec![4, 5]).await.unwrap();
        let blob = writer.finish().await.unwrap();
        assert_eq!(
            store.read_range(blob.clone(), 1, 3).await.unwrap(),
            vec![2, 3, 4]
        );
        std::fs::write(directory.join(&blob.id.0), [0]).unwrap();
        assert!(store.read_range(blob.clone(), 0, 10).await.is_err());
        store.remove(blob).await.unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    })
}

#[test]
fn native_workspace_lock_excludes_second_writer() {
    block_on(async {
        let directory = std::env::temp_dir().join(format!(
            "n2x-lock-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut first = NativeLock::new(&directory);
        let guard = first.acquire().await.unwrap();
        let mut second = NativeLock::new(&directory);
        assert!(second.acquire().await.is_err());
        drop(guard);
        assert!(second.acquire().await.is_ok());
        std::fs::remove_dir_all(directory).unwrap();
    })
}

fn event(sequence: u64) -> WorkspaceMutation {
    WorkspaceMutation::AppendNotice(StoredNotice {
        sequence,
        notice: Notice {
            run: "r".into(),
            scope: "s".into(),
            at: Timestamp(0),
            kind: NoticeKind::RunStarted,
        },
    })
}
#[test]
fn reliable_subscription_blocks_retention_until_ack_then_replays_once() {
    block_on(async {
        let mut workspace = StateWorkspace::new(MemoryBackend::default()).with_event_limit(2);
        let ack = NoticeAck {
            subscriber: "consumer".into(),
            run: "r".into(),
            through_sequence: 0,
        };
        workspace
            .commit(commit(
                "sub",
                vec![WorkspaceMutation::SubscribeNotice(ack.clone())],
                Durability::Memory,
            ))
            .await
            .unwrap();
        workspace
            .commit(commit(
                "first",
                vec![event(1), event(2)],
                Durability::Memory,
            ))
            .await
            .unwrap();
        let blocked = commit("third", vec![event(3)], Durability::Memory);
        assert_eq!(
            workspace.commit(blocked.clone()).await.unwrap_err().code.0,
            "n2x.notice.backpressure"
        );
        let mut ack = ack;
        ack.through_sequence = 1;
        workspace
            .commit(commit(
                "ack",
                vec![WorkspaceMutation::AckNotice(ack)],
                Durability::Memory,
            ))
            .await
            .unwrap();
        workspace.commit(blocked.clone()).await.unwrap();
        workspace.commit(blocked).await.unwrap();
        let EventPage::Events { items, .. } =
            workspace.events_after("r".into(), 1, 10).await.unwrap()
        else {
            panic!()
        };
        assert_eq!(
            items.iter().map(|v| v.sequence).collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert!(
            workspace
                .commit(commit(
                    "expired",
                    vec![WorkspaceMutation::SubscribeNotice(NoticeAck {
                        subscriber: "late".into(),
                        run: "r".into(),
                        through_sequence: 0
                    })],
                    Durability::Memory
                ))
                .await
                .is_err()
        );
    })
}

#[test]
fn expired_event_cursor_returns_gap_and_current_snapshot() {
    block_on(async {
        let mut workspace = StateWorkspace::new(MemoryBackend::default()).with_event_limit(1);
        let run = RunSnapshot {
            run: "r".into(),
            scope: "s".into(),
            plan: Digest([0; 32]),
            status: RunStatus::Running,
            current: "v0".into(),
            target: "v1".into(),
            step: None,
            progress: Default::default(),
            last_sequence: 2,
            last_error: None,
        };
        let recovery = RecoverySnapshot {
            format_version: MODEL_VERSION,
            epoch: 1,
            run: run.clone(),
            checkpoint: Checkpoint {
                position: ExecutionPosition::Finished,
                transfer: None,
                confirmed: Default::default(),
            },
            pending: None,
            started_at: Timestamp(0),
        };
        workspace
            .commit(commit(
                "events",
                vec![
                    WorkspaceMutation::SaveRecovery(Box::new(recovery)),
                    event(1),
                    event(2),
                ],
                Durability::Memory,
            ))
            .await
            .unwrap();
        let EventPage::Gap {
            oldest_sequence,
            snapshot,
        } = workspace.events_after("r".into(), 0, 10).await.unwrap()
        else {
            panic!()
        };
        assert_eq!(oldest_sequence, 2);
        assert_eq!(*snapshot, run);
    })
}

#[test]
fn native_entry_locks_before_opening_state() {
    let directory = std::env::temp_dir().join(format!(
        "n2x-entry-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let first = Migrator::open_native(&directory, vec![], Registry::default()).unwrap();
    assert!(Migrator::open_native(&directory, vec![], Registry::default()).is_err());
    drop(first);
    drop(Migrator::open_native(&directory, vec![], Registry::default()).unwrap());
    std::fs::remove_dir_all(directory).unwrap();
}

#[derive(Clone, Default)]
struct Shared(Arc<std::sync::Mutex<BTreeMap<String, Vec<u8>>>>);
impl Shared {
    fn keys(&self, prefix: &str) -> Vec<String> {
        self.0
            .lock()
            .unwrap()
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect()
    }
}
impl StateBackend for Shared {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, N2xError> {
        Ok(self.0.lock().unwrap().get(key).cloned())
    }
    fn keys(&self, prefix: &str) -> Result<Vec<String>, N2xError> {
        Ok(Shared::keys(self, prefix))
    }
    fn write(
        &mut self,
        values: &BTreeMap<String, Vec<u8>>,
        removes: &BTreeSet<String>,
        _: Durability,
    ) -> Result<(), N2xError> {
        let mut guard = self.0.lock().unwrap();
        guard.extend(values.clone());
        for key in removes {
            guard.remove(key);
        }
        Ok(())
    }
}

fn terminal_snapshot(run: &str, scope: &str) -> RunSnapshot {
    RunSnapshot {
        run: run.into(),
        scope: scope.into(),
        plan: Digest([1; 32]),
        status: RunStatus::Completed,
        current: "v1".into(),
        target: "v1".into(),
        step: None,
        progress: Progress::default(),
        last_sequence: 0,
        last_error: None,
    }
}
fn plan_for(scope: &str, from: &str, to: &str) -> PlanSnapshot {
    let mut plan = PlanSnapshot {
        format_version: MODEL_VERSION,
        scope: scope.into(),
        from: from.into(),
        to: to.into(),
        route: vec![],
        bindings: vec![],
        policy: RunPolicy::default(),
        digest: Digest([0; 32]),
    };
    plan.digest = codec::plan_digest(&plan).unwrap();
    plan
}
fn recovery_for(snapshot: &RunSnapshot) -> RecoverySnapshot {
    RecoverySnapshot {
        format_version: MODEL_VERSION,
        epoch: 1,
        run: snapshot.clone(),
        checkpoint: Checkpoint {
            position: ExecutionPosition::Finished,
            transfer: None,
            confirmed: Counters::default(),
        },
        pending: None,
        started_at: Timestamp(0),
    }
}
fn report_for(snapshot: &RunSnapshot) -> RunReport {
    RunReport {
        snapshot: snapshot.clone(),
        outcome: RunOutcome::Completed,
        started_at: Timestamp(0),
        finished_at: Timestamp(1),
        evidence: None,
    }
}
fn notice_for(run: &str, sequence: u64) -> WorkspaceMutation {
    WorkspaceMutation::AppendNotice(StoredNotice {
        sequence,
        notice: Notice {
            run: run.into(),
            scope: "s".into(),
            at: Timestamp(0),
            kind: NoticeKind::RunStarted,
        },
    })
}
fn closed_scope(run: Option<&str>) -> ScopeState {
    ScopeState {
        scope: "s".into(),
        current: "v1".into(),
        active_run: run.map(RunId::from),
        bindings: vec![],
        last_revision_commit: None,
    }
}

#[test]
fn commit_receipts_are_bounded_per_run() {
    block_on(async {
        let backend = Shared::default();
        let probe = backend.clone();
        let mut workspace = StateWorkspace::new(backend).with_commit_retention(2);
        for index in 1..=4 {
            workspace
                .commit(owned_commit(
                    &format!("c{index}"),
                    Some("r1"),
                    vec![],
                    Durability::Memory,
                ))
                .await
                .unwrap();
        }
        let receipts = probe.keys("n2x/v1/commit/");
        assert_eq!(receipts.len(), 2);
        assert!(receipts.iter().any(|key| key.ends_with("\"c3\"")));
        assert!(receipts.iter().any(|key| key.ends_with("\"c4\"")));
        let log: Vec<CommitId> = serde_json::from_slice(
            &probe
                .get("n2x/v1/commit-log/\"r1\"")
                .unwrap()
                .expect("commit log"),
        )
        .unwrap();
        assert_eq!(log, vec![CommitId::from("c3"), CommitId::from("c4")]);
    })
}

#[test]
fn retire_run_reclaims_material_and_can_drop_the_report() {
    block_on(async {
        let backend = Shared::default();
        let probe = backend.clone();
        let mut workspace = StateWorkspace::new(backend);
        let snapshot = terminal_snapshot("r1", "s");
        workspace
            .commit(owned_commit(
                "boot",
                Some("r1"),
                vec![
                    WorkspaceMutation::SavePlan {
                        run: "r1".into(),
                        plan: Box::new(plan_for("s", "v0", "v1")),
                    },
                    WorkspaceMutation::SaveScope(closed_scope(Some("r1"))),
                    WorkspaceMutation::SaveRecovery(Box::new(recovery_for(&snapshot))),
                    WorkspaceMutation::SaveReport(Box::new(report_for(&snapshot))),
                    notice_for("r1", 1),
                    WorkspaceMutation::Quarantine {
                        run: "r1".into(),
                        step: StepAddress {
                            manifest: "m".into(),
                            step: "s".into(),
                        },
                        batch: 0,
                        records: vec![],
                    },
                ],
                Durability::Memory,
            ))
            .await
            .unwrap();
        workspace
            .commit(owned_commit(
                "done",
                Some("r1"),
                vec![WorkspaceMutation::SaveScope(closed_scope(None))],
                Durability::Memory,
            ))
            .await
            .unwrap();
        assert!(
            workspace
                .commit(owned_commit(
                    "retire",
                    Some("r1"),
                    vec![WorkspaceMutation::RetireRun {
                        run: "r1".into(),
                        keep_report: true,
                    }],
                    Durability::Memory,
                ))
                .await
                .is_ok()
        );
        assert!(workspace.plan("r1".into()).await.unwrap().is_none());
        assert!(workspace.load("r1".into()).await.unwrap().is_none());
        assert!(workspace.report("r1".into()).await.unwrap().is_some());
        for prefix in [
            "n2x/v1/commit/",
            "n2x/v1/commit-log/",
            "n2x/v1/events/",
            "n2x/v1/plan/",
            "n2x/v1/quarantine/",
            "n2x/v1/recovery/",
            "n2x/v1/subscribers/",
        ] {
            assert!(
                probe.keys(prefix).is_empty(),
                "leftover keys under {prefix}"
            );
        }
        workspace
            .commit(owned_commit(
                "retire-report",
                Some("r1"),
                vec![WorkspaceMutation::RetireRun {
                    run: "r1".into(),
                    keep_report: false,
                }],
                Durability::Memory,
            ))
            .await
            .unwrap();
        assert!(workspace.report("r1".into()).await.unwrap().is_none());
    })
}

#[test]
fn retire_run_refuses_active_or_subscribed_runs() {
    block_on(async {
        let backend = Shared::default();
        let mut workspace = StateWorkspace::new(backend);
        let snapshot = terminal_snapshot("active", "s");
        workspace
            .commit(commit(
                "active",
                vec![
                    WorkspaceMutation::SaveScope(closed_scope(Some("active"))),
                    WorkspaceMutation::SaveRecovery(Box::new(recovery_for(&snapshot))),
                ],
                Durability::Memory,
            ))
            .await
            .unwrap();
        let error = workspace
            .commit(commit(
                "retire-active",
                vec![WorkspaceMutation::RetireRun {
                    run: "active".into(),
                    keep_report: true,
                }],
                Durability::Memory,
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code.0, "n2x.workspace.conflict");

        let subscribed = terminal_snapshot("subscribed", "s");
        workspace
            .commit(commit(
                "subscribed",
                vec![
                    WorkspaceMutation::SaveScope(closed_scope(None)),
                    WorkspaceMutation::SaveRecovery(Box::new(recovery_for(&subscribed))),
                    WorkspaceMutation::SubscribeNotice(NoticeAck {
                        subscriber: "ui".into(),
                        run: "subscribed".into(),
                        through_sequence: 0,
                    }),
                ],
                Durability::Memory,
            ))
            .await
            .unwrap();
        let error = workspace
            .commit(commit(
                "retire-subscribed",
                vec![WorkspaceMutation::RetireRun {
                    run: "subscribed".into(),
                    keep_report: true,
                }],
                Durability::Memory,
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code.0, "n2x.workspace.conflict");
    })
}

#[test]
fn global_event_budget_blocks_until_runs_are_retired() {
    block_on(async {
        let backend = Shared::default();
        let mut workspace = StateWorkspace::new(backend)
            .with_event_limit(10)
            .with_event_budget(3);
        let snapshot = terminal_snapshot("r1", "s");
        workspace
            .commit(owned_commit(
                "boot",
                Some("r1"),
                vec![
                    WorkspaceMutation::SaveScope(closed_scope(None)),
                    WorkspaceMutation::SaveRecovery(Box::new(recovery_for(&snapshot))),
                ],
                Durability::Memory,
            ))
            .await
            .unwrap();
        for sequence in 1..=3 {
            workspace
                .commit(commit(
                    &format!("e{sequence}"),
                    vec![notice_for("r1", sequence)],
                    Durability::Memory,
                ))
                .await
                .unwrap();
        }
        let error = workspace
            .commit(commit("e4", vec![notice_for("r1", 4)], Durability::Memory))
            .await
            .unwrap_err();
        assert_eq!(error.code.0, "n2x.notice.backpressure");
        workspace
            .commit(owned_commit(
                "retire",
                Some("r1"),
                vec![WorkspaceMutation::RetireRun {
                    run: "r1".into(),
                    keep_report: true,
                }],
                Durability::Memory,
            ))
            .await
            .unwrap();
        for sequence in 1..=3 {
            workspace
                .commit(commit(
                    &format!("r2-{sequence}"),
                    vec![notice_for("r2", sequence)],
                    Durability::Memory,
                ))
                .await
                .unwrap();
        }
        assert!(
            workspace
                .commit(commit(
                    "r2-4",
                    vec![notice_for("r2", 4)],
                    Durability::Memory
                ))
                .await
                .is_err()
        );
    })
}

#[derive(Clone, Default)]
struct RecordArchive(Arc<std::sync::Mutex<Vec<ArchiveBatch>>>);
impl RunArchive for RecordArchive {
    fn append(&mut self, batch: &ArchiveBatch) -> Result<(), N2xError> {
        self.0.lock().unwrap().push(batch.clone());
        Ok(())
    }
}

struct FailArchive;
impl RunArchive for FailArchive {
    fn append(&mut self, _: &ArchiveBatch) -> Result<(), N2xError> {
        Err(N2xError::new(
            "test.archive",
            ErrorKind::Persistence,
            ErrorOrigin::Workspace,
            "simulated archive failure",
        ))
    }
}

async fn boot_completed_run<B: StateBackend>(
    workspace: &mut StateWorkspace<B>,
    run: &str,
    durability: Durability,
) {
    let snapshot = terminal_snapshot(run, "s");
    workspace
        .commit(owned_commit(
            &format!("{run}-boot"),
            Some(run),
            vec![
                WorkspaceMutation::SavePlan {
                    run: run.into(),
                    plan: Box::new(plan_for("s", "v0", "v1")),
                },
                WorkspaceMutation::SaveScope(closed_scope(Some(run))),
                WorkspaceMutation::SaveRecovery(Box::new(recovery_for(&snapshot))),
                WorkspaceMutation::SaveReport(Box::new(report_for(&snapshot))),
                notice_for(run, 1),
                WorkspaceMutation::Quarantine {
                    run: run.into(),
                    step: StepAddress {
                        manifest: "m".into(),
                        step: "s".into(),
                    },
                    batch: 0,
                    records: vec![],
                },
            ],
            durability,
        ))
        .await
        .unwrap();
    workspace
        .commit(owned_commit(
            &format!("{run}-done"),
            Some(run),
            vec![WorkspaceMutation::SaveScope(closed_scope(None))],
            durability,
        ))
        .await
        .unwrap();
}

#[test]
fn retire_run_archives_material_before_delete() {
    block_on(async {
        let backend = Shared::default();
        let probe = backend.clone();
        let archive = RecordArchive::default();
        let batches = archive.0.clone();
        let mut workspace = StateWorkspace::new(backend).with_archive(Box::new(archive));
        boot_completed_run(&mut workspace, "r1", Durability::Memory).await;
        workspace
            .commit(owned_commit(
                "retire",
                Some("r1"),
                vec![WorkspaceMutation::RetireRun {
                    run: "r1".into(),
                    keep_report: true,
                }],
                Durability::Memory,
            ))
            .await
            .unwrap();

        let batches = batches.lock().unwrap();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.run, RunId::from("r1"));
        assert_eq!(batch.scope, Some(ScopeId::from("s")));
        assert_eq!(batch.current, Some(Revision::from("v1")));
        assert_eq!(batch.target, Some(Revision::from("v1")));
        assert!(batch.plan.is_some());
        for kind in [
            "plan",
            "recovery",
            "report",
            "events",
            "quarantine",
            "commit-log",
            "commit",
        ] {
            assert!(
                batch.entries.iter().any(|entry| entry.kind == kind),
                "archive is missing {kind}"
            );
        }
        let report = batch
            .entries
            .iter()
            .find(|entry| entry.kind == "report")
            .unwrap();
        assert_eq!(report.value["outcome"].as_str(), Some("Completed"));
        assert!(probe.keys("n2x/v1/plan/").is_empty());
    })
}

#[test]
fn archive_failure_keeps_run_material_intact() {
    block_on(async {
        let backend = Shared::default();
        let probe = backend.clone();
        let mut workspace = StateWorkspace::new(backend).with_archive(Box::new(FailArchive));
        boot_completed_run(&mut workspace, "r1", Durability::Memory).await;
        let error = workspace
            .commit(owned_commit(
                "retire",
                Some("r1"),
                vec![WorkspaceMutation::RetireRun {
                    run: "r1".into(),
                    keep_report: true,
                }],
                Durability::Memory,
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code.0, "test.archive");
        assert!(workspace.load("r1".into()).await.unwrap().is_some());
        assert!(workspace.report("r1".into()).await.unwrap().is_some());
        assert!(!probe.keys("n2x/v1/plan/").is_empty());
        assert!(!probe.keys("n2x/v1/events/").is_empty());
        assert!(!probe.keys("n2x/v1/quarantine/").is_empty());
    })
}

#[test]
fn native_archive_writes_jsonl_before_deleting_report() {
    block_on(async {
        let directory = std::env::temp_dir().join(format!(
            "n2x-archive-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let archive_dir = directory.join("archive");
        let backend = KvBackend::open(&directory).unwrap();
        let mut workspace = StateWorkspace::new(backend)
            .with_archive(Box::new(NativeArchive::open(&archive_dir).unwrap()));
        boot_completed_run(&mut workspace, "r1", Durability::Durable).await;
        workspace
            .commit(owned_commit(
                "retire",
                Some("r1"),
                vec![WorkspaceMutation::RetireRun {
                    run: "r1".into(),
                    keep_report: false,
                }],
                Durability::Durable,
            ))
            .await
            .unwrap();
        assert!(workspace.report("r1".into()).await.unwrap().is_none());

        let mut files: Vec<_> = std::fs::read_dir(&archive_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(files.len(), 1);
        assert_eq!(
            files[0].extension().and_then(|ext| ext.to_str()),
            Some("ndjson")
        );
        let text = std::fs::read_to_string(files.pop().unwrap()).unwrap();
        let lines: Vec<serde_json::Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(lines[0]["format_version"].as_u64(), Some(1));
        assert_eq!(lines[0]["run"].as_str(), Some("r1"));
        assert_eq!(lines[0]["current"].as_str(), Some("v1"));
        assert!(
            lines
                .iter()
                .any(|line| line["kind"].as_str() == Some("report"))
        );
        assert!(
            lines
                .iter()
                .any(|line| line["kind"].as_str() == Some("events"))
        );
        drop(workspace);
        std::fs::remove_dir_all(directory).unwrap();
    })
}
