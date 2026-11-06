#![cfg(feature = "native-kv")]
use n2x_core::*;
use n2x_runtime::*;
use std::{
    collections::BTreeMap,
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
    let mut value = WorkspaceCommit {
        id: id.into(),
        digest: Digest([0; 32]),
        durability,
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
    fn write(&mut self, values: &BTreeMap<String, Vec<u8>>, _: Durability) -> Result<(), N2xError> {
        self.values.extend(values.clone());
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
