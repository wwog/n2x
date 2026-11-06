//! 持久模型的序列化契约测试；调度与恢复行为见其余集成测试。

#![cfg(feature = "serde")]

use n2x_core::*;

fn payload(text: &str) -> Envelope {
    Envelope {
        key: None,
        payload: Payload::Inline(Encoded {
            codec: "text/plain".into(),
            version: 1,
            bytes: text.as_bytes().to_vec(),
        }),
        attributes: Default::default(),
    }
}

fn address() -> StepAddress {
    StepAddress {
        manifest: "import".into(),
        step: "users".into(),
    }
}

fn request(id: &str, value: &str) -> ApplyRequest {
    ApplyRequest {
        context: OperationContext {
            id: id.into(),
            run: "run-1".into(),
            step: address(),
            plan: Digest([1; 32]),
            request_digest: Digest([value.as_bytes()[0]; 32]),
        },
        effect: EffectRequest::Write {
            target: StreamSpec {
                port: "target".into(),
                stream: "users".into(),
                config: None,
            },
            items: vec![payload(value)],
        },
    }
}

fn receipt(request: &ApplyRequest) -> Receipt {
    Receipt {
        operation: request.context.id.clone(),
        request_digest: request.context.request_digest,
        proof: None,
    }
}

#[test]
fn partially_committed_batch_retains_replay_and_source_material() {
    let first = request("op-1", "alice");
    let second = request("op-2", "bob");
    let failure = OperationFailure {
        error: N2xError::new(
            ErrorCode::OUTCOME_UNKNOWN,
            ErrorKind::Timeout,
            ErrorOrigin::Writer,
            "response was lost",
        ),
        apply_state: ApplyState::Unknown,
    };
    let snapshot = RecoverySnapshot {
        format_version: MODEL_VERSION,
        epoch: 7,
        run: RunSnapshot {
            run: "run-1".into(),
            scope: "account/42".into(),
            plan: Digest([1; 32]),
            status: RunStatus::NeedsAttention,
            current: "wcdb/3".into(),
            target: "sqlite/1".into(),
            step: Some(address()),
            progress: Progress::default(),
            last_sequence: 3,
            last_error: Some(failure.error.clone()),
        },
        checkpoint: Checkpoint {
            position: ExecutionPosition::Step { edge: 0, step: 0 },
            transfer: Some(TransferCheckpoint {
                batch: 0,
                cursor: None,
                source: Some(SourceState {
                    resource: "wcdb-instance-1".into(),
                    consistency: ReadConsistency::Frozen,
                    token: None,
                }),
                transform_state: None,
                prepared: Some(PreparedBatch {
                    next_cursor: Some(Cursor(Encoded {
                        codec: "wcdb/users-cursor".into(),
                        version: 1,
                        bytes: vec![2],
                    })),
                    eof: false,
                    units: vec![first.clone(), second.clone()],
                    receipts: vec![receipt(&first)],
                    rejected: vec![],
                    delta: Counters {
                        read: 2,
                        written: 2,
                        ..Default::default()
                    },
                    next_transform_state: None,
                }),
            }),
            confirmed: Counters::default(),
        },
        pending: Some(Intent {
            request: second.clone(),
            created_at: Timestamp(1_800_000_000_000),
            attempts: 1,
            last_failure: Some(failure),
        }),
        started_at: Timestamp(1_800_000_000_000),
    };

    let bytes = serde_json::to_vec(&snapshot).unwrap();
    let restored: RecoverySnapshot = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(snapshot, restored);
    assert_eq!(restored.pending.unwrap().request, second);
    let transfer = restored.checkpoint.transfer.unwrap();
    assert!(
        transfer.cursor.is_none(),
        "input cursor is not yet committed"
    );
    let prepared = transfer.prepared.unwrap();
    assert_eq!(prepared.receipts.len(), 1);
    assert_eq!(prepared.units.len(), 2);
}

#[test]
fn failure_preserves_commit_certainty_independently_of_error_kind() {
    let request = request("op-1", "alice");
    let error = N2xError::new(
        "adapter.cleanup_failed",
        ErrorKind::Unavailable,
        ErrorOrigin::Writer,
        "commit succeeded but cleanup failed",
    );
    let reply = Reply {
        command: CommandId {
            epoch: 2,
            sequence: 8,
            attempt: 1,
        },
        kind: ReplyKind::Applied(Err(OperationFailure {
            error,
            apply_state: ApplyState::Applied(receipt(&request)),
        })),
    };
    let bytes = serde_json::to_vec(&reply).unwrap();
    let restored: Reply = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(restored, reply);
}

#[test]
fn atomic_workspace_commit_preserves_event_identity_and_ack() {
    let commit = WorkspaceCommit {
        id: "commit-1".into(),
        digest: Digest([9; 32]),
        durability: Durability::Durable,
        mutations: vec![
            WorkspaceMutation::AppendNotice(StoredNotice {
                sequence: 4,
                notice: Notice {
                    run: "run-1".into(),
                    scope: "account/42".into(),
                    at: Timestamp(1_800_000_000_000),
                    kind: NoticeKind::RunPaused,
                },
            }),
            WorkspaceMutation::AckNotice(NoticeAck {
                subscriber: "ui".into(),
                run: "run-1".into(),
                through_sequence: 3,
            }),
        ],
    };
    let bytes = serde_json::to_vec(&commit).unwrap();
    let restored: WorkspaceCommit = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(restored, commit);
}
