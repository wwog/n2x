#![cfg(feature = "engine")]
use n2x_core::*;
use std::time::Duration;

struct Resolver(ReplaySafety);
impl ContractResolver for Resolver {
    fn resolve(&self, _: &Step) -> Result<StepContract, N2xError> {
        Ok(StepContract::Task(RecoveryCapabilities {
            replay: self.0.clone(),
            authoritative_lookup: false,
            durability: Durability::Memory,
        }))
    }
}
fn engine(replay: ReplaySafety) -> (Engine, Transition) {
    let request = RunRequest {
        run: "r".into(),
        scope: "s".into(),
        initial: Some("v0".into()),
        target: "v1".into(),
        route: Route::Unique,
        policy: RunPolicy {
            durability: Durability::Memory,
            recovery: RecoveryPolicy::AllowManual,
            retry: RetryPolicy {
                initial_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(2),
                ..Default::default()
            },
        },
    };
    let manifest = Manifest {
        id: "m".into(),
        from: "v0".into(),
        to: "v1".into(),
        steps: vec![Step {
            id: "t".into(),
            kind: StepKind::Task(ActionCall {
                action: ActionRef {
                    id: "test".into(),
                    revision: 1,
                },
                ports: vec![],
                config: None,
            }),
            policy: Default::default(),
        }],
        metadata: Default::default(),
    };
    let plan = compile_plan(
        &[manifest],
        &request,
        "v0".into(),
        vec![],
        &Resolver(replay),
    )
    .unwrap();
    let mut engine = Engine::default();
    let transition = engine
        .start(
            EngineStart::New(Box::new(StartInput {
                run: request.run,
                plan,
                now: Timestamp(0),
            })),
            1,
        )
        .unwrap();
    (engine, transition)
}
fn accept(engine: &mut Engine, command: Command, kind: ReplyKind) -> Transition {
    engine
        .accept(Reply {
            command: command.id,
            kind,
        })
        .unwrap()
}
fn commit(engine: &mut Engine, transition: Transition) -> Transition {
    let command = transition.command.unwrap();
    let CommandKind::Commit(value) = &command.kind else {
        panic!("expected commit")
    };
    let receipt = WorkspaceReceipt {
        id: value.id.clone(),
        digest: value.digest,
        durability: value.durability,
    };
    accept(engine, command, ReplyKind::Committed(Ok(receipt)))
}
fn to_apply(engine: &mut Engine, mut transition: Transition) -> Command {
    while matches!(
        transition.command.as_ref().unwrap().kind,
        CommandKind::Commit(_)
    ) {
        transition = commit(engine, transition);
    }
    let command = transition.command.unwrap();
    assert!(matches!(command.kind, CommandKind::Apply(_)));
    command
}
fn failure(state: ApplyState, hint: RetryHint) -> ReplyKind {
    let mut error = N2xError::new(
        "test.failure",
        ErrorKind::Unavailable,
        ErrorOrigin::Action,
        "injected",
    );
    error.retry_hint = hint;
    ReplyKind::Applied(Err(OperationFailure {
        error,
        apply_state: state,
    }))
}
fn mutations(transition: &Transition) -> &[WorkspaceMutation] {
    let CommandKind::Commit(commit) = &transition.command.as_ref().unwrap().kind else {
        panic!("expected commit")
    };
    &commit.mutations
}
fn saved_plan(transition: &Transition) -> PlanSnapshot {
    mutations(transition)
        .iter()
        .find_map(|mutation| match mutation {
            WorkspaceMutation::SavePlan { plan, .. } => Some(*plan.clone()),
            _ => None,
        })
        .unwrap()
}
fn saved_state(transition: &Transition) -> RecoverySnapshot {
    mutations(transition)
        .iter()
        .find_map(|mutation| match mutation {
            WorkspaceMutation::SaveRecovery(state) => Some(*state.clone()),
            _ => None,
        })
        .unwrap()
}

#[test]
fn unsafe_unknown_stops_and_manual_receipt_is_audited_before_advancing() {
    let (mut engine, transition) = engine(ReplaySafety::Unsafe);
    let command = to_apply(&mut engine, transition);
    let CommandKind::Apply(request) = &command.kind else {
        unreachable!()
    };
    let request = request.clone();
    let transition = accept(
        &mut engine,
        command,
        failure(ApplyState::Unknown, RetryHint::Never),
    );
    let transition = commit(&mut engine, transition);
    let transition = commit(&mut engine, transition);
    assert!(transition.command.is_none());
    assert_eq!(engine.snapshot().unwrap().status, RunStatus::NeedsAttention);
    let resolution = Resolution {
        operation: request.context.id.clone(),
        reason: "checked target receipt".into(),
        decision: ResolutionDecision::Applied(Receipt {
            operation: request.context.id,
            request_digest: request.context.request_digest,
            proof: None,
        }),
    };
    let mut transition = engine.control(Control::Resolve(resolution)).unwrap();
    let CommandKind::Commit(value) = &transition.command.as_ref().unwrap().kind else {
        panic!()
    };
    assert!(value.mutations.iter().any(|m| matches!(
        m,
        WorkspaceMutation::AppendNotice(StoredNotice {
            notice: Notice {
                kind: NoticeKind::OperationResolved(_),
                ..
            },
            ..
        })
    )));
    for _ in 0..10 {
        if transition.command.is_none() {
            break;
        }
        transition = commit(&mut engine, transition);
    }
    assert_eq!(engine.snapshot().unwrap().status, RunStatus::Completed);
    let completed = engine.snapshot().unwrap().clone();
    for control in [Control::Pause, Control::Cancel, Control::Resume] {
        assert!(engine.control(control).is_err());
        assert_eq!(engine.snapshot().unwrap(), &completed);
    }
}

#[test]
fn retry_crash_cannot_reuse_previous_not_applied_outcome() {
    let (mut engine, transition) = engine(ReplaySafety::Unsafe);
    let plan = saved_plan(&transition);
    let command = to_apply(&mut engine, transition);
    let transition = accept(
        &mut engine,
        command,
        failure(ApplyState::NotApplied, RetryHint::Transient),
    );
    let transition = commit(&mut engine, transition);
    let wake = transition.command.unwrap();
    let CommandKind::WakeAt(at) = wake.kind else {
        panic!()
    };
    let transition = accept(&mut engine, wake, ReplyKind::Woken(Ok(at)));
    let state = saved_state(&transition);
    assert_eq!(state.pending.as_ref().unwrap().attempts, 2);
    assert!(state.pending.as_ref().unwrap().last_failure.is_none());
    let _second_apply = to_apply(&mut engine, transition);

    // The second effect may already have committed when the process disappears.
    // An unsafe port cannot replay it based on the first attempt's NotApplied.
    let mut recovered = Engine::default();
    let mut transition = recovered
        .start(
            EngineStart::Recover {
                snapshot: Box::new(state),
                plan: Box::new(plan),
                now: at,
            },
            2,
        )
        .unwrap();
    while transition.command.is_some() {
        transition = commit(&mut recovered, transition);
    }
    assert_eq!(
        recovered.snapshot().unwrap().status,
        RunStatus::NeedsAttention
    );
}

#[test]
fn manual_not_applied_is_durable_until_the_next_attempt() {
    let (mut engine, transition) = engine(ReplaySafety::Unsafe);
    let plan = saved_plan(&transition);
    let command = to_apply(&mut engine, transition);
    let CommandKind::Apply(request) = &command.kind else {
        panic!()
    };
    let operation = request.context.id.clone();
    let transition = accept(
        &mut engine,
        command,
        failure(ApplyState::Unknown, RetryHint::Never),
    );
    let transition = commit(&mut engine, transition);
    commit(&mut engine, transition);
    let transition = engine
        .control(Control::Resolve(Resolution {
            operation: operation.clone(),
            decision: ResolutionDecision::NotApplied,
            reason: "operator verified no external commit or in-flight work".into(),
        }))
        .unwrap();
    let state = saved_state(&transition);
    assert_eq!(
        state
            .pending
            .as_ref()
            .unwrap()
            .last_failure
            .as_ref()
            .unwrap()
            .apply_state,
        ApplyState::NotApplied
    );

    // Simulate a restart immediately after the resolution audit is committed.
    let mut recovered = Engine::default();
    let transition = recovered
        .start(
            EngineStart::Recover {
                snapshot: Box::new(state),
                plan: Box::new(plan),
                now: Timestamp(0),
            },
            2,
        )
        .unwrap();
    let transition = commit(&mut recovered, transition);
    let transition = commit(&mut recovered, transition);
    let wake = transition.command.unwrap();
    let CommandKind::WakeAt(at) = wake.kind else {
        panic!()
    };
    let transition = accept(&mut recovered, wake, ReplyKind::Woken(Ok(at)));
    assert!(
        saved_state(&transition)
            .pending
            .unwrap()
            .last_failure
            .is_none()
    );
    let command = to_apply(&mut recovered, transition);
    let CommandKind::Apply(request) = command.kind else {
        panic!()
    };
    assert_eq!(request.context.id, operation);
    assert_eq!(command.id.attempt, 1);
}

#[test]
fn retry_budget_is_finite_and_operation_identity_stays_fixed() {
    let (mut engine, transition) = engine(ReplaySafety::Idempotent);
    let mut command = to_apply(&mut engine, transition);
    let mut identity = None;
    for attempt in 1..=3 {
        let CommandKind::Apply(request) = &command.kind else {
            panic!()
        };
        if let Some(id) = &identity {
            assert_eq!(id, &request.context.id);
        } else {
            identity = Some(request.context.id.clone());
        }
        assert_eq!(command.id.attempt, attempt);
        let transition = accept(
            &mut engine,
            command,
            failure(ApplyState::NotApplied, RetryHint::Transient),
        );
        let mut transition = commit(&mut engine, transition);
        if attempt == 3 {
            assert!(transition.command.is_none());
            break;
        }
        let wake = transition.command.take().unwrap();
        let CommandKind::WakeAt(at) = wake.kind else {
            panic!()
        };
        let transition = accept(&mut engine, wake, ReplyKind::Woken(Ok(at)));
        command = to_apply(&mut engine, transition);
    }
    assert_eq!(
        engine
            .snapshot()
            .unwrap()
            .last_error
            .as_ref()
            .unwrap()
            .code
            .0,
        ErrorCode::RETRY_EXHAUSTED
    );
    assert_eq!(engine.snapshot().unwrap().current, Revision::from("v0"));
}

#[test]
fn timer_failure_preserves_intent_and_reports_attention() {
    let (mut engine, transition) = engine(ReplaySafety::Idempotent);
    let command = to_apply(&mut engine, transition);
    let transition = accept(
        &mut engine,
        command,
        failure(ApplyState::NotApplied, RetryHint::Transient),
    );
    let transition = commit(&mut engine, transition);
    let error = N2xError::new(
        "test.timer",
        ErrorKind::Unavailable,
        ErrorOrigin::Runtime,
        "timer unavailable",
    );
    let transition = accept(
        &mut engine,
        transition.command.unwrap(),
        ReplyKind::Woken(Err(error)),
    );
    commit(&mut engine, transition);
    assert_eq!(engine.snapshot().unwrap().status, RunStatus::NeedsAttention);
    assert_eq!(
        engine
            .snapshot()
            .unwrap()
            .last_error
            .as_ref()
            .unwrap()
            .code
            .0,
        "test.timer"
    );
}
