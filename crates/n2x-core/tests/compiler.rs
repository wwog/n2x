#![cfg(feature = "engine")]
use n2x_core::*;
struct Resolver;
impl ContractResolver for Resolver {
    fn resolve(&self, _: &Step) -> Result<StepContract, N2xError> {
        Ok(StepContract::Check)
    }
}
fn edge(id: &str, from: &str, to: &str) -> Manifest {
    Manifest {
        id: id.into(),
        from: from.into(),
        to: to.into(),
        steps: vec![],
        metadata: Default::default(),
    }
}
fn request(target: &str) -> RunRequest {
    RunRequest {
        run: "run".into(),
        scope: "scope".into(),
        initial: Some("v0".into()),
        target: target.into(),
        route: Route::Unique,
        policy: RunPolicy {
            durability: Durability::Memory,
            ..Default::default()
        },
    }
}
#[test]
fn routes_are_explicit_when_ambiguous() {
    let edges = vec![
        edge("a", "v0", "v1"),
        edge("b", "v1", "v2"),
        edge("shortcut", "v0", "v2"),
    ];
    let mut request = request("v2");
    assert_eq!(
        compile_plan(&edges, &request, "v0".into(), vec![], &Resolver)
            .unwrap_err()
            .code
            .0,
        ErrorCode::ROUTE_AMBIGUOUS
    );
    request.route = Route::Explicit(vec!["shortcut".into()]);
    assert_eq!(
        compile_plan(&edges, &request, "v0".into(), vec![], &Resolver)
            .unwrap()
            .route
            .len(),
        1
    );
}
#[test]
fn backward_target_reports_rollback_error() {
    let edges = vec![edge("a", "v0", "v1")];
    assert_eq!(
        compile_plan(&edges, &request("v0"), "v1".into(), vec![], &Resolver)
            .unwrap_err()
            .code
            .0,
        ErrorCode::ROLLBACK_UNSUPPORTED
    );
}
#[test]
fn engine_rejects_stale_reply_and_does_not_publish_unconfirmed_version() {
    let plan = compile_plan(
        &[edge("a", "v0", "v1")],
        &request("v1"),
        "v0".into(),
        vec![],
        &Resolver,
    )
    .unwrap();
    let mut engine = Engine::default();
    let transition = engine
        .start(
            EngineStart::New(Box::new(StartInput {
                run: "run".into(),
                plan,
                now: Timestamp(0),
            })),
            1,
        )
        .unwrap();
    let command = transition.command.unwrap();
    let mut stale = command.id.clone();
    stale.epoch = 0;
    assert!(
        engine
            .accept(Reply {
                command: stale,
                kind: ReplyKind::Time(Timestamp(0))
            })
            .is_err()
    );
    let CommandKind::Commit(commit) = command.kind else {
        panic!()
    };
    let next = engine
        .accept(Reply {
            command: command.id,
            kind: ReplyKind::Committed(Ok(WorkspaceReceipt {
                id: commit.id,
                digest: commit.digest,
                durability: commit.durability,
            })),
        })
        .unwrap();
    assert_eq!(engine.snapshot().unwrap().current, Revision::from("v0"));
    assert!(matches!(next.command.unwrap().kind, CommandKind::Commit(_)));
}
