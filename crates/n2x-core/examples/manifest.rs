//! 只构造统一管线声明，不连接数据库或执行迁移。

use n2x_core::{
    ActionCall, ActionRef, Manifest, ReadConsistency, Step, StepKind, StreamSpec, TransferSpec,
    TransformSpec,
};

fn call(id: &str, ports: &[&str]) -> ActionCall {
    ActionCall {
        action: ActionRef {
            id: id.into(),
            revision: 1,
        },
        ports: ports.iter().map(|port| (*port).into()).collect(),
        config: None,
    }
}

fn step(id: &str, kind: StepKind) -> Step {
    Step {
        id: id.into(),
        kind,
        policy: Default::default(),
    }
}

fn stream(port: &str, name: &str) -> StreamSpec {
    StreamSpec {
        port: port.into(),
        stream: name.into(),
        config: None,
    }
}

fn transfer(source: StreamSpec, target: StreamSpec, action: &str) -> StepKind {
    StepKind::Transfer(Box::new(TransferSpec {
        read: source,
        transform: Some(TransformSpec {
            action: ActionRef {
                id: action.into(),
                revision: 1,
            },
            config: None,
        }),
        write: target,
        // 示例要求冻结源。实际必须由适配器或应用保证并在能力校验时确认。
        consistency: ReadConsistency::Frozen,
        read_limits: Default::default(),
        write_limits: Default::default(),
        invalid_data: Default::default(),
    }))
}

fn main() {
    let manifests = vec![
        Manifest {
            id: "import-wcdb".into(),
            from: "wcdb/3".into(),
            to: "sqlite/1".into(),
            steps: vec![
                step("create", StepKind::Task(call("sqlite.create", &["target"]))),
                step(
                    "users",
                    transfer(
                        stream("source", "users"),
                        stream("target", "users"),
                        "user.wcdb_to_sqlite",
                    ),
                ),
                step(
                    "indexes",
                    StepKind::Task(call("sqlite.indexes", &["target"])),
                ),
                step(
                    "verify",
                    StepKind::Check(call("users.verify", &["source", "target"])),
                ),
            ],
            metadata: Default::default(),
        },
        Manifest {
            id: "add-display-name".into(),
            from: "sqlite/1".into(),
            to: "sqlite/2".into(),
            steps: vec![
                step(
                    "alter",
                    StepKind::Task(call("sqlite.add_display_name", &["target"])),
                ),
                step(
                    "backfill",
                    transfer(
                        stream("target", "users_without_display_name"),
                        stream("target", "user_display_names"),
                        "user.display_name",
                    ),
                ),
                step(
                    "verify",
                    StepKind::Check(call("display_names.verify", &["target"])),
                ),
            ],
            metadata: Default::default(),
        },
    ];

    for manifest in manifests {
        println!(
            "{}: {} -> {} ({} steps)",
            manifest.id,
            manifest.from,
            manifest.to,
            manifest.steps.len()
        );
    }
}
