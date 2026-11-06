//! 无 IO 的版本路径选择和计划校验。能力通过 ContractResolver 显式注入。
use crate::*;
use std::collections::{BTreeMap, BTreeSet};

/// 注册表向计划编译器提供具体步骤的能力。
pub trait ContractResolver {
    /// 验证注册实现、配置和端口，并返回本步骤能力。
    fn resolve(&self, step: &Step) -> Result<StepContract, N2xError>;
}

/// 编译不可变计划；多路径必须显式选择，反向请求需有显式反向边。
pub fn compile_plan(
    manifests: &[Manifest],
    request: &RunRequest,
    current: Revision,
    mut bindings: Vec<PortBinding>,
    resolver: &dyn ContractResolver,
) -> Result<PlanSnapshot, N2xError> {
    if request.scope.0.is_empty() || request.run.0.is_empty() || request.target.0.is_empty() {
        return Err(invalid("empty run, scope or target"));
    }
    let mut ids = BTreeSet::new();
    for edge in manifests {
        if edge.id.0.is_empty()
            || edge.from == edge.to
            || edge.from.0.is_empty()
            || edge.to.0.is_empty()
            || !ids.insert(&edge.id)
        {
            return Err(invalid("empty/duplicate manifest or self edge"));
        }
        let mut steps = BTreeSet::new();
        for step in &edge.steps {
            if step.id.0.is_empty() || !steps.insert(&step.id) {
                return Err(invalid("empty/duplicate step"));
            }
        }
    }
    let selected = match &request.route {
        Route::Unique => {
            let found = paths(manifests, &current, &request.target);
            if found.is_empty() {
                if !paths(manifests, &request.target, &current).is_empty() {
                    return Err(N2xError::new(
                        ErrorCode::ROLLBACK_UNSUPPORTED,
                        ErrorKind::Unsupported,
                        ErrorOrigin::Plan,
                        format!(
                            "cannot roll back {current} to {}; declare an explicit reverse migration",
                            request.target
                        ),
                    ));
                }
                return Err(invalid("no migration path to target"));
            }
            if found.len() != 1 {
                return Err(N2xError::new(
                    ErrorCode::ROUTE_AMBIGUOUS,
                    ErrorKind::InvalidPlan,
                    ErrorOrigin::Plan,
                    "multiple routes; specify Route::Explicit",
                ));
            }
            found.into_iter().next().unwrap()
        }
        Route::Explicit(route) => {
            let mut cursor = current.clone();
            let mut selected = Vec::new();
            for id in route {
                let edge = manifests
                    .iter()
                    .position(|edge| &edge.id == id)
                    .ok_or_else(|| invalid("unknown route edge"))?;
                if manifests[edge].from != cursor {
                    return Err(invalid("route is not contiguous"));
                }
                cursor = manifests[edge].to.clone();
                selected.push(edge);
            }
            if cursor != request.target {
                return Err(invalid("route does not reach target"));
            }
            selected
        }
    };
    bindings.sort_by(|a, b| a.port.cmp(&b.port));
    let mut ports = BTreeMap::new();
    for binding in &bindings {
        if binding.port.0.is_empty()
            || binding.resource.0.is_empty()
            || ports.insert(&binding.port, binding).is_some()
        {
            return Err(invalid("empty/duplicate port binding"));
        }
    }
    let mut route = Vec::new();
    for index in selected {
        let manifest = &manifests[index];
        let mut steps = Vec::new();
        for step in &manifest.steps {
            let contract = resolver.resolve(step)?;
            let recovery = step.policy.recovery.unwrap_or(request.policy.recovery);
            let retry = step.policy.retry.as_ref().unwrap_or(&request.policy.retry);
            if retry.initial_delay > retry.max_delay || retry.budget.is_zero() {
                return Err(invalid("invalid retry timing"));
            }
            let required: Vec<&PortId> = match (&step.kind, &contract) {
                (StepKind::Task(call), StepContract::Task(cap)) => {
                    validate_recovery(cap, recovery, request.policy.durability)?;
                    call.ports.iter().collect()
                }
                (StepKind::Check(call), StepContract::Check) => call.ports.iter().collect(),
                (StepKind::Transfer(spec), StepContract::Transfer { reader, writer }) => {
                    if !reader.consistency.contains(&spec.consistency) || !reader.resumable {
                        return Err(invalid("reader cannot satisfy consistency/recovery"));
                    }
                    if spec.read_limits.max_inline_bytes == 0
                        || spec.write_limits.max_inline_bytes == 0
                        || spec.write_limits.max_items > writer.limits.max_items
                        || spec.write_limits.max_inline_bytes > writer.limits.max_inline_bytes
                    {
                        return Err(invalid("invalid or unsupported batch limits"));
                    }
                    validate_recovery(&writer.recovery, recovery, request.policy.durability)?;
                    vec![&spec.read.port, &spec.write.port]
                }
                _ => return Err(invalid("step contract does not match kind")),
            };
            if required.iter().any(|port| !ports.contains_key(port)) {
                return Err(invalid("unbound port"));
            }
            steps.push(PlannedStep {
                definition: step.clone(),
                contract,
            });
        }
        route.push(PlannedManifest {
            id: manifest.id.clone(),
            from: manifest.from.clone(),
            to: manifest.to.clone(),
            steps,
        });
    }
    let mut plan = PlanSnapshot {
        format_version: MODEL_VERSION,
        scope: request.scope.clone(),
        from: current,
        to: request.target.clone(),
        route,
        bindings,
        policy: request.policy.clone(),
        digest: Digest([0; 32]),
    };
    plan.digest = codec::plan_digest(&plan)?;
    Ok(plan)
}

fn validate_recovery(
    cap: &RecoveryCapabilities,
    policy: RecoveryPolicy,
    durability: Durability,
) -> Result<(), N2xError> {
    if durability == Durability::Durable && cap.durability != Durability::Durable {
        return Err(invalid("output cannot provide durable commits"));
    }
    if policy == RecoveryPolicy::RequireAutomatic
        && cap.replay == ReplaySafety::Unsafe
        && !cap.authoritative_lookup
    {
        return Err(invalid("operation has no automatic recovery contract"));
    }
    Ok(())
}

fn paths(edges: &[Manifest], from: &Revision, to: &Revision) -> Vec<Vec<usize>> {
    fn visit(
        edges: &[Manifest],
        current: &Revision,
        to: &Revision,
        seen: &mut BTreeSet<Revision>,
        route: &mut Vec<usize>,
        found: &mut Vec<Vec<usize>>,
    ) {
        if found.len() >= 2 {
            return;
        }
        if current == to {
            found.push(route.clone());
            return;
        }
        if !seen.insert(current.clone()) {
            return;
        }
        for (index, edge) in edges
            .iter()
            .enumerate()
            .filter(|(_, edge)| &edge.from == current)
        {
            route.push(index);
            visit(edges, &edge.to, to, seen, route, found);
            route.pop();
        }
        seen.remove(current);
    }
    let mut found = Vec::new();
    visit(
        edges,
        from,
        to,
        &mut BTreeSet::new(),
        &mut Vec::new(),
        &mut found,
    );
    found
}

fn invalid(message: &str) -> N2xError {
    N2xError::new(
        "n2x.plan.invalid",
        ErrorKind::InvalidPlan,
        ErrorOrigin::Plan,
        message,
    )
}
