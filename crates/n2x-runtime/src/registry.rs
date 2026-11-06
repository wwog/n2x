use crate::*;
use n2x_core::*;
use std::collections::BTreeMap;

/// 端口与 Action 的运行时注册表，不提供具体数据库或文件迁移实现。
#[derive(Default)]
pub struct Registry {
    /// 仅供 Task/Check 使用的资源也可以显式绑定，不必实现 Reader 或 Writer。
    pub resources: Vec<PortBinding>,
    /// 读取实现，按 PortId 注册。
    pub readers: BTreeMap<PortId, Box<dyn Reader>>,
    /// 写入实现，按 PortId 注册。
    pub writers: BTreeMap<PortId, Box<dyn Writer>>,
    /// 副作用实现，按 ActionRef 注册。
    pub tasks: BTreeMap<ActionRef, Box<dyn TaskAction>>,
    /// 只读检查实现。
    pub checks: BTreeMap<ActionRef, Box<dyn CheckAction>>,
    /// 纯转换实现。
    pub transforms: BTreeMap<ActionRef, Box<dyn TransformAction>>,
}

impl Registry {
    /// 汇总并验证相同 PortId 的读写实现确实指向相同资源。
    pub fn bindings(&self) -> Result<Vec<PortBinding>, N2xError> {
        if self
            .readers
            .iter()
            .any(|(id, port)| *id != port.binding().port)
            || self
                .writers
                .iter()
                .any(|(id, port)| *id != port.binding().port)
        {
            return Err(missing("port registration key"));
        }
        let mut bindings = BTreeMap::new();
        for binding in self.resources.iter().cloned().chain(
            self.readers
                .values()
                .map(|port| port.binding())
                .chain(self.writers.values().map(|port| port.binding())),
        ) {
            if bindings
                .get(&binding.port)
                .is_some_and(|old| old != &binding)
            {
                return Err(missing("conflicting resource bindings"));
            }
            bindings.insert(binding.port.clone(), binding);
        }
        Ok(bindings.into_values().collect())
    }

    pub(crate) async fn execute(
        &mut self,
        kind: CommandKind,
        workspace: &mut dyn Workspace,
        clock: &mut dyn Clock,
    ) -> ReplyKind {
        match kind {
            CommandKind::Read(request) => {
                ReplyKind::Read(match self.readers.get_mut(&request.stream.port) {
                    Some(reader) => reader.read(request).await,
                    None => Err(missing("reader")),
                })
            }
            CommandKind::Transform(request) => {
                ReplyKind::Transformed(match self.transforms.get_mut(&request.transform.action) {
                    Some(action) => action.transform(request).await,
                    None => Err(missing("transform")),
                })
            }
            CommandKind::Apply(request) => {
                let failure = || OperationFailure {
                    error: missing("effect executor"),
                    apply_state: ApplyState::NotApplied,
                };
                let result = match &request.effect {
                    EffectRequest::Task(call) => match self.tasks.get_mut(&call.action) {
                        Some(action) => action.apply(request).await,
                        None => Err(failure()),
                    },
                    EffectRequest::Write { target, .. } => match self.writers.get_mut(&target.port)
                    {
                        Some(writer) => writer.apply(request).await,
                        None => Err(failure()),
                    },
                };
                ReplyKind::Applied(result)
            }
            CommandKind::Reconcile(request) => {
                ReplyKind::Reconciled(match &request.request.effect {
                    EffectRequest::Task(call) => match self.tasks.get_mut(&call.action) {
                        Some(action) => action.reconcile(request).await,
                        None => Err(missing("task")),
                    },
                    EffectRequest::Write { target, .. } => match self.writers.get_mut(&target.port)
                    {
                        Some(writer) => writer.reconcile(request).await,
                        None => Err(missing("writer")),
                    },
                })
            }
            CommandKind::Check(call) => {
                ReplyKind::Checked(match self.checks.get_mut(&call.action) {
                    Some(action) => action.check(call).await,
                    None => Err(missing("check")),
                })
            }
            CommandKind::Commit(commit) => ReplyKind::Committed(workspace.commit(commit).await),
            CommandKind::Clock => ReplyKind::Time(clock.now()),
            CommandKind::WakeAt(at) => ReplyKind::Woken(clock.wake_at(at).await),
        }
    }
}

impl ContractResolver for Registry {
    fn resolve(&self, step: &Step) -> Result<StepContract, N2xError> {
        match &step.kind {
            StepKind::Task(call) => {
                let action = self
                    .tasks
                    .get(&call.action)
                    .ok_or_else(|| missing("task"))?;
                if action.action() != call.action {
                    return Err(missing("task revision"));
                }
                Ok(StepContract::Task(action.capabilities(call)?))
            }
            StepKind::Check(call) => {
                if self
                    .checks
                    .get(&call.action)
                    .is_none_or(|action| action.action() != call.action)
                {
                    return Err(missing("check"));
                }
                Ok(StepContract::Check)
            }
            StepKind::Transfer(spec) => {
                if let Some(transform) = &spec.transform
                    && self
                        .transforms
                        .get(&transform.action)
                        .is_none_or(|action| action.action() != transform.action)
                {
                    return Err(missing("transform"));
                }
                Ok(StepContract::Transfer {
                    reader: self
                        .readers
                        .get(&spec.read.port)
                        .ok_or_else(|| missing("reader"))?
                        .capabilities(&spec.read)?,
                    writer: self
                        .writers
                        .get(&spec.write.port)
                        .ok_or_else(|| missing("writer"))?
                        .capabilities(&spec.write)?,
                })
            }
        }
    }
}

pub(crate) fn missing(what: &str) -> N2xError {
    N2xError::new(
        "n2x.registry.missing",
        ErrorKind::InvalidPlan,
        ErrorOrigin::Runtime,
        format!("missing or invalid {what}"),
    )
}
