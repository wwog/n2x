use crate::*;
use n2x_core::*;

/// 组合引擎、注册表与工作区的运行入口；由应用选择异步执行器和平台时钟。
pub struct Migrator {
    manifests: Vec<Manifest>,
    registry: Registry,
    workspace: Box<dyn Workspace>,
    clock: Box<dyn Clock>,
    lock: Box<dyn WorkspaceLock>,
    guard: Option<Box<dyn WorkspaceGuard>>,
    engine: Option<Engine>,
    next: Option<Command>,
    notices: Vec<StoredNotice>,
    durability: Durability,
}

impl Migrator {
    /// 构造迁移服务，数据库和文件业务行为由 Registry 提供。
    pub fn new(
        manifests: Vec<Manifest>,
        registry: Registry,
        workspace: Box<dyn Workspace>,
        clock: Box<dyn Clock>,
        lock: Box<dyn WorkspaceLock>,
    ) -> Self {
        Self {
            manifests,
            registry,
            workspace,
            clock,
            lock,
            guard: None,
            engine: None,
            next: None,
            notices: vec![],
            durability: Durability::Durable,
        }
    }

    /// 使用已取得的所有权构造服务，适合需要先取锁再打开存储的平台组合层。
    pub fn with_guard(mut self, guard: Box<dyn WorkspaceGuard>) -> Self {
        self.guard = Some(guard);
        self
    }

    /// 编译并启动或恢复 Scope 的活动运行，取得的独占锁在本服务生命周期内持有。
    pub async fn start(&mut self, request: RunRequest) -> Result<RunSnapshot, N2xError> {
        if self.next.is_some() {
            return Err(conflict("another command is pending"));
        }
        if self.guard.is_none() {
            self.guard = Some(self.lock.acquire().await?);
        }
        let bindings = self.registry.bindings()?;
        let scope = self.workspace.scope(request.scope.clone()).await?;
        if let Some(scope) = &scope {
            if !scope.bindings.is_empty() && scope.bindings != bindings {
                return Err(conflict("resource bindings changed"));
            }
            if let Some(active) = &scope.active_run {
                let state = self
                    .workspace
                    .load(active.clone())
                    .await?
                    .ok_or_else(|| conflict("active run has no recovery snapshot"))?;
                if state.run.scope != scope.scope || state.run.current != scope.current {
                    return Err(conflict("scope and recovery snapshot disagree"));
                }
                if state.run.target != request.target {
                    return Err(conflict(
                        "resume active run before requesting a different target",
                    ));
                }
                return self.recover_state(state).await;
            }
        }
        if self.workspace.plan(request.run.clone()).await?.is_some() {
            return Err(conflict("RunId already exists"));
        }
        let current = scope
            .map(|scope| scope.current)
            .or(request.initial.clone())
            .ok_or_else(|| conflict("initial revision required for new scope"))?;
        let plan = compile_plan(&self.manifests, &request, current, bindings, &self.registry)?;
        self.durability = plan.policy.durability;
        let mut engine = Engine::default();
        let transition = engine.start(
            EngineStart::New(Box::new(StartInput {
                run: request.run,
                plan,
                now: self.clock.now(),
            })),
            1,
        )?;
        self.engine = Some(engine);
        self.consume(transition);
        Ok(self.snapshot().unwrap().clone())
    }

    async fn recover_state(&mut self, state: RecoverySnapshot) -> Result<RunSnapshot, N2xError> {
        let plan = self
            .workspace
            .plan(state.run.run.clone())
            .await?
            .ok_or_else(|| conflict("saved plan missing"))?;
        let request = RunRequest {
            run: state.run.run.clone(),
            scope: plan.scope.clone(),
            initial: None,
            target: plan.to.clone(),
            route: Route::Explicit(plan.route.iter().map(|edge| edge.id.clone()).collect()),
            policy: plan.policy.clone(),
        };
        let compiled = compile_plan(
            &self.manifests,
            &request,
            plan.from.clone(),
            self.registry.bindings()?,
            &self.registry,
        )?;
        if compiled != plan {
            return Err(N2xError::new(
                ErrorCode::PLAN_CHANGED,
                ErrorKind::Incompatible,
                ErrorOrigin::Runtime,
                "plan or registered behavior changed since run started",
            ));
        }
        let epoch = state
            .epoch
            .checked_add(1)
            .ok_or_else(|| conflict("epoch exhausted"))?;
        self.durability = plan.policy.durability;
        let mut engine = Engine::default();
        let transition = engine.start(
            EngineStart::Recover {
                snapshot: Box::new(state),
                plan: Box::new(plan),
                now: self.clock.now(),
            },
            epoch,
        )?;
        self.engine = Some(engine);
        self.consume(transition);
        Ok(self.snapshot().unwrap().clone())
    }

    /// 执行一条协议命令，适合 UI 单步、宿主事件循环或故障注入。
    ///
    /// 工作区确认失败时保留相同命令，可安全重试；不会继续新业务操作。
    pub async fn tick(&mut self) -> Result<Option<RunSnapshot>, N2xError> {
        let Some(command) = self.next.clone() else {
            return Ok(self.snapshot().cloned());
        };
        let reply = self
            .registry
            .execute(
                command.kind.clone(),
                self.workspace.as_mut(),
                self.clock.as_mut(),
            )
            .await;
        self.engine.as_mut().unwrap().observe_time(self.clock.now());
        let transition = self.engine.as_mut().unwrap().accept(Reply {
            command: command.id,
            kind: reply,
        })?;
        self.consume(transition);
        Ok(self.snapshot().cloned())
    }

    /// 连续驱动直到完成、暂停、失败或需要人工处理，失败报告以 Err 返回。
    pub async fn run(&mut self) -> Result<RunReport, N2xError> {
        while self.next.is_some() {
            self.tick().await?;
        }
        let snapshot = self
            .snapshot()
            .ok_or_else(|| conflict("no run started"))?
            .clone();
        if matches!(
            snapshot.status,
            RunStatus::Failed | RunStatus::NeedsAttention
        ) {
            return Err(snapshot
                .last_error
                .unwrap_or_else(|| conflict("run stopped")));
        }
        self.workspace
            .report(snapshot.run)
            .await?
            .ok_or_else(|| conflict("run paused without a final report"))
    }

    /// 取得当前观察状态。
    pub fn snapshot(&self) -> Option<&RunSnapshot> {
        self.engine.as_ref().and_then(|engine| engine.snapshot())
    }
    /// 下一条命令，用于显示或单步断点；不能绕过 tick 直接应用。
    pub fn pending_command(&self) -> Option<&Command> {
        self.next.as_ref()
    }
    /// 已确认、尚未被调用方取出的关键事件。
    pub fn drain_notices(&mut self) -> Vec<StoredNotice> {
        std::mem::take(&mut self.notices)
    }
    /// 发送暂停、继续、取消或人工核对控制。
    pub fn control(&mut self, control: Control) -> Result<(), N2xError> {
        let transition = self
            .engine
            .as_mut()
            .ok_or_else(|| conflict("no active engine"))?
            .control(control)?;
        if transition.command.is_some() {
            self.consume(transition);
        } else {
            self.notices.extend(transition.committed_notices);
        }
        Ok(())
    }
    /// 查询持久关键事件，支持按 sequence 补读和订阅者 ACK。
    pub async fn events_after(
        &mut self,
        run: RunId,
        after: u64,
        limit: u32,
    ) -> Result<EventPage, N2xError> {
        self.ensure_guard().await?;
        self.workspace.events_after(run, after, limit).await
    }
    /// 查询 Scope 的持久权威状态。
    pub async fn scope(&mut self, scope: ScopeId) -> Result<Option<ScopeState>, N2xError> {
        self.ensure_guard().await?;
        self.workspace.scope(scope).await
    }

    /// 读取重开恢复入口，可用于启动页面显示持久状态。
    pub async fn recovery(&mut self, run: RunId) -> Result<Option<RecoverySnapshot>, N2xError> {
        self.ensure_guard().await?;
        self.workspace.load(run).await
    }

    /// 查询已持久化的标准报告，包含业务失败与待处理结果。
    pub async fn report(&mut self, run: RunId) -> Result<Option<RunReport>, N2xError> {
        self.ensure_guard().await?;
        self.workspace.report(run).await
    }

    /// 注册可靠订阅起点。容量耗尽时 tick 返回背压错误，ACK 后可重试同一提交。
    pub async fn subscribe(&mut self, id: CommitId, ack: NoticeAck) -> Result<(), N2xError> {
        self.notice_mutation(id, WorkspaceMutation::SubscribeNotice(ack))
            .await
    }

    /// 确认订阅者已处理的连续事件前缀。
    pub async fn acknowledge(&mut self, id: CommitId, ack: NoticeAck) -> Result<(), N2xError> {
        self.notice_mutation(id, WorkspaceMutation::AckNotice(ack))
            .await
    }

    /// 释放可靠订阅及其历史保护。
    pub async fn unsubscribe(
        &mut self,
        id: CommitId,
        run: RunId,
        subscriber: SubscriberId,
    ) -> Result<(), N2xError> {
        self.notice_mutation(id, WorkspaceMutation::UnsubscribeNotice { run, subscriber })
            .await
    }

    async fn notice_mutation(
        &mut self,
        id: CommitId,
        mutation: WorkspaceMutation,
    ) -> Result<(), N2xError> {
        if self.guard.is_none() {
            self.guard = Some(self.lock.acquire().await?);
        }
        let mut commit = WorkspaceCommit {
            id,
            digest: Digest([0; 32]),
            durability: self.durability,
            mutations: vec![mutation],
        };
        commit.digest = codec::commit_digest(&commit)?;
        self.workspace.commit(commit).await?;
        Ok(())
    }

    async fn ensure_guard(&mut self) -> Result<(), N2xError> {
        if self.guard.is_none() { self.guard = Some(self.lock.acquire().await?); }
        Ok(())
    }

    fn consume(&mut self, transition: Transition) {
        self.next = transition.command;
        self.notices.extend(transition.committed_notices);
    }
}

fn conflict(message: &str) -> N2xError {
    N2xError::new(
        "n2x.run.conflict",
        ErrorKind::Conflict,
        ErrorOrigin::Runtime,
        message,
    )
}
