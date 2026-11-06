//! 顺序、可恢复的标准管线调度机。所有外部效果均等待 Intent 的持久确认。
use crate::*;

#[derive(Clone)]
enum AfterCommit {
    Drive,
    Apply,
    Reconcile,
    Wait(Timestamp),
    Stop,
    Check(ActionCall),
}

/// 可由任意宿主逐命令驱动的 sans-IO 调度机。
#[derive(Default)]
pub struct Engine {
    plan: Option<PlanSnapshot>,
    state: Option<RecoverySnapshot>,
    expected: Option<Command>,
    after_commit: Option<AfterCommit>,
    now: Option<Timestamp>,
    sequence: u64,
    control: Option<Control>,
    read_batch: Option<ReadBatch>,
    published: Option<RunSnapshot>,
}

impl Engine {
    /// 当前观察状态；持久权威仍以 Workspace 为准。
    pub fn snapshot(&self) -> Option<&RunSnapshot> {
        self.published.as_ref()
    }

    /// 接受宿主提供的时间观察；本执行代内防止系统时钟回退扩大重试预算。
    pub fn observe_time(&mut self, now: Timestamp) {
        self.now = Some(Timestamp(self.now.map_or(now.0, |old| old.0.max(now.0))));
    }

    fn state(&self) -> &RecoverySnapshot {
        self.state.as_ref().expect("started")
    }
    fn state_mut(&mut self) -> &mut RecoverySnapshot {
        self.state.as_mut().expect("started")
    }
    fn plan(&self) -> &PlanSnapshot {
        self.plan.as_ref().expect("started")
    }
    fn time(&self) -> Timestamp {
        self.now.expect("started")
    }

    fn emit(&mut self, kind: CommandKind) -> Transition {
        self.sequence += 1;
        let command = Command {
            id: CommandId {
                epoch: self.state().epoch,
                sequence: self.sequence,
                attempt: self
                    .state()
                    .pending
                    .as_ref()
                    .map_or(1, |intent| intent.attempts),
            },
            kind,
        };
        self.expected = Some(command.clone());
        Transition {
            command: Some(command),
            ..Default::default()
        }
    }

    fn persist(
        &mut self,
        kinds: Vec<NoticeKind>,
        mut extra: Vec<WorkspaceMutation>,
        after: AfterCommit,
    ) -> Result<Transition, N2xError> {
        let mut notices = Vec::new();
        let mut kinds = kinds;
        if self.state().run.progress.confirmed != self.state().checkpoint.confirmed {
            kinds.push(NoticeKind::ProgressChanged(Progress {
                observed: self.state().checkpoint.confirmed.clone(),
                confirmed: self.state().checkpoint.confirmed.clone(),
                estimated_input: None,
            }));
        }
        for kind in kinds {
            self.state_mut().run.last_sequence += 1;
            notices.push(StoredNotice {
                sequence: self.state().run.last_sequence,
                notice: Notice {
                    run: self.state().run.run.clone(),
                    scope: self.state().run.scope.clone(),
                    at: self.time(),
                    kind,
                },
            });
        }
        self.state_mut().run.progress.confirmed = self.state().checkpoint.confirmed.clone();
        self.state_mut().run.progress.observed = self.state().checkpoint.confirmed.clone();
        extra.push(WorkspaceMutation::SaveRecovery(Box::new(
            self.state().clone(),
        )));
        for notice in notices {
            extra.push(WorkspaceMutation::AppendNotice(notice));
        }
        let mut commit = WorkspaceCommit {
            id: format!(
                "{}/e{}/c{}",
                self.state().run.run,
                self.state().epoch,
                self.sequence + 1
            )
            .into(),
            digest: Digest([0; 32]),
            durability: self.plan().policy.durability,
            mutations: extra,
        };
        commit.digest = codec::commit_digest(&commit)?;
        self.after_commit = Some(after);
        Ok(self.emit(CommandKind::Commit(commit)))
    }

    fn scope(&self, active: bool, edge: Option<usize>) -> ScopeState {
        let last = edge.map(|index| {
            let edge = &self.plan().route[index];
            RevisionCommit {
                run: self.state().run.run.clone(),
                plan: self.plan().digest,
                manifest: edge.id.clone(),
                from: edge.from.clone(),
                to: edge.to.clone(),
            }
        });
        ScopeState {
            scope: self.plan().scope.clone(),
            current: self.state().run.current.clone(),
            active_run: active.then(|| self.state().run.run.clone()),
            bindings: self.plan().bindings.clone(),
            last_revision_commit: last,
        }
    }

    fn step(&self) -> Result<(StepAddress, PlannedStep), N2xError> {
        match self.state().checkpoint.position {
            ExecutionPosition::Step { edge, step } => {
                let manifest = self
                    .plan()
                    .route
                    .get(edge as usize)
                    .ok_or_else(|| invalid("edge out of range"))?;
                let planned = manifest
                    .steps
                    .get(step as usize)
                    .ok_or_else(|| invalid("step out of range"))?;
                Ok((
                    StepAddress {
                        manifest: manifest.id.clone(),
                        step: planned.definition.id.clone(),
                    },
                    planned.clone(),
                ))
            }
            _ => Err(invalid("not at a step")),
        }
    }

    fn next_step(&mut self) {
        if let ExecutionPosition::Step { edge, step } = self.state().checkpoint.position {
            self.state_mut().checkpoint.position =
                if (step as usize) + 1 < self.plan().route[edge as usize].steps.len() {
                    ExecutionPosition::Step {
                        edge,
                        step: step + 1,
                    }
                } else {
                    ExecutionPosition::AdvanceRevision { edge }
                };
        }
        self.state_mut().checkpoint.transfer = None;
        self.state_mut().run.step = None;
    }

    fn drive(&mut self) -> Result<Transition, N2xError> {
        if let Some(OperationFailure {
            apply_state: ApplyState::Applied(receipt),
            ..
        }) = self
            .state()
            .pending
            .as_ref()
            .and_then(|intent| intent.last_failure.clone())
        {
            return self.applied(receipt);
        }
        if let Some(control) = self.control.take() {
            if self.state().pending.is_some() {
                self.control = Some(control);
            } else {
                match control {
                    Control::Pause => {
                        self.state_mut().run.status = RunStatus::Paused;
                        return self.persist(
                            vec![NoticeKind::RunPaused],
                            vec![],
                            AfterCommit::Stop,
                        );
                    }
                    Control::Cancel => {
                        self.state_mut().run.status = RunStatus::Cancelled;
                        return self
                            .terminate(RunOutcome::Cancelled, vec![NoticeKind::RunCancelled]);
                    }
                    _ => {}
                }
            }
        }
        if self
            .state()
            .pending
            .as_ref()
            .and_then(|intent| intent.last_failure.as_ref())
            .is_some_and(|failure| failure.apply_state == ApplyState::NotApplied)
        {
            let failure = self.state().pending.as_ref().unwrap().last_failure.as_ref().unwrap();
            if failure.error.retry_hint == RetryHint::Never {
                return self.fail(failure.error.clone(), false);
            }
            return self.retry(false);
        }
        if self.state().pending.is_some() {
            return self.reconcile();
        }
        match self.state().checkpoint.position.clone() {
            ExecutionPosition::Finished => {
                self.state_mut().run.status = RunStatus::Completed;
                let outcome = if self.state().checkpoint.confirmed.rejected > 0 {
                    RunOutcome::CompletedWithRejections
                } else {
                    RunOutcome::Completed
                };
                self.terminate(outcome.clone(), vec![NoticeKind::RunCompleted(outcome)])
            }
            ExecutionPosition::AdvanceRevision { edge } => {
                let manifest = self.plan().route[edge as usize].clone();
                self.state_mut().run.current = manifest.to.clone();
                self.state_mut().checkpoint.position =
                    if edge as usize + 1 == self.plan().route.len() {
                        ExecutionPosition::Finished
                    } else {
                        self.first_position(edge + 1)
                    };
                self.persist(
                    vec![NoticeKind::RevisionAdvanced {
                        manifest: manifest.id,
                        revision: manifest.to,
                    }],
                    vec![WorkspaceMutation::SaveScope(
                        self.scope(true, Some(edge as usize)),
                    )],
                    AfterCommit::Drive,
                )
            }
            ExecutionPosition::Step { .. } => {
                let (address, step) = self.step()?;
                self.state_mut().run.step = Some(address.clone());
                match step.definition.kind {
                    StepKind::Task(call) => {
                        let request = self.make_request(EffectRequest::Task(call), 0, 0)?;
                        self.state_mut().pending = Some(Intent {
                            request,
                            created_at: self.time(),
                            attempts: 1,
                            last_failure: None,
                        });
                        self.persist(
                            vec![NoticeKind::StepStarted(address)],
                            vec![],
                            AfterCommit::Apply,
                        )
                    }
                    StepKind::Check(call) => self.persist(
                        vec![NoticeKind::StepStarted(address)],
                        vec![],
                        AfterCommit::Check(call),
                    ),
                    StepKind::Transfer(spec) => {
                        if self.state().checkpoint.transfer.is_none() {
                            self.state_mut().checkpoint.transfer = Some(TransferCheckpoint {
                                batch: 0,
                                cursor: None,
                                source: None,
                                transform_state: None,
                                prepared: None,
                            });
                            return self.persist(
                                vec![NoticeKind::StepStarted(address)],
                                vec![],
                                AfterCommit::Drive,
                            );
                        }
                        let transfer = self.state().checkpoint.transfer.as_ref().unwrap().clone();
                        if let Some(prepared) = transfer.prepared {
                            if prepared.receipts.len() == prepared.units.len() {
                                return self.finish_batch();
                            }
                            let request = prepared.units[prepared.receipts.len()].clone();
                            self.state_mut().pending = Some(Intent {
                                request,
                                created_at: self.time(),
                                attempts: 1,
                                last_failure: None,
                            });
                            return self.persist(vec![], vec![], AfterCommit::Apply);
                        }
                        Ok(self.emit(CommandKind::Read(ReadRequest {
                            step: address,
                            stream: spec.read,
                            cursor: transfer.cursor,
                            source: transfer.source,
                            consistency: spec.consistency,
                            limits: spec.read_limits,
                        })))
                    }
                }
            }
        }
    }

    fn first_position(&self, edge: u32) -> ExecutionPosition {
        if self.plan().route[edge as usize].steps.is_empty() {
            ExecutionPosition::AdvanceRevision { edge }
        } else {
            ExecutionPosition::Step { edge, step: 0 }
        }
    }

    fn make_request(
        &self,
        effect: EffectRequest,
        batch: u64,
        unit: usize,
    ) -> Result<ApplyRequest, N2xError> {
        let (step, _) = self.step()?;
        let hash = codec::digest(&(self.state().run.run.clone(), step.clone(), batch, unit))?;
        let id = hash
            .0
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        Ok(ApplyRequest {
            context: OperationContext {
                id: id.into(),
                run: self.state().run.run.clone(),
                step,
                plan: self.plan().digest,
                request_digest: codec::digest(&effect)?,
            },
            effect,
        })
    }

    fn applied(&mut self, receipt: Receipt) -> Result<Transition, N2xError> {
        let pending = self
            .state()
            .pending
            .as_ref()
            .ok_or_else(|| invalid("receipt without pending intent"))?;
        if receipt.operation != pending.request.context.id
            || receipt.request_digest != pending.request.context.request_digest
        {
            return self.fail(invalid("receipt does not match intent"), true);
        }
        self.state_mut().pending = None;
        if let Some(transfer) = self.state_mut().checkpoint.transfer.as_mut() {
            transfer
                .prepared
                .as_mut()
                .ok_or_else(|| invalid("missing prepared batch"))?
                .receipts
                .push(receipt);
            self.persist(vec![], vec![], AfterCommit::Drive)
        } else {
            let (address, _) = self.step()?;
            self.next_step();
            self.persist(
                vec![NoticeKind::StepCompleted(address)],
                vec![],
                AfterCommit::Drive,
            )
        }
    }

    fn prepare(&mut self, result: TransformResult) -> Result<Transition, N2xError> {
        let batch = self
            .read_batch
            .take()
            .ok_or_else(|| invalid("missing input batch"))?;
        let (_, step) = self.step()?;
        let StepKind::Transfer(spec) = step.definition.kind else {
            return Err(invalid("not a transfer"));
        };
        let rejected = result.rejected.len() as u64;
        if result.items.len() > spec.read_limits.max_items.get() as usize
            || codec::canonical(&result)?.len() as u64 > spec.read_limits.max_inline_bytes
        {
            return self.fail(
                invalid("transform exceeds declared output limits; use smaller inputs or BlobRef"),
                false,
            );
        }
        if result.filtered + rejected > batch.items.len() as u64 {
            return self.fail(invalid("invalid transform input counters"), false);
        }
        if rejected > 0 {
            match spec.invalid_data {
                InvalidDataPolicy::Stop => {
                    return self.fail(result.rejected[0].error.clone(), false);
                }
                InvalidDataPolicy::Quarantine { max_records }
                    if self.state().checkpoint.confirmed.rejected + rejected > max_records =>
                {
                    return self.fail(invalid("quarantine limit exceeded"), false);
                }
                _ => {}
            }
        }
        let batch_number = self.state().checkpoint.transfer.as_ref().unwrap().batch;
        let mut groups: Vec<Vec<Envelope>> = Vec::new();
        let mut group = Vec::new();
        let mut bytes = 0;
        let written = result.items.len() as u64;
        let mut written_bytes = 0;
        for item in result.items {
            let size = codec::canonical(&item)?.len() as u64;
            if size > spec.write_limits.max_inline_bytes {
                return self.fail(invalid("item exceeds write byte limit; use BlobRef"), false);
            }
            if !group.is_empty()
                && (group.len() >= spec.write_limits.max_items.get() as usize
                    || bytes + size > spec.write_limits.max_inline_bytes)
            {
                groups.push(std::mem::take(&mut group));
                bytes = 0;
            }
            bytes += size;
            written_bytes += payload_bytes(&item);
            group.push(item);
        }
        if !group.is_empty() {
            groups.push(group);
        }
        let units = groups
            .into_iter()
            .enumerate()
            .map(|(index, items)| {
                self.make_request(
                    EffectRequest::Write {
                        target: spec.write.clone(),
                        items,
                    },
                    batch_number,
                    index,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let prepared = PreparedBatch {
            next_cursor: batch.next,
            eof: batch.eof,
            units,
            receipts: vec![],
            rejected: result.rejected,
            delta: Counters {
                read: batch.items.len() as u64,
                written,
                filtered: result.filtered,
                rejected,
                bytes_read: batch.items.iter().map(payload_bytes).sum(),
                bytes_written: written_bytes,
            },
            next_transform_state: result.next_state,
        };
        self.state_mut()
            .checkpoint
            .transfer
            .as_mut()
            .unwrap()
            .prepared = Some(prepared);
        self.persist(vec![], vec![], AfterCommit::Drive)
    }

    fn finish_batch(&mut self) -> Result<Transition, N2xError> {
        let (address, _) = self.step()?;
        let transfer = self.state_mut().checkpoint.transfer.as_mut().unwrap();
        let prepared = transfer.prepared.take().unwrap();
        let mut extra = vec![];
        if !prepared.rejected.is_empty() {
            extra.push(WorkspaceMutation::Quarantine {
                run: self.state().run.run.clone(),
                step: address.clone(),
                batch: self.state().checkpoint.transfer.as_ref().unwrap().batch,
                records: prepared.rejected,
            });
        }
        let transfer = self.state_mut().checkpoint.transfer.as_mut().unwrap();
        transfer.cursor = prepared.next_cursor;
        transfer.transform_state = prepared.next_transform_state;
        transfer.batch += 1;
        add(&mut self.state_mut().checkpoint.confirmed, &prepared.delta);
        let mut notices = vec![];
        if prepared.eof {
            self.next_step();
            notices.push(NoticeKind::StepCompleted(address));
        }
        self.persist(notices, extra, AfterCommit::Drive)
    }

    fn recovery_cap(&self) -> Result<RecoveryCapabilities, N2xError> {
        match self.step()?.1.contract {
            StepContract::Task(cap) => Ok(cap),
            StepContract::Transfer { writer, .. } => Ok(writer.recovery),
            _ => Err(invalid("no effect contract")),
        }
    }

    fn reconcile(&mut self) -> Result<Transition, N2xError> {
        let cap = self.recovery_cap()?;
        let intent = self.state().pending.as_ref().unwrap().clone();
        if cap.authoritative_lookup {
            Ok(self.emit(CommandKind::Reconcile(ReconcileRequest {
                request: intent.request,
            })))
        } else if cap.replay != ReplaySafety::Unsafe {
            self.retry(true)
        } else {
            self.fail(
                N2xError::new(
                    ErrorCode::OUTCOME_UNKNOWN,
                    ErrorKind::Conflict,
                    ErrorOrigin::Engine,
                    "operation requires manual reconciliation",
                ),
                true,
            )
        }
    }

    fn retry(&mut self, unknown: bool) -> Result<Transition, N2xError> {
        let (_, step) = self.step()?;
        let policy = step
            .definition
            .policy
            .retry
            .as_ref()
            .unwrap_or(&self.plan().policy.retry)
            .clone();
        let intent = self.state().pending.as_ref().unwrap().clone();
        let elapsed = self.time().0.saturating_sub(intent.created_at.0).max(0) as u128;
        if let ReplaySafety::Deduplicated {
            retention: Some(retention),
        } = self.recovery_cap()?.replay
            && elapsed >= retention.as_millis()
        {
            return self.fail(
                N2xError::new(
                    "n2x.operation.dedup_expired",
                    ErrorKind::Conflict,
                    ErrorOrigin::Engine,
                    "deduplication retention expired",
                ),
                true,
            );
        }
        if intent.attempts >= policy.max_attempts.get() || elapsed >= policy.budget.as_millis() {
            return self.fail(
                N2xError::new(
                    ErrorCode::RETRY_EXHAUSTED,
                    ErrorKind::Unavailable,
                    ErrorOrigin::Engine,
                    "retry attempts or time budget exhausted",
                ),
                unknown,
            );
        }
        let factor = 1u32
            .checked_shl(intent.attempts.saturating_sub(1).min(20))
            .unwrap_or(u32::MAX);
        let mut delay = policy
            .initial_delay
            .saturating_mul(factor)
            .min(policy.max_delay);
        if let Some(OperationFailure {
            error:
                N2xError {
                    retry_hint: RetryHint::After(hint),
                    ..
                },
            ..
        }) = &intent.last_failure
        {
            delay = delay.max(*hint);
        }
        if elapsed + delay.as_millis() > policy.budget.as_millis() {
            return self.fail(
                N2xError::new(
                    ErrorCode::RETRY_EXHAUSTED,
                    ErrorKind::Unavailable,
                    ErrorOrigin::Engine,
                    "retry exceeds time budget",
                ),
                unknown,
            );
        }
        self.state_mut().run.status = RunStatus::Waiting(WaitReason::Retry {
            operation: intent.request.context.id.clone(),
            delay,
        });
        let at = Timestamp(
            self.time()
                .0
                .saturating_add(delay.as_millis().min(i64::MAX as u128) as i64),
        );
        self.persist(
            vec![NoticeKind::RetryScheduled {
                operation: intent.request.context.id,
                attempt: intent.attempts + 1,
                delay,
            }],
            vec![],
            AfterCommit::Wait(at),
        )
    }

    fn fail(&mut self, mut error: N2xError, attention: bool) -> Result<Transition, N2xError> {
        error.context.location.scope = Some(self.state().run.scope.clone());
        error.context.location.run = Some(self.state().run.run.clone());
        error.context.location.step = self.state().run.step.clone();
        if let Some(intent) = &self.state().pending {
            error.context.location.operation = Some(intent.request.context.id.clone());
            error.context.location.attempt = Some(intent.attempts);
        }
        self.state_mut().run.last_error = Some(error.clone());
        self.state_mut().run.status = if attention {
            RunStatus::NeedsAttention
        } else {
            RunStatus::Failed
        };
        let kind = if attention {
            NoticeKind::RunNeedsAttention(error)
        } else {
            NoticeKind::RunFailed(error)
        };
        let outcome = if attention {
            RunOutcome::NeedsAttention
        } else {
            RunOutcome::Failed
        };
        self.terminate(outcome, vec![kind])
    }

    fn terminate(
        &mut self,
        outcome: RunOutcome,
        kinds: Vec<NoticeKind>,
    ) -> Result<Transition, N2xError> {
        let done = matches!(
            outcome,
            RunOutcome::Completed | RunOutcome::CompletedWithRejections
        );
        let mut snapshot = self.state().run.clone();
        snapshot.last_sequence += kinds.len() as u64;
        let report = RunReport {
            snapshot,
            outcome,
            started_at: self.state().started_at,
            finished_at: self.time(),
            evidence: None,
        };
        let last = self
            .plan()
            .route
            .iter()
            .position(|edge| edge.to == self.state().run.current);
        self.persist(
            kinds,
            vec![
                WorkspaceMutation::SaveScope(self.scope(!done, last)),
                WorkspaceMutation::SaveReport(Box::new(report)),
            ],
            AfterCommit::Stop,
        )
    }
}

impl PipelineEngine for Engine {
    fn start(&mut self, start: EngineStart, epoch: u64) -> Result<Transition, N2xError> {
        if self.state.is_some() {
            return Err(invalid("engine already started"));
        }
        let (plan, state, now, recovered) = match start {
            EngineStart::New(start) => {
                let plan = start.plan;
                let state = RecoverySnapshot {
                    format_version: MODEL_VERSION,
                    epoch,
                    run: RunSnapshot {
                        run: start.run,
                        scope: plan.scope.clone(),
                        plan: plan.digest,
                        status: RunStatus::Running,
                        current: plan.from.clone(),
                        target: plan.to.clone(),
                        step: None,
                        progress: Progress::default(),
                        last_sequence: 0,
                        last_error: None,
                    },
                    checkpoint: Checkpoint {
                        position: ExecutionPosition::Finished,
                        transfer: None,
                        confirmed: Counters::default(),
                    },
                    pending: None,
                    started_at: start.now,
                };
                (plan, state, start.now, false)
            }
            EngineStart::Recover {
                snapshot,
                plan,
                now,
            } => {
                if epoch <= snapshot.epoch
                    || snapshot.format_version != MODEL_VERSION
                    || snapshot.run.plan != plan.digest
                {
                    return Err(invalid("invalid recovery epoch/model/plan"));
                }
                (*plan, *snapshot, now, true)
            }
        };
        if codec::plan_digest(&plan)? != plan.digest || plan.format_version != MODEL_VERSION {
            return Err(invalid("plan digest/model mismatch"));
        }
        if recovered {
            validate_recovery(&plan, &state)?;
        }
        self.plan = Some(plan);
        self.state = Some(state);
        self.now = Some(now);
        self.state_mut().epoch = epoch;
        if !recovered {
            self.state_mut().checkpoint.position = if self.plan().route.is_empty() {
                ExecutionPosition::Finished
            } else {
                self.first_position(0)
            };
        }
        self.state_mut().run.status = RunStatus::Running;
        self.state_mut().run.last_error = None;
        let mut initial = self.state().run.clone();
        if !recovered {
            initial.status = RunStatus::Ready;
        }
        self.published = Some(initial);
        let last = self
            .plan()
            .route
            .iter()
            .position(|edge| edge.to == self.state().run.current);
        let mut extra = vec![WorkspaceMutation::SaveScope(self.scope(true, last))];
        if !recovered {
            extra.push(WorkspaceMutation::SavePlan {
                run: self.state().run.run.clone(),
                plan: Box::new(self.plan().clone()),
            });
        }
        self.persist(
            vec![if recovered {
                NoticeKind::RunRecovering
            } else {
                NoticeKind::RunStarted
            }],
            extra,
            AfterCommit::Drive,
        )
    }

    fn accept(&mut self, reply: Reply) -> Result<Transition, N2xError> {
        let expected = self
            .expected
            .as_ref()
            .ok_or_else(|| invalid("unexpected reply"))?
            .clone();
        if expected.id != reply.command {
            return Err(invalid("stale or mismatched reply"));
        }
        let compatible = matches!(
            (&expected.kind, &reply.kind),
            (CommandKind::Commit(_), ReplyKind::Committed(_))
                | (CommandKind::Read(_), ReplyKind::Read(_))
                | (CommandKind::Transform(_), ReplyKind::Transformed(_))
                | (CommandKind::Apply(_), ReplyKind::Applied(_))
                | (CommandKind::Reconcile(_), ReplyKind::Reconciled(_))
                | (CommandKind::Check(_), ReplyKind::Checked(_))
                | (CommandKind::Clock, ReplyKind::Time(_))
                | (CommandKind::WakeAt(_), ReplyKind::Woken(_))
        );
        if !compatible {
            return Err(invalid("reply kind does not match command"));
        }
        self.expected = None;
        match reply.kind {
            ReplyKind::Committed(result) => {
                let CommandKind::Commit(commit) = expected.kind else {
                    unreachable!()
                };
                let receipt = match result {
                    Ok(receipt) => receipt,
                    Err(error) => {
                        self.expected = Some(Command {
                            id: expected.id,
                            kind: CommandKind::Commit(commit),
                        });
                        return Err(error);
                    }
                };
                if receipt.id != commit.id
                    || receipt.digest != commit.digest
                    || (commit.durability == Durability::Durable
                        && receipt.durability != Durability::Durable)
                {
                    self.expected = Some(Command {
                        id: expected.id,
                        kind: CommandKind::Commit(commit),
                    });
                    return Err(invalid("invalid workspace receipt"));
                }
                for mutation in &commit.mutations {
                    if let WorkspaceMutation::SaveRecovery(state) = mutation {
                        self.published = Some(state.run.clone());
                    }
                }
                let notices = commit
                    .mutations
                    .iter()
                    .filter_map(|mutation| {
                        if let WorkspaceMutation::AppendNotice(notice) = mutation {
                            Some(notice.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                let mut transition = match self
                    .after_commit
                    .take()
                    .ok_or_else(|| invalid("missing commit continuation"))?
                {
                    AfterCommit::Drive => self.drive()?,
                    AfterCommit::Apply => self.emit(CommandKind::Apply(
                        self.state().pending.as_ref().unwrap().request.clone(),
                    )),
                    AfterCommit::Reconcile => self.reconcile()?,
                    AfterCommit::Wait(at) => self.emit(CommandKind::WakeAt(at)),
                    AfterCommit::Stop => Transition::default(),
                    AfterCommit::Check(call) => self.emit(CommandKind::Check(call)),
                };
                transition.committed_notices = notices;
                Ok(transition)
            }
            ReplyKind::Read(result) => match result {
                Err(error) => self.fail(error, false),
                Ok(batch) => {
                    let (address, step) = self.step()?;
                    let StepKind::Transfer(spec) = step.definition.kind else {
                        unreachable!()
                    };
                    let previous = self.state().checkpoint.transfer.as_ref().unwrap();
                    if batch.items.len() > spec.read_limits.max_items.get() as usize
                        || codec::canonical(&batch.items)?.len() as u64
                            > spec.read_limits.max_inline_bytes
                        || batch.source.consistency != spec.consistency
                        || (!batch.eof && (batch.next.is_none() || batch.next == previous.cursor))
                        || previous
                            .source
                            .as_ref()
                            .is_some_and(|source| source != &batch.source)
                        || self
                            .plan()
                            .bindings
                            .iter()
                            .find(|binding| binding.port == spec.read.port)
                            .is_none_or(|binding| binding.resource != batch.source.resource)
                    {
                        return self
                            .fail(invalid("invalid, changed or oversized read batch"), false);
                    }
                    self.state_mut()
                        .checkpoint
                        .transfer
                        .as_mut()
                        .unwrap()
                        .source = Some(batch.source.clone());
                    self.read_batch = Some(batch.clone());
                    if let Some(transform) = spec.transform {
                        Ok(self.emit(CommandKind::Transform(TransformRequest {
                            step: address,
                            transform,
                            items: batch.items,
                            state: self
                                .state()
                                .checkpoint
                                .transfer
                                .as_ref()
                                .unwrap()
                                .transform_state
                                .clone(),
                            limits: spec.read_limits,
                        })))
                    } else {
                        self.prepare(TransformResult {
                            items: batch.items,
                            filtered: 0,
                            rejected: vec![],
                            next_state: None,
                        })
                    }
                }
            },
            ReplyKind::Transformed(result) => match result {
                Ok(result) => self.prepare(result),
                Err(error) => self.fail(error, false),
            },
            ReplyKind::Checked(result) => match result {
                Ok(_) => {
                    let (address, _) = self.step()?;
                    self.next_step();
                    self.persist(
                        vec![NoticeKind::StepCompleted(address)],
                        vec![],
                        AfterCommit::Drive,
                    )
                }
                Err(error) => self.fail(error, false),
            },
            ReplyKind::Applied(result) => match result {
                Ok(receipt) => self.applied(receipt),
                Err(failure) => {
                    self.state_mut().pending.as_mut().unwrap().last_failure = Some(failure.clone());
                    match failure.apply_state {
                        ApplyState::Applied(receipt) => self.applied(receipt),
                        ApplyState::NotApplied => {
                            if failure.error.retry_hint == RetryHint::Never {
                                self.fail(failure.error, false)
                            } else {
                                self.retry(false)
                            }
                        }
                        ApplyState::Unknown => {
                            let id = self
                                .state()
                                .pending
                                .as_ref()
                                .unwrap()
                                .request
                                .context
                                .id
                                .clone();
                            self.persist(
                                vec![NoticeKind::ReconciliationRequired(id)],
                                vec![],
                                AfterCommit::Reconcile,
                            )
                        }
                    }
                }
            },
            ReplyKind::Reconciled(result) => match result {
                Ok(ReconcileResult::Applied(receipt)) => self.applied(receipt),
                Ok(ReconcileResult::NotApplied) => {
                    let failure = self.state().pending.as_ref().unwrap().last_failure.clone();
                    if failure
                        .as_ref()
                        .is_some_and(|failure| failure.error.retry_hint == RetryHint::Never)
                    {
                        self.fail(failure.unwrap().error, false)
                    } else {
                        self.retry(false)
                    }
                }
                Ok(ReconcileResult::InFlight) => self.fail(
                    N2xError::new(
                        ErrorCode::OUTCOME_UNKNOWN,
                        ErrorKind::Conflict,
                        ErrorOrigin::Engine,
                        "original operation is still in flight; retry recovery later",
                    ),
                    true,
                ),
                Ok(ReconcileResult::Unknown) | Err(_) => {
                    if self.recovery_cap()?.replay != ReplaySafety::Unsafe {
                        self.retry(true)
                    } else {
                        self.fail(
                            N2xError::new(
                                ErrorCode::OUTCOME_UNKNOWN,
                                ErrorKind::Conflict,
                                ErrorOrigin::Engine,
                                "cannot establish authoritative operation outcome",
                            ),
                            true,
                        )
                    }
                }
            },
            ReplyKind::Woken(Err(error)) => self.fail(error, true),
            ReplyKind::Woken(Ok(now)) => {
                self.observe_time(now);
                if let CommandKind::WakeAt(at) = expected.kind
                    && now < at
                {
                    return Ok(self.emit(CommandKind::WakeAt(at)));
                }
                let (_, step) = self.step()?;
                let policy = step.definition.policy.retry.as_ref().unwrap_or(&self.plan().policy.retry);
                let intent = self.state().pending.as_ref().unwrap();
                let elapsed = self.time().0.saturating_sub(intent.created_at.0).max(0) as u128;
                if elapsed >= policy.budget.as_millis() {
                    return self.fail(N2xError::new(ErrorCode::RETRY_EXHAUSTED, ErrorKind::Unavailable,
                        ErrorOrigin::Engine, "retry timer exceeded time budget"), true);
                }
                if let ReplaySafety::Deduplicated { retention: Some(retention) } = self.recovery_cap()?.replay
                    && elapsed >= retention.as_millis()
                {
                    return self.fail(N2xError::new("n2x.operation.dedup_expired", ErrorKind::Conflict,
                        ErrorOrigin::Engine, "deduplication retention expired while waiting"), true);
                }
                self.state_mut().run.status = RunStatus::Running;
                self.state_mut().pending.as_mut().unwrap().attempts += 1;
                self.persist(vec![], vec![], AfterCommit::Apply)
            }
            ReplyKind::Time(now) => {
                self.observe_time(now);
                self.drive()
            }
        }
    }

    fn control(&mut self, control: Control) -> Result<Transition, N2xError> {
        if self.state.is_none() {
            return Err(invalid("engine not started"));
        }
        if self.expected.is_some() {
            match control {
                Control::Pause | Control::Cancel => {
                    self.control = Some(control);
                    return Ok(Transition::default());
                }
                _ => return Err(invalid("command still in flight")),
            }
        }
        match control {
            Control::Resolve(resolution) => {
                if self
                    .state()
                    .pending
                    .as_ref()
                    .is_none_or(|intent| intent.request.context.id != resolution.operation)
                    || resolution.reason.is_empty()
                {
                    return Err(invalid("invalid resolution"));
                }
                match resolution.decision.clone() {
                    ResolutionDecision::Applied(receipt) => {
                        let request = &self.state().pending.as_ref().unwrap().request;
                        if receipt.operation != request.context.id
                            || receipt.request_digest != request.context.request_digest
                        {
                            return Err(invalid("resolution receipt does not match intent"));
                        }
                        // Audit must be committed before applying the resolution to the checkpoint.
                        self.state_mut().pending.as_mut().unwrap().last_failure =
                            Some(OperationFailure {
                                error: N2xError::new(
                                    "n2x.operation.resolved",
                                    ErrorKind::Conflict,
                                    ErrorOrigin::Runtime,
                                    resolution.reason.clone(),
                                ),
                                apply_state: ApplyState::Applied(receipt),
                            });
                    }
                    ResolutionDecision::NotApplied => {
                        self.state_mut().pending.as_mut().unwrap().last_failure = None;
                    }
                }
                self.state_mut().run.status = RunStatus::Running;
                let after = match resolution.decision {
                    ResolutionDecision::Applied(_) => AfterCommit::Drive,
                    ResolutionDecision::NotApplied => AfterCommit::Apply,
                };
                self.state_mut().run.last_error = None;
                self.persist(
                    vec![NoticeKind::OperationResolved(resolution)],
                    vec![],
                    after,
                )
            }
            Control::Resume => {
                if matches!(self.state().run.status, RunStatus::Completed) {
                    return Err(invalid("terminal run cannot resume"));
                }
                let now = self.time();
                if let Some(intent) = self.state_mut().pending.as_mut()
                    && intent
                        .last_failure
                        .as_ref()
                        .is_some_and(|failure| failure.apply_state == ApplyState::NotApplied)
                {
                    intent.attempts = 0;
                    intent.created_at = now;
                    intent.last_failure.as_mut().unwrap().error.retry_hint = RetryHint::Transient;
                }
                self.state_mut().run.status = RunStatus::Running;
                self.state_mut().run.last_error = None;
                self.persist(vec![NoticeKind::RunResumed], vec![], AfterCommit::Drive)
            }
            other => {
                self.control = Some(other);
                self.drive()
            }
        }
    }
}

fn add(total: &mut Counters, delta: &Counters) {
    total.read += delta.read;
    total.written += delta.written;
    total.filtered += delta.filtered;
    total.rejected += delta.rejected;
    total.bytes_read += delta.bytes_read;
    total.bytes_written += delta.bytes_written;
}
fn payload_bytes(item: &Envelope) -> u64 {
    match &item.payload {
        Payload::Inline(value) => value.bytes.len() as u64,
        Payload::Blob(blob) => blob.bytes,
    }
}
fn invalid(message: &str) -> N2xError {
    N2xError::new(
        "n2x.engine.protocol",
        ErrorKind::Integrity,
        ErrorOrigin::Engine,
        message,
    )
}

fn validate_recovery(plan: &PlanSnapshot, state: &RecoverySnapshot) -> Result<(), N2xError> {
    if state.run.scope != plan.scope || state.run.target != plan.to || state.run.plan != plan.digest
    {
        return Err(invalid("recovery run does not match plan"));
    }
    match state.checkpoint.position {
        ExecutionPosition::Step { edge, step } => {
            let manifest = plan
                .route
                .get(edge as usize)
                .ok_or_else(|| invalid("recovery edge out of bounds"))?;
            let planned = manifest
                .steps
                .get(step as usize)
                .ok_or_else(|| invalid("recovery step out of bounds"))?;
            if state.run.current != manifest.from {
                return Err(invalid("recovery current revision mismatch"));
            }
            if state.checkpoint.transfer.is_some()
                && !matches!(planned.definition.kind, StepKind::Transfer(_))
            {
                return Err(invalid("transfer state at non-transfer step"));
            }
            if let Some(transfer) = &state.checkpoint.transfer
                && let Some(prepared) = &transfer.prepared
            {
                if prepared.receipts.len() > prepared.units.len() {
                    return Err(invalid("invalid receipt prefix"));
                }
                for (request, receipt) in prepared.units.iter().zip(&prepared.receipts) {
                    if request.context.id != receipt.operation
                        || request.context.request_digest != receipt.request_digest
                    {
                        return Err(invalid("recovery receipt mismatch"));
                    }
                }
                if let Some(intent) = &state.pending
                    && prepared.units.get(prepared.receipts.len()) != Some(&intent.request)
                {
                    return Err(invalid("pending does not match first unconfirmed unit"));
                }
            }
            if let Some(intent) = &state.pending {
                let context = &intent.request.context;
                if context.run != state.run.run
                    || context.plan != plan.digest
                    || context.step.manifest != manifest.id
                    || context.step.step != planned.definition.id
                    || codec::digest(&intent.request.effect)? != context.request_digest
                {
                    return Err(invalid("invalid recovery intent"));
                }
            }
        }
        ExecutionPosition::AdvanceRevision { edge } => {
            if plan
                .route
                .get(edge as usize)
                .is_none_or(|edge| edge.from != state.run.current)
                || state.pending.is_some()
                || state.checkpoint.transfer.is_some()
            {
                return Err(invalid("invalid revision commit position"));
            }
        }
        ExecutionPosition::Finished => {
            if state.run.current != plan.to
                || state.pending.is_some()
                || state.checkpoint.transfer.is_some()
            {
                return Err(invalid("invalid finished checkpoint"));
            }
        }
    }
    Ok(())
}
