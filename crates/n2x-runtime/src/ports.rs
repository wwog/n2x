//! 平台和业务执行端口。所有连接、文件与 JS 交互在本层的实现中完成。

use std::future::Future;
use std::pin::Pin;

use n2x_core::{
    ActionCall, ActionRef, ApplyRequest, BlobRef, CheckResult, Digest, EventPage, N2xError,
    OperationFailure, PlanSnapshot, PortBinding, ReadBatch, ReadRequest, ReaderCapabilities,
    Receipt, ReconcileRequest, ReconcileResult, RecoveryCapabilities, RecoverySnapshot, RunId,
    RunReport, ScopeId, ScopeState, Timestamp, TransformRequest, TransformResult, WorkspaceCommit,
    WorkspaceReceipt, WriterCapabilities,
};

/// 对象安全且不强制 Send 的异步返回值，兼容 Web 单线程宿主。
pub type PortFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// 普通无副作用请求的标准结果。
pub type PortResult<'a, T> = PortFuture<'a, Result<T, N2xError>>;

/// 一个真实资源到计划端口的绑定。
pub trait ResourcePort {
    /// 返回稳定资源身份与适配器修订号；不得返回凭证或临时连接 ID。
    fn binding(&self) -> PortBinding;
}

/// 标准读取端口。快照、冻结条件和游标有效性都由实现验证。
pub trait Reader: ResourcePort {
    /// 按具体数据集查询能力，不能仅按数据库产品笼统声明。
    fn capabilities(&self, stream: &n2x_core::StreamSpec) -> Result<ReaderCapabilities, N2xError>;

    /// 读取一个有界批次。首批建立 SourceState，后续必须验证相同视图。
    ///
    /// 同库读写时，读实现和计划必须确保写入不会破坏扫描一致性。
    fn read(&mut self, request: ReadRequest) -> PortResult<'_, ReadBatch>;
}

/// 标准输出端口，负责一个 EffectRequest::Write 提交单位的恢复语义。
pub trait Writer: ResourcePort {
    /// 声明具体写入方式的能力，包含部分执行时如何恢复。
    fn capabilities(&self, stream: &n2x_core::StreamSpec) -> Result<WriterCapabilities, N2xError>;

    /// 执行原请求；只接受 Write，错误的请求类型应明确拒绝且声明 NotApplied。
    ///
    /// Runtime 必须先持久化 Intent。实现必须验证 OperationId 与请求摘要，禁止
    /// 使用相同 ID 提交不同内容。返回 Receipt 前达到声明的持久级别。
    fn apply(&mut self, request: ApplyRequest)
    -> PortFuture<'_, Result<Receipt, OperationFailure>>;

    /// 查询原请求，必须是权威结果；不支持时返回标准 Unsupported 错误。
    fn reconcile(&mut self, request: ReconcileRequest) -> PortResult<'_, ReconcileResult>;
}

/// Task 的注册实现；依赖的资源由 Runtime 注入并验证与声明绑定一致。
pub trait TaskAction {
    /// 稳定注册 ID 和实现修订号。
    fn action(&self) -> ActionRef;

    /// 按完整调用参数声明恢复保证。
    fn capabilities(&self, call: &ActionCall) -> Result<RecoveryCapabilities, N2xError>;

    /// 只接受 Task 请求；与 Writer 相同，必须先持久化 Intent。
    fn apply(&mut self, request: ApplyRequest)
    -> PortFuture<'_, Result<Receipt, OperationFailure>>;

    /// 查询该 Task 的权威执行结果。
    fn reconcile(&mut self, request: ReconcileRequest) -> PortResult<'_, ReconcileResult>;
}

/// 只读 Check 的注册实现。失败阻止版本推进，不修改业务资源。
pub trait CheckAction {
    /// 稳定注册 ID 和实现修订号。
    fn action(&self) -> ActionRef;

    /// 条件不满足时返回 InvalidData 或 Integrity 等明确错误。
    fn check(&mut self, call: ActionCall) -> PortResult<'_, CheckResult>;
}

/// 纯转换实现，可读取显式 Blob，但不能执行隐藏的业务 IO。
pub trait TransformAction {
    /// 稳定注册 ID 和实现修订号。
    fn action(&self) -> ActionRef;

    /// 相同输入、状态和配置必须生成相同结果。
    ///
    /// 业务无效记录通过 rejected 返回；整个转换无法执行时返回 Err。
    /// 实现需要大输出时应使用注入的 BlobStore，避免无限扩展内联 Vec。
    fn transform(&mut self, request: TransformRequest) -> PortResult<'_, TransformResult>;
}

/// 工作区语义服务；调用者必须在整个使用期独占工作区写权限。
///
/// 当前模型只支持单活动写者。实现不能把 get + set 伪装成跨进程锁或分布式 CAS。
/// Event 历史、隔离数据和恢复日志有不同保留规则，未确认恢复材料禁止提前清理。
pub trait Workspace {
    /// 读取权威版本；None 表示尚未引导该 Scope。
    fn scope(&mut self, scope: ScopeId) -> PortResult<'_, Option<ScopeState>>;

    /// 按运行 ID 读取不可变计划。
    fn plan(&mut self, run: RunId) -> PortResult<'_, Option<PlanSnapshot>>;

    /// 读取恢复入口，必须与 PlanSnapshot、ScopeState 一致地解释。
    fn load(&mut self, run: RunId) -> PortResult<'_, Option<RecoverySnapshot>>;

    /// 所有 mutations 原子提交，并达到请求的持久级别。
    ///
    /// 相同 CommitId、摘要和变更可以重放；相同 ID 不同内容必须拒绝。
    /// flush 失败表示确认未知，不能假设变更没有生效。重试必须沿用原 CommitId。
    fn commit(&mut self, commit: WorkspaceCommit) -> PortResult<'_, WorkspaceReceipt>;

    /// 有界补读 sequence > after 的事件，过期位置必须返回 EventPage::Gap。
    fn events_after(&mut self, run: RunId, after: u64, limit: u32) -> PortResult<'_, EventPage>;

    /// 读取已保存的报告。
    fn report(&mut self, run: RunId) -> PortResult<'_, Option<RunReport>>;
}

/// 独占运行权的守卫；实现者在 Drop 时释放平台所有权。
///
/// 单进程 Mutex 只能保护同进程。原生跨进程锁建议使用独立锁文件，不锁住可能
/// 被 KV compact 替换的文件 inode。丢失所有权后不得继续产生副作用。
pub trait WorkspaceGuard {}

/// 本地或平台级工作区所有权机制，不定义分布式租约。
pub trait WorkspaceLock {
    /// 取得独占权，调用者在所有在途副作用结束前必须持有守卫。
    fn acquire(&mut self) -> PortResult<'_, Box<dyn WorkspaceGuard>>;
}

/// 大内容的持久暂存服务；引用在恢复材料中存在时不得回收。
pub trait BlobStore {
    /// 创建流式写入器；未 finish 的孤立内容允许后续回收。
    fn create(&mut self, codec: String, version: u32) -> PortResult<'_, Box<dyn BlobWriter>>;

    /// 按范围打开已确认内容；必须检查引用的身份、长度和摘要。
    fn read_range(&mut self, blob: BlobRef, offset: u64, limit: u32) -> PortResult<'_, Vec<u8>>;

    /// 只允许删除已经确认无恢复或隔离引用的内容。
    fn remove(&mut self, blob: BlobRef) -> PortResult<'_, ()>;
}

/// 逐块写入大内容，避免把整个文件放入内存。
pub trait BlobWriter {
    /// 顺序追加；Runtime 负责背压，不能无限缓存未完成 append。
    fn append(&mut self, bytes: Vec<u8>) -> PortResult<'_, ()>;

    /// 冻结并持久化内容；返回后才允许把 BlobRef 写入 Intent。
    ///
    /// Native 实现应确认文件内容与必要的目录项持久化。重开后必须仍可访问。
    fn finish(self: Box<Self>) -> PortResult<'static, BlobRef>;
}

/// 平台时钟与定时器，Engine 通过 Command 请求这些输入。
pub trait Clock {
    /// Unix 毫秒时间，用于日志与唤醒目标；不作为可信的单调耗时来源。
    fn now(&self) -> Timestamp;

    /// 等待目标时刻，由平台实现取消安全和系统时钟变化处理。
    fn wake_at(&mut self, at: Timestamp) -> PortResult<'_, Timestamp>;
}

/// 规范摘要计算接口，具体编码器和 SHA-256 实现由组合层选择。
///
/// 在持久化编码标准确定前，不应宣称跨语言 Plan 摘要互通。
pub trait Digester {
    /// 对已按协议规范编码的字节计算 SHA-256。
    fn digest(&self, canonical_bytes: &[u8]) -> Digest;
}
