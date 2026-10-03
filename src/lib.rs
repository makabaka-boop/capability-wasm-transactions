//! 受控多租户 Wasm 插件执行服务。
//!
//! 设计要点见 README。ABI 摘要：
//!
//! * 模块最多从宿主导入三个函数，全部位于命名空间 `plugin` 下：
//!   `get(ptr,len)->i32`、`put(ptr,len)->i32`、`emit(ptr,len)->i32`；
//!   不允许任何 WASI 导入或其他宿主能力。
//! * 模块必须导出线性内存 `mem` 与函数 `run(i32) -> i32`。
//! * 线性内存开头 1 KiB 是宿主写入的输入区：`run(input_len)` 的参数给出长度。
//!
//! 所有写操作先暂存在该次执行自己的 `Ctx` 中，只有 `run` 正常返回后，
//! 才以输入时读到的租户修订号做一次乐观提交；任何失败路径都会丢弃暂存区。

use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior};
use wasmtime::{
    Config, Engine, ExternType, Linker, Module, Store, Trap, TypedFunc,
};

// ---------------------------------------------------------------------------
// 公共数据类型
// ---------------------------------------------------------------------------

/// 插件允许执行的一次受控任务。
#[derive(Debug, Clone)]
pub struct Task {
    pub tenant: String,
    /// 预编译插件模块（Wasm 字节码）。
    pub module_bytes: Vec<u8>,
    /// 写入插件输入区（线性内存起点）的数据。
    pub input: Vec<u8>,
    /// 允许 `get` 的键前缀；前缀匹配按字节。
    pub read_prefixes: Vec<Vec<u8>>,
    /// 允许 `put` 的键前缀；与读前缀独立授权。
    pub write_prefixes: Vec<Vec<u8>>,
    /// 燃料上限（耗尽即 trap）。
    pub fuel: u64,
    /// 线性内存硬上限（字节），初始大小与增长都不得超过。
    pub max_memory_bytes: usize,
    /// 所有 `emit` 事件载荷总大小上限（字节）。
    pub max_output_bytes: usize,
}

/// 宿主函数类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostOp {
    Get,
    Put,
    Emit,
}

/// 一次宿主调用的证据。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallRecord {
    pub seq: usize,
    pub op: HostOp,
    /// 调用使用的键（get/put）；emit 时为空。
    pub key: Vec<u8>,
    pub result: CallResult,
}

/// 一次宿主调用的返回结果（证据用）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallResult {
    /// 插件可见的成功返回值（值/长度，或 0）。
    Ok(i32),
    /// 插件可见的 ABI 级非致命返回：1 = 键不存在(get)。
    NotFound,
    /// 插件可见的 ABI 级非致命返回：2 = 输出缓冲太小(get)。
    BufferTooSmall,
    /// 宿主拒绝并 trap，执行中止。
    Fault(FaultKind),
}

/// 宿主故障分类（同时用于最终终态）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultKind {
    /// 访问了未授权的键前缀。
    UnauthorizedKey,
    /// 指针/长度越出线性内存，或长度非法。
    BadPointer,
    /// `emit` 总输出超过任务的输出上限。
    OutputTooLarge,
    /// 线性内存申请超过任务内存上限（含初始大小）。
    MemoryLimit,
    /// 宿主存储自身出错。
    HostError,
}

/// `run` 返回后提交失败时的原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbortReason {
    /// Wasm trap，且剩余燃料为 0 / trap 码为 OutOfFuel。
    OutOfFuel,
    UnauthorizedKey,
    BadPointer,
    OutputTooLarge,
    MemoryLimit,
    HostError,
    /// 其他 trap（例如插件自己的 `unreachable`）。
    Trap,
    /// 乐观提交时基准修订已过期。
    RevConflict,
}

/// 一次执行的明确终态。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Terminal {
    /// `run` 正常返回且暂存写入已原子提交。
    Committed {
        revision_before: u64,
        revision_after: u64,
        /// 插件 `run` 的 i32 返回值。
        plugin_return: i32,
        staged_writes: usize,
    },
    /// 未能创建实例（模块不合法 / 违反导入白名单等），未消耗燃料、未写入。
    Rejected { reason: String },
    /// 实例已创建但执行中止；全部暂存写入撤销。
    Aborted { reason: AbortReason, detail: String },
}

/// 一次执行的完整结果：终态 + 证据 + 资源信息。
#[derive(Debug, Clone)]
pub struct Outcome {
    pub tenant: String,
    pub terminal: Terminal,
    /// 按调用顺序排列的宿主调用证据；即使最终 abort 也保留到中止前最后一条。
    pub calls: Vec<CallRecord>,
    /// `emit` 出的事件载荷（仅成功提交时有意义）。
    pub events: Vec<Vec<u8>>,
    pub fuel_consumed: u64,
    /// 实例及 Store 是否被释放（每次执行必须为 true）。
    pub instance_released: bool,
}

/// 准备好的执行：模块已编译校验、基准修订已读取。
///
/// 拆成 `prepare` / `PreparedExecution::run` 是为了让调用方能够
/// 精确制造“准备后、提交前修订被别的执行推进”的冲突场景。
pub struct PreparedExecution {
    task: Task,
    module: Module,
    revision_before: u64,
}

// ---------------------------------------------------------------------------
// 存储层
// ---------------------------------------------------------------------------

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS kv(
    tenant   TEXT NOT NULL,
    key      BLOB NOT NULL,
    value    BLOB NOT NULL,
    PRIMARY KEY (tenant, key)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS tenants(
    tenant   TEXT PRIMARY KEY,
    revision INTEGER NOT NULL
);
"#;

fn open_conn(db_path: &str) -> rusqlite::Result<Connection> {
    let conn = Connection::open(db_path)?;
    // 每次执行独立连接；并发执行时忙碌即等待。
    conn.busy_timeout(std::time::Duration::from_secs(10))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    Ok(conn)
}

fn read_revision(conn: &Connection, tenant: &str) -> rusqlite::Result<u64> {
    conn.query_row(
        "SELECT revision FROM tenants WHERE tenant = ?1",
        [tenant],
        |r| r.get::<_, i64>(0),
    )
    .optional()
    .map(|v| v.unwrap_or(0) as u64)
}

fn lookup(conn: &Connection, tenant: &str, key: &[u8]) -> rusqlite::Result<Option<Vec<u8>>> {
    conn.query_row(
        "SELECT value FROM kv WHERE tenant = ?1 AND key = ?2",
        rusqlite::params![tenant, key],
        |r| r.get::<_, Vec<u8>>(0),
    )
    .optional()
}

// ---------------------------------------------------------------------------
// 插件 ABI 常量
// ---------------------------------------------------------------------------

/// 线性内存开头保留的输入区容量（get 的结果也写回这里）。
pub const INPUT_REGION: usize = 1024;

// 插件可见的 ABI 返回码。
const ABI_OK: i32 = 0;
const ABI_NOT_FOUND: i32 = 1;
const ABI_BUFFER_TOO_SMALL: i32 = 2;

fn prefix_allowed(prefixes: &[Vec<u8>], key: &[u8]) -> bool {
    // 没有授权任何前缀 = 一律拒绝；空前缀必须显式列出才代表放行所有键。
    prefixes.iter().any(|p| key.starts_with(p))
}

// ---------------------------------------------------------------------------
// 每次执行的宿主上下文
// ---------------------------------------------------------------------------

struct Ctx {
    tenant: String,
    read_prefixes: Vec<Vec<u8>>,
    write_prefixes: Vec<Vec<u8>>,
    max_output_bytes: usize,
    max_memory_bytes: usize,

    /// 该次执行自己的存储会话；暂存键也在这里处理覆盖语义。
    /// commit 期间会被临时取出。
    conn: Option<Connection>,
    /// 暂存写入：Some(value) 表示写（空值=删除语义在 ABI 中约定）。
    staged: HashMap<Vec<u8>, Vec<u8>>,
    events: Vec<Vec<u8>>,
    calls: Vec<CallRecord>,

    /// 宿主故障：一旦设置，外层即判定 abort。
    fault: Option<FaultKind>,
    output_bytes: usize,
    call_seq: usize,
    /// 实例释放证据：Ctx 随 Store Drop 时回写。
    released: *mut bool,
}

// 裸指针仅在 Store/实例存活期间使用，drop 顺序保证安全。
unsafe impl Send for Ctx {}

impl Ctx {
    /// 读取键：先看本次暂存区，再落到已提交数据。
    fn staged_get(&mut self, key: &[u8]) -> rusqlite::Result<Option<Vec<u8>>> {
        if let Some(v) = self.staged.get(key) {
            return Ok(Some(v.clone()));
        }
        let conn = self.conn.as_ref().expect("connection present during run");
        lookup(conn, &self.tenant, key)
    }

    fn record(&mut self, op: HostOp, key: Vec<u8>, result: CallResult) {
        self.call_seq += 1;
        self.calls.push(CallRecord {
            seq: self.call_seq,
            op,
            key,
            result,
        });
    }

    /// 记录一次故障并返回 trap 错误；调用方立即返回。
    fn fail(&mut self, op: HostOp, key: Vec<u8>, kind: FaultKind, msg: &str) -> anyhow::Error {
        // 一次执行只认定第一个故障为终态原因。
        if self.fault.is_none() {
            self.fault = Some(kind);
        }
        self.record(op, key, CallResult::Fault(kind));
        anyhow::anyhow!("{msg}")
    }

    /// 校验 guest 给出的 [offset, offset+len) 是否完全落在内存内。
    fn checked_range(offset: i32, len: i32, size: usize) -> Option<std::ops::Range<usize>> {
        if offset < 0 || len < 0 {
            return None;
        }
        let (o, l) = (offset as u64, len as u64);
        let end = o.checked_add(l)?;
        if end > size as u64 {
            return None;
        }
        Some(o as usize..end as usize)
    }

    fn read_guest<C: wasmtime::AsContextMut>(
        memory: &wasmtime::Memory,
        store: C,
        range: std::ops::Range<usize>,
    ) -> Vec<u8> {
        let mut buf = vec![0u8; range.len()];
        memory
            .read(store, range.start, &mut buf)
            .expect("range pre-checked");
        buf
    }
}

impl wasmtime::ResourceLimiter for Ctx {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> anyhow::Result<bool> {
        if desired > self.max_memory_bytes {
            if self.fault.is_none() {
                self.fault = Some(FaultKind::MemoryLimit);
            }
            // 返回 Err：直接以 trap 终止，memory.grow 不会仅返回 -1。
            return Err(anyhow::anyhow!(
                "linear memory growth to {desired} bytes exceeds task limit {}",
                self.max_memory_bytes
            ));
        }
        Ok(true)
    }

    fn table_growing(
        &mut self,
        _current: u32,
        desired: u32,
        _maximum: Option<u32>,
    ) -> anyhow::Result<bool> {
        // 插件不需要动态表；给一个保守上限。
        if desired > 10_000 {
            return Err(anyhow::anyhow!("table growth exceeds limit"));
        }
        Ok(true)
    }

    fn instances(&self) -> usize {
        1
    }
    fn tables(&self) -> usize {
        1
    }
    fn memories(&self) -> usize {
        1
    }
}

// ---------------------------------------------------------------------------
// 宿主函数
// ---------------------------------------------------------------------------

/// 值的布局约定：`put(key_ptr,key_len)`，键后紧跟
/// 4 字节小端 `value_len` 与 `value_len` 字节的值。
fn build_linker(engine: &Engine) -> anyhow::Result<Linker<Ctx>> {
    let mut linker = Linker::new(engine);

    // get(key_ptr,key_len) -> i32
    // 键存在：值写回输入区(偏移0)，返回值长度；1=不存在；2=值超过输入区。
    linker.func_wrap(
        "plugin",
        "get",
        |mut caller: wasmtime::Caller<'_, Ctx>, kp: i32, kl: i32| -> Result<i32, anyhow::Error> {
            let memory = caller
                .get_export("mem")
                .and_then(|e| e.into_memory())
                .ok_or_else(|| anyhow::anyhow!("guest has no exported memory 'mem'"))?;
            let size = memory.data_size(&caller);
            let key_range = Ctx::checked_range(kp, kl, size).ok_or_else(|| {
                caller.data_mut().fail(
                    HostOp::Get,
                    Vec::new(),
                    FaultKind::BadPointer,
                    "get: key pointer out of bounds",
                )
            })?;
            let key = Ctx::read_guest(&memory, &mut caller, key_range);

            let data = caller.data_mut();
            if !prefix_allowed(&data.read_prefixes, &key) {
                return Err(data.fail(
                    HostOp::Get,
                    key,
                    FaultKind::UnauthorizedKey,
                    "get: key prefix not permitted",
                ));
            }
            let value = match data.staged_get(&key) {
                Ok(v) => v,
                Err(e) => {
                    return Err(caller.data_mut().fail(
                        HostOp::Get,
                        key,
                        FaultKind::HostError,
                        &format!("get: storage error: {e}"),
                    ));
                }
            };
            match value {
                None => {
                    caller.data_mut().record(HostOp::Get, key, CallResult::NotFound);
                    Ok(ABI_NOT_FOUND)
                }
                Some(v) if v.len() > INPUT_REGION => {
                    caller
                        .data_mut()
                        .record(HostOp::Get, key, CallResult::BufferTooSmall);
                    Ok(ABI_BUFFER_TOO_SMALL)
                }
                Some(v) => {
                    if Ctx::checked_range(0, v.len() as i32, size).is_none() {
                        return Err(caller.data_mut().fail(
                            HostOp::Get,
                            key,
                            FaultKind::BadPointer,
                            "get: result region out of bounds",
                        ));
                    }
                    if let Err(e) = memory.write(&mut caller, 0, &v) {
                        return Err(caller.data_mut().fail(
                            HostOp::Get,
                            key,
                            FaultKind::BadPointer,
                            &format!("get: write to guest memory failed: {e}"),
                        ));
                    }
                    let n = v.len() as i32;
                    caller
                        .data_mut()
                        .record(HostOp::Get, key, CallResult::Ok(n));
                    Ok(n)
                }
            }
        },
    )?;

    linker.func_wrap(
        "plugin",
        "put",
        |mut caller: wasmtime::Caller<'_, Ctx>, kp: i32, kl: i32| -> Result<i32, anyhow::Error> {
            let memory = caller
                .get_export("mem")
                .and_then(|e| e.into_memory())
                .ok_or_else(|| anyhow::anyhow!("guest has no exported memory 'mem'"))?;
            let size = memory.data_size(&caller);

            let key_range = Ctx::checked_range(kp, kl, size).ok_or_else(|| {
                caller.data_mut().fail(
                    HostOp::Put,
                    Vec::new(),
                    FaultKind::BadPointer,
                    "put: key pointer out of bounds",
                )
            })?;
            // 值长度头（4 字节小端）紧跟键后。
            let header_end = key_range
                .end
                .checked_add(4)
                .filter(|e| *e <= size)
                .ok_or_else(|| {
                    caller.data_mut().fail(
                        HostOp::Put,
                        Vec::new(),
                        FaultKind::BadPointer,
                        "put: value length header out of bounds",
                    )
                })?;
            let mut len_bytes = [0u8; 4];
            memory
                .read(&caller, key_range.end, &mut len_bytes)
                .expect("header range checked");
            let vl = u32::from_le_bytes(len_bytes) as i32;
            let val_range =
                Ctx::checked_range(header_end as i32, vl, size).ok_or_else(|| {
                    caller.data_mut().fail(
                        HostOp::Put,
                        Vec::new(),
                        FaultKind::BadPointer,
                        "put: value pointer out of bounds",
                    )
                })?;

            let key = Ctx::read_guest(&memory, &mut caller, key_range);
            let value = Ctx::read_guest(&memory, &mut caller, val_range);

            let data = caller.data_mut();
            if !prefix_allowed(&data.write_prefixes, &key) {
                return Err(data.fail(
                    HostOp::Put,
                    key,
                    FaultKind::UnauthorizedKey,
                    "put: key prefix not permitted",
                ));
            }
            // 全部暂存在本次执行自己的上下文里，提交前不落盘。
            data.staged.insert(key.clone(), value);
            data.record(HostOp::Put, key, CallResult::Ok(ABI_OK));
            Ok(ABI_OK)
        },
    )?;

    // emit(ptr,len) -> i32：追加事件载荷，总输出受任务上限约束。
    linker.func_wrap(
        "plugin",
        "emit",
        |mut caller: wasmtime::Caller<'_, Ctx>, p: i32, l: i32| -> Result<i32, anyhow::Error> {
            let memory = caller
                .get_export("mem")
                .and_then(|e| e.into_memory())
                .ok_or_else(|| anyhow::anyhow!("guest has no exported memory 'mem'"))?;
            let size = memory.data_size(&caller);
            let range = Ctx::checked_range(p, l, size).ok_or_else(|| {
                caller.data_mut().fail(
                    HostOp::Emit,
                    Vec::new(),
                    FaultKind::BadPointer,
                    "emit: pointer out of bounds",
                )
            })?;
            let payload = Ctx::read_guest(&memory, &mut caller, range);
            let data = caller.data_mut();
            if data.output_bytes + payload.len() > data.max_output_bytes {
                return Err(data.fail(
                    HostOp::Emit,
                    Vec::new(),
                    FaultKind::OutputTooLarge,
                    "emit: total output exceeds task output limit",
                ));
            }
            data.output_bytes += payload.len();
            data.events.push(payload);
            data.record(HostOp::Emit, Vec::new(), CallResult::Ok(ABI_OK));
            Ok(ABI_OK)
        },
    )?;

    Ok(linker)
}

// ---------------------------------------------------------------------------
// 模块校验：导入白名单（无 WASI）+ 导出要求 + 内存初始上限
// ---------------------------------------------------------------------------

fn validate_module(module: &Module, max_memory_bytes: usize) -> Result<(), String> {
    fn sig_matches(
        ft: &wasmtime::FuncType,
        params: &[wasmtime::ValType],
        results: &[wasmtime::ValType],
    ) -> bool {
        let p: Vec<_> = ft.params().collect();
        let r: Vec<_> = ft.results().collect();
        p.len() == params.len()
            && r.len() == results.len()
            && p.iter().zip(params).all(|(a, b)| a.matches(b))
            && r.iter().zip(results).all(|(a, b)| a.matches(b))
    }

    for imp in module.imports() {
        let (m, n) = (imp.module(), imp.name());
        match (m, n) {
            ("plugin", "get" | "put" | "emit") => {
                let ft = match imp.ty() {
                    ExternType::Func(ft) => ft,
                    other => {
                        return Err(format!("import {m}::{n} must be a function, found {other:?}"))
                    }
                };
                if !sig_matches(
                    &ft,
                    &[wasmtime::ValType::I32, wasmtime::ValType::I32],
                    &[wasmtime::ValType::I32],
                ) {
                    return Err(format!("import {m}::{n} has wrong signature"));
                }
            }
            // 显式拒绝 WASI 与任何其他宿主能力。
            _ => {
                return Err(format!(
                    "import {m}::{n} is not on the allow-list (WASI is forbidden)"
                ))
            }
        }
    }

    let (mut has_mem, mut has_run) = (false, false);
    for exp in module.exports() {
        match (exp.name(), exp.ty()) {
            ("mem", ExternType::Memory(mt)) => {
                let initial = mt.minimum() as usize * 64 * 1024;
                if initial > max_memory_bytes {
                    return Err(format!(
                        "module initial memory {initial} bytes exceeds task limit {max_memory_bytes}"
                    ));
                }
                has_mem = true;
            }
            ("run", ExternType::Func(ft)) => {
                if !sig_matches(&ft, &[wasmtime::ValType::I32], &[wasmtime::ValType::I32]) {
                    return Err("export `run` must have type (i32) -> i32".into());
                }
                has_run = true;
            }
            _ => {}
        }
    }
    if !has_mem {
        return Err("module must export linear memory `mem`".into());
    }
    if !has_run {
        return Err("module must export function `run(i32) -> i32`".into());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 服务
// ---------------------------------------------------------------------------

/// 受控插件执行服务。
#[derive(Clone)]
pub struct PluginService {
    engine: Engine,
    db_path: std::sync::Arc<String>,
}

impl PluginService {
    /// 打开（必要时创建）位于 `db_path` 的服务。
    pub fn open(db_path: impl Into<String>) -> anyhow::Result<Self> {
        let db_path = db_path.into();
        {
            let conn = open_conn(&db_path)?;
            conn.execute_batch(SCHEMA)?;
        }

        let mut config = Config::new();
        config.consume_fuel(true);
        config.wasm_bulk_memory(true);
        // 不实例化 wasi 上下文；Linker 仅含 plugin:: 三函数，模块也无法获得 WASI。
        let engine = Engine::new(&config)?;
        Ok(Self {
            engine,
            db_path: std::sync::Arc::new(db_path),
        })
    }

    /// 预置键值（测试/引导用），并推进租户修订号。
    pub fn seed(&self, tenant: &str, items: &[(Vec<u8>, Vec<u8>)]) -> anyhow::Result<()> {
        let mut conn = open_conn(&self.db_path)?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for (k, v) in items {
            tx.execute(
                "INSERT INTO kv(tenant,key,value) VALUES(?1,?2,?3)
                 ON CONFLICT(tenant,key) DO UPDATE SET value=excluded.value",
                rusqlite::params![tenant, k, v],
            )?;
        }
        tx.execute(
            "INSERT INTO tenants(tenant,revision) VALUES(?1,1)
             ON CONFLICT(tenant) DO UPDATE SET revision=revision+1",
            [tenant],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// 读取某租户已提交的键值快照。
    pub fn snapshot(&self, tenant: &str) -> anyhow::Result<HashMap<Vec<u8>, Vec<u8>>> {
        let conn = open_conn(&self.db_path)?;
        let mut stmt = conn.prepare("SELECT key,value FROM kv WHERE tenant=?1")?;
        let rows = stmt
            .query_map([tenant], |r| {
                Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?))
            })?
            .collect::<rusqlite::Result<HashMap<_, _>>>()?;
        Ok(rows)
    }

    /// 一步到位：校验、准备并执行。
    pub fn run_task(&self, task: Task) -> Outcome {
        match self.prepare_inner(task) {
            Ok(prep) => prep.run(self),
            Err(outcome) => outcome,
        }
    }

    /// 仅准备：编译/校验模块、读取输入基准修订。
    pub fn prepare(&self, task: Task) -> Result<PreparedExecution, Outcome> {
        self.prepare_inner(task)
    }

    fn prepare_inner(&self, task: Task) -> Result<PreparedExecution, Outcome> {
        if task.fuel == 0 {
            return Err(reject(&task, "task fuel must be non-zero"));
        }
        if task.input.len() > INPUT_REGION {
            return Err(reject(
                &task,
                &format!("input larger than {INPUT_REGION}-byte input region"),
            ));
        }

        let module = match Module::new(&self.engine, &task.module_bytes) {
            Ok(m) => m,
            Err(e) => return Err(reject(&task, &format!("invalid wasm module: {e}"))),
        };
        if let Err(e) = validate_module(&module, task.max_memory_bytes) {
            return Err(reject(&task, &e));
        }

        // 基准修订只用于乐观提交；连接随即关闭，执行时开新连接。
        let revision_before = {
            let conn = open_conn(&self.db_path)
                .map_err(|e| reject(&task, &format!("db: {e}")))?;
            read_revision(&conn, &task.tenant)
                .map_err(|e| reject(&task, &format!("db: {e}")))?
        };

        Ok(PreparedExecution {
            task,
            module,
            revision_before,
        })
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }
    pub fn db_path(&self) -> &str {
        &self.db_path
    }
}

fn reject(task: &Task, reason: &str) -> Outcome {
    Outcome {
        tenant: task.tenant.clone(),
        terminal: Terminal::Rejected {
            reason: reason.to_string(),
        },
        calls: Vec::new(),
        events: Vec::new(),
        fuel_consumed: 0,
        instance_released: true,
    }
}

impl PreparedExecution {
    /// prepare 时读到的基准修订。
    pub fn revision_before(&self) -> u64 {
        self.revision_before
    }

    /// 在全新的 Store/实例中执行一次。
    pub fn run(self, svc: &PluginService) -> Outcome {
        let PreparedExecution {
            task,
            module,
            revision_before,
        } = self;

        // 每次执行：独立存储连接 + 独立 Store + 独立实例。
        let conn = match open_conn(svc.db_path()) {
            Ok(c) => c,
            Err(e) => {
                return early_abort(
                    &task,
                    AbortReason::HostError,
                    format!("open execution connection: {e}"),
                )
            }
        };

        // 实例释放证据：Ctx 随 Store Drop 时由 Drop 守卫置位。
        let mut released_flag = false;
        let ctx = Ctx {
            tenant: task.tenant.clone(),
            read_prefixes: task.read_prefixes.clone(),
            write_prefixes: task.write_prefixes.clone(),
            max_output_bytes: task.max_output_bytes,
            max_memory_bytes: task.max_memory_bytes,
            conn: Some(conn),
            staged: HashMap::new(),
            events: Vec::new(),
            calls: Vec::new(),
            fault: None,
            output_bytes: 0,
            call_seq: 0,
            released: &mut released_flag as *mut bool,
        };

        let mut store = Store::new(&svc.engine, ctx);
        store.limiter(|s| s as &mut dyn wasmtime::ResourceLimiter);
        if let Err(e) = store.set_fuel(task.fuel) {
            return early_abort(&task, AbortReason::HostError, format!("fuel setup: {e}"));
        }

        let mut outcome = run_in_store(&mut store, &svc.engine, &module, &task, revision_before);

        // 显式释放 Store：实例、线性内存与该执行自己的连接一并关闭。
        drop(store);
        outcome.instance_released = released_flag;
        outcome
    }
}

/// Ctx 释放（Store/实例 drop）时回写证据标志。
impl Drop for Ctx {
    fn drop(&mut self) {
        // 安全：标志位于 run() 栈帧，Ctx 必先于该栈帧销毁；
        // 每次执行各自一个标志，不存在跨执行复用。
        unsafe { *self.released = true };
    }
}

fn early_abort(task: &Task, reason: AbortReason, detail: String) -> Outcome {
    Outcome {
        tenant: task.tenant.clone(),
        terminal: Terminal::Aborted { reason, detail },
        calls: Vec::new(),
        events: Vec::new(),
        fuel_consumed: 0,
        instance_released: true,
    }
}

/// 在 Store 内完成实例化、输入写入、调用 run 与（可能的）提交。
fn run_in_store(
    store: &mut Store<Ctx>,
    engine: &Engine,
    module: &Module,
    task: &Task,
    revision_before: u64,
) -> Outcome {
    let linker = match build_linker(engine) {
        Ok(l) => l,
        Err(e) => {
            return abort_from_ctx(
                store,
                task,
                AbortReason::HostError,
                format!("linker: {e}"),
            )
        }
    };

    let instance = match linker.instantiate(&mut *store, module) {
        Ok(i) => i,
        Err(e) => {
            let reason = store
                .data()
                .fault
                .map(FaultKind::into_reason)
                .unwrap_or(AbortReason::HostError);
            return abort_from_ctx(store, task, reason, e.to_string());
        }
    };

    let memory = match instance.get_memory(&mut *store, "mem") {
        Some(m) => m,
        None => {
            return abort_from_ctx(store, task, AbortReason::Trap, "missing exported memory".into())
        }
    };
    let run: TypedFunc<i32, i32> = match instance.get_typed_func(&mut *store, "run") {
        Ok(f) => f,
        Err(e) => {
            return abort_from_ctx(store, task, AbortReason::Trap, format!("missing run: {e}"))
        }
    };

    // 输入区清零后写入，避免上一次残留（同一实例也只调用一次，这里做足防护）。
    {
        let size = memory.data_size(&store);
        if size < INPUT_REGION {
            return abort_from_ctx(
                store,
                task,
                AbortReason::MemoryLimit,
                format!("linear memory {size} bytes smaller than input region {INPUT_REGION}"),
            );
        }
        memory
            .data_mut(&mut *store)
            .get_mut(0..INPUT_REGION)
            .expect("size checked")
            .fill(0);
    }
    let input_len = task.input.len();
    if let Err(e) = memory.write(&mut *store, 0, &task.input) {
        return abort_from_ctx(
            store,
            task,
            AbortReason::BadPointer,
            format!("write input: {e}"),
        );
    }

    let call = run.call(&mut *store, input_len as i32);
    let fuel_left = store.get_fuel().unwrap_or(0);
    let fuel_consumed = task.fuel.saturating_sub(fuel_left);

    match call {
        Err(e) => {
            // 故障优先级：显式记录的宿主故障 > 燃料耗尽 > 普通 trap。
            let reason = if let Some(f) = store.data().fault {
                f.into_reason()
            } else if fuel_left == 0
                || e.downcast_ref::<Trap>()
                    .copied()
                    == Some(Trap::OutOfFuel)
            {
                AbortReason::OutOfFuel
            } else {
                AbortReason::Trap
            };
            let mut out = abort_from_ctx(store, task, reason, e.to_string());
            out.fuel_consumed = fuel_consumed;
            out
        }
        Ok(plugin_return) => {
            // run 正常返回：取走暂存，按输入基准修订做一次原子提交。
            let (staged, events, calls) = take_evidence(store);
            let staged_writes = staged.len();
            match commit(store, &task.tenant, revision_before, staged) {
                Ok(revision_after) => Outcome {
                    tenant: task.tenant.clone(),
                    terminal: Terminal::Committed {
                        revision_before,
                        revision_after,
                        plugin_return,
                        staged_writes,
                    },
                    calls,
                    events,
                    fuel_consumed,
                    instance_released: false,
                },
                Err((reason, detail)) => Outcome {
                    tenant: task.tenant.clone(),
                    terminal: Terminal::Aborted { reason, detail },
                    calls,
                    // 提交失败：事件同样不发布。
                    events: Vec::new(),
                    fuel_consumed,
                    instance_released: false,
                },
            }
        }
    }
}

type StagedMap = HashMap<Vec<u8>, Vec<u8>>;

fn take_evidence(store: &mut Store<Ctx>) -> (StagedMap, Vec<Vec<u8>>, Vec<CallRecord>) {
    let d = store.data_mut();
    (
        std::mem::take(&mut d.staged),
        std::mem::take(&mut d.events),
        std::mem::take(&mut d.calls),
    )
}

/// 一次乐观提交：IMMEDIATE 事务取锁 -> 校验修订 -> 写全部暂存键 -> 推进修订。
fn commit(
    store: &mut Store<Ctx>,
    tenant: &str,
    revision_before: u64,
    staged: StagedMap,
) -> Result<u64, (AbortReason, String)> {
    // 事务需要 &mut Connection：临时取出，结束后无论成败都放回 Ctx。
    let mut conn = store.data_mut().conn.take().expect("connection present");
    let result = commit_inner(&mut conn, tenant, revision_before, staged);
    store.data_mut().conn = Some(conn);
    result
}

fn commit_inner(
    conn: &mut Connection,
    tenant: &str,
    revision_before: u64,
    staged: StagedMap,
) -> Result<u64, (AbortReason, String)> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| (AbortReason::HostError, format!("begin commit transaction: {e}")))?;

    let current: i64 = tx
        .query_row(
            "SELECT revision FROM tenants WHERE tenant=?1",
            [tenant],
            |r| r.get(0),
        )
        .optional()
        .map_err(|e| (AbortReason::HostError, e.to_string()))?
        .unwrap_or(0);
    if current as u64 != revision_before {
        // 修订冲突：显式回滚，丢弃全部暂存。
        tx.rollback()
            .map_err(|e| (AbortReason::HostError, e.to_string()))?;
        return Err((
            AbortReason::RevConflict,
            format!("revision conflict: base {revision_before}, current {current}"),
        ));
    }

    for (k, v) in staged.iter() {
        tx.execute(
            "INSERT INTO kv(tenant,key,value) VALUES(?1,?2,?3)
             ON CONFLICT(tenant,key) DO UPDATE SET value=excluded.value",
            rusqlite::params![tenant, k, v],
        )
        .map_err(|e| (AbortReason::HostError, e.to_string()))?;
    }

    let revision_after = revision_before + 1;
    tx.execute(
        "INSERT INTO tenants(tenant,revision) VALUES(?1,?2)
         ON CONFLICT(tenant) DO UPDATE SET revision=excluded.revision",
        rusqlite::params![tenant, revision_after as i64],
    )
    .map_err(|e| (AbortReason::HostError, e.to_string()))?;

    tx.commit()
        .map_err(|e| (AbortReason::HostError, format!("commit: {e}")))?;
    Ok(revision_after)
}

/// 中止路径：撤销全部暂存与事件，仅保留宿主调用证据。
fn abort_from_ctx(
    store: &mut Store<Ctx>,
    task: &Task,
    reason: AbortReason,
    detail: String,
) -> Outcome {
    let d = store.data_mut();
    let calls = std::mem::take(&mut d.calls);
    d.staged.clear();
    d.events.clear();
    let fuel_consumed = task.fuel.saturating_sub(store.get_fuel().unwrap_or(0));
    Outcome {
        tenant: task.tenant.clone(),
        terminal: Terminal::Aborted { reason, detail },
        calls,
        events: Vec::new(),
        fuel_consumed,
        instance_released: false,
    }
}

impl FaultKind {
    fn into_reason(self) -> AbortReason {
        match self {
            FaultKind::UnauthorizedKey => AbortReason::UnauthorizedKey,
            FaultKind::BadPointer => AbortReason::BadPointer,
            FaultKind::OutputTooLarge => AbortReason::OutputTooLarge,
            FaultKind::MemoryLimit => AbortReason::MemoryLimit,
            FaultKind::HostError => AbortReason::HostError,
        }
    }
}
