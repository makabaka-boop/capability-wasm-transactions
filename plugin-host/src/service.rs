//! 插件执行服务：注册模块、按任务在隔离实例中运行、提交或撤销写入。

use crate::host::{self, HostCallRecord, OobError, TaskCtx};
use crate::store::{CommitError, KvStore};
use std::collections::HashMap;
use std::sync::Arc;
use wasmtime::{Config, Engine, Linker, Module, Store, StoreLimitsBuilder};

/// 单次执行的资源限制。
#[derive(Debug, Clone)]
pub struct Limits {
    /// 燃料上限：耗尽即 trap。
    pub fuel: u64,
    /// 线性内存上限（字节）。
    pub max_memory_bytes: usize,
    /// `emit` 输出总字节上限。
    pub max_output_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            fuel: 1_000_000,
            max_memory_bytes: 16 * 1024 * 1024,
            max_output_bytes: 64 * 1024,
        }
    }
}

/// 一次插件执行任务：租户、模块、输入、允许读写的键前缀。
#[derive(Debug, Clone)]
pub struct Task {
    pub tenant: String,
    pub module: String,
    pub input: Vec<u8>,
    pub read_prefixes: Vec<String>,
    pub write_prefixes: Vec<String>,
    pub limits: Limits,
}

impl Task {
    pub fn new(tenant: impl Into<String>, module: impl Into<String>, input: Vec<u8>) -> Self {
        Self {
            tenant: tenant.into(),
            module: module.into(),
            input,
            read_prefixes: Vec::new(),
            write_prefixes: Vec::new(),
            limits: Limits::default(),
        }
    }

    pub fn with_read_prefixes(mut self, prefixes: &[&str]) -> Self {
        self.read_prefixes = prefixes.iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn with_write_prefixes(mut self, prefixes: &[&str]) -> Self {
        self.write_prefixes = prefixes.iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }
}

/// 执行的明确终态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalState {
    /// 正常返回，暂存写入已按基准修订号提交。
    Committed,
    /// 提交时修订号冲突，暂存写入已撤销。
    RevisionConflict,
    /// 燃料耗尽，暂存写入已撤销。
    FuelExhausted,
    /// 模块 trap 或返回非零码，暂存写入已撤销。
    Trapped,
    /// 宿主侧故障（如 guest 指针越界），暂存写入已撤销。
    HostError,
    /// 任务被拒绝执行（未知模块、请求了未授权导入如 WASI 等）。
    Rejected,
}

/// 执行报告：终态、证据、输出、燃料消耗。
#[derive(Debug)]
pub struct RunReport {
    pub tenant: String,
    pub module: String,
    pub terminal: TerminalState,
    pub baseline_rev: i64,
    pub committed_rev: Option<i64>,
    pub evidence: Vec<HostCallRecord>,
    pub emitted: Vec<(String, Vec<u8>)>,
    pub fuel_consumed: Option<u64>,
    pub error: Option<String>,
}

impl RunReport {
    fn blank(task: &Task) -> Self {
        Self {
            tenant: task.tenant.clone(),
            module: task.module.clone(),
            terminal: TerminalState::Rejected,
            baseline_rev: 0,
            committed_rev: None,
            evidence: Vec::new(),
            emitted: Vec::new(),
            fuel_consumed: None,
            error: None,
        }
    }
}

/// 受控插件执行服务。`Send + Sync`，可被多线程并发调用。
pub struct PluginService {
    engine: Engine,
    modules: HashMap<String, Module>,
    kv: Arc<KvStore>,
}

impl PluginService {
    pub fn new(kv: Arc<KvStore>) -> wasmtime::Result<Self> {
        let mut config = Config::new();
        config.consume_fuel(true);
        let engine = Engine::new(&config)?;
        Ok(Self {
            engine,
            modules: HashMap::new(),
            kv,
        })
    }

    /// 注册一个模块（接受 Wasm 二进制或 WAT 文本）。
    pub fn register_module(&mut self, name: &str, wasm: &[u8]) -> wasmtime::Result<()> {
        let module = Module::new(&self.engine, wasm)?;
        self.modules.insert(name.to_string(), module);
        Ok(())
    }

    /// 执行一个任务。
    ///
    /// 每次执行都使用独立的 `Store` 与实例；链接器只提供受许可的
    /// `env.get` / `env.put` / `env.emit`，不提供任何 WASI 能力。
    /// 函数返回时实例、线性内存等资源随 `Store` 一起释放。
    pub fn execute(&self, task: &Task) -> RunReport {
        let mut report = RunReport::blank(task);

        let module = match self.modules.get(&task.module) {
            Some(m) => m.clone(),
            None => {
                report.error = Some(format!("unknown module `{}`", task.module));
                return report;
            }
        };

        // 输入基准修订号：提交时据此做乐观并发检查。
        let baseline = match self.kv.baseline(&task.tenant) {
            Ok(b) => b,
            Err(e) => {
                report.terminal = TerminalState::HostError;
                report.error = Some(format!("failed to read baseline revision: {e}"));
                return report;
            }
        };
        report.baseline_rev = baseline;

        // 每任务独立上下文与 Store —— 实例间不共享任何可变状态。
        let ctx = TaskCtx {
            tenant: task.tenant.clone(),
            read_prefixes: task.read_prefixes.clone(),
            write_prefixes: task.write_prefixes.clone(),
            staged: HashMap::new(),
            evidence: Vec::new(),
            emitted: Vec::new(),
            emitted_bytes: 0,
            max_output_bytes: task.limits.max_output_bytes,
            store_limits: StoreLimitsBuilder::new()
                .memory_size(task.limits.max_memory_bytes)
                .instances(1)
                .memories(1)
                .tables(1)
                .build(),
            kv: self.kv.clone(),
            memory: None,
        };
        let mut store = Store::new(&self.engine, ctx);
        if let Err(e) = store.set_fuel(task.limits.fuel) {
            report.terminal = TerminalState::HostError;
            report.error = Some(format!("failed to set fuel: {e}"));
            return report;
        }
        store.limiter(|c| &mut c.store_limits);

        // 只链接受许可的三个宿主函数；不链接 WASI。
        // 任何请求其它导入（如 wasi_snapshot_preview1）的模块都会实例化失败。
        let mut linker = Linker::new(&self.engine);
        linker
            .func_wrap("env", "get", host::host_get)
            .and_then(|l| l.func_wrap("env", "put", host::host_put))
            .and_then(|l| l.func_wrap("env", "emit", host::host_emit))
            .expect("host functions have valid signatures");

        let instance = match linker.instantiate(&mut store, &module) {
            Ok(i) => i,
            Err(e) => {
                report.terminal = TerminalState::Rejected;
                report.error = Some(format!("instantiation rejected: {e:#}"));
                return report;
            }
        };

        // 运行：alloc 输入缓冲 → 写入输入 → 调用 run。
        let run_result: wasmtime::Result<i32> = (|| {
            let memory = instance
                .get_memory(&mut store, "memory")
                .ok_or_else(|| wasmtime::Error::msg("module does not export `memory`"))?;
            store.data_mut().memory = Some(memory);
            let alloc = instance.get_typed_func::<i32, i32>(&mut store, "alloc")?;
            let run = instance.get_typed_func::<(i32, i32), i32>(&mut store, "run")?;
            let in_len = task.input.len() as i32;
            let in_ptr = alloc.call(&mut store, in_len)?;
            memory.write(&mut store, in_ptr as usize, &task.input)?;
            run.call(&mut store, (in_ptr, in_len))
        })();

        let fuel_remaining = store.get_fuel().unwrap_or(0);
        report.fuel_consumed = Some(task.limits.fuel.saturating_sub(fuel_remaining));

        match run_result {
            Ok(0) => {
                // 正常返回：按基准修订号一次性提交暂存写入。
                let staged = std::mem::take(&mut store.data_mut().staged);
                match self.kv.commit(&task.tenant, baseline, &staged) {
                    Ok(rev) => {
                        report.terminal = TerminalState::Committed;
                        report.committed_rev = Some(rev);
                    }
                    Err(CommitError::Conflict { expected, actual }) => {
                        report.terminal = TerminalState::RevisionConflict;
                        report.error = Some(format!(
                            "revision conflict: baseline {expected}, current {actual}; staged writes revoked"
                        ));
                    }
                    Err(e) => {
                        report.terminal = TerminalState::HostError;
                        report.error = Some(format!("commit failed, staged writes revoked: {e}"));
                    }
                }
            }
            Ok(code) => {
                report.terminal = TerminalState::Trapped;
                report.error =
                    Some(format!("module returned non-zero exit code {code}; staged writes revoked"));
            }
            Err(e) => {
                report.terminal = classify(&e);
                report.error = Some(format!("{e:#}; staged writes revoked"));
            }
        }

        // 从该任务自己的上下文中取出证据与输出。
        let ctx = store.data();
        report.evidence = ctx.evidence.clone();
        report.emitted = ctx.emitted.clone();

        // 显式释放本实例的 Store：线性内存、燃料状态、限制器随之回收。
        drop(store);
        report
    }
}

/// 把运行期错误归类为明确终态。
fn classify(err: &wasmtime::Error) -> TerminalState {
    if err.downcast_ref::<OobError>().is_some() {
        return TerminalState::HostError;
    }
    let msg = format!("{err} {err:?}").to_lowercase();
    if msg.contains("out of bounds") {
        TerminalState::HostError
    } else if msg.contains("fuel") {
        TerminalState::FuelExhausted
    } else {
        TerminalState::Trapped
    }
}
