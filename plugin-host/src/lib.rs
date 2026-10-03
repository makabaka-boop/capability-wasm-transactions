//! 受控插件执行服务：Rust + Wasmtime + SQLite。
//!
//! - 任务指定租户、模块、输入与允许读写的键前缀；
//! - 模块只能导入受许可的 `get` / `put` / `emit`，不获得 WASI 能力；
//! - 每次执行使用独立实例，限制燃料、线性内存与输出大小；
//! - 宿主调用检查指针边界，且只读写该任务自己的上下文；
//! - 插件写入先暂存，正常返回后按输入基准修订号一次提交；
//!   越界、燃料耗尽、宿主错误或修订冲突全部撤销写入；
//! - 返回宿主调用证据与明确终态，并释放实例资源。

#![forbid(unsafe_code)]

pub mod host;
pub mod service;
pub mod store;

pub use host::{CallResult, HostCallRecord};
pub use service::{Limits, PluginService, RunReport, Task, TerminalState};
pub use store::{CommitError, KvStore};
