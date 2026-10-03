# 受控插件执行服务（Rust + Wasmtime + SQLite）

多租户、能力受限的 Wasm 插件执行器。每个任务指定**租户、模块、输入、允许读/写的键前缀**；
插件只能调用宿主授予的 `get` / `put` / `emit`，**得不到任何 WASI 能力**。
每次执行使用**独立实例**，受**燃料、线性内存、输出大小**三重限制；
宿主调用全部做**指针边界检查**并读取**该任务自己的上下文**。
写入先**暂存**，插件正常返回后以**输入时的基准修订**做一次乐观提交；
越界、燃料耗尽、宿主错误、修订冲突——**全部撤销**。

## 运行

```bash
cargo test            # 14 个真实 Wasm 模块测试
cargo run --example e2e
```

> 工具链注记：本机 `/usr/local/bin/cc` 是沙箱包装器，无法链接；
> `.cargo/config.toml` 已把 linker/CC 固定为真实的 `/usr/bin/gcc`。

## 安全模型

| 维度 | 机制 |
|---|---|
| 能力 | 模块导入白名单：仅 `plugin::{get,put,emit}` 且签名必须为 `(i32,i32)->i32`；任何 `wasi_snapshot_preview1::*` 或其他导入在实例化前被 `Rejected`。宿主从不创建 WASI 上下文 |
| 实例隔离 | 每次执行新建 SQLite 连接 + `Store` + `Instance`；结束时显式释放，`Outcome.instance_released` 由 `Ctx` 的 Drop 守卫回写为证据 |
| 键授权 | 读前缀与写前缀**独立**；按字节 `starts_with`；空前缀需显式授权 |
| 指针安全 | 所有 `[offset, offset+len)` 先经 64 位加法溢出检查再比对 `memory.data_size`，通过后才 `memory.read/write`；不存在裸指针解引用 |
| 燃料 | `Config::consume_fuel(true)` + `Store::set_fuel(task.fuel)`；耗尽得到 `Trap::OutOfFuel`，终态 `Aborted::OutOfFuel` |
| 内存 | 任务 `max_memory_bytes`：初始大小在模块校验阶段检查；运行期 `memory.grow` 经 `ResourceLimiter::memory_growing` 拒绝并 trap（`Aborted::MemoryLimit`） |
| 输出 | `emit` 载荷累计字节数受 `max_output_bytes` 限制，超限即 trap（`Aborted::OutputTooLarge`） |
| 原子性 | `put` 只写入本次 `Ctx` 的暂存 map；`run` 正常返回后才开启 `BEGIN IMMEDIATE` 事务，校验修订、upsert 全部暂存键、推进修订、提交。任何失败路径丢弃暂存与事件 |
| 乐观并发 | 每个租户一行 `revision`；提交要求 `current == revision_before`，否则 `Aborted::RevConflict` 并回滚。冲突者的写入在数据库中不可见 |

## 插件 ABI

模块必须导出线性内存 `mem` 和函数 `run(i32) -> i32`。

- 内存开头 **1024 字节**是输入区：宿主清零后写入任务输入，`run` 的参数为输入长度。
- `get(key_ptr, key_len) -> i32`
  - 命中：把值写回偏移 0（输入区），返回值长度；
  - `1` = 键不存在；`2` = 值长超过 1024。
  - 读前缀不匹配 / 指针越界 / 存储错误 → trap 中止。
- `put(key_ptr, key_len) -> i32`
  - 布局：`[key: key_len][u32 LE value_len][value: value_len]`，三段都必须在内存内；
  - 仅暂存，成功返回 `0`；写前缀不匹配 / 任一段越界 → trap 中止。
- `emit(ptr, len) -> i32`：追加一条事件，返回 `0`；越界 / 输出总量超限 → trap 中止。

`put` 后同一次执行内的 `get` 能读到暂存值（读己之写）；但在提交前不触碰数据库。

## 终态与证据

`Outcome`：

- `terminal`
  - `Committed { revision_before, revision_after, plugin_return, staged_writes }`
  - `Rejected { reason }`（模块/导入/初始内存不合格，未建实例、未耗燃料）
  - `Aborted { reason, detail }`，`reason ∈ {OutOfFuel, UnauthorizedKey, BadPointer, OutputTooLarge, MemoryLimit, HostError, Trap, RevConflict}`
- `calls: Vec<CallRecord>`——每次宿主调用一条，含序号、操作、键、结果（`Ok/NotFound/BufferTooSmall/Fault(kind)`），中止时保留到最后一次调用；
- `events`（仅提交成功时发布）、`fuel_consumed`、`instance_released`。

## 存储

SQLite（rusqlite，bundled），WAL 模式：

```sql
CREATE TABLE kv(tenant TEXT, key BLOB, value BLOB, PRIMARY KEY(tenant,key)) WITHOUT ROWID;
CREATE TABLE tenants(tenant TEXT PRIMARY KEY, revision INTEGER NOT NULL);
```

## 测试矩阵（均为 wat 汇编的真实模块）

| 测试 | 覆盖点 |
|---|---|
| `happy_path_commits_once_with_evidence` | 2 写 1 事件、修订 0→1、证据链、落盘 |
| `infinite_loop_burns_fuel_and_rolls_back_staged_writes` | 无限循环→燃料耗尽；先前暂存写撤销 |
| `unauthorized_put_is_denied_and_rolled_back` | 越权写键 |
| `unauthorized_get_uses_read_prefix_independent_of_write` | 读/写前缀独立 |
| `bad_pointer_is_rejected` | 指针越过 64KiB |
| `oversized_initial_memory_is_rejected_before_instantiation` | 初始内存超限 |
| `runtime_memory_growth_beyond_cap_traps_as_memory_limit` | 运行期 grow 超限 |
| `output_cap_aborts_and_rolls_back` | emit 总量超限，事件全部丢弃 |
| `wasi_imports_are_rejected` | 导入 `wasi_snapshot_preview1::fd_write` 被拒 |
| `staged_put_is_visible_to_following_get` | 读己之写 |
| `optimistic_revision_conflict_aborts_and_rolls_back` | prepare 后他人先提交→冲突回滚 |
| `two_tenants_execute_concurrently_with_isolation` | 两租户各 16 线程并发；同名键租户隔离、标记不串、提交数+冲突数守恒 |
| `concurrent_same_tenant_actually_observes_revision_conflicts` | 40 同租户并发，实证观察到真实冲突（典型 2 提交 / 38 冲突） |
| `fresh_instance_per_execution_has_no_state_leak` | 连跑两次，修订连续、无内存残留 |
