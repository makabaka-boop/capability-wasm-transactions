//! 集成测试：真实小型 Wasm 模块（WAT 编译）覆盖
//! 无限循环、越权键、错误指针、WASI 拒绝、输出上限、
//! 修订冲突与两个租户并发执行。

use plugin_host::{CallResult, CommitError, KvStore, Limits, PluginService, Task, TerminalState};
use std::collections::HashMap;
use std::sync::Arc;

/// 正常模块：get("data/config") → put("data/echo")；put("data/result", 输入)；emit("done", 输入)。
const OK_WAT: &str = r#"
(module
  (import "env" "get" (func $get (param i32 i32 i32 i32) (result i32)))
  (import "env" "put" (func $put (param i32 i32 i32 i32) (result i32)))
  (import "env" "emit" (func $emit (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (global $heap (mut i32) (i32.const 1024))
  (func (export "alloc") (param $len i32) (result i32)
    (local $p i32)
    (local.set $p (global.get $heap))
    (global.set $heap (i32.add (global.get $heap) (local.get $len)))
    (local.get $p))
  (func (export "run") (param $in_ptr i32) (param $in_len i32) (result i32)
    (local $n i32)
    ;; n = get("data/config") 到暂存缓冲 768
    (local.set $n (call $get (i32.const 16) (i32.const 11) (i32.const 768) (i32.const 64)))
    ;; 找到则复制到 "data/echo"
    (if (i32.ge_s (local.get $n) (i32.const 0))
      (then
        (drop (call $put (i32.const 32) (i32.const 9) (i32.const 768) (local.get $n)))))
    ;; 把任务输入写入 "data/result"
    (drop (call $put (i32.const 48) (i32.const 11) (local.get $in_ptr) (local.get $in_len)))
    ;; 发出 "done" 事件，载荷为输入
    (drop (call $emit (i32.const 64) (i32.const 4) (local.get $in_ptr) (local.get $in_len)))
    (i32.const 0))
  (data (i32.const 16) "data/config")
  (data (i32.const 32) "data/echo")
  (data (i32.const 48) "data/result")
  (data (i32.const 64) "done"))
"#;

/// 无限循环模块：先暂存一个 put，然后空转烧光燃料。
const SPIN_WAT: &str = r#"
(module
  (import "env" "put" (func $put (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (global $heap (mut i32) (i32.const 1024))
  (func (export "alloc") (param $len i32) (result i32)
    (local $p i32)
    (local.set $p (global.get $heap))
    (global.set $heap (i32.add (global.get $heap) (local.get $len)))
    (local.get $p))
  (func (export "run") (param i32 i32) (result i32)
    ;; 这笔暂存写入必须在燃料耗尽后被撤销
    (drop (call $put (i32.const 16) (i32.const 6) (i32.const 64) (i32.const 1)))
    (loop $spin (br $spin))
    (i32.const 0))
  (data (i32.const 16) "data/x")
  (data (i32.const 64) "v"))
"#;

/// 越权模块：先试探越权 get，再暂存合法 put，最后越权 put 被拒后中止。
const UNAUTH_WAT: &str = r#"
(module
  (import "env" "get" (func $get (param i32 i32 i32 i32) (result i32)))
  (import "env" "put" (func $put (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (global $heap (mut i32) (i32.const 1024))
  (func (export "alloc") (param $len i32) (result i32)
    (local $p i32)
    (local.set $p (global.get $heap))
    (global.set $heap (i32.add (global.get $heap) (local.get $len)))
    (local.get $p))
  (func (export "run") (param i32 i32) (result i32)
    ;; 1. 读允许前缀之外的键必须被拒绝（返回非零）
    (if (i32.eqz (call $get (i32.const 32) (i32.const 12) (i32.const 512) (i32.const 16)))
      (then unreachable))
    ;; 2. 合法写入 —— 暂存；运行中止时必须被撤销
    (drop (call $put (i32.const 16) (i32.const 7) (i32.const 64) (i32.const 1)))
    ;; 3. 写允许前缀之外的键必须被拒绝；随后中止运行
    (if (i32.ne (call $put (i32.const 32) (i32.const 12) (i32.const 64) (i32.const 1)) (i32.const 0))
      (then unreachable))
    (i32.const 0))
  (data (i32.const 16) "data/ok")
  (data (i32.const 32) "other/secret")
  (data (i32.const 64) "v"))
"#;

/// 错误指针模块：键合法，但值指针远超出线性内存。
const BADPTR_WAT: &str = r#"
(module
  (import "env" "put" (func $put (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (global $heap (mut i32) (i32.const 1024))
  (func (export "alloc") (param $len i32) (result i32)
    (local $p i32)
    (local.set $p (global.get $heap))
    (global.set $heap (i32.add (global.get $heap) (local.get $len)))
    (local.get $p))
  (func (export "run") (param i32 i32) (result i32)
    (drop (call $put (i32.const 16) (i32.const 6) (i32.const 0x7fff0000) (i32.const 0xffff)))
    (i32.const 0))
  (data (i32.const 16) "data/y"))
"#;

/// 请求 WASI 能力的模块：必须被拒绝实例化。
const WASI_WAT: &str = r#"
(module
  (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (func (export "alloc") (param i32) (result i32) (i32.const 1024))
  (func (export "run") (param i32 i32) (result i32) (i32.const 0)))
"#;

fn service() -> (Arc<PluginService>, Arc<KvStore>) {
    let kv = Arc::new(KvStore::open_in_memory().unwrap());
    let mut svc = PluginService::new(kv.clone()).unwrap();
    svc.register_module("ok", OK_WAT.as_bytes()).unwrap();
    svc.register_module("spin", SPIN_WAT.as_bytes()).unwrap();
    svc.register_module("unauth", UNAUTH_WAT.as_bytes()).unwrap();
    svc.register_module("badptr", BADPTR_WAT.as_bytes()).unwrap();
    svc.register_module("wasi", WASI_WAT.as_bytes()).unwrap();
    (Arc::new(svc), kv)
}

fn rw_task(tenant: &str, module: &str, input: Vec<u8>) -> Task {
    Task::new(tenant, module, input)
        .with_read_prefixes(&["data/"])
        .with_write_prefixes(&["data/"])
}

#[test]
fn happy_path_commits_and_returns_evidence() {
    let (svc, kv) = service();
    kv.seed("tenant-a", "data/config", b"cfg-v1").unwrap();

    let report = svc.execute(&rw_task("tenant-a", "ok", b"hello-input".to_vec()));

    assert_eq!(report.terminal, TerminalState::Committed, "{:?}", report.error);
    assert_eq!(report.baseline_rev, 0);
    assert_eq!(report.committed_rev, Some(1));
    // 暂存写入已落库：派生键 + 输入键
    assert_eq!(
        kv.get("tenant-a", "data/echo").unwrap().as_deref(),
        Some(b"cfg-v1".as_ref())
    );
    assert_eq!(
        kv.get("tenant-a", "data/result").unwrap().as_deref(),
        Some(b"hello-input".as_ref())
    );
    // emit 输出随报告返回
    assert_eq!(
        report.emitted,
        vec![("done".to_string(), b"hello-input".to_vec())]
    );
    // 宿主调用证据完整：get、两个 put、emit
    assert_eq!(report.evidence.len(), 4);
    assert!(
        report
            .evidence
            .iter()
            .any(|r| r.func == "get" && r.key == "data/config" && r.result == CallResult::Ok)
    );
    assert!(
        report
            .evidence
            .iter()
            .any(|r| r.func == "put" && r.key == "data/result" && r.result == CallResult::Ok)
    );
    assert!(
        report
            .evidence
            .iter()
            .any(|r| r.func == "emit" && r.key == "done" && r.result == CallResult::Ok)
    );
    assert!(report.fuel_consumed.unwrap() > 0);
}

#[test]
fn infinite_loop_runs_out_of_fuel_and_revokes_writes() {
    let (svc, kv) = service();

    let task = rw_task("tenant-a", "spin", vec![]).with_limits(Limits {
        fuel: 100_000,
        ..Default::default()
    });
    let report = svc.execute(&task);

    assert_eq!(report.terminal, TerminalState::FuelExhausted, "{:?}", report.error);
    // 循环前暂存的 put 被撤销，修订号不变
    assert!(kv.get("tenant-a", "data/x").unwrap().is_none());
    assert_eq!(kv.baseline("tenant-a").unwrap(), 0);
    // 证据里能看到那笔被撤销的暂存写入
    assert!(
        report
            .evidence
            .iter()
            .any(|r| r.func == "put" && r.key == "data/x" && r.result == CallResult::Ok)
    );
}

#[test]
fn unauthorized_key_is_denied_and_run_aborts() {
    let (svc, kv) = service();

    let report = svc.execute(&rw_task("tenant-a", "unauth", vec![]));

    assert_eq!(report.terminal, TerminalState::Trapped, "{:?}", report.error);
    // 越权读写都在证据中留下 Denied 记录
    assert!(
        report
            .evidence
            .iter()
            .any(|r| r.func == "get" && r.key == "other/secret" && r.result == CallResult::Denied)
    );
    assert!(
        report
            .evidence
            .iter()
            .any(|r| r.func == "put" && r.key == "other/secret" && r.result == CallResult::Denied)
    );
    // 之前的合法暂存写入也一并撤销
    assert!(kv.get("tenant-a", "data/ok").unwrap().is_none());
    assert!(kv.get("tenant-a", "other/secret").unwrap().is_none());
    assert_eq!(kv.baseline("tenant-a").unwrap(), 0);
}

#[test]
fn bad_pointer_is_caught_by_host_bounds_check() {
    let (svc, kv) = service();

    let report = svc.execute(&rw_task("tenant-a", "badptr", vec![]));

    assert_eq!(report.terminal, TerminalState::HostError, "{:?}", report.error);
    assert!(
        report.error.as_deref().unwrap().contains("out of bounds"),
        "unexpected error: {:?}",
        report.error
    );
    // 越界尝试留在证据里，但没有任何写入落库
    assert!(
        report
            .evidence
            .iter()
            .any(|r| r.func == "put" && r.key == "data/y" && r.result == CallResult::OutOfBounds)
    );
    assert!(kv.get("tenant-a", "data/y").unwrap().is_none());
    assert_eq!(kv.baseline("tenant-a").unwrap(), 0);
}

#[test]
fn wasi_import_is_rejected() {
    let (svc, _kv) = service();

    let report = svc.execute(&rw_task("tenant-a", "wasi", vec![]));

    assert_eq!(report.terminal, TerminalState::Rejected, "{:?}", report.error);
    assert!(
        report
            .error
            .as_deref()
            .unwrap()
            .contains("wasi_snapshot_preview1"),
        "unexpected error: {:?}",
        report.error
    );
}

#[test]
fn output_limit_is_enforced() {
    let (svc, _kv) = service();

    // 输入 128 字节，但 emit 输出上限只有 16 字节
    let task = rw_task("tenant-a", "ok", vec![b'x'; 128]).with_limits(Limits {
        max_output_bytes: 16,
        ..Default::default()
    });
    let report = svc.execute(&task);

    // 模块选择忽略 emit 错误码继续执行并提交
    assert_eq!(report.terminal, TerminalState::Committed, "{:?}", report.error);
    assert!(report.emitted.is_empty());
    assert!(
        report
            .evidence
            .iter()
            .any(|r| r.func == "emit" && r.result == CallResult::OutputLimited)
    );
}

#[test]
fn revision_conflict_revokes_writes() {
    let kv = KvStore::open_in_memory().unwrap();

    let mut staged = HashMap::new();
    staged.insert("data/a".to_string(), b"1".to_vec());
    assert_eq!(kv.commit("t", 0, &staged).unwrap(), 1);

    // 第二个提交仍基于旧基准 0：冲突，什么都不应用
    let mut staged2 = HashMap::new();
    staged2.insert("data/b".to_string(), b"2".to_vec());
    let err = kv.commit("t", 0, &staged2).unwrap_err();
    assert!(matches!(err, CommitError::Conflict { .. }));
    assert!(kv.get("t", "data/b").unwrap().is_none());
    assert_eq!(kv.get("t", "data/a").unwrap().as_deref(), Some(b"1".as_ref()));
    assert_eq!(kv.baseline("t").unwrap(), 1);
}

#[test]
fn two_tenants_run_concurrently() {
    let (svc, kv) = service();

    let mut handles = Vec::new();
    for (tenant, tag) in [("tenant-a", "A"), ("tenant-b", "B")] {
        let svc = svc.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..8 {
                let input = format!("{tag}-{i}").into_bytes();
                let report = svc.execute(&rw_task(tenant, "ok", input));
                assert_eq!(
                    report.terminal,
                    TerminalState::Committed,
                    "tenant {tenant} iter {i}: {:?}",
                    report.error
                );
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // 两个租户各自提交 8 次，互不冲突
    assert_eq!(kv.baseline("tenant-a").unwrap(), 8);
    assert_eq!(kv.baseline("tenant-b").unwrap(), 8);
    // 命名空间隔离：同名键各存各的值
    assert_eq!(
        kv.get("tenant-a", "data/result").unwrap().as_deref(),
        Some(b"A-7".as_ref())
    );
    assert_eq!(
        kv.get("tenant-b", "data/result").unwrap().as_deref(),
        Some(b"B-7".as_ref())
    );
}
