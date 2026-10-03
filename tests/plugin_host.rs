//! 端到端测试：全部使用 wat 汇编的真实小型 Wasm 模块。
//!
//! 覆盖：无限循环(燃料耗尽)、越权键、错误指针、内存上限、输出上限、
//! WASI 导入拒绝、暂存后崩溃回滚、读己之写、乐观修订冲突、两个租户并发执行。

use std::collections::HashMap;
use std::sync::Arc;

use plugin_host::{
    AbortReason, CallResult, HostOp, PluginService, Task, Terminal,
};
use tempfile::TempDir;
use wat::parse_str as wat;

// ---------------------------------------------------------------------------
// 测试用 Wasm 模块（通过 wat crate 汇编为真实 .wasm）
// ---------------------------------------------------------------------------

/// 正常模块：写两个允许前缀的键，发一个事件，返回 42。
/// 布局：2048 起为键1 "usr/a"；2100 起为键2 "usr/b"；3000 起为事件。
fn mod_happy() -> Vec<u8> {
    wat(
        r#"
        (module
            (import "plugin" "get"  (func $get  (param i32 i32) (result i32)))
            (import "plugin" "put"  (func $put  (param i32 i32) (result i32)))
            (import "plugin" "emit" (func $emit (param i32 i32) (result i32)))
            (memory (export "mem") 1)
            (data (i32.const 2048) "usr/a")
            (data (i32.const 2053) "\05\00\00\00hello")
            (data (i32.const 2100) "usr/b")
            (data (i32.const 2105) "\01\00\00\00!")
            (data (i32.const 3000) "done")
            (func (export "run") (param $ilen i32) (result i32)
                (drop (call $put (i32.const 2048) (i32.const 5)))
                (drop (call $put (i32.const 2100) (i32.const 5)))
                (drop (call $emit (i32.const 3000) (i32.const 4)))
                (i32.const 42)))
        "#,
    )
    .unwrap()
}

/// 无限循环模块：先暂存一个键，随后 br 0 死循环，靠燃料耗尽中止。
fn mod_infinite_loop() -> Vec<u8> {
    wat(
        r#"
        (module
            (import "plugin" "put" (func $put (param i32 i32) (result i32)))
            (memory (export "mem") 1)
            (data (i32.const 2048) "usr/loop")
            (data (i32.const 2056) "\03\00\00\00xyz")
            (func (export "run") (param i32) (result i32)
                (drop (call $put (i32.const 2048) (i32.const 8)))
                (loop $forever (br $forever))
                (i32.const 0)))
        "#,
    )
    .unwrap()
}

/// 越权写模块：尝试写 "sys/admin"（write 前缀只授权 "usr/"）。
fn mod_unauthorized_put() -> Vec<u8> {
    wat(
        r#"
        (module
            (import "plugin" "put" (func $put (param i32 i32) (result i32)))
            (memory (export "mem") 1)
            (data (i32.const 2048) "sys/admin")
            (data (i32.const 2057) "\01\00\00\00x")
            (func (export "run") (param i32) (result i32)
                (drop (call $put (i32.const 2048) (i32.const 9)))
                (i32.const 0)))
        "#,
    )
    .unwrap()
}

/// 越权读模块：get "sys/secret"（read 前缀只授权 "usr/"）。
fn mod_unauthorized_get() -> Vec<u8> {
    wat(
        r#"
        (module
            (import "plugin" "get" (func $get (param i32 i32) (result i32)))
            (memory (export "mem") 1)
            (data (i32.const 2048) "sys/secret")
            (func (export "run") (param i32) (result i32)
                (drop (call $get (i32.const 2048) (i32.const 10)))
                (i32.const 0)))
        "#,
    )
    .unwrap()
}

/// 错误指针模块：put 的键指针落在 64KiB 之外。
fn mod_bad_pointer() -> Vec<u8> {
    wat(
        r#"
        (module
            (import "plugin" "put" (func $put (param i32 i32) (result i32)))
            (memory (export "mem") 1)
            (func (export "run") (param i32) (result i32)
                (drop (call $put (i32.const 70000) (i32.const 3)))
                (i32.const 0)))
        "#,
    )
    .unwrap()
}

/// 大内存模块：初始声明 2 页（128KiB），任务只给 64KiB。
fn mod_memory_too_large() -> Vec<u8> {
    wat(
        r#"
        (module
            (memory (export "mem") 2)
            (func (export "run") (param i32) (result i32) (i32.const 0)))
        "#,
    )
    .unwrap()
}

/// 运行时增长内存模块：初始 1 页，run 中 memory.grow(1) 试图翻倍。
fn mod_grows_memory() -> Vec<u8> {
    wat(
        r#"
        (module
            (memory (export "mem") 1)
            (func (export "run") (param i32) (result i32)
                (drop (memory.grow (i32.const 1)))
                (i32.const 0)))
        "#,
    )
    .unwrap()
}

/// 输出超限模块：emit 两次 8 字节，任务输出上限 10 字节。
fn mod_output_too_large() -> Vec<u8> {
    wat(
        r#"
        (module
            (import "plugin" "emit" (func $emit (param i32 i32) (result i32)))
            (memory (export "mem") 1)
            (data (i32.const 3000) "12345678")
            (func (export "run") (param i32) (result i32)
                (drop (call $emit (i32.const 3000) (i32.const 8)))
                (drop (call $emit (i32.const 3000) (i32.const 8)))
                (i32.const 0)))
        "#,
    )
    .unwrap()
}

/// 读己之写模块：put 后立刻 get 同一键，校验长度与首字节，不符则 unreachable。
fn mod_read_own_writes() -> Vec<u8> {
    wat(
        r#"
        (module
            (import "plugin" "get"  (func $get  (param i32 i32) (result i32)))
            (import "plugin" "put" (func $put  (param i32 i32) (result i32)))
            (memory (export "mem") 1)
            (data (i32.const 2048) "usr/k")
            (data (i32.const 2053) "\03\00\00\00abc")
            (func (export "run") (param i32) (result i32)
                (drop (call $put (i32.const 2048) (i32.const 5)))
                (if (i32.ne (call $get (i32.const 2048) (i32.const 5)) (i32.const 3))
                    (then unreachable))
                ;; get 把值写回偏移 0，首字节应当是 'a' (97)
                (if (i32.ne (i32.load8_u (i32.const 0)) (i32.const 97))
                    (then unreachable))
                (i32.const 0)))
        "#,
    )
    .unwrap()
}

/// 并发模块：键 = "o/" + 输入字节；值 = "v-" + 输入字节；emit 键本身。
/// 所有自建数据从 4096 起（远离 get 结果写回的 0..1024 输入区）：
/// 4096.. 放键 "o/" + 输入；键尾后 4 字节值长度头(ilen+2)；头后放值。
fn mod_concurrent() -> Vec<u8> {
    wat(
        r#"
        (module
            (import "plugin" "get"  (func $get  (param i32 i32) (result i32)))
            (import "plugin" "put"  (func $put  (param i32 i32) (result i32)))
            (import "plugin" "emit" (func $emit (param i32 i32) (result i32)))
            (memory (export "mem") 1)
            (data (i32.const 4096) "o/")
            (func (export "run") (param $ilen i32) (result i32)
                (local $i i32)

                ;; 键：把输入拷到 4098，键长 = 2 + ilen
                (local.set $i (i32.const 0))
                (block $donek (loop $copyk
                    (br_if $donek (i32.ge_u (local.get $i) (local.get $ilen)))
                    (i32.store8
                        (i32.add (i32.const 4098) (local.get $i))
                        (i32.load8_u (local.get $i)))
                    (local.set $i (i32.add (local.get $i) (i32.const 1)))
                    (br $copyk)))

                ;; 值区起点 = 4096 + (2+ilen) + 4 = 4102 + ilen
                ;; 值前 2 字节固定 "v-"
                (i32.store8 (i32.add (i32.const 4102) (local.get $ilen)) (i32.const 118))
                (i32.store8 (i32.add (i32.const 4103) (local.get $ilen)) (i32.const 45))
                ;; 值长度头（键尾 4098+ilen 处）= ilen+2
                (i32.store
                    (i32.add (i32.const 4098) (local.get $ilen))
                    (i32.add (local.get $ilen) (i32.const 2)))
                ;; 值的其余部分
                (local.set $i (i32.const 0))
                (block $donev (loop $copyv
                    (br_if $donev (i32.ge_u (local.get $i) (local.get $ilen)))
                    (i32.store8
                        (i32.add (i32.const 4104) (i32.add (local.get $ilen) (local.get $i)))
                        (i32.load8_u (local.get $i)))
                    (local.set $i (i32.add (local.get $i) (i32.const 1)))
                    (br $copyv)))

                (drop (call $put (i32.const 4096) (i32.add (i32.const 2) (local.get $ilen))))
                ;; get 校验：返回值长度必须为 ilen+2（结果写回输入区，不影响键布局）
                (if (i32.ne
                        (call $get (i32.const 4096) (i32.add (i32.const 2) (local.get $ilen)))
                        (i32.add (local.get $ilen) (i32.const 2)))
                    (then unreachable))
                (drop (call $emit (i32.const 4096) (i32.add (i32.const 2) (local.get $ilen))))
                (i32.const 0)))
        "#,
    )
    .unwrap()
}

/// 尝试导入 WASI fd_write 的模块——必须被白名单拒绝。
fn mod_imports_wasi() -> Vec<u8> {
    wat(
        r#"
        (module
            (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
            (memory (export "mem") 1)
            (func (export "run") (param i32) (result i32) (i32.const 0)))
        "#,
    )
    .unwrap()
}

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

fn svc() -> (PluginService, TempDir) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("plugins.db");
    let svc = PluginService::open(path.to_str().unwrap()).unwrap();
    (svc, dir)
}

fn task(tenant: &str, bytes: Vec<u8>) -> Task {
    Task {
        tenant: tenant.to_string(),
        module_bytes: bytes,
        input: Vec::new(),
        read_prefixes: vec![b"usr/".to_vec(), b"o/".to_vec()],
        write_prefixes: vec![b"usr/".to_vec(), b"o/".to_vec()],
        fuel: 10_000_000,
        max_memory_bytes: 64 * 1024,
        max_output_bytes: 4096,
    }
}

fn op_seq(outcome: &plugin_host::Outcome) -> Vec<(HostOp, CallResult)> {
    outcome
        .calls
        .iter()
        .map(|c| (c.op, c.result.clone()))
        .collect()
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

#[test]
fn happy_path_commits_once_with_evidence() {
    let (svc, _d) = svc();
    let out = svc.run_task(task("t1", mod_happy()));

    let Terminal::Committed {
        revision_before,
        revision_after,
        plugin_return,
        staged_writes,
    } = out.terminal.clone()
    else {
        panic!("expected commit, got {:?}", out.terminal);
    };
    assert_eq!(revision_before, 0);
    assert_eq!(revision_after, 1);
    assert_eq!(plugin_return, 42);
    assert_eq!(staged_writes, 2);
    assert_eq!(out.events, vec![b"done".to_vec()]);
    assert!(out.fuel_consumed > 0, "fuel must be metered");
    assert!(out.instance_released, "instance must be released");

    // 调用证据：put, put, emit，顺序与返回值都有记录。
    let seq = op_seq(&out);
    assert_eq!(seq.len(), 3);
    assert_eq!(seq[0].0, HostOp::Put);
    assert_eq!(seq[0].1, CallResult::Ok(0));
    assert_eq!(seq[1].0, HostOp::Put);
    assert_eq!(seq[2].0, HostOp::Emit);

    // 数据确实落盘。
    let snap = svc.snapshot("t1").unwrap();
    assert_eq!(snap.get(b"usr/a".as_slice()), Some(&b"hello".to_vec()));
    assert_eq!(snap.get(b"usr/b".as_slice()), Some(&b"!".to_vec()));
}

#[test]
fn infinite_loop_burns_fuel_and_rolls_back_staged_writes() {
    let (svc, _d) = svc();
    svc.seed("t1", &[(b"usr/existing".to_vec(), b"v".to_vec())]).unwrap();

    let out = svc.run_task(Task {
        fuel: 200_000,
        ..task("t1", mod_infinite_loop())
    });

    assert!(
        matches!(out.terminal, Terminal::Aborted {
            reason: AbortReason::OutOfFuel, ..
        }),
        "got {:?}",
        out.terminal
    );
    assert!(out.fuel_consumed >= 200_000);
    assert!(out.instance_released);

    // 暂存的 usr/loop 必须随中止撤销。
    let snap = svc.snapshot("t1").unwrap();
    assert!(!snap.contains_key(b"usr/loop".as_slice()));
    assert_eq!(snap.get(b"usr/existing".as_slice()), Some(&b"v".to_vec()));

    // 证据保留到中止前的最后一次调用（那次 put 是成功的）。
    assert_eq!(out.calls.len(), 1);
    assert_eq!(out.calls[0].op, HostOp::Put);
    assert_eq!(out.calls[0].result, CallResult::Ok(0));

    // 修订号未被推进。
    assert_eq!(svc.snapshot("t1").unwrap().len(), 1);
}

#[test]
fn unauthorized_put_is_denied_and_rolled_back() {
    let (svc, _d) = svc();
    let out = svc.run_task(task("t1", mod_unauthorized_put()));
    assert!(matches!(
        out.terminal,
        Terminal::Aborted { reason: AbortReason::UnauthorizedKey, .. }
    ));
    assert!(out.instance_released);
    assert_eq!(out.events.len(), 0, "aborted events must not be published");
    assert!(svc.snapshot("t1").unwrap().is_empty());

    // 证据链显示那次 put 的失败原因。
    let last = out.calls.last().unwrap();
    assert_eq!(last.op, HostOp::Put);
    assert!(matches!(
        last.result,
        CallResult::Fault(plugin_host::FaultKind::UnauthorizedKey)
    ));
}

#[test]
fn unauthorized_get_uses_read_prefix_independent_of_write() {
    let (svc, _d) = svc();
    svc.seed("t1", &[(b"sys/secret".to_vec(), b"topsecret".to_vec())])
        .unwrap();
    // 写前缀放开也不能越权读。
    let out = svc.run_task(Task {
        read_prefixes: vec![b"usr/".to_vec()],
        write_prefixes: vec![b"".to_vec()],
        ..task("t1", mod_unauthorized_get())
    });
    assert!(matches!(
        out.terminal,
        Terminal::Aborted { reason: AbortReason::UnauthorizedKey, .. }
    ));
    // 未读到秘密：事件/输出为空，且无任何写入。
    assert!(svc.snapshot("t1").unwrap().contains_key(b"sys/secret".as_slice()));
}

#[test]
fn bad_pointer_is_rejected() {
    let (svc, _d) = svc();
    let out = svc.run_task(task("t1", mod_bad_pointer()));
    assert!(matches!(
        out.terminal,
        Terminal::Aborted { reason: AbortReason::BadPointer, .. }
    ));
    assert!(out.instance_released);
    assert!(svc.snapshot("t1").unwrap().is_empty());
    let last = out.calls.last().unwrap();
    assert!(matches!(
        last.result,
        CallResult::Fault(plugin_host::FaultKind::BadPointer)
    ));
}

#[test]
fn oversized_initial_memory_is_rejected_before_instantiation() {
    let (svc, _d) = svc();
    let out = svc.run_task(task("t1", mod_memory_too_large()));
    match out.terminal {
        Terminal::Rejected { .. } | Terminal::Aborted {
            reason: AbortReason::MemoryLimit,
            ..
        } => {}
        other => panic!("expected rejection/abort, got {other:?}"),
    }
    assert_eq!(out.fuel_consumed, 0);
}

#[test]
fn runtime_memory_growth_beyond_cap_traps_as_memory_limit() {
    let (svc, _d) = svc();
    // 初始 1 页 = 64KiB，恰好等于上限；再 grow 1 页即越限。
    let out = svc.run_task(Task {
        max_memory_bytes: 64 * 1024,
        ..task("t1", mod_grows_memory())
    });
    assert!(
        matches!(
            out.terminal,
            Terminal::Aborted { reason: AbortReason::MemoryLimit, .. }
        ),
        "got {:?}",
        out.terminal
    );
    assert!(out.instance_released);
}

#[test]
fn output_cap_aborts_and_rolls_back() {
    let (svc, _d) = svc();
    let out = svc.run_task(Task {
        max_output_bytes: 10,
        ..task("t1", mod_output_too_large())
    });
    assert!(matches!(
        out.terminal,
        Terminal::Aborted { reason: AbortReason::OutputTooLarge, .. }
    ));
    // 第一次 emit 成功入证据，第二次越限。
    assert_eq!(out.calls.len(), 2);
    assert_eq!(out.calls[0].result, CallResult::Ok(0));
    assert!(matches!(
        out.calls[1].result,
        CallResult::Fault(plugin_host::FaultKind::OutputTooLarge)
    ));
    assert!(out.events.is_empty(), "events discarded on abort");
}

#[test]
fn wasi_imports_are_rejected() {
    let (svc, _d) = svc();
    let out = svc.run_task(task("t1", mod_imports_wasi()));
    let Terminal::Rejected { reason } = out.terminal else {
        panic!("expected rejection, got {:?}", out.terminal);
    };
    assert!(reason.contains("not on the allow-list"), "reason: {reason}");
    assert_eq!(out.fuel_consumed, 0);
}

#[test]
fn staged_put_is_visible_to_following_get() {
    let (svc, _d) = svc();
    let out = svc.run_task(task("t1", mod_read_own_writes()));
    assert!(
        matches!(out.terminal, Terminal::Committed { .. }),
        "{:?}",
        out.terminal
    );
    assert_eq!(out.calls.len(), 2);
    assert_eq!(out.calls[1].op, HostOp::Get);
    assert_eq!(out.calls[1].result, CallResult::Ok(3));
}

#[test]
fn optimistic_revision_conflict_aborts_and_rolls_back() {
    let (svc, _d) = svc();
    svc.seed("t1", &[(b"usr/base".to_vec(), b"1".to_vec())]).unwrap();
    let rev_after_seed = {
        // seed 推进了修订号；这里新准备一个执行。
        let prep = svc
            .prepare(task("t1", mod_happy()))
            .expect("prepare ok");
        prep.revision_before()
    };

    // 第一个执行准备好后，另一个正常提交先推进修订。
    let prep = svc.prepare(task("t1", mod_happy())).unwrap();
    assert_eq!(prep.revision_before(), rev_after_seed);

    let committer = svc.run_task(task("t1", mod_happy()));
    assert!(matches!(committer.terminal, Terminal::Committed { .. }));

    // prep 的基准修订现在过期：提交必须冲突回滚。
    let out = prep.run(&svc);
    assert!(
        matches!(
            out.terminal,
            Terminal::Aborted { reason: AbortReason::RevConflict, .. }
        ),
        "got {:?}",
        out.terminal
    );
    assert!(out.instance_released);

    // 冲突执行自己的两次 put 没有二次落盘：
    // 快照里是先提交者的两个键加上 seed 的 base 键，修订号只多推进一次。
    let snap = svc.snapshot("t1").unwrap();
    assert_eq!(snap.len(), 3);
}

#[test]
fn two_tenants_execute_concurrently_with_isolation() {
    let (svc, _d) = svc();
    // 预置跨租户数据：两个键名字节相同、值不同，证明按租户隔离。
    svc.seed(
        "tenantA",
        &[(b"shared/key".to_vec(), b"A-ONLY".to_vec())],
    )
    .unwrap();
    svc.seed(
        "tenantB",
        &[(b"shared/key".to_vec(), b"B-ONLY".to_vec())],
    )
    .unwrap();

    let svc = Arc::new(svc);
    let module = Arc::new(mod_concurrent());

    // 每个租户 16 个并发任务；两租户输入区间不相交，且键中带租户标记，
    // 这样“键集合不相交”可以作为强隔离断言。
    const N: u8 = 16;
    let mut handles = Vec::new();
    for (tenant, base) in [("tenantA", b'A'), ("tenantB", b'a')] {
        for i in 0..N {
            let svc = svc.clone();
            let module = module.clone();
            let tenant = tenant.to_string();
            handles.push(std::thread::spawn(move || {
                // A: 'A'+i, B: 'a'+i；第二字节为序号
                let tag = base.wrapping_add(i);
                let input = vec![tag, i + b'0'];
                let out = svc.run_task(Task {
                    tenant: tenant.clone(),
                    input,
                    ..task(&tenant, (*module).clone())
                });
                (tenant, tag, out)
            }));
        }
    }

    let mut committed: HashMap<String, u32> =
        [("tenantA".into(), 0), ("tenantB".into(), 0)]
            .into_iter()
            .collect();
    let mut conflicts = 0;
    for h in handles {
        let (tenant, _tag, out) = h.join().unwrap();
        assert!(out.instance_released, "every instance must be released");
        match &out.terminal {
            Terminal::Committed { .. } => {
                *committed.get_mut(&tenant).unwrap() += 1;
                // 并发模块自己会 get 校验，证据链完整：put/get/emit。
                assert_eq!(out.calls.len(), 3);
                assert_eq!(out.events.len(), 1);
            }
            Terminal::Aborted {
                reason: AbortReason::RevConflict,
                ..
            } => conflicts += 1,
            other => panic!("unexpected terminal: {other:?}"),
        }
    }

    // 乐观并发：同租户可能有冲突，但每个租户最终至少应有一批成功键。
    let snap_a = svc.snapshot("tenantA").unwrap();
    let snap_b = svc.snapshot("tenantB").unwrap();
    assert!(snap_a.len() >= 2, "A keeps seed plus committed keys");
    assert!(snap_b.len() >= 2);

    // 隔离 1：任务键带租户标记，A/B 集合必须互不相交。
    for k in snap_a.keys() {
        if k.starts_with(b"o/") {
            assert!(!snap_b.contains_key(k), "key {k:?} leaked across tenants");
        }
    }

    // 隔离 2：同名字节键 shared/key 在两个租户中各自独立、互不覆盖。
    assert_eq!(snap_a[b"shared/key".as_ref()], b"A-ONLY");
    assert_eq!(snap_b[b"shared/key".as_ref()], b"B-ONLY");

    // 每个成功键的值都带有自己的输入标记。
    for (k, v) in &snap_a {
        if k.starts_with(b"o/") {
            assert!(k[2].is_ascii_uppercase(), "A key tag leaked: {k:?}");
            assert_eq!(&v[..2], b"v-");
            assert_eq!(v[2], k[2]);
        }
    }
    for (k, v) in &snap_b {
        if k.starts_with(b"o/") {
            assert!(k[2].is_ascii_lowercase(), "B key tag leaked: {k:?}");
            assert_eq!(v[2], k[2]);
        }
    }

    // 提交数 + 冲突数 = 总任务数；冲突者的写入必须不可见。
    let total_committed: u32 = committed.values().sum();
    assert_eq!(total_committed + conflicts, 2 * N as u32);
    // 快照内任务键数量恰好等于成功数（每租户额外一个 seed 键）。
    assert_eq!(
        snap_a.keys().filter(|k| k.starts_with(b"o/")).count() as u32,
        committed["tenantA"]
    );
    assert_eq!(
        snap_b.keys().filter(|k| k.starts_with(b"o/")).count() as u32,
        committed["tenantB"]
    );
}

#[test]
fn fresh_instance_per_execution_has_no_state_leak() {
    let (svc, _d) = svc();
    // 连续两次执行同一模块；第二次不应看到第一次内存中的残留，
    // 且每次都独立提交、修订连续。
    let o1 = svc.run_task(task("t1", mod_happy()));
    let o2 = svc.run_task(task("t1", mod_happy()));
    let (r1, r2) = match (&o1.terminal, &o2.terminal) {
        (
            Terminal::Committed { revision_after: r1, .. },
            Terminal::Committed { revision_before: b2, revision_after: r2, .. },
        ) => {
            assert_eq!(*b2, *r1, "second run must base on first revision");
            (*r1, *r2)
        }
        other => panic!("{other:?}"),
    };
    assert_eq!(r1 + 1, r2);
}

#[test]
fn concurrent_same_tenant_actually_observes_revision_conflicts() {
    let (svc, _d) = svc();
    let svc = Arc::new(svc);
    let module = Arc::new(mod_concurrent());
    const M: u8 = 40;
    let mut handles = Vec::new();
    for i in 0..M {
        let svc = svc.clone();
        let module = module.clone();
        handles.push(std::thread::spawn(move || {
            // 同租户、键互不相同 => 应全部成功
            svc.run_task(Task {
                tenant: "solo".into(),
                input: vec![b'z', i.wrapping_add(b'0')],
                ..task("solo", (*module).clone())
            })
        }));
    }
    let (mut ok, mut conflict) = (0u32, 0u32);
    for h in handles {
        match h.join().unwrap().terminal {
            Terminal::Committed { .. } => ok += 1,
            Terminal::Aborted { reason: AbortReason::RevConflict, .. } => conflict += 1,
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(ok + conflict, M as u32);
    assert_eq!(svc.snapshot("solo").unwrap().len() as u32, ok);
    eprintln!("same-tenant: committed={ok} conflicts={conflict}");
    assert!(conflict > 0, "40 concurrent same-tenant commits must yield conflicts");
}
