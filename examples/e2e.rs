//! 端到端示例：打开服务 -> 预置数据 -> 运行插件 -> 打印终态与宿主调用证据。
//!
//! 运行：`cargo run --example e2e`
//!
//! 这里的插件用 WAT 内联汇编并在进程内编译；真实部署时 module_bytes
//! 来自受信构建管道分发的 .wasm。

use plugin_host::{PluginService, Task, Terminal};
use wat::parse_str as wat;

fn main() -> anyhow::Result<()> {
    // 临时数据库，示例退出即清理。
    let dir = std::env::temp_dir().join(format!(
        "plugin-host-e2e-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir)?;
    let db = dir.join("example.db");
    let svc = PluginService::open(db.to_str().unwrap())?;

    // 租户 acme 预置一个可读键。
    svc.seed("acme", &[(b"usr/name".to_vec(), b"alice".to_vec())])?;

    // 插件：读 usr/name，写 usr/greeting，发一个事件，返回 0。
    // 内存布局约定见 README（put 的值：键后跟 4 字节 LE 长度 + 值）。
    let module = wat(
        r#"
        (module
            (import "plugin" "get"  (func $get  (param i32 i32) (result i32)))
            (import "plugin" "put"  (func $put  (param i32 i32) (result i32)))
            (import "plugin" "emit" (func $emit (param i32 i32) (result i32)))
            (memory (export "mem") 1)
            (data (i32.const 2048) "usr/name")
            (data (i32.const 3000) "usr/greeting")
            (data (i32.const 3012) "\05\00\00\00hello")
            (func (export "run") (param i32) (result i32)
                ;; 读名字（长度 8），结果写回输入区偏移 0
                (drop (call $get (i32.const 2048) (i32.const 8)))
                (drop (call $put (i32.const 3000) (i32.const 12)))
                (drop (call $emit (i32.const 3000) (i32.const 12)))
                (i32.const 0)))
        "#,
    )?;

    let task = Task {
        tenant: "acme".into(),
        module_bytes: module,
        input: Vec::new(),
        read_prefixes: vec![b"usr/".to_vec()],
        write_prefixes: vec![b"usr/".to_vec()],
        fuel: 10_000_000,
        max_memory_bytes: 64 * 1024,
        max_output_bytes: 4096,
    };

    let out = svc.run_task(task);

    println!("tenant      : {}", out.tenant);
    println!("terminal    : {:?}", out.terminal);
    println!("fuel consumed: {}", out.fuel_consumed);
    println!("released    : {}", out.instance_released);
    println!("host calls  :");
    for c in &out.calls {
        println!(
            "  #{:<2} {:?} key={:?} -> {:?}",
            c.seq,
            c.op,
            String::from_utf8_lossy(&c.key),
            c.result
        );
    }
    for (i, e) in out.events.iter().enumerate() {
        println!("event[{i}]    : {:?}", String::from_utf8_lossy(e));
    }

    assert!(matches!(out.terminal, Terminal::Committed { .. }));
    assert!(out.instance_released);

    let snap = svc.snapshot("acme")?;
    println!("kv snapshot : {snap:?}");
    Ok(())
}
