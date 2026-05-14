use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

const COUNTER_KEY: &str = "counter:visits";

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("nx-cli should live under crates/nx-cli")
        .to_path_buf()
}

fn free_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().unwrap().to_string()
}

fn temp_path(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    path.push(format!("numax-{name}-{nanos}"));
    path
}

fn nx_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_nx"))
}

fn distributed_counter_wasm() -> PathBuf {
    workspace_root().join(
        "examples/distributed_counter/target/wasm32-unknown-unknown/release/distributed_counter.wasm",
    )
}

fn assert_success(output: &Output, label: &str) {
    assert!(
        output.status.success(),
        "{label} failed\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_printed_counter(output: &Output, label: &str, expected: u64) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let needle = format!("{COUNTER_KEY} = {expected}");
    assert!(
        stdout.contains(&needle),
        "{label} did not print final converged value {expected}\nstdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[ignore = "requires built distributed_counter.wasm and local TCP sockets"]
fn two_nx_run_processes_converge_distributed_counter() {
    let wasm = distributed_counter_wasm();
    assert!(
        wasm.exists(),
        "missing {wasm:?}; build it with: cargo build --release --target wasm32-unknown-unknown --manifest-path examples/distributed_counter/Cargo.toml"
    );

    let addr_a = free_addr();
    let addr_b = free_addr();
    let data_a = temp_path("cli-smoke-a");
    let data_b = temp_path("cli-smoke-b");
    let nx = nx_bin();

    let node_a = Command::new(&nx)
        .arg("run")
        .arg(&wasm)
        .arg("--listen")
        .arg(&addr_a)
        .arg("--peer")
        .arg(&addr_b)
        .arg("--datastore-path")
        .arg(&data_a)
        .arg("--wait-before-run")
        .arg("1500ms")
        .arg("--settle-for")
        .arg("1800ms")
        .arg("--print-gcounter")
        .arg(COUNTER_KEY)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn node A");

    std::thread::sleep(std::time::Duration::from_millis(200));

    let output_b = Command::new(&nx)
        .arg("run")
        .arg(&wasm)
        .arg("--listen")
        .arg(&addr_b)
        .arg("--peer")
        .arg(&addr_a)
        .arg("--datastore-path")
        .arg(&data_b)
        .arg("--wait-before-run")
        .arg("1500ms")
        .arg("--settle-for")
        .arg("1800ms")
        .arg("--print-gcounter")
        .arg(COUNTER_KEY)
        .output()
        .expect("run node B");

    let output_a = node_a.wait_with_output().expect("wait node A");

    assert_success(&output_a, "node A");
    assert_success(&output_b, "node B");
    assert_printed_counter(&output_a, "node A", 2);
    assert_printed_counter(&output_b, "node B", 2);
}
