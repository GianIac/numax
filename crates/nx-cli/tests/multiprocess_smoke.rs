use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
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

fn previous_nx_bin() -> PathBuf {
    let path = std::env::var_os("NUMAX_PREVIOUS_NX_BIN")
        .map(PathBuf::from)
        .expect("NUMAX_PREVIOUS_NX_BIN must point to the previous release binary");
    assert!(
        path.is_file(),
        "NUMAX_PREVIOUS_NX_BIN does not point to a file: {path:?}"
    );
    path
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

#[cfg(unix)]
fn wait_for_http(addr: SocketAddr, authorization: Option<&str>) -> String {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        match TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(100)) {
            Ok(mut stream) => {
                stream
                    .set_read_timeout(Some(std::time::Duration::from_secs(1)))
                    .unwrap();
                let authorization = authorization
                    .map(|value| format!("Authorization: {value}\r\n"))
                    .unwrap_or_default();
                let request = format!(
                    "GET /api/v1/health HTTP/1.1\r\nHost: {addr}\r\n{authorization}Connection: close\r\n\r\n"
                );
                stream.write_all(request.as_bytes()).unwrap();
                let mut response = String::new();
                stream.read_to_string(&mut response).unwrap();
                return response;
            }
            Err(error) if std::time::Instant::now() < deadline => {
                let _ = error;
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
            Err(error) => panic!("management API did not start on {addr}: {error}"),
        }
    }
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

#[cfg(unix)]
fn printed_counter_value(output: &Output, label: &str) -> u64 {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let prefix = format!("{COUNTER_KEY} = ");
    stdout
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_else(|| {
            panic!(
                "{label} did not print a final counter value\nstdout:\n{stdout}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stderr)
            )
        })
}

fn assert_counter_wasm_exists() -> PathBuf {
    let wasm = distributed_counter_wasm();
    assert!(
        wasm.exists(),
        "missing {wasm:?}; build it with: cargo build --release --target wasm32-unknown-unknown --manifest-path examples/distributed_counter/Cargo.toml"
    );
    wasm
}

#[cfg(unix)]
fn send_signal(pid: u32, signal: &str) {
    let status = Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(pid.to_string())
        .status()
        .expect("send signal with kill");
    assert!(status.success(), "kill -{signal} {pid} failed: {status}");
}

#[cfg(unix)]
fn restart_and_print_counter(nx: &Path, wasm: &Path, data_dir: &Path, label: &str) -> Output {
    let output = Command::new(nx)
        .arg("run")
        .arg(wasm)
        .arg("--listen")
        .arg("127.0.0.1:0")
        .arg("--datastore-path")
        .arg(data_dir)
        .arg("--settle-for")
        .arg("200ms")
        .arg("--print-gcounter")
        .arg(COUNTER_KEY)
        .output()
        .unwrap_or_else(|e| panic!("restart {label}: {e}"));

    assert_success(&output, label);
    output
}

#[cfg(unix)]
#[test]
fn serve_without_module_waits_for_sigterm_and_shuts_down_cleanly() {
    let data_dir = temp_path("serve-no-module");
    let nx = nx_bin();
    let mut node = Command::new(&nx)
        .arg("serve")
        .arg("--datastore-path")
        .arg(&data_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn nx serve");

    std::thread::sleep(std::time::Duration::from_millis(150));
    assert!(
        node.try_wait().expect("poll nx serve").is_none(),
        "nx serve exited before receiving a shutdown signal"
    );

    send_signal(node.id(), "TERM");
    let output = node.wait_with_output().expect("wait for nx serve");
    assert_success(&output, "nx serve");
    assert!(
        data_dir.is_dir(),
        "nx serve did not initialize the datastore"
    );
}

#[cfg(unix)]
#[test]
fn serve_starts_authenticated_management_listener_and_stops_it_on_sigterm() {
    let data_dir = temp_path("serve-management-data");
    let config_path = temp_path("serve-management-config.toml");
    let token_path = temp_path("serve-management-token");
    let listen: SocketAddr = free_addr().parse().unwrap();
    std::fs::write(&token_path, "top-secret\n").unwrap();
    std::fs::write(
        &config_path,
        format!(
            "[management]\nlisten = \"{listen}\"\ntoken_file = \"{}\"\n",
            token_path.display()
        ),
    )
    .unwrap();

    let nx = nx_bin();
    let node = Command::new(&nx)
        .arg("serve")
        .arg("--config")
        .arg(&config_path)
        .arg("--datastore-path")
        .arg(&data_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn nx serve with management API");

    let unauthorized = wait_for_http(listen, None);
    assert!(unauthorized.starts_with("HTTP/1.1 401 Unauthorized"));
    let authenticated = wait_for_http(listen, Some("Bearer top-secret"));
    assert!(authenticated.starts_with("HTTP/1.1 404 Not Found"));

    send_signal(node.id(), "TERM");
    let output = node.wait_with_output().expect("wait for nx serve");
    assert_success(&output, "nx serve with management API");
    assert!(TcpStream::connect(listen).is_err());
}

#[cfg(unix)]
#[test]
fn management_bind_failure_rolls_back_started_observability_service() {
    let data_dir = temp_path("serve-management-rollback-data");
    let config_path = temp_path("serve-management-rollback-config.toml");
    let token_path = temp_path("serve-management-rollback-token");
    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    let management_addr = occupied.local_addr().unwrap();
    let observability_addr = free_addr();
    std::fs::write(&token_path, "top-secret\n").unwrap();
    std::fs::write(
        &config_path,
        format!(
            "[observability]\nlisten = \"{observability_addr}\"\n\n[management]\nlisten = \"{management_addr}\"\ntoken_file = \"{}\"\n",
            token_path.display()
        ),
    )
    .unwrap();

    let output = Command::new(nx_bin())
        .arg("serve")
        .arg("--config")
        .arg(&config_path)
        .arg("--datastore-path")
        .arg(&data_dir)
        .output()
        .expect("run nx serve with occupied management port");

    assert!(!output.status.success());
    assert!(
        TcpStream::connect(&observability_addr).is_err(),
        "observability listener remained open after management startup failed"
    );
    assert!(data_dir.is_dir());
}

#[test]
#[ignore = "requires built distributed_counter.wasm and local TCP sockets"]
fn two_nx_run_processes_converge_distributed_counter() {
    let wasm = assert_counter_wasm_exists();

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

#[test]
#[ignore = "requires v0.1.0 nx binary, built distributed_counter.wasm and local TCP sockets"]
fn different_protocol_versions_reject_connection_without_exchanging_ops() {
    let wasm = assert_counter_wasm_exists();

    let current_addr = free_addr();
    let previous_addr = free_addr();
    let current_data = temp_path("protocol-v3");
    let previous_data = temp_path("protocol-v2");
    let current_nx = nx_bin();
    let previous_nx = previous_nx_bin();

    let current_node = Command::new(&current_nx)
        .env("RUST_LOG", "nx_net=error")
        .arg("run")
        .arg(&wasm)
        .arg("--listen")
        .arg(&current_addr)
        .arg("--peer")
        .arg(&previous_addr)
        .arg("--datastore-path")
        .arg(&current_data)
        .arg("--wait-before-run")
        .arg("1500ms")
        .arg("--settle-for")
        .arg("1800ms")
        .arg("--print-gcounter")
        .arg(COUNTER_KEY)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn current protocol node");

    std::thread::sleep(std::time::Duration::from_millis(200));

    let previous_output = Command::new(&previous_nx)
        .env("RUST_LOG", "nx_net=error")
        .arg("run")
        .arg(&wasm)
        .arg("--listen")
        .arg(&previous_addr)
        .arg("--peer")
        .arg(&current_addr)
        .arg("--datastore-path")
        .arg(&previous_data)
        .arg("--wait-before-run")
        .arg("1500ms")
        .arg("--settle-for")
        .arg("1800ms")
        .arg("--print-gcounter")
        .arg(COUNTER_KEY)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run previous protocol node");

    let current_output = current_node
        .wait_with_output()
        .expect("wait current protocol node");

    assert_success(&current_output, "current protocol node");
    assert_success(&previous_output, "previous protocol node");
    assert_printed_counter(&current_output, "current protocol node", 1);
    assert_printed_counter(&previous_output, "previous protocol node", 1);

    let current_stdout = String::from_utf8_lossy(&current_output.stdout);
    let current_stderr = String::from_utf8_lossy(&current_output.stderr);
    let previous_stdout = String::from_utf8_lossy(&previous_output.stdout);
    let previous_stderr = String::from_utf8_lossy(&previous_output.stderr);
    let protocol_mismatch_reported = current_stdout
        .contains("protocol version mismatch: expected 4, got 2")
        || current_stdout.contains("protocol version mismatch: expected 2, got 4")
        || current_stderr.contains("protocol version mismatch: expected 2, got 4")
        || current_stderr.contains("protocol version mismatch: expected 4, got 2")
        || previous_stdout.contains("protocol version mismatch: expected 4, got 2")
        || previous_stdout.contains("protocol version mismatch: expected 2, got 4")
        || previous_stderr.contains("protocol version mismatch: expected 4, got 2")
        || previous_stderr.contains("protocol version mismatch: expected 2, got 4");
    assert!(
        protocol_mismatch_reported,
        "neither node reported the protocol mismatch\ncurrent stdout:\n{current_stdout}\ncurrent stderr:\n{current_stderr}\nprevious stdout:\n{previous_stdout}\nprevious stderr:\n{previous_stderr}"
    );
}

#[cfg(unix)]
#[test]
#[ignore = "requires built distributed_counter.wasm, local TCP sockets and Unix signals"]
fn sigterm_shutdown_preserves_counter_state() {
    let wasm = assert_counter_wasm_exists();
    let data = temp_path("cli-sigterm-data");
    let nx = nx_bin();

    let child = Command::new(&nx)
        .arg("run")
        .arg(&wasm)
        .arg("--listen")
        .arg("127.0.0.1:0")
        .arg("--datastore-path")
        .arg(&data)
        .arg("--shutdown-timeout")
        .arg("3s")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn SIGTERM node");

    std::thread::sleep(std::time::Duration::from_millis(1500));
    send_signal(child.id(), "TERM");
    let output = child.wait_with_output().expect("wait SIGTERM node");
    assert_success(&output, "SIGTERM node");

    let restart = restart_and_print_counter(&nx, &wasm, &data, "restart after SIGTERM");
    assert_printed_counter(&restart, "restart after SIGTERM", 2);
}

#[cfg(unix)]
#[test]
#[ignore = "requires built distributed_counter.wasm, local TCP sockets and Unix signals"]
fn crash_restart_keeps_counter_state_consistent() {
    let wasm = assert_counter_wasm_exists();
    let data = temp_path("cli-crash-data");
    let nx = nx_bin();

    let child = Command::new(&nx)
        .arg("run")
        .arg(&wasm)
        .arg("--listen")
        .arg("127.0.0.1:0")
        .arg("--datastore-path")
        .arg(&data)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn crash node");

    std::thread::sleep(std::time::Duration::from_millis(1500));
    send_signal(child.id(), "KILL");
    let output = child.wait_with_output().expect("wait crash node");
    assert!(
        !output.status.success(),
        "crash node should not exit successfully after SIGKILL"
    );

    let restart = restart_and_print_counter(&nx, &wasm, &data, "restart after crash");
    let value = printed_counter_value(&restart, "restart after crash");
    assert!(
        (1..=2).contains(&value),
        "restart after crash produced inconsistent value {value}"
    );
}
