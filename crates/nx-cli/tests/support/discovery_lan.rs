//! Real multicast, TCP, WASM and HTTP; three processes on ONE host, not three devices.
use super::{management_request, nx_bin, response_body, send_signal, temp_path, workspace_root};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::Read;
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const POLL: Duration = Duration::from_millis(100);
const WAIT: Duration = Duration::from_secs(60);
const SNAPSHOT_PATH: &str = "/api/v1/keys/ZGlzY292ZXJ5LWxhbg";
// Retention is in operation COUNTS, not seconds. The scenario produces six ops.
const RETAINED_OPS: usize = 128;

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let path = temp_path("mdns-three-daemons");
        fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        // Only this run's exclusively created temporary directory is removed.
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Daemon {
    child: Option<Child>,
    config: PathBuf,
    log: PathBuf,
    management: SocketAddr,
    authorization: String,
    reader: String,
    writer: String,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Daemon {
    fn start(&mut self) {
        assert!(self.child.is_none());
        let output = File::options()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(&self.log)
            .unwrap();
        let mut command = Command::new(nx_bin());
        // A developer's NX_* settings must not inject static peers or disable auth.
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("NX_") {
                command.env_remove(name);
            }
        }
        self.child = Some(
            command
                .args(["serve", "--config"])
                .arg(&self.config)
                .env("RUST_LOG", "info")
                .stdin(Stdio::null())
                .stderr(output.try_clone().unwrap())
                .stdout(output)
                .spawn()
                .expect("spawn mDNS daemon"),
        );
        self.wait("authenticated management readiness", |node| {
            if TcpStream::connect_timeout(&node.management, Duration::from_millis(100)).is_err() {
                return false;
            }
            node.request("GET", "/api/v1/ready", None, &[])
                .starts_with("HTTP/1.1 200 ")
        });
        let denied = management_request(self.management, "GET", "/api/v1/health", None, None, &[]);
        assert!(
            denied.starts_with("HTTP/1.1 401 "),
            "unauthenticated request was not denied"
        );
    }

    fn assert_alive(&mut self) {
        let status = self
            .child
            .as_mut()
            .expect("running daemon")
            .try_wait()
            .unwrap();
        assert!(
            status.is_none(),
            "daemon exited: {status:?}\n{}",
            self.logs()
        );
    }

    fn logs(&self) -> String {
        // Tokens never go in CLI arguments; redact defensively before diagnostics.
        fs::read_to_string(&self.log).unwrap_or_default().replace(
            self.authorization.trim_start_matches("Bearer "),
            "[REDACTED]",
        )
    }

    fn wait(&mut self, label: &str, mut condition: impl FnMut(&Self) -> bool) {
        let deadline = Instant::now() + WAIT;
        loop {
            self.assert_alive();
            if condition(self) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out: {label}\n{}",
                self.logs()
            );
            std::thread::sleep(POLL);
        }
    }

    fn request(&self, method: &str, path: &str, content_type: Option<&str>, body: &[u8]) -> String {
        management_request(
            self.management,
            method,
            path,
            Some(&self.authorization),
            content_type,
            body,
        )
    }

    fn register(&self, wasm: &[u8]) -> String {
        let response = self.request("POST", "/api/v1/modules", Some("application/wasm"), wasm);
        assert!(
            response.starts_with("HTTP/1.1 201 ") || response.starts_with("HTTP/1.1 200 "),
            "register guest: {response}"
        );
        let body: serde_json::Value = serde_json::from_str(response_body(&response)).unwrap();
        body["id"].as_str().unwrap().to_owned()
    }

    fn register_guests(&mut self, reader: &[u8], writer: &[u8]) {
        self.reader = self.register(reader);
        self.writer = self.register(writer);
        assert_ne!(
            self.reader, self.writer,
            "reader and writer must be distinct builds"
        );
    }

    fn run(&self, module: &str) {
        let response = self.request("POST", &format!("/api/v1/modules/{module}/runs"), None, &[]);
        assert!(
            response.starts_with("HTTP/1.1 204 "),
            "guest execution failed: {response}"
        );
    }

    fn persisted_snapshot(&self) -> (String, u64) {
        let response = self.request("GET", SNAPSHOT_PATH, None, &[]);
        assert!(
            response.starts_with("HTTP/1.1 200 "),
            "read snapshot: {response}"
        );
        let (id, value) = response_body(&response)
            .split_once('\n')
            .expect("snapshot id and value");
        assert!(!id.is_empty());
        (
            id.to_owned(),
            value.parse().expect("decimal counter snapshot"),
        )
    }

    fn snapshot(&self) -> (String, u64) {
        self.run(&self.reader);
        self.persisted_snapshot()
    }

    fn peer_ids(&self) -> BTreeSet<String> {
        let response = self.request("GET", "/api/v1/peers?limit=10", None, &[]);
        assert!(
            response.starts_with("HTTP/1.1 200 "),
            "read peers: {response}"
        );
        let body: serde_json::Value = serde_json::from_str(response_body(&response)).unwrap();
        assert!(body["next_cursor"].is_null(), "unexpected extra peers");
        let items = body["items"].as_array().unwrap();
        // The API lists connection addresses, not unique identities: symmetric
        // dialing can leave both an inbound and an outbound link to one node.
        let ids: BTreeSet<_> = items
            .iter()
            .map(|peer| peer["node_id"].as_str().unwrap().to_owned())
            .collect();
        let addresses: BTreeSet<_> = items
            .iter()
            .map(|peer| peer["address"].as_str().unwrap())
            .collect();
        assert_eq!(
            addresses.len(),
            items.len(),
            "duplicate connection addresses"
        );
        ids
    }

    fn stop(&mut self) {
        self.assert_alive();
        send_signal(self.child.as_ref().unwrap().id(), "TERM");
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(status) = self.child.as_mut().unwrap().try_wait().unwrap() {
                assert!(
                    status.success(),
                    "daemon shutdown failed: {status}\n{}",
                    self.logs()
                );
                self.child.take();
                assert!(
                    TcpStream::connect(self.management).is_err(),
                    "management listener still open"
                );
                return;
            }
            assert!(
                Instant::now() < deadline,
                "daemon shutdown timed out\n{}",
                self.logs()
            );
            std::thread::sleep(POLL);
        }
    }
}

fn wasm(mode: &str) -> Vec<u8> {
    let path = workspace_root().join(format!(
        "examples/discovery_lan/target/{mode}/wasm32-unknown-unknown/release/discovery_lan.wasm"
    ));
    fs::read(&path).unwrap_or_else(|error| {
        panic!("build both discovery_lan guest variants first; missing {path:?}: {error}")
    })
}

fn write_private(path: &Path, bytes: &[u8]) {
    use std::io::Write;
    File::options()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .unwrap()
        .write_all(bytes)
        .unwrap();
}

#[test]
#[ignore = "requires NUMAX_MDNS_E2E=1, NUMAX_MDNS_LAN_IP, real multicast and both discovery_lan WASM builds"]
fn mdns_three_daemons_recover_missed_crdt_ops_after_restart() {
    assert_eq!(
        std::env::var("NUMAX_MDNS_E2E").as_deref(),
        Ok("1"),
        "explicit multicast opt-in required"
    );
    let lan: Ipv4Addr = std::env::var("NUMAX_MDNS_LAN_IP")
        .expect("set NUMAX_MDNS_LAN_IP to a real local LAN interface IPv4 address")
        .parse()
        .expect("LAN IPv4 address");
    assert!(
        !lan.is_loopback() && !lan.is_unspecified() && !lan.is_multicast() && !lan.is_broadcast()
    );
    let reader = wasm("reader");
    let writer = wasm("writer");
    let directory = TestDirectory::new();
    let cluster = directory.0.file_name().unwrap().to_str().unwrap();
    let mut nodes = Vec::new();
    // Hold all reservations until their daemon starts, avoiding duplicate ephemeral ports.
    let mut reservations = Vec::new();
    for index in 0..3 {
        let network = TcpListener::bind((lan, 0)).expect("bind real LAN interface");
        let management = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = network.local_addr().unwrap();
        let management_addr = management.local_addr().unwrap();
        let mut entropy = [0u8; 32];
        File::open("/dev/urandom")
            .unwrap()
            .read_exact(&mut entropy)
            .unwrap();
        let token: String = entropy.iter().map(|byte| format!("{byte:02x}")).collect();
        let token_path = directory.0.join(format!("{index}.token"));
        write_private(&token_path, token.as_bytes());
        let config = directory.0.join(format!("{index}.toml"));
        let text = format!(
            "[network]\nlisten = {endpoint:?}\npeers = []\n\
             [storage]\ndatastore_path = {data:?}\n\
             [management]\nlisten = {management:?}\ntoken_file = {token:?}\nallow_non_loopback = false\n\
             [discovery]\nmode = \"mdns\"\ncluster_id = {cluster:?}\ninstance_name = \"{cluster}-{index}\"\nadvertised_endpoint = {endpoint:?}\nmax_candidates = 8\nmax_instances = 8\n\
             [limits]\nmax_peers = 4\nqueued_ops_limit = 128\nop_log_limit = {RETAINED_OPS}\nseen_ops_limit = {RETAINED_OPS}\nanti_entropy_interval = \"200ms\"\nreconnect_initial_delay = \"100ms\"\nreconnect_max_delay = \"1s\"\n",
            endpoint = endpoint.to_string(),
            management = management_addr.to_string(),
            data = directory.0.join(format!("data-{index}")).to_str().unwrap(),
            token = token_path.to_str().unwrap(),
        );
        write_private(&config, text.as_bytes());
        nodes.push(Daemon {
            child: None,
            config,
            log: directory.0.join(format!("{index}.log")),
            management: management_addr,
            authorization: format!("Bearer {token}"),
            reader: String::new(),
            writer: String::new(),
        });
        reservations.push((network, management));
    }
    for (node, reservation) in nodes.iter_mut().zip(reservations) {
        drop(reservation);
        node.start();
        node.register_guests(&reader, &writer);
    }
    let identities: Vec<_> = nodes
        .iter()
        .map(|node| {
            let (id, value) = node.snapshot();
            assert_eq!(value, 0, "fresh datastore must start empty");
            id
        })
        .collect();
    let all_ids: BTreeSet<_> = identities.iter().cloned().collect();
    assert_eq!(all_ids.len(), 3);
    for (node, id) in nodes.iter_mut().zip(&identities) {
        let expected: BTreeSet<_> = all_ids
            .iter()
            .filter(|other| *other != id)
            .cloned()
            .collect();
        node.wait("discover the other two identities without --peer", |node| {
            node.peer_ids() == expected
        });
    }
    eprintln!("mDNS: three daemons on one host discovered each other on {lan}");
    for node in &nodes {
        node.run(&node.writer);
    }
    for node in &mut nodes {
        node.wait("initial CRDT convergence to 3", |node| {
            node.snapshot().1 == 3
        });
    }
    nodes[2].stop();
    for index in 0..2 {
        let expected = BTreeSet::from([identities[1 - index].clone()]);
        nodes[index].wait("offline node removed from active connections", |node| {
            node.peer_ids() == expected
        });
        nodes[index].run(&nodes[index].writer);
    }
    for node in &mut nodes[..2] {
        node.wait(
            "survivors converge to 5 while third process is stopped",
            |node| node.snapshot().1 == 5,
        );
    }
    nodes[2].start();
    // Read the old local observation BEFORE running the reader: this proves KV durability.
    assert_eq!(nodes[2].persisted_snapshot(), (identities[2].clone(), 3));
    nodes[2].register_guests(&reader, &writer);
    for (node, id) in nodes.iter_mut().zip(&identities) {
        let expected: BTreeSet<_> = all_ids
            .iter()
            .filter(|other| *other != id)
            .cloned()
            .collect();
        node.wait("same identities rediscovered after restart", |node| {
            node.peer_ids() == expected
        });
        node.wait("missed-op recovery to 5 within 128-op retention", |node| {
            node.snapshot() == (id.clone(), 5)
        });
    }
    nodes[2].run(&nodes[2].writer);
    for (node, id) in nodes.iter_mut().zip(&identities) {
        node.wait("restarted node can write; final convergence to 6", |node| {
            node.snapshot() == (id.clone(), 6)
        });
    }
    for node in &mut nodes {
        node.stop();
    }
    eprintln!(
        "mDNS E2E passed: 0 -> 3 -> offline writes -> 5 -> restart recovery -> 6; stable identities; retention {RETAINED_OPS} ops; clean shutdown"
    );
}
