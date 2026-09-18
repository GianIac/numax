# mDNS LAN discovery, CRDT replication and restart recovery

Three `nx serve` daemons discover each other through real mDNS, **without any
`--peer`, static peers, bootstrap seed, or management peer injection**. HTTP
management registers and runs real WebAssembly guests using `nx-sdk`.

- Writer build: increments `discovery-lan:visits` once through the GCounter SDK.
- Reader build: reads that counter and the local NodeId without creating CRDT
  operations. It writes a **local-only observation** (`NodeId\nvalue`) under the
  ordinary KV key `discovery-lan`. HTTP reads this observation; ordinary KV writes
  are **not** the replication mechanism. Reserved `__nx/` data is never exposed.
- The test checks initial convergence, stopped-node absence, writes while stopped,
  same-datastore restart, durable identity/local snapshot, missed-op recovery and
  a new write from the restarted node.

## Prerequisites and boundaries

Rust with `wasm32-unknown-unknown`; Node.js 20+ for the device script; macOS or
Linux with usable IPv4 multicast. Build on each device (or transfer the two WASM
artifacts and a matching native `nx` executable yourself). No installation,
publication, remote command execution or external-resource deletion is performed
by the demo script.

Use a **trusted isolated LAN**: mDNS announcements and the default TCP replication
transport are not authenticated/encrypted. The cluster label isolates discovery,
**not authorization**. This is not an mTLS demonstration. Do not use confidential
data; use the separate TLS example for certificate provisioning. Management is
authenticated using a random per-node token file, binds only to `127.0.0.1`, and
never requires `allow_non_loopback` or an insecure external HTTP endpoint.

Allow UDP multicast 5353 and the selected TCP replication port between devices.
Wi-Fi client isolation, VLAN boundaries, VPN routing and firewalls may prevent
discovery/replication. Advertise the real local LAN IPv4, not loopback or `0.0.0.0`.
No NAT/WAN, routed multicast, device power loss, or recovery beyond retention is
claimed here.

## Execute in 5 minutes

The [build](#build-repository-root) must already be complete on all three
devices. Use the same source revision and `CLUSTER`. Replace the example IPs.

On A:

```sh
export STATE="$HOME/numax-lan-a-015"
export LAN_IP="192.168.1.20"
export CLUSTER="numax-release-015-unique"
export INSTANCE="device-a"
```

On B:

```sh
export STATE="$HOME/numax-lan-b-015"
export LAN_IP="192.168.1.21"
export CLUSTER="numax-release-015-unique"
export INSTANCE="device-b"
```

On C:

```sh
export STATE="$HOME/numax-lan-c-015"
export LAN_IP="192.168.1.22"
export CLUSTER="numax-release-015-unique"
export INSTANCE="device-c"
```

On A, B and C:

```sh
node examples/discovery_lan/demo.mjs init \
  --state "$STATE" \
  --lan-ip "$LAN_IP" \
  --cluster "$CLUSTER" \
  --instance "$INSTANCE"
```

Start a daemon on each device and leave it running:

```sh
node examples/discovery_lan/demo.mjs start \
  --state "$STATE" \
  --nx "$PWD/target/release/nx"
```

Open a second terminal on each device, export its `STATE` again, then run:

```sh
node examples/discovery_lan/demo.mjs wait --state "$STATE" --peers 2 --value 0
```

On A, B and C, increment once:

```sh
node examples/discovery_lan/demo.mjs increment --state "$STATE"
```

After all three increments, on A, B and C:

```sh
node examples/discovery_lan/demo.mjs wait --state "$STATE" --peers 2 --value 3
```

Stop C with Ctrl-C. On A and B, wait for its removal:

```sh
node examples/discovery_lan/demo.mjs wait --state "$STATE" --peers 1 --value 3
```

After both waits complete, increment once on A and B:

```sh
node examples/discovery_lan/demo.mjs increment --state "$STATE"
```

Then on A and B:

```sh
node examples/discovery_lan/demo.mjs wait --state "$STATE" --peers 1 --value 5
```

Restart C with the same `STATE`:

```sh
node examples/discovery_lan/demo.mjs start \
  --state "$STATE" \
  --nx "$PWD/target/release/nx"
```

On A, B and C:

```sh
node examples/discovery_lan/demo.mjs wait --state "$STATE" --peers 2 --value 5
```

Increment once on C:

```sh
node examples/discovery_lan/demo.mjs increment --state "$STATE"
```

Then on A, B and C:

```sh
node examples/discovery_lan/demo.mjs wait --state "$STATE" --peers 2 --value 6
```

Stop every daemon with Ctrl-C. The test passes if:

- every node finds two peers without `--peer`;
- values reach `0`, `3`, `5` and `6`;
- C keeps the same NodeId after restart;
- C recovers the two offline writes;
- all three daemons exit cleanly.

Keep the `wait` output. Do not publish token files or state directories.

## Build (repository root)

```sh
rustup target add wasm32-unknown-unknown
cargo build -p nx-cli
cargo build --release --target wasm32-unknown-unknown --manifest-path examples/discovery_lan/Cargo.toml --target-dir examples/discovery_lan/target/reader
cargo build --release --target wasm32-unknown-unknown --manifest-path examples/discovery_lan/Cargo.toml --target-dir examples/discovery_lan/target/writer --features increment
```

Keep separate target directories: otherwise the second build overwrites the
reader artifact. The test asserts the modules have different content IDs.

## Automated same-host E2E (also the CI invocation)

Set `NUMAX_MDNS_LAN_IP` to an IPv4 address actually assigned to the host's LAN
interface. For example on macOS, find the active device with
`route -n get default`, then use `ipconfig getifaddr en0` (replace `en0` with that
device). A runner without a usable multicast interface must report the job as
unavailable, **not silently pass**.

```sh
NUMAX_MDNS_E2E=1 NUMAX_MDNS_LAN_IP=192.168.1.20 cargo test -p nx-cli --test multiprocess_smoke discovery_lan::mdns_three_daemons_recover_missed_crdt_ops_after_restart -- --ignored --exact --nocapture --test-threads=1
```

Replace the example IP. The test is both `#[ignore]` and explicitly environment
gated; running it explicitly without its prerequisites **fails**. The ordinary
workspace suite does not execute it. CI builds both guests and explicitly runs
this test on macOS, deriving the advertised IPv4 from the current default LAN
interface. An address from an earlier run may no longer belong to that interface.

The test starts three **processes on one host**, binds TCP to a real LAN interface,
and exercises real mDNS multicast. It is **not proof of three-machine discovery**.
It uses an exclusive temporary directory, unique cluster/instances, independently
generated tokens, ephemeral port reservations, bounded condition polling and
process guards. Every assertion failure kills/reaps owned daemons and removes
only that test's directory. Normal completion checks graceful SIGTERM shutdown.
Failure diagnostics redact tokens. The test ignores inherited `NX_*` variables
so local settings cannot inject peers or weaken management authentication.

## Three actual devices: one foreground daemon per device

Use the same fresh cluster label on A/B/C, a different instance name on each,
and each device's own LAN IPv4. The following uses documentation/example values;
substitute addresses and an unused cluster name. Run from each checkout root.
`$HOME` already exists; each state directory must **not** exist before `init`.

On **device A**:

```sh
node examples/discovery_lan/demo.mjs init --state "$HOME/numax-lan-a" --lan-ip 192.168.1.20 --cluster lan-demo-unique --instance device-a
node examples/discovery_lan/demo.mjs start --state "$HOME/numax-lan-a"
```

On **device B**:

```sh
node examples/discovery_lan/demo.mjs init --state "$HOME/numax-lan-b" --lan-ip 192.168.1.21 --cluster lan-demo-unique --instance device-b
node examples/discovery_lan/demo.mjs start --state "$HOME/numax-lan-b"
```

On **device C**:

```sh
node examples/discovery_lan/demo.mjs init --state "$HOME/numax-lan-c" --lan-ip 192.168.1.22 --cluster lan-demo-unique --instance device-c
node examples/discovery_lan/demo.mjs start --state "$HOME/numax-lan-c"
```

The defaults are TCP replication 9000 and local management 9102; optional
`--network-port` and `--management-port` are accepted by `init`. If placing more
than one daemon on a single host, assign distinct ports and state directories;
that remains a **same-host** experiment. `start --nx /absolute/path/to/nx` selects
another native executable. Inherited `NX_*` overrides are removed on launch.

Keep `start` running in the foreground. In a **second local terminal on each
device**, set `STATE` to its directory and run:

```sh
STATE="$HOME/numax-lan-a" # use numax-lan-b or numax-lan-c on B/C
node examples/discovery_lan/demo.mjs wait --state "$STATE" --peers 2 --value 0
```

Record each `node_id` and verify each device's `peer_ids` are exactly the other
two recorded identities. `connections` may contain inbound and outbound links
to the same identity; `--peers` counts **unique identities**, not TCP connections.

### Reproducible offline/restart scenario

Use fresh datastores and perform each increment exactly once. Wait commands poll
conditions with a default 60-second bound (`--timeout` allows 1–600 seconds);
there are no fixed startup or settling sleeps.

1. **On A, B and C**, run one increment, then wait on all devices for value 3:

   ```sh
   node examples/discovery_lan/demo.mjs increment --state "$STATE"
   node examples/discovery_lan/demo.mjs wait --state "$STATE" --peers 2 --value 3
   ```

   Run all three increments before expecting any wait for 3 to complete.

2. **On C**, press Ctrl-C in its foreground `start` terminal. Wait for that
   command to exit; do not reinitialize or remove its state directory. **On A
   and B**, observe disconnection, then increment each once:

   ```sh
   node examples/discovery_lan/demo.mjs wait --state "$STATE" --peers 1 --value 3
   node examples/discovery_lan/demo.mjs increment --state "$STATE"
   node examples/discovery_lan/demo.mjs wait --state "$STATE" --peers 1 --value 5
   ```

   Complete both disconnection waits before either increment, and both increments
   before the waits for 5. These two operations occur while C has no running process.

3. **On C**, restart with the exact same state directory:

   ```sh
   node examples/discovery_lan/demo.mjs start --state "$HOME/numax-lan-c"
   ```

   **On all three devices**, wait for two identities and value 5:

   ```sh
   node examples/discovery_lan/demo.mjs wait --state "$STATE" --peers 2 --value 5
   ```

   Verify the recorded IDs are unchanged. The script also checks each local ID
   against its exclusively created identity record. C has recovered the two
   missed operations; the reader does not increment to manufacture convergence.

4. **Only on C**, increment once. **On all devices**, wait for value 6:

   ```sh
   # C only:
   node examples/discovery_lan/demo.mjs increment --state "$STATE"
   # All three:
   node examples/discovery_lan/demo.mjs wait --state "$STATE" --peers 2 --value 6
   ```

5. Stop all foreground daemons with Ctrl-C and wait for clean exit. Datastores,
   private logs, configuration, identity records and token files are deliberately
   preserved; the script never removes external directories or kills processes
   it did not spawn. Initialization refuses to overwrite an existing directory.
   Startup failures and signals terminate/reap the owned child, escalating to
   SIGKILL after a bounded 15-second graceful-shutdown attempt.

`increment` is never automatically retried: a lost HTTP response can have an
ambiguous outcome. Inspect with `status` before deciding what to do. `status`
refreshes the reader projection without adding a CRDT operation:

```sh
node examples/discovery_lan/demo.mjs status --state "$STATE"
```

## Retention and evidence

Both demo and test explicitly configure `op_log_limit = 128` and
`seen_ops_limit = 128`, with `queued_ops_limit = 128` and anti-entropy every
200 ms. **Retention is count-based, not “128 seconds”**. This fresh-cluster
scenario produces six CRDT operations total, only two during C's downtime.
Reading/status polling creates local KV observations but no CRDT operations.
It therefore remains below both retention bounds. Unrelated writers sharing a
cluster/datastore or repeated manual runs can invalidate that guarantee.
Do not infer that arbitrary downtime or an evicted operation will recover.

For a release evidence record, retain command exit statuses and the identity/value
outputs at 0, 3, 5 and 6 from **each actual device**, plus platform, interface and
network topology. Do not attach token files. A passing local multiprocess test
is useful automated coverage, but does not substitute for this cross-device run.

The automated test uses 60-second phase deadlines and 15-second shutdown
deadlines. It verifies unauthenticated management requests receive 401 and C's
previous local KV snapshot remains `(original NodeId, 3)` before running the
reader after restart. Recovery then comes from the CRDT path, not the snapshot.

### Local verification — 2026-09-14

On the working tree based on `1674d5ee` (with the release-preparation changes),
the explicitly selected three-daemon E2E passed on macOS over the host's real
LAN interface: `0 -> 3 -> offline writes -> 5 -> restart recovery -> 6`.
The two-daemon mDNS discovery/removal test and the opt-in script lifecycle test
also passed. The latter verified a real SDK write and durable restart.

These are **same-host** results. No three-device LAN run or remote CI matrix is
attested here; publication remains subject to those separate checks. Tokens and
private node directories are not release evidence and must not be published.

## Script checks

```sh
node --check examples/discovery_lan/demo.mjs
node --test examples/discovery_lan/demo.test.mjs
# Optional real single-daemon script lifecycle test, after building both guests:
NUMAX_DEMO_E2E=1 node --test examples/discovery_lan/demo.test.mjs
```
