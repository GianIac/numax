---
title: CLI
description: Reference for the `nx` command line interface.
---

`nx` is the Numax command line interface. It has three commands:

- `nx run` - load and execute a WASM module
- `nx config` - manage configuration files
- `nx migrate` - migrate a datastore schema offline

---

## nx run

```
nx run <MODULE> [OPTIONS]
```

Loads `<MODULE>`, starts the runtime, calls `run()` once, then exits.
If `--listen` is passed, sync is enabled and the runtime stays alive
until `--settle-for` elapses, or until SIGINT/SIGTERM if no settle window is set.

### Required

| Argument | Description |
|---|---|
| `<MODULE>` | Path to the `.wasm` file to execute |

### Storage

| Flag | Env | Description |
|---|---|---|
| `--datastore-path <PATH>` | `NX_DATASTORE_PATH` | Directory for the local sled datastore. Default: `./nx-data` |
| `--config <PATH>` | - | Path to a Numax TOML config file |

### Networking

Sync is disabled by default. Pass `--listen` to enable it.

| Flag | Env | Description |
|---|---|---|
| `--listen <ADDR>` | `NX_LISTEN` | Address to listen on (e.g. `0.0.0.0:9000`). Required for sync |
| `--peer <ADDR>` | `NX_PEER` / `NX_PEERS` | Peer address to connect to. Can be repeated. Requires `--listen` |

`NX_PEERS` accepts a comma-separated list: `NX_PEERS=127.0.0.1:9001,127.0.0.1:9002`

### Timing

| Flag | Description |
|---|---|
| `--wait-before-run <DURATION>` | Wait after starting sync and before calling `run()`. Gives peers time to connect. Requires `--listen` |
| `--settle-for <DURATION>` | Keep sync alive for this duration after `run()` returns, then shut down. Requires `--listen` |
| `--shutdown-timeout <DURATION>` | Maximum time allowed for graceful shutdown before returning an error |

Duration format: `500ms`, `5s`, `2m`, or a plain number (interpreted as seconds).

### Inspecting CRDT state

These flags print the final host-side value of a CRDT key after the settle window completes.
All of them require `--listen`.

| Flag | CRDT type | Output format |
|---|---|---|
| `--print-gcounter <KEY>` | GCounter | `key = 42` |
| `--print-pncounter <KEY>` | PNCounter | `key = -3` |
| `--print-lww-register <KEY>` | LWW-Register | `key = value` or `key = <unset>` |
| `--print-lww-map <KEY>` | LWW-Map | `key = {field1=val1, field2=val2}` |
| `--print-orset <KEY>` | ORSet | `key = [tag1, tag2]` |
| `--print-rga <KEY>` | RGA | `key = [item1, item2]` |

### Logging

| Flag | Env | Values | Description |
|---|---|---|---|
| `-v` / `--verbose` | - | - | Sets log level to `debug` |
| `--log-level <LEVEL>` | `NX_LOG_LEVEL` | `trace` `debug` `info` `warn` `error` | Explicit log level. Overrides `--verbose` |
| `--log-format <FORMAT>` | `NX_LOG_FORMAT` | `text` `json` | Output format for runtime logs. Default: `text` |

### Observability

| Flag | Env | Description |
|---|---|---|
| `--observability-listen <ADDR>` | `NX_OBSERVABILITY_LISTEN` | Expose a local HTTP metrics endpoint (e.g. `127.0.0.1:9100`) |

### TLS / mTLS

TLS is optional. To enable it, provide `--tls-cert` and `--tls-key` together.
For mTLS (mutual authentication), also provide `--tls-ca`.

| Flag | Env | Description |
|---|---|---|
| `--tls-cert <PATH>` | `NX_TLS_CERT` | Path to this node's TLS certificate (PEM) |
| `--tls-key <PATH>` | `NX_TLS_KEY` | Path to this node's TLS private key (PEM) |
| `--tls-ca <PATH>` | `NX_TLS_CA` | Path to the CA certificate used to verify peers (PEM). Enables mTLS |
| `--allowed-peers <ID1,ID2,...>` | `NX_ALLOWED_PEERS` | Comma-separated allowlist of peer NodeIds (hex). Requires `--tls-ca` |
| `--tls-insecure` | `NX_TLS_INSECURE` | Skip TLS certificate verification. **Development only. Never use in production.** |

Rules:
- `--tls-cert` and `--tls-key` must always be provided together.
- `--tls-insecure` is mutually exclusive with `--tls-ca` and `--allowed-peers`.
- `--allowed-peers` requires `--tls-ca`.

### Debug

| Flag | Description |
|---|---|
| `--debug-protocol` | Use JSON instead of bincode for the sync wire protocol. Useful for inspecting traffic with a packet capture tool. Requires `--listen` |

### Configuration precedence

When a flag has a corresponding environment variable and a TOML file entry,
the resolution order is:

```
CLI flags > NX_* environment variables > TOML config file > runtime defaults
```

---

## nx migrate

```
nx migrate [OPTIONS]
```

Opens an existing local datastore, runs the registered schema migrations,
flushes the store, and exits. Run this while no `nx run` process is using the
same datastore.

| Flag | Default | Description |
|---|---|---|
| `--datastore-path <PATH>` | `./nx-data` | Existing datastore directory to migrate |
| `--max-records <COUNT>` | `512` | Maximum records processed per migration batch |
| `--max-bytes <BYTES>` | `4MiB` | Maximum bytes processed per migration batch, including generated mutations |

Example:

```bash
nx migrate --datastore-path ./node-a-data
```

---

## nx config

### nx config init

```
nx config init [--output <PATH>] [--force]
```

Generates a commented Numax TOML configuration file with all available fields and their defaults.

| Flag | Default | Description |
|---|---|---|
| `--output <PATH>` | `numax.toml` | Where to write the file |
| `--force` | - | Overwrite the file if it already exists |

Example:

```bash
nx config init --output node-a.toml
```

The generated file looks like this:

```toml
# Numax configuration file.
# Precedence: CLI flags > NX_* environment variables > this file > defaults.

[storage]
datastore_path = "./nx-data"

[network]
listen = "0.0.0.0:9000"
peers = []
serialization_format = "bincode"

[tls]
# cert = "./certs/node.pem"
# key = "./certs/node-key.pem"
# ca = "./certs/ca.pem"
allowed_peers = []
insecure = false

[observability]
# listen = "127.0.0.1:9100"
log_level = "info"
log_format = "text"
request_timeout_secs = 5

[limits]
max_peers = 64
queued_ops_limit = 10000
op_log_limit = 10000
seen_ops_limit = 100000
max_message_size = "16MiB"
socket_timeout_secs = 30
reconnect_initial_delay = "500ms"
reconnect_max_delay = "30s"
peer_dead_after_failures = 3
anti_entropy_interval = "30s"

[discovery]
mode = "static"
```

### nx config validate

```
nx config validate [--config <PATH>]
```

Parses and validates a TOML config file without running a module.
Exits with a non-zero code and an error message if the file is invalid.

| Flag | Default | Description |
|---|---|---|
| `--config <PATH>` | `numax.toml` | Path to the config file to validate |

Example:

```bash
nx config validate --config node-a.toml
# configuration is valid: node-a.toml
```

### nx config show

```
nx config show --config <PATH> --effective
```

Resolves and prints the effective configuration after applying CLI flags, environment
variables, the config file, and runtime defaults - in that order.

| Flag | Default | Description |
|---|---|---|
| `--config <PATH>` | `numax.toml` | Path to the config file |
| `--effective` | - | Required. Prints the fully resolved config |

Example:

```bash
nx config show --config node-a.toml --effective
```

---

## TOML config file reference

A config file can provide any subset of the following sections.
All fields are optional. Unknown fields are rejected.

### [storage]

```toml
[storage]
datastore_path = "./nx-data"
```

| Field | Type | Description |
|---|---|---|
| `datastore_path` | path | Directory for the local sled datastore |

### [network]

```toml
[network]
listen = "0.0.0.0:9000"
peers = ["127.0.0.1:9001"]
serialization_format = "bincode"
```

| Field | Type | Values | Description |
|---|---|---|---|
| `listen` | string | `host:port` | Address to listen on. Required for sync |
| `peers` | string[] | `host:port` | Peer addresses to connect to |
| `serialization_format` | string | `bincode` `json` | Wire format. Default: `bincode` |

### [tls]

```toml
[tls]
cert = "./certs/node.pem"
key = "./certs/node-key.pem"
ca = "./certs/ca.pem"
allowed_peers = ["node-a", "node-b"]
insecure = false
```

| Field | Type | Description |
|---|---|---|
| `cert` | path | Node certificate (PEM) |
| `key` | path | Node private key (PEM) |
| `ca` | path | CA certificate for peer verification (PEM). Enables mTLS |
| `allowed_peers` | string[] | Allowlist of peer NodeIds |
| `insecure` | bool | Skip verification. Development only |

### [observability]

```toml
[observability]
listen = "127.0.0.1:9100"
log_level = "info"
log_format = "text"
request_timeout_secs = 5
```

| Field | Type | Values | Description |
|---|---|---|---|
| `listen` | string | `host:port` | HTTP metrics endpoint address |
| `log_level` | string | `trace` `debug` `info` `warn` `error` | Log verbosity |
| `log_format` | string | `text` `json` | Log output format |
| `request_timeout_secs` | integer | > 0 | Observability request timeout in seconds |

### [limits]

```toml
[limits]
max_peers = 64
queued_ops_limit = 10000
op_log_limit = 10000
seen_ops_limit = 100000
max_message_size = "16MiB"
socket_timeout_secs = 30
reconnect_initial_delay = "500ms"
reconnect_max_delay = "30s"
peer_dead_after_failures = 3
anti_entropy_interval = "30s"
```

| Field | Type | Description |
|---|---|---|
| `max_peers` | integer | Maximum number of connected peers |
| `queued_ops_limit` | integer | Maximum ops queued for broadcast |
| `op_log_limit` | integer | Maximum ops kept in the local op-log |
| `seen_ops_limit` | integer | Maximum op IDs tracked for deduplication |
| `max_message_size` | string | Maximum sync message size (e.g. `16MiB`, `4KiB`) |
| `socket_timeout_secs` | integer | Socket read/write timeout in seconds |
| `reconnect_initial_delay` | duration | Initial backoff before reconnecting to a peer |
| `reconnect_max_delay` | duration | Maximum backoff for reconnect attempts |
| `peer_dead_after_failures` | integer | Consecutive failures before a peer is marked dead |
| `anti_entropy_interval` | duration | Interval between anti-entropy repair cycles |

`reconnect_initial_delay` and `reconnect_max_delay` must be provided together.
`reconnect_initial_delay` must be less than or equal to `reconnect_max_delay`.

### [discovery]

```toml
[discovery]
mode = "static"
```

| Field | Type | Values | Description |
|---|---|---|---|
| `mode` | string | `static` | Peer discovery mode. Only `static` is supported today. Dynamic discovery is on the roadmap |

---

## Full two-node example

```bash
# Generate config files
nx config init --output node-a.toml --force
nx config init --output node-b.toml --force

# Edit node-a.toml: set listen = "0.0.0.0:9000", peers = ["127.0.0.1:9001"]
# Edit node-b.toml: set listen = "0.0.0.0:9001", peers = ["127.0.0.1:9000"]

# Validate
nx config validate --config node-a.toml
nx config validate --config node-b.toml

# Inspect resolved config
nx config show --config node-a.toml --effective

# Run
nx run my_module.wasm --config node-a.toml --settle-for 5s --print-gcounter counter:visits
nx run my_module.wasm --config node-b.toml --settle-for 5s --print-gcounter counter:visits
```
