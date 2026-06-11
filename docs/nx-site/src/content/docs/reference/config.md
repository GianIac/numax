---
title: Configuration
description: Reference for `numax.toml` and environment overrides.
---

Numax resolves its runtime configuration from four sources, applied in this order:

```
CLI flags  >  NX_* environment variables  >  numax.toml  >  runtime defaults
```

A later source only fills in what the earlier ones left unset.
You can run a single node with nothing but CLI flags, or describe a full cluster
with a TOML file and override individual fields at launch time.

---

## Generating a config file

```bash
nx config init --output numax.toml
```

This writes a fully commented file with all available fields and their defaults.
Pass `--force` to overwrite an existing file.

To inspect what the runtime will actually use after all sources are merged:

```bash
nx config show --config numax.toml --effective
```

To validate a file without running a module:

```bash
nx config validate --config numax.toml
```

---

## Full default file

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

All fields are optional. Unknown fields are rejected at validation time.

---

## [storage]

Local datastore location. The store is a sled embedded database.

| Field | Type | Default | Description |
|---|---|---|---|
| `datastore_path` | path | `./nx-data` | Directory where the local sled datastore is written |

The datastore persists between runs. Each node must use its own directory.
To start fresh, delete the directory before running.

```toml
[storage]
datastore_path = "./data/node-a"
```

---

## [network]

Controls whether sync is enabled and who to connect to.
Sync is disabled when this section is absent and no CLI/env flags provide a listen address.

| Field | Type | Default | Description |
|---|---|---|---|
| `listen` | string | — | Address to listen on, e.g. `0.0.0.0:9000`. Required to enable sync |
| `peers` | string[] | `[]` | Peer addresses to connect to, e.g. `["127.0.0.1:9001"]` |
| `serialization_format` | string | `bincode` | Wire format: `bincode` (production) or `json` (debug inspection) |

```toml
[network]
listen = "0.0.0.0:9000"
peers = ["127.0.0.1:9001", "127.0.0.1:9002"]
serialization_format = "bincode"
```

---

## [tls]

Optional TLS and mTLS configuration. If this section is absent, connections are unencrypted.

To enable TLS, provide `cert` and `key`.
To enable mTLS (mutual authentication), also provide `ca`.

| Field | Type | Default | Description |
|---|---|---|---|
| `cert` | path | — | This node's TLS certificate (PEM) |
| `key` | path | — | This node's TLS private key (PEM) |
| `ca` | path | — | CA certificate used to verify peer certificates (PEM). Enables mTLS |
| `allowed_peers` | string[] | `[]` | Allowlist of peer NodeIds (hex). Requires `ca` |
| `insecure` | bool | `false` | Skip TLS certificate verification. **Development only. Never use in production** |

Rules:
- `cert` and `key` must be provided together.
- `insecure` is mutually exclusive with `ca` and `allowed_peers`.
- `allowed_peers` requires `ca`.

```toml
[tls]
cert = "./certs/node-a.pem"
key = "./certs/node-a-key.pem"
ca = "./certs/ca.pem"
allowed_peers = ["node-b-id-hex", "node-c-id-hex"]
insecure = false
```

---

## [observability]

Optional HTTP endpoint for metrics and log configuration.

| Field | Type | Default | Description |
|---|---|---|---|
| `listen` | string | — | Address to expose the metrics HTTP endpoint, e.g. `127.0.0.1:9100` |
| `log_level` | string | `info` | Log verbosity: `trace`, `debug`, `info`, `warn`, `error` |
| `log_format` | string | `text` | Log output format: `text` or `json` |
| `request_timeout_secs` | integer | `5` | Observability HTTP request timeout in seconds. Must be > 0 |

```toml
[observability]
listen = "127.0.0.1:9100"
log_level = "debug"
log_format = "json"
request_timeout_secs = 5
```

---

## [limits]

Fine-grained control over sync behavior and resource bounds.
These apply only when sync is enabled. The defaults are conservative and suitable
for most single-machine multi-node setups.

| Field | Type | Default | Description |
|---|---|---|---|
| `max_peers` | integer | `64` | Maximum number of simultaneously connected peers |
| `queued_ops_limit` | integer | `10000` | Maximum ops queued for broadcast before backpressure |
| `op_log_limit` | integer | `10000` | Maximum ops kept in the local op-log for anti-entropy |
| `seen_ops_limit` | integer | `100000` | Maximum op IDs tracked for deduplication |
| `max_message_size` | string | `16MiB` | Maximum sync message size. Accepts `KiB`, `MiB` or plain bytes |
| `socket_timeout_secs` | integer | `30` | Socket read/write timeout in seconds. Must be > 0 |
| `reconnect_initial_delay` | duration | `500ms` | Initial backoff before reconnecting to a lost peer |
| `reconnect_max_delay` | duration | `30s` | Maximum backoff ceiling for reconnect attempts |
| `peer_dead_after_failures` | integer | `3` | Consecutive failures before a peer is marked dead |
| `anti_entropy_interval` | duration | `30s` | Interval between anti-entropy repair cycles |

`reconnect_initial_delay` and `reconnect_max_delay` must be provided together
and `reconnect_initial_delay` must be ≤ `reconnect_max_delay`.

All integer fields must be > 0.

```toml
[limits]
max_peers = 16
queued_ops_limit = 5000
op_log_limit = 5000
seen_ops_limit = 50000
max_message_size = "8MiB"
socket_timeout_secs = 15
reconnect_initial_delay = "250ms"
reconnect_max_delay = "15s"
peer_dead_after_failures = 5
anti_entropy_interval = "60s"
```

---

## [discovery]

Controls how peers are discovered.

| Field | Type | Default | Description |
|---|---|---|---|
| `mode` | string | `static` | Discovery mode. Only `static` is supported today |

In `static` mode, peers are explicitly listed in `[network].peers` or via `--peer` flags.
Dynamic discovery (mDNS, DNS-SRV, SWIM) is on the roadmap.

```toml
[discovery]
mode = "static"
```

---

## Environment variables

Environment variables sit between CLI flags and the TOML file in the precedence chain.
They are useful for secrets (TLS paths), container environments, and CI.

| Variable | Type | Equivalent field | Description |
|---|---|---|---|
| `NX_DATASTORE_PATH` | path | `[storage].datastore_path` | Local datastore directory |
| `NX_LISTEN` | string | `[network].listen` | Sync listen address |
| `NX_PEER` | string | `[network].peers` (single) | Single peer address |
| `NX_PEERS` | string | `[network].peers` (list) | Comma-separated peer list |
| `NX_SERIALIZATION_FORMAT` | string | `[network].serialization_format` | `bincode` or `json` |
| `NX_TLS_CERT` | path | `[tls].cert` | Node certificate path |
| `NX_TLS_KEY` | path | `[tls].key` | Node key path |
| `NX_TLS_CA` | path | `[tls].ca` | CA certificate path |
| `NX_ALLOWED_PEERS` | string | `[tls].allowed_peers` | Comma-separated peer NodeId allowlist |
| `NX_TLS_INSECURE` | bool | `[tls].insecure` | `1`, `true`, `yes`, `on` / `0`, `false`, `no`, `off` |
| `NX_OBSERVABILITY_LISTEN` | string | `[observability].listen` | Metrics endpoint address |
| `NX_LOG_LEVEL` | string | `[observability].log_level` | `trace`, `debug`, `info`, `warn`, `error` |
| `NX_LOG_FORMAT` | string | `[observability].log_format` | `text` or `json` |

`NX_PEER` and `NX_PEERS` are additive: if both are set, both peers are used.

---

## Duration format

Duration fields in the TOML file and CLI flags accept:

| Format | Example | Meaning |
|---|---|---|
| Milliseconds | `500ms` | 500 milliseconds |
| Seconds | `5s` | 5 seconds |
| Minutes | `2m` | 2 minutes |
| Plain number | `5` | 5 seconds |

Zero durations are rejected.

---

## Two-node setup pattern

```toml
# node-a.toml
[storage]
datastore_path = "./data-a"

[network]
listen = "0.0.0.0:9000"
peers = ["127.0.0.1:9001"]
serialization_format = "bincode"

[limits]
anti_entropy_interval = "30s"

[discovery]
mode = "static"
```

```toml
# node-b.toml
[storage]
datastore_path = "./data-b"

[network]
listen = "0.0.0.0:9001"
peers = ["127.0.0.1:9000"]
serialization_format = "bincode"

[limits]
anti_entropy_interval = "30s"

[discovery]
mode = "static"
```

```bash
nx config validate --config node-a.toml
nx config validate --config node-b.toml

nx run my_module.wasm --config node-a.toml --settle-for 5s
nx run my_module.wasm --config node-b.toml --settle-for 5s
```

---

## Related

- [CLI reference](/reference/cli/) - full flag and subcommand reference
- [Host API](/reference/host-api/) - functions available to WASM modules