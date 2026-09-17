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
with a TOML file and override individual fields at launch time. The same node
configuration is accepted by both `nx run` and `nx serve`.

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

[management]
# listen = "127.0.0.1:9102"
# token_file = "./management.token"
allow_non_loopback = false
request_timeout_secs = 10

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
# cluster_id = "default"
# advertised_endpoint = "127.0.0.1:9000"
# max_candidates = 1024
# Bootstrap: seeds, refresh_interval, retry_initial, retry_max, stale_after, max_seeds
# mDNS: instance_name, max_instances
# DNS-SRV: service_name, retry_interval, max_refresh_interval
# File: path, poll_interval, max_file_bytes
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

## [management]

Controls the authenticated Management API started by `nx serve`. The listener
is disabled unless a bearer token is available. Store the token in a file or
provide it through `NX_MANAGEMENT_TOKEN`; `nx config show` never prints it.

| Field | Type | Default | Description |
|---|---|---|---|
| `listen` | string | `127.0.0.1:9102` | Management API address when a token is configured |
| `token_file` | path | — | File containing the bearer token; trailing CR/LF characters are ignored |
| `allow_non_loopback` | bool | `false` | Explicitly permit binding to a non-loopback address |
| `request_timeout_secs` | integer | `10` | Maximum time to read HTTP headers and, separately, to execute a routed request. Must be > 0 |

```toml
[management]
listen = "127.0.0.1:9102"
token_file = "./management.token"
allow_non_loopback = false
request_timeout_secs = 10
```

The transport always caps request bodies at 16 MiB and processes at most 64
authenticated requests concurrently. These hard safety bounds are not TOML
settings.

The Management API does not terminate TLS. Keep it on loopback behind a TLS-terminating reverse proxy whenever possible. A non-loopback bind requires
`allow_non_loopback = true` and must only be used on a transport protected by
TLS or equivalent network controls. Bearer tokens are never written to logs or
effective configuration output.

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

Controls how peers are discovered in `v0.1.5`, the current Numax version.
Dynamic discovery is available alongside backward-compatible static peer lists.

| Field | Type | Default | Description |
|---|---|---|---|
| `mode` | string | `static` | `static`, `bootstrap`, `mdns`, `dns-srv`, or `file` |
| `cluster_id` | string | `default` | Discovery routing scope; not an authorization boundary |
| `advertised_endpoint` | string | derived from listener | Concrete endpoint published by bootstrap or mDNS |
| `max_candidates` | integer | `1024` | Positive aggregate bound across all discovery sources; at most `4096` in bootstrap mode |

Provider-specific fields are accepted only for their selected mode:

| Mode | Required fields | Optional fields and defaults |
|---|---|---|
| `static` | none | none |
| `bootstrap` | `seeds` | `refresh_interval = "20s"`, `retry_initial = "500ms"`, `retry_max = "30s"`, `stale_after = "2m"`, `max_seeds = 32` |
| `mdns` | `instance_name` | `max_instances = 1024` |
| `dns-srv` | `service_name` | `retry_interval = "5s"`, `max_refresh_interval = "5m"` |
| `file` | `path` | `poll_interval = "2s"`, `max_file_bytes = "1MiB"` |

Explicit peers from `[network].peers`, `--peer`, `NX_PEER`, or `NX_PEERS`
remain an additional static source when a dynamic mode is selected. They never
become bootstrap seeds. Every non-static mode enables sync and therefore
requires `[network].listen`, `--listen`, or `NX_LISTEN`.

For compatibility, the effective candidate capacity is raised to at least the
number of explicit peer entries. In bootstrap mode that effective value must
also be at most `4096`: a larger explicit peer list is rejected, not silently
truncated. The bootstrap upper bound does not apply to static, mDNS, DNS-SRV or
file mode. `NX_DISCOVERY_MAX_CANDIDATES` overrides the TOML value; validation uses
the resolved mode and capacity.

Successful startup means local services are ready, not that discovery has
found peers or CRDT state has converged. Candidate expiry stops new dialing but
does not close admitted connections; periodic anti-entropy continues over those
active connections. Recovery depends on retained operation and deduplication
history, not merely on rediscovery. See the
[discovery contract](/numax/design/discovery-contract/) for freshness, shutdown
and mDNS resource limits.

```toml
[discovery]
mode = "bootstrap"
cluster_id = "production"
advertised_endpoint = "10.0.0.12:9000"
seeds = ["10.0.0.10:9000", "10.0.0.11:9000"]
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
| `NX_MANAGEMENT_LISTEN` | string | `[management].listen` | Management API address |
| `NX_MANAGEMENT_TOKEN` | string | secret | Bearer token; overrides every token file |
| `NX_MANAGEMENT_TOKEN_FILE` | path | `[management].token_file` | Bearer-token file |
| `NX_MANAGEMENT_ALLOW_NON_LOOPBACK` | bool | `[management].allow_non_loopback` | Explicit external-bind opt-in |
| `NX_MANAGEMENT_REQUEST_TIMEOUT_SECS` | integer | `[management].request_timeout_secs` | HTTP header-read and routed-request timeout in seconds |
| `NX_LOG_LEVEL` | string | `[observability].log_level` | `trace`, `debug`, `info`, `warn`, `error` |
| `NX_LOG_FORMAT` | string | `[observability].log_format` | `text` or `json` |
| `NX_DISCOVERY_MODE` | string | `[discovery].mode` | `static`, `bootstrap`, `mdns`, `dns-srv`, or `file` |
| `NX_DISCOVERY_CLUSTER_ID` | string | `[discovery].cluster_id` | Discovery routing scope |
| `NX_DISCOVERY_ADVERTISED_ENDPOINT` | string | `[discovery].advertised_endpoint` | Endpoint to publish |
| `NX_DISCOVERY_MAX_CANDIDATES` | integer | `[discovery].max_candidates` | Aggregate candidate bound |
| `NX_DISCOVERY_SEEDS` | CSV | `[discovery].seeds` | Bootstrap seed endpoints |
| `NX_DISCOVERY_REFRESH_INTERVAL` | duration | `[discovery].refresh_interval` | Bootstrap refresh interval |
| `NX_DISCOVERY_RETRY_INITIAL` / `NX_DISCOVERY_RETRY_MAX` | duration | matching fields | Bootstrap retry bounds |
| `NX_DISCOVERY_STALE_AFTER` | duration | `[discovery].stale_after` | Bootstrap candidate lease |
| `NX_DISCOVERY_MAX_SEEDS` | integer | `[discovery].max_seeds` | Bootstrap seed bound |
| `NX_DISCOVERY_INSTANCE_NAME` | string | `[discovery].instance_name` | mDNS instance name |
| `NX_DISCOVERY_MAX_INSTANCES` | integer | `[discovery].max_instances` | mDNS instance bound |
| `NX_DISCOVERY_SERVICE_NAME` | string | `[discovery].service_name` | Fully qualified DNS-SRV name |
| `NX_DISCOVERY_RETRY_INTERVAL` | duration | `[discovery].retry_interval` | DNS retry interval |
| `NX_DISCOVERY_MAX_REFRESH_INTERVAL` | duration | `[discovery].max_refresh_interval` | DNS refresh ceiling |
| `NX_DISCOVERY_FILE` | path | `[discovery].path` | Watched peer file |
| `NX_DISCOVERY_POLL_INTERVAL` | duration | `[discovery].poll_interval` | File polling interval |
| `NX_DISCOVERY_MAX_FILE_BYTES` | byte size | `[discovery].max_file_bytes` | Peer-file size bound |

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

- [CLI reference](/numax/reference/cli/) - full flag and subcommand reference
- [Host API](/numax/reference/host-api/) - functions available to WASM modules
