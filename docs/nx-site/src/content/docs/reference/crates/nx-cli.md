---
title: nx-cli
description: Command line frontend for Numax.
---

`nx-cli` is the entry point for every user interaction with Numax.
It parses flags, resolves configuration from four sources, validates the result,
and hands a fully-built `RuntimeConfig` to `nx-core`. It never contains runtime logic.

**Produces:** the `nx` binary (`crates/nx-cli/src/main.rs`, `[[bin]] name = "nx"`).

---

## Responsibilities

| Responsibility | Where |
|---|---|
| Parse CLI flags | `main.rs` - `Cli` enum via clap |
| Read and validate TOML config files | `config.rs` - `RunFileConfig`, `validate_run_file_config` |
| Read environment variables | `config.rs` - `EnvRunConfig::from_env` |
| Resolve precedence (CLI > env > file > defaults) | `config.rs` - `EffectiveRunConfig::resolve` |
| Validate flag combinations (TLS, sync, settle) | `config.rs` - `validate_tls_flags`, `validate_settle_mode`, etc. |
| Build `SyncConfig`, `TlsConfig`, `ObservabilityConfig` | `config.rs` - `build_sync_config`, `build_tls_config`, `build_observability_config` |
| Initialize logging | `config.rs` - `init_logging` |
| Generate `numax.toml` template | `config.rs` - `CONFIG_TEMPLATE`, `init_config_file` |
| Print effective resolved config | `config.rs` - `EffectiveRunConfig::render_effective_toml` |

---

## Command structure

```
nx
├── run <MODULE> [OPTIONS]
└── config
    ├── init [--output PATH] [--force]
    ├── validate [--config PATH]
    └── show --config PATH --effective
```

Defined in `main.rs` as:

```rust
enum Cli {
    Run { module, datastore_path, config, listen, peers, ... },
    Config { command: ConfigCommand },
}

enum ConfigCommand {
    Init { output, force },
    Validate { config },
    Show { config, effective },
}
```

All parsing is done by clap `#[derive(Parser)]`. No manual argument parsing.

---

## Configuration resolution pipeline

This is the core of `nx-cli`. Every flag has up to four sources. The pipeline is:

```
1. CLI flags            (highest priority)
2. NX_* env vars
3. numax.toml sections
4. Runtime defaults     (lowest priority)
```

The entry point is `EffectiveRunConfig::resolve(cli, file_config)` which internally calls
`resolve_with_env(cli, EnvRunConfig::from_env()?, file_config)`.

### Structs involved

**`RunCliOptions`** - what came from the command line flags.

```rust
pub struct RunCliOptions {
    pub datastore_path: Option<PathBuf>,
    pub listen: Option<String>,
    pub peers: Vec<String>,
    pub observability_listen: Option<String>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub tls_ca: Option<PathBuf>,
    pub allowed_peers: Option<String>,
    pub tls_insecure: bool,
    pub debug_protocol: bool,
    pub verbose: bool,
    pub log_level: Option<String>,
    pub log_format: Option<LogFormat>,
}
```

**`EnvRunConfig`** - what came from `NX_*` environment variables.

Built by `EnvRunConfig::from_env()`. Each field maps to one env var:

| Field | Env var | Notes |
|---|---|---|
| `datastore_path` | `NX_DATASTORE_PATH` | |
| `listen` | `NX_LISTEN` | |
| `peers` | `NX_PEER` + `NX_PEERS` | additive - both used if set |
| `observability_listen` | `NX_OBSERVABILITY_LISTEN` | |
| `tls_cert` | `NX_TLS_CERT` | |
| `tls_key` | `NX_TLS_KEY` | |
| `tls_ca` | `NX_TLS_CA` | |
| `allowed_peers` | `NX_ALLOWED_PEERS` | comma-separated |
| `tls_insecure` | `NX_TLS_INSECURE` | `1/true/yes/on` or `0/false/no/off` |
| `serialization_format` | `NX_SERIALIZATION_FORMAT` | `bincode` or `json` |
| `log_level` | `NX_LOG_LEVEL` | |
| `log_format` | `NX_LOG_FORMAT` | `text` or `json` |

**`RunFileConfig`** - what came from `numax.toml`. Sections:

```rust
pub struct RunFileConfig {
    pub network:      Option<NetworkFileConfig>,
    pub tls:          Option<TlsFileConfig>,
    pub storage:      Option<StorageFileConfig>,
    pub limits:       Option<LimitsFileConfig>,
    pub observability: Option<ObservabilityFileConfig>,
    pub discovery:    Option<DiscoveryFileConfig>,
}
```

All structs use `#[serde(deny_unknown_fields)]` - unknown keys in the TOML are rejected at parse time.

**`EffectiveRunConfig`** - the final merged result passed to `nx-core`.

```rust
pub struct EffectiveRunConfig {
    pub datastore_path: Option<PathBuf>,
    pub sync:           Option<SyncConfig>,
    pub observability:  Option<ObservabilityConfig>,
    pub log_level:      String,
    pub log_format:     LogFormat,
}
```

---

## Sync enablement logic

Sync is not always enabled. `build_sync_config` decides:

- If no `listen` is set and no sync-related inputs exist anywhere → sync disabled, returns `None`.
- If any sync-related field is present (env, file, TLS, format) but `listen` is missing → **error**. Dialer-only mode is not supported.
- If `listen` is set → sync enabled, `SyncConfig` is built and returned.

`force_enabled` is `true` when the config file has `[network]`, `[tls]`, or `[limits]` sections,
or when env vars provide sync inputs. This makes `nx config show --effective` work correctly
even without CLI `--listen`.

---

## Validation functions

All validation happens before the runtime starts. Errors are returned as `anyhow::Result`.

| Function | What it checks |
|---|---|
| `validate_tls_flags` | `cert`+`key` must be together; `insecure` mutually exclusive with `ca`/`allowed_peers`; `allowed_peers` requires `ca` |
| `validate_settle_mode` | `--settle-for` requires `--listen` |
| `validate_wait_before_run` | `--wait-before-run` requires `--listen` |
| `validate_print_gcounter` | `--print-gcounter` requires `--listen` |
| `validate_print_pncounter` | `--print-pncounter` requires `--listen` |
| `validate_print_lww_register` | `--print-lww-register` requires `--listen` |
| `validate_print_lww_map` | `--print-lww-map` requires `--listen` |
| `validate_print_orset` | `--print-orset` requires `--listen` |
| `validate_print_rga` | `--print-rga` requires `--listen` |
| `validate_run_file_config` | full TOML structural validation: non-empty fields, valid paths, consistent limit pairs, non-zero values |

---

## Parsers

### `parse_duration`

Accepts: `500ms`, `5s`, `2m`, or plain integer (seconds). Zero is rejected.
Used by clap as a `value_parser` for `--settle-for`, `--wait-before-run`, `--shutdown-timeout`.

```rust
parse_duration("500ms") // Ok(Duration::from_millis(500))
parse_duration("5s")    // Ok(Duration::from_secs(5))
parse_duration("2m")    // Ok(Duration::from_secs(120))
parse_duration("3")     // Ok(Duration::from_secs(3))
parse_duration("0s")    // Err - zero is rejected
parse_duration("soon")  // Err - invalid
```

### `parse_byte_size`

Accepts: `16MiB`, `4KiB`, `128` (bytes), with optional space (`16 MiB`). Zero is rejected.
Used to validate `limits.max_message_size` in the TOML file.

```rust
parse_byte_size("16MiB")   // Ok(16 * 1024 * 1024)
parse_byte_size("4 KiB")   // Ok(4 * 1024)
parse_byte_size("128")      // Ok(128)
parse_byte_size("0MiB")     // Err - zero is rejected
```

---

## Logging setup

`init_logging(log_level, log_format)` initializes `tracing_subscriber` once, before the runtime starts.

Log level resolution order:
1. `--log-level` CLI flag
2. `NX_LOG_LEVEL` env var
3. `[observability].log_level` in TOML
4. `--verbose` flag → forces `debug`
5. Default → `info`

Valid levels: `trace`, `debug`, `info`, `warn`, `error`. Any other value is rejected.

Log format: `text` (default, human-readable) or `json` (structured, for log aggregators).

---

## Config file template

`CONFIG_TEMPLATE` is a `const &str` embedded in the binary.
`nx config init` writes it to disk via `init_config_file(path, force)`.

The template includes every section with sane defaults and comments.
It is the canonical reference for what the file format supports.

To add a new TOML field, add it to the template and to the corresponding `*FileConfig` struct.

---

## `render_effective_toml`

`EffectiveRunConfig::render_effective_toml()` builds a TOML string from the resolved config.
It is used by `nx config show --effective`.

It reconstructs all sections manually (not via serde serialization) to control output order
and include computed values like serialization format, TLS state, and limit defaults.

---

## How to add a new flag (developer guide)

1. **Add the field** to `Cli::Run` in `main.rs` with a `#[arg(...)]` annotation.
2. **Add it to `RunCliOptions`** in `config.rs`.
3. **Add the env var** to `EnvRunConfig` and `EnvRunConfig::from_env()` if it has one.
4. **Add the TOML field** to the appropriate `*FileConfig` struct if it belongs in the file.
5. **Wire the precedence** in `EffectiveRunConfig::resolve_with_env`: CLI → env → file → default.
6. **Add a validator** if the field has constraints (e.g. requires another field, must be non-zero).
7. **Add it to `render_effective_toml`** if it should appear in `nx config show --effective`.
8. **Add it to `CONFIG_TEMPLATE`** if it has a TOML representation.
9. **Write tests** in the `#[cfg(test)]` block: clap parsing, precedence, validation.

---

## Test coverage

The `#[cfg(test)]` block in `main.rs` covers:

| Module | What it tests |
|---|---|
| `duration_parser` | valid and invalid duration strings, zero rejection |
| `byte_size_parser` | MiB/KiB/bytes parsing, zero rejection |
| `file_config` | TOML parsing for all sections, precedence CLI > env > file, limit application, observability config |
| `validate_tls` | all valid and invalid TLS flag combinations |
| `validate_settle` | settle with/without sync |
| `validate_wait_before_run` | wait with/without sync |
| `validate_print_counter` | all print-CRDT flags with/without sync |
| `build_tls` | TLS config construction, allowlist dedup/trim |
| `build_sync` | sync enabled/disabled, listen-only, peer-without-listen error |
| `clap_parsing` | every flag round-trip through clap, regression guards (e.g. `--sync-prefix` removed) |

To run only the cli tests:

```bash
cargo test -p nx-cli
```

---

## Related

Use this page together with the user-facing CLI and config docs:

- [CLI reference](/numax/reference/cli/) - flags and subcommands exposed by `nx`
- [Configuration](/numax/reference/configuration/) - TOML and environment variable reference
- [nx-core crate](/numax/reference/crates/nx-core/) - the runtime layer `nx-cli` calls into
- [Crates overview](/numax/reference/crates/) - where `nx-cli` fits in the dependency graph
