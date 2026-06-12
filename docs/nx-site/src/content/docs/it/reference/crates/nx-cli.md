---
title: nx-cli
description: Frontend a riga di comando per Numax.
---

`nx-cli` è il punto di ingresso per ogni interazione utente con Numax.
Fa il parsing dei flag, risolve la configurazione da quattro sorgenti, valida il risultato,
e consegna un `RuntimeConfig` completamente costruito a `nx-core`. Non contiene mai logica di runtime.

**Produce:** il binario `nx` (`crates/nx-cli/src/main.rs`, `[[bin]] name = "nx"`).

---

## Responsabilità

| Responsabilità | Dove |
|---|---|
| Parsing flag CLI | `main.rs` - enum `Cli` via clap |
| Lettura e validazione file TOML | `config.rs` - `RunFileConfig`, `validate_run_file_config` |
| Lettura variabili d'ambiente | `config.rs` - `EnvRunConfig::from_env` |
| Risoluzione precedenza (CLI > env > file > default) | `config.rs` - `EffectiveRunConfig::resolve` |
| Validazione combinazioni flag (TLS, sync, settle) | `config.rs` - `validate_tls_flags`, `validate_settle_mode`, ecc. |
| Costruzione `SyncConfig`, `TlsConfig`, `ObservabilityConfig` | `config.rs` - `build_sync_config`, `build_tls_config`, `build_observability_config` |
| Inizializzazione logging | `config.rs` - `init_logging` |
| Generazione template `numax.toml` | `config.rs` - `CONFIG_TEMPLATE`, `init_config_file` |
| Stampa configurazione effettiva risolta | `config.rs` - `EffectiveRunConfig::render_effective_toml` |

---

## Struttura comandi

```
nx
├── run <MODULE> [OPTIONS]
└── config
    ├── init [--output PATH] [--force]
    ├── validate [--config PATH]
    └── show --config PATH --effective
```

Definita in `main.rs` come:

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

Tutto il parsing è fatto da clap `#[derive(Parser)]`. Nessun parsing manuale degli argomenti.

---

## Pipeline di risoluzione configurazione

Questo è il cuore di `nx-cli`. Ogni flag ha fino a quattro sorgenti. La pipeline è:

```
1. CLI flags            (priorità massima)
2. Variabili NX_*
3. Sezioni numax.toml
4. Default del runtime  (priorità minima)
```

Il punto di ingresso è `EffectiveRunConfig::resolve(cli, file_config)` che internamente chiama
`resolve_with_env(cli, EnvRunConfig::from_env()?, file_config)`.

### Struct coinvolte

**`RunCliOptions`** - cosa è arrivato dai flag della riga di comando.

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

**`EnvRunConfig`** - cosa è arrivato dalle variabili d'ambiente `NX_*`.

Costruito da `EnvRunConfig::from_env()`. Ogni campo mappa a una variabile:

| Campo | Variabile | Note |
|---|---|---|
| `datastore_path` | `NX_DATASTORE_PATH` | |
| `listen` | `NX_LISTEN` | |
| `peers` | `NX_PEER` + `NX_PEERS` | additivi - entrambi usati se impostati |
| `observability_listen` | `NX_OBSERVABILITY_LISTEN` | |
| `tls_cert` | `NX_TLS_CERT` | |
| `tls_key` | `NX_TLS_KEY` | |
| `tls_ca` | `NX_TLS_CA` | |
| `allowed_peers` | `NX_ALLOWED_PEERS` | separati da virgola |
| `tls_insecure` | `NX_TLS_INSECURE` | `1/true/yes/on` o `0/false/no/off` |
| `serialization_format` | `NX_SERIALIZATION_FORMAT` | `bincode` o `json` |
| `log_level` | `NX_LOG_LEVEL` | |
| `log_format` | `NX_LOG_FORMAT` | `text` o `json` |

**`RunFileConfig`** - cosa è arrivato da `numax.toml`. Sezioni:

```rust
pub struct RunFileConfig {
    pub network:       Option<NetworkFileConfig>,
    pub tls:           Option<TlsFileConfig>,
    pub storage:       Option<StorageFileConfig>,
    pub limits:        Option<LimitsFileConfig>,
    pub observability: Option<ObservabilityFileConfig>,
    pub discovery:     Option<DiscoveryFileConfig>,
}
```

Tutte le struct usano `#[serde(deny_unknown_fields)]` - le chiavi sconosciute nel TOML vengono rifiutate al parsing.

**`EffectiveRunConfig`** - il risultato finale unito, passato a `nx-core`.

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

## Logica di abilitazione sync

La sync non è sempre abilitata. `build_sync_config` decide:

- Se nessun `listen` è impostato e non ci sono input sync-related da nessuna parte → sync disabilitata, restituisce `None`.
- Se qualsiasi campo sync-related è presente (env, file, TLS, format) ma `listen` manca → **errore**. La modalità dialer-only non è supportata.
- Se `listen` è impostato → sync abilitata, `SyncConfig` viene costruita e restituita.

`force_enabled` è `true` quando il file di configurazione ha sezioni `[network]`, `[tls]`, o `[limits]`,
o quando le variabili d'ambiente forniscono input sync. Questo fa funzionare correttamente
`nx config show --effective` anche senza `--listen` da CLI.

---

## Funzioni di validazione

Tutta la validazione avviene prima che il runtime parta. Gli errori vengono restituiti come `anyhow::Result`.

| Funzione | Cosa controlla |
|---|---|
| `validate_tls_flags` | `cert`+`key` devono stare insieme; `insecure` mutualmente esclusivo con `ca`/`allowed_peers`; `allowed_peers` richiede `ca` |
| `validate_settle_mode` | `--settle-for` richiede `--listen` |
| `validate_wait_before_run` | `--wait-before-run` richiede `--listen` |
| `validate_print_gcounter` | `--print-gcounter` richiede `--listen` |
| `validate_print_pncounter` | `--print-pncounter` richiede `--listen` |
| `validate_print_lww_register` | `--print-lww-register` richiede `--listen` |
| `validate_print_lww_map` | `--print-lww-map` richiede `--listen` |
| `validate_print_orset` | `--print-orset` richiede `--listen` |
| `validate_print_rga` | `--print-rga` richiede `--listen` |
| `validate_run_file_config` | validazione strutturale TOML completa: campi non vuoti, path validi, coppie limit consistenti, valori non-zero |

---

## Parser

### `parse_duration`

Accetta: `500ms`, `5s`, `2m`, o intero semplice (secondi). Zero viene rifiutato.
Usato da clap come `value_parser` per `--settle-for`, `--wait-before-run`, `--shutdown-timeout`.

```rust
parse_duration("500ms") // Ok(Duration::from_millis(500))
parse_duration("5s")    // Ok(Duration::from_secs(5))
parse_duration("2m")    // Ok(Duration::from_secs(120))
parse_duration("3")     // Ok(Duration::from_secs(3))
parse_duration("0s")    // Err - zero rifiutato
parse_duration("soon")  // Err - non valido
```

### `parse_byte_size`

Accetta: `16MiB`, `4KiB`, `128` (byte), con spazio opzionale (`16 MiB`). Zero viene rifiutato.
Usato per validare `limits.max_message_size` nel file TOML.

```rust
parse_byte_size("16MiB")  // Ok(16 * 1024 * 1024)
parse_byte_size("4 KiB")  // Ok(4 * 1024)
parse_byte_size("128")    // Ok(128)
parse_byte_size("0MiB")   // Err - zero rifiutato
```

---

## Setup logging

`init_logging(log_level, log_format)` inizializza `tracing_subscriber` una volta, prima che il runtime parta.

Ordine di risoluzione log level:
1. Flag CLI `--log-level`
2. Env var `NX_LOG_LEVEL`
3. `[observability].log_level` nel TOML
4. Flag `--verbose` → forza `debug`
5. Default → `info`

Livelli validi: `trace`, `debug`, `info`, `warn`, `error`. Qualsiasi altro valore viene rifiutato.

Formato log: `text` (default, human-readable) o `json` (strutturato, per aggregatori di log).

---

## Template file di configurazione

`CONFIG_TEMPLATE` è una `const &str` embedded nel binario.
`nx config init` la scrive su disco tramite `init_config_file(path, force)`.

Il template include ogni sezione con default sensati e commenti.
È il riferimento canonico per quello che il formato file supporta.

Per aggiungere un nuovo campo TOML, aggiungilo al template e alla struct `*FileConfig` corrispondente.

---

## `render_effective_toml`

`EffectiveRunConfig::render_effective_toml()` costruisce una stringa TOML dalla config risolta.
Viene usato da `nx config show --effective`.

Ricostruisce tutte le sezioni manualmente (non tramite serializzazione serde) per controllare
l'ordine dell'output e includere valori calcolati come formato di serializzazione, stato TLS e default dei limit.

---

## Come aggiungere un nuovo flag (guida per developer)

1. **Aggiungi il campo** a `Cli::Run` in `main.rs` con un'annotazione `#[arg(...)]`.
2. **Aggiungilo a `RunCliOptions`** in `config.rs`.
3. **Aggiungi la variabile d'ambiente** a `EnvRunConfig` e `EnvRunConfig::from_env()` se ce l'ha.
4. **Aggiungi il campo TOML** alla struct `*FileConfig` appropriata se appartiene al file.
5. **Cablare la precedenza** in `EffectiveRunConfig::resolve_with_env`: CLI → env → file → default.
6. **Aggiungi un validatore** se il campo ha vincoli (es. richiede un altro campo, deve essere non-zero).
7. **Aggiungilo a `render_effective_toml`** se deve apparire in `nx config show --effective`.
8. **Aggiungilo a `CONFIG_TEMPLATE`** se ha una rappresentazione TOML.
9. **Scrivi i test** nel blocco `#[cfg(test)]`: parsing clap, precedenza, validazione.

---

## Copertura test

Il blocco `#[cfg(test)]` in `main.rs` copre:

| Modulo | Cosa testa |
|---|---|
| `duration_parser` | stringhe durata valide e non valide, rifiuto zero |
| `byte_size_parser` | parsing MiB/KiB/byte, rifiuto zero |
| `file_config` | parsing TOML per tutte le sezioni, precedenza CLI > env > file, applicazione limit, configurazione observability |
| `validate_tls` | tutte le combinazioni valide e non valide di flag TLS |
| `validate_settle` | settle con/senza sync |
| `validate_wait_before_run` | wait con/senza sync |
| `validate_print_counter` | tutti i flag print-CRDT con/senza sync |
| `build_tls` | costruzione TLS config, dedup/trim allowlist |
| `build_sync` | sync abilitata/disabilitata, solo listen, errore peer-senza-listen |
| `clap_parsing` | round-trip di ogni flag attraverso clap, guard regressione (es. `--sync-prefix` rimosso) |

Per eseguire solo i test cli:

```bash
cargo test -p nx-cli
```

---

## Correlati

Leggi questa pagina insieme ai docs CLI e configurazione esposti all'utente:

- [Riferimento CLI](/numax/it/reference/cli/) - flag e sottocomandi esposti da `nx`
- [Configurazione](/numax/it/reference/configuration/) - riferimento TOML e variabili d'ambiente
- [Crate nx-core](/numax/it/reference/crates/nx-core/) - il layer runtime chiamato da `nx-cli`
- [Panoramica crate](/numax/it/reference/crates/) - dove `nx-cli` si inserisce nel grafo delle dipendenze
