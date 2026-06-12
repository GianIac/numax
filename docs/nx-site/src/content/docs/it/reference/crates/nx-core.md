---
title: nx-core
description: Runtime, host API e orchestrazione sync.
---

`nx-core` è il centro dello stack Numax. Possiede l'esecuzione WASM, l'intera superficie host API,
il sync manager e l'osservabilità. `nx-cli` costruisce un `RuntimeConfig` e lo consegna a questo crate.
Tutto ciò che sta sotto quel confine vive qui o nei crate che compone.

---

## Responsabilità

| Responsabilità | Dove |
|---|---|
| Caricamento, compilazione e caching moduli WASM | `runtime.rs` - `Runtime::run_module`, `compile_or_get_cached_module` |
| Superficie host API (tutti gli import namespace `nx`) | `host_api/` - un file per gruppo API |
| Lifecycle: avvia sync, esegui modulo, settle, serve, shutdown | `runtime.rs` - `Runtime` |
| Registry CRDT in-memory e op-log | `sync_manager.rs` - `SyncManager` |
| Stato CRDT durevole all'avvio | `sync_manager.rs` - idratazione da `nx-store` |
| Scheduling anti-entropy | `sync_manager.rs` |
| Broadcast ai peer | `sync_manager.rs` + `nx-net` |
| Persistenza NodeId | `runtime.rs` - `load_or_create_node_id` |
| Endpoint HTTP osservabilità | `observability.rs` - `ObservabilityServer` |
| Tipi di config esposti a `nx-cli` | `sync_config.rs` - `SyncConfig`, re-export `TlsConfig`, `ObservabilityConfig` |

---

## Runtime

La struct `Runtime` è la API pubblica di `nx-core`. `nx-cli` ne crea una, chiama i metodi in ordine,
poi la spegne.

```rust
pub struct Runtime {
    engine:               Engine,              // wasmtime engine, condiviso tra le run
    linker:               Linker<HostState>,   // tutte le funzioni host API registrate qui
    config:               RuntimeConfig,
    store:                Arc<NxStore>,        // condiviso con ogni HostState e il sync manager
    metrics:              Arc<RuntimeMetrics>,
    module_cache:         Mutex<HashMap<[u8; 32], Module>>, // chiave blake3
    sync_manager:         Option<SyncManager>,
    sync_handle:          Option<SyncHandle>,  // clone economico, passato a ogni HostState
    observability_server: Option<ObservabilityServer>,
}
```

### RuntimeConfig

```rust
pub struct RuntimeConfig {
    pub enable_wasi:       bool,           // default: true
    pub max_memory_bytes:  Option<u64>,    // cap memoria per invocazione via StoreLimits
    pub datastore_path:    PathBuf,        // default: ./nx-data
    pub sync:              Option<SyncConfig>,
    pub observability:     Option<ObservabilityConfig>,
    pub module_id:         String,         // esposto al guest tramite system::module_id()
}
```

`sync: None` significa nessun networking, nessuna replica CRDT, nessun `SyncManager`.
Il runtime funziona comunque - lo store è solo locale.

### HostState

Un `HostState` viene creato per ogni invocazione di `run_module` e associato al `Store` di wasmtime.

```rust
pub struct HostState {
    pub wasi:        Option<p1::WasiP1Ctx>,  // None quando enable_wasi = false
    pub store:       Arc<NxStore>,           // stesso Arc del Runtime
    pub sync_handle: Option<SyncHandle>,     // None quando sync disabilitata
    pub module_id:   Arc<str>,
    pub limits:      wasmtime::StoreLimits,  // enforcement cap memoria
}
```

### Metodi lifecycle

Ordine di chiamata standard da `nx-cli`:

```
Runtime::new(config)
  └── start_observability()   opzionale, avvia endpoint HTTP
  └── start_sync()            opzionale, avvia SyncManager + networking
  └── wait_before_run(dur)    opzionale, attende i peer prima di eseguire
  └── run_module(bytes)       carica, linka, istanzia, chiama run()
  └── settle_for(dur)         opzionale, tiene la sync attiva per una finestra limitata
      OPPURE serve()          opzionale, tiene la sync attiva fino a SIGINT/SIGTERM/SIGHUP
  └── shutdown_with_timeout(dur)
```

| Metodo | Cosa fa |
|---|---|
| `new(config)` | Apre lo store sled, costruisce engine + linker wasmtime con tutte le funzioni host API registrate, crea `SyncManager` se configurato |
| `start_observability()` | Fa il bind dell'endpoint HTTP metriche. No-op se non configurato |
| `start_sync()` | Chiama `SyncManager::start()`, avvia TCP listener + dial loop. No-op se sync disabilitata |
| `wait_before_run(dur)` | Riconnette i peer configurati ripetutamente fino alla scadenza. No-op se sync disabilitata |
| `run_module(bytes)` | Compila o recupera il modulo dalla cache, costruisce `HostState`, istanzia, chiama `run()` o `_start()` |
| `settle_for(dur)` | Dorme per `dur`, mantenendo la sync attiva. No-op se sync disabilitata |
| `serve()` | Blocca fino al segnale OS (SIGINT/SIGTERM/SIGHUP su Unix, Ctrl+C su Windows). No-op se sync disabilitata |
| `shutdown_with_timeout(dur)` | Ferma il sync manager, fa flush dello store sled, spegne il server osservabilità. Limitato da `dur` (default 30s) |

### Cache compilazione moduli

I moduli vengono compilati una volta e cachati in una `Mutex<HashMap<[u8; 32], Module>>`, con chiave
il hash blake3 dei byte raw. Chiamate ripetute a `run_module` con gli stessi byte saltano completamente
la compilazione. La cache vive per tutta la lifetime del `Runtime`.

### Persistenza NodeId

Al primo avvio con sync abilitata, `load_or_create_node_id` genera un `NodeId` e lo memorizza
sotto `__nx/runtime/node_id` in sled. Agli avvii successivi legge la stessa chiave.
Questo garantisce che un nodo presenti sempre la stessa identità ai suoi peer tra i restart.

---

## SyncConfig

`SyncConfig` è il builder passato dentro `RuntimeConfig` quando la sync è necessaria.
`nx-cli` lo costruisce in `config.rs`; `nx-core` lo consuma in `SyncManager::new`.

```rust
SyncConfig::new()
    .with_listen_addr("0.0.0.0:9000")
    .with_peer("127.0.0.1:9001")
    .with_tls(TlsConfig::new(cert, key, ca))
    .with_max_peers(16)
    .with_queued_ops_limit(5000)
    .with_op_log_limit(5000)
    .with_seen_ops_limit(50000)
    .with_max_message_size(8 * 1024 * 1024)
    .with_socket_timeout(Duration::from_secs(15))
    .with_reconnect_backoff(Duration::from_millis(250), Duration::from_secs(15))
    .with_peer_dead_after_failures(5)
    .with_anti_entropy_interval(Duration::from_secs(60))
    .with_serialization_format(SerializationFormat::Bincode)
```

**`is_enabled()`** restituisce `true` solo quando `listen_addr` è impostato.
I peer da soli non abilitano la sync - un nodo deve anche ascoltare.

### Default

| Campo | Default |
|---|---|
| `max_peers` | 64 |
| `queued_ops_limit` | 10 000 |
| `op_log_limit` | 10 000 |
| `seen_ops_limit` | 100 000 |
| `max_message_size` | 16 MiB |
| `socket_timeout` | 30s |
| `reconnect_initial_delay` | 500ms |
| `reconnect_max_delay` | 30s |
| `peer_dead_after_failures` | 3 |
| `anti_entropy_interval` | 30s |
| `serialization_format` | `Bincode` |

---

## SyncManager

`SyncManager` possiede il lato runtime della replica. È il ponte tra le chiamate host API
dai moduli guest e il layer di rete in `nx-net`.

**Cosa possiede:**
- Registry CRDT in-memory (uno stato per chiave CRDT, tutti i tipi)
- Op-log (limitato da `op_log_limit`) per il replay anti-entropy
- Set degli op già visti (limitato da `seen_ops_limit`) per la deduplicazione
- Loop di scheduling anti-entropy
- Coda broadcast ai peer (limitata da `queued_ops_limit`)

**Cosa non possiede:**
- Connessioni TCP e TLS (delegato a `nx-net::SyncNode`)
- Strutture dati CRDT e logica di merge (delegato a `nx-sync`)
- Store sled (condiviso `Arc<NxStore>` dal `Runtime`)

### SyncHandle

`SyncHandle` è un clone economico di un endpoint di canale verso il `SyncManager`.
È quello che tengono le funzioni host API - spingono le op nel manager tramite l'handle
senza bloccare il guest.

```rust
// Dentro una funzione host API (es. crdt.rs):
state.sync_handle.as_ref()
    .ok_or(ERR_SYNC_DISABLED)?
    .push_op(op)
    .await?;
```

### Metodi di lettura CRDT

Dopo `settle_for` o `serve`, `nx-cli` può leggere lo stato CRDT tramite `Runtime`:

```rust
runtime.get_counter_value("counter:visits").await    // Option<u64>
runtime.get_pncounter_value("inventory:sku").await   // Option<i64>
runtime.get_lww_register_value("status:svc").await   // Option<Option<Vec<u8>>>
runtime.get_orset_elements("tags:item").await        // Option<Vec<String>>
runtime.get_lww_map_entries("settings:svc").await    // Option<Vec<(String, Vec<u8>)>>
runtime.get_rga_values("comments:doc").await         // Option<Vec<Vec<u8>>>
```

Tutti restituiscono `None` quando la sync è disabilitata (usati dai flag `--print-*` in `nx-cli`).

---

## Host API

Tutte le funzioni host vengono registrate in `Runtime::new` tramite chiamate `add_to_linker`:

```rust
host_api::log::add_to_linker(&mut linker)?;
host_api::db::add_to_linker(&mut linker)?;
host_api::time::add_to_linker(&mut linker)?;
host_api::crypto::add_to_linker(&mut linker)?;
host_api::system::add_to_linker(&mut linker)?;
host_api::net::add_to_linker(&mut linker)?;
host_api::crdt::add_to_linker(&mut linker)?;
```

Ogni file possiede un gruppo. Seguono tutti lo stesso pattern: leggi dalla memoria lineare del guest,
fai il lavoro, scrivi nel buffer di output, restituisci byte count o codice errore.

| File | Funzioni registrate |
|---|---|
| `host_api/log.rs` | `host_log`, `host_log_v2` |
| `host_api/db.rs` | `db_get`, `db_set`, `db_delete`, `db_exists`, `db_scan`, `db_scan_after`, `db_keys`, `db_keys_after` |
| `host_api/time.rs` | `time_now`, `time_monotonic` |
| `host_api/crypto.rs` | `random_bytes`, `hash_sha256`, `hash_blake3` |
| `host_api/system.rs` | `env_get`, `module_id`, `host_capabilities`, `event_emit`, `abort` |
| `host_api/net.rs` | `net_node_id`, `net_peers` |
| `host_api/crdt.rs` | tutte le 18 funzioni CRDT |

Per le firme complete e il comportamento vedi [Host API](/numax/it/reference/host-api/).

### Come aggiungere una nuova funzione host (guida per developer)

1. Aggiungi l'import FFI raw a `nx-sdk/src/ffi.rs`.
2. Aggiungi il wrapper SDK sicuro nel file `nx-sdk/src/*.rs` appropriato.
3. Aggiungi l'implementazione host nel file `nx-core/src/host_api/*.rs` appropriato,
   seguendo il pattern leggi-da-memoria-guest / scrivi-nel-buffer-output.
4. Registrala con `linker.func_wrap("nx", "nome_funzione", ...)` dentro `add_to_linker`.
5. Chiama `add_to_linker` da `Runtime::new`.
6. Se richiede la sync, controlla `state.sync_handle.is_some()` e restituisci `ERR_SYNC_DISABLED` se no.
7. Scrivi i test. Per le funzioni CRDT, aggiungi un test E2E in `sync_manager.rs`.

---

## Osservabilità

`ObservabilityServer` espone un endpoint HTTP locale quando `RuntimeConfig.observability` è impostato.

`RuntimeMetrics` è una struct condivisa via `Arc`, aggiornata dal runtime e dal sync manager.
Traccia la readiness e contatori base accessibili attraverso l'endpoint HTTP.

`metrics.set_ready(true)` viene chiamato dopo l'avvio della sync. `set_ready(false)` viene chiamato all'inizio dello shutdown.

---

## Copertura test

I test si trovano in `runtime.rs` (`#[cfg(test)]` in fondo) e `sync_config.rs`.

| Test | Cosa copre |
|---|---|
| `serve_returns_immediately_when_sync_is_disabled` | `serve` è no-op senza sync |
| `serve_keeps_runtime_alive_until_shutdown` | `serve` blocca fino al segnale, restituisce il `ShutdownSignal` corretto |
| `serve_returns_none_when_sync_is_disabled` | `serve_until_shutdown` restituisce segnale `None` con sync off |
| `settle_returns_immediately_when_sync_is_disabled` | `settle_for` è no-op senza sync |
| `settle_waits_for_requested_duration_when_sync_is_enabled` | `settle_for` dorme davvero |
| `shutdown_with_timeout_flushes_store_without_sync` | lo store viene flushed allo shutdown |
| `run_module_reuses_compiled_module_for_same_bytes` | la cache moduli funziona, il contatore resta a 1 |
| `sync_runtime_reuses_persisted_node_id` | il NodeId sopravvive al drop e alla riapertura del `Runtime` |
| Test `SyncConfig` | `is_enabled` richiede listen addr, peer da soli non abilitano, tutti i campi builder |

```bash
cargo test -p nx-core
```

---

## Correlati

Leggi questa pagina insieme ai crate che alimentano o vengono orchestrati dal runtime:

- [Host API](/numax/it/reference/host-api/) - riferimento completo per `host_api/`
- [Crate nx-cli](/numax/it/reference/crates/nx-cli/) - costruisce `RuntimeConfig` e chiama questo crate
- [Crate nx-sync](/numax/it/reference/crates/nx-sync/) - tipi CRDT consumati dal sync manager
- [Crate nx-net](/numax/it/reference/crates/nx-net/) - networking delegato dal sync manager
- [Panoramica crate](/numax/it/reference/crates/) - grafo completo delle dipendenze
