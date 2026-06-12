---
title: Crate
description: Panoramica dei crate Rust di Numax.
---

Numax è un workspace Cargo con sei crate. Ognuno possiede un singolo layer dello stack.
Nessun crate attraversa il proprio confine.

```
nx-cli
  └── nx-core
        ├── nx-store
        ├── nx-sync
        └── nx-net
              └── nx-sync

nx-sdk          (standalone — target wasm32, nessuna dipendenza interna)
```

---

## nx-cli

**Cosa possiede:** il binario `nx`. Parsing della configurazione, validazione dei flag CLI, risoluzione della precedenza, setup del logging.

**Non possiede:** logica di runtime, esecuzione WASM, networking. Tutto sotto la superficie CLI è delegato a `nx-core`.

**Produce:** l'eseguibile `nx`.

**File chiave:**
- `src/main.rs` - definizioni comandi (`nx run`, `nx config`), parsing flag via clap, wiring del runtime
- `src/config.rs` - struct per file TOML, risoluzione variabili d'ambiente, builder configurazione effettiva, template `nx config init`

**Dipendenze esterne:** `clap`, `tokio`, `tracing`, `tracing-subscriber`, `toml`, `serde`

---

## nx-core

**Cosa possiede:** il runtime. Caricamento ed esecuzione moduli WASM via Wasmtime, l'intera superficie host API, il sync manager, l'osservabilità.

**Non possiede:** strutture dati CRDT (nx-sync), TCP/TLS raw (nx-net), il sled store (nx-store). Li compone.

**File chiave:**
- `src/runtime.rs` - struct `Runtime`: avvia/ferma la sync, esegue il modulo WASM, espone `settle_for`, `serve`, `shutdown_with_timeout`
- `src/sync_manager.rs` - possiede il registry CRDT in-memory, op-log, scheduling anti-entropy, broadcast ai peer, idratazione dello stato durevole all'avvio
- `src/sync_config.rs` - builder `SyncConfig`, `TlsConfig`, `ObservabilityConfig`
- `src/observability.rs` - endpoint HTTP per le metriche
- `src/host_api/db.rs` - `db_get`, `db_set`, `db_delete`, `db_exists`, `db_scan`, `db_scan_after`, `db_keys`, `db_keys_after`
- `src/host_api/crdt.rs` - tutte le funzioni host CRDT: gcounter, pncounter, lww_register, lww_map, orset, rga
- `src/host_api/crypto.rs` - `random_bytes`, `hash_sha256`, `hash_blake3`
- `src/host_api/log.rs` - `host_log`, `host_log_v2`
- `src/host_api/net.rs` - `net_node_id`, `net_peers`
- `src/host_api/system.rs` - `env_get`, `module_id`, `host_capabilities`, `event_emit`, `abort`
- `src/host_api/time.rs` - `time_now`, `time_monotonic`

**Dipendenze esterne:** `wasmtime`, `wasmtime-wasi`, `tokio`, `tracing`, `blake3`, `sha2`, `getrandom`, `serde_json`

---

## nx-store

**Cosa possiede:** il key/value store embedded locale. Un wrapper tipizzato e sottile su sled.

**Non possiede:** logica CRDT, networking, nulla di sync-related.

**File chiave:**
- `src/store.rs` - `Store`: `get`, `set`, `delete`, `exists`, scan per prefisso con cursore offset e cursore chiave
- `src/lib.rs` - re-export API pubblica
- `src/error.rs` - `StoreError`

**Dipendenze esterne:** `sled`, `thiserror`

**Bench:** `single_node_load` - benchmark throughput operazioni locali lettura/scrittura.

---

## nx-sync

**Cosa possiede:** strutture dati CRDT e tipi di operazione. Logica pura — niente I/O, niente async, niente networking.

Questo è il crate su cui si può ragionare e fare test senza nessun runtime. Definisce cos'è un'operazione, come i CRDT fanno merge, e come appare il formato wire serializzato.

**File chiave:**
- `src/op.rs` - `OpId`, `OpKind` (una variante per ogni operazione CRDT), `Op`, serializzazione per tutti i tipi di op
- `src/node_id.rs` - tipo `NodeId` e generazione casuale
- `src/crdt/gcounter.rs` - stato e merge GCounter
- `src/crdt/pncounter.rs` - stato e merge PNCounter
- `src/crdt/lww_register.rs` - stato e merge LWW-Register
- `src/crdt/lww_map.rs` - stato e merge LWW-Map
- `src/crdt/orset.rs` - stato e merge ORSet
- `src/crdt/rga.rs` - stato e merge RGA

**Dipendenze esterne:** `serde`, `serde_json`, `uuid`, `thiserror`

---

## nx-net

**Cosa possiede:** TCP networking, TLS/mTLS, framing messaggi, gossip, gestione peer, loop anti-entropy.

**Non possiede:** logica CRDT (consumata da nx-sync), accesso allo store (passa attraverso il sync manager di nx-core).

**File chiave:**
- `src/node.rs` - `SyncNode`: ascolta connessioni in ingresso, chiama i peer, gestisce il backoff di riconnessione, esegue i loop di broadcast e anti-entropy
- `src/message.rs` - tipi wire `Message` e `MessageKind`: `Hello`, `HelloAck`, `PushOps`, `PushOpsAck`, `PullSince`, `Ping`, `Pong`
- `src/tls.rs` - setup acceptor/connector TLS, parsing certificati, estrazione identità peer mTLS, enforcement allowlist
- `src/peer.rs` - `PeerInfo`: indirizzo, NodeId, stato connessione
- `src/error.rs` - `NetError`

**Dipendenze esterne:** `tokio`, `tokio-rustls`, `rustls`, `rustls-pemfile`, `rcgen`, `bincode`, `serde`, `serde_json`, `sha2`, `hex`, `x509-parser`, `tracing`

**Bench:** `serialization` - throughput encode/decode formato wire.

---

## nx-sdk

**Cosa possiede:** l'SDK lato guest per i moduli WASM. Gira dentro il binario `.wasm`, non dentro il runtime Numax.

Compila a `wasm32-unknown-unknown`. Non ha dipendenze interne al workspace e nessuna dipendenza runtime/async. L'unica cosa che tocca è l'host tramite import FFI.

**File chiave:**
- `src/ffi.rs` - import `unsafe extern "C"` raw dal namespace `nx`: ogni funzione host che l'SDK chiama
- `src/db.rs` - wrapper sicuri `get`, `set`, `delete`, `exists`, `scan`, `keys`
- `src/log.rs` - funzione `log(msg)` e macro `nx_log!`
- `src/crypto.rs` - `random_bytes`, `hash_sha256`, `hash_blake3`
- `src/time.rs` - `now`, `monotonic`
- `src/net.rs` - `node_id`, `peers`
- `src/system.rs` - `env_get`, `module_id`
- `src/error.rs` - `NxError`, `Result`
- `src/crdt/gcounter.rs` - `inc`, `value`
- `src/crdt/pncounter.rs` - `inc`, `dec`, `value`
- `src/crdt/lww_register.rs` - `set`, `get`
- `src/crdt/lww_map.rs` - `set`, `remove`, `get`, `contains`, `entries`
- `src/crdt/orset.rs` - `add`, `remove`, `contains`, `elements`
- `src/crdt/rga.rs` - `insert_after`, `delete`, `values`

**Dipendenze esterne:** nessuna. La feature `std` è opzionale e disabilitata per default.

---

## Grafo delle dipendenze completo

```
nx-cli ──────────────────────────────────── bin: nx
  │
  └── nx-core ──────────────────────────── runtime, host API, sync manager
        │
        ├── nx-store ─────────────────────  sled KV store
        │
        ├── nx-sync ──────────────────────  tipi CRDT, tipi op, logica pura
        │
        └── nx-net ───────────────────────  TCP, TLS, gossip, anti-entropy
              │
              └── nx-sync

nx-sdk ───────────────────────────────────  SDK guest (wasm32, nessuna dipendenza interna)
```

---

## Da dove continuare

- [Host API](/numax/it/reference/host-api/) - le funzioni che `nx-sdk` chiama e `nx-core` implementa
- [Configurazione](/numax/it/reference/configuration/) - come `nx-cli` risolve la config prima di passarla a `nx-core`
- [CLI](/numax/it/reference/cli/) - la superficie utente del comando `nx`
