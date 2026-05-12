# Roadmap Numax v0.1.0

> **Release corrente**: `v0.1.0-alpha.1` - developer preview.
> **Obiettivo finale `v0.1.0`**: runtime production-ready per workload non-critici.
> **Stato**: alpha per feedback; hardening production ancora in corso.

---

## Stato Release

### v0.1.0-alpha.1 ✅
**Scopo**: prima preview tecnica da pubblicare per raccogliere feedback.

Include:
- Runtime Wasmtime + WASI preview1.
- Host API KV locale (`db_get`, `db_set`, `db_delete`) e logging.
- Store embedded sled.
- GCounter CRDT con JSON serialization.
- Networking base con Hello/PushOps/PullSince/Ping.
- TLS/mTLS, NodeID derivato da certificato e allowlist.
- Wiring end-to-end interno tra guest CRDT API, SyncManager e datastore.
- Materializzazione sled del totale GCounter su update locale/remoto.
- Test E2E `SyncManager` per handshake, PushOps, convergenza e idempotenza.
- Esempi: `distributed_counter`, `distributed_chat` local-only, `vote_tally_tls`.

Limitazioni note:
- `nx run` esegue il guest una volta e termina; il criterio CLI multi-process
  della Fase 6.5 non è ancora completamente rispettato alla lettera.
- Hydration del registry GCounter dai valori materializzati in sled non ancora
  implementata.
- Lifecycle long-running, graceful shutdown, backpressure, observability e
  resilienza rete sono ancora fasi aperte.
- API e wire format possono cambiare prima di `v0.1.0`.

### v0.1.0 🎯
**Scopo**: prima release production-ready per workload non-critici.

Richiede il completamento delle fasi P0/P1 indicate sotto, in particolare:
Fase 7 lifecycle, Fase 8 backpressure, Fase 9 observability minima,
Fase 10 resilienza rete, Fase 11 serializzazione dual-mode, Fase 12 host API
minime e Fase 13 test di carico.

---

## Fasi Completate

### Fase 0: Bootstrap ✅
- [x] Workspace Cargo multi-crate
- [x] Struttura directory
- [x] CI base

### Fase 1: nx-core ✅
- [x] Runtime Wasmtime
- [x] Host API (db_get, db_set, db_delete, host_log_v2)
- [x] Integrazione WASI preview1
- [x] Guardrail sicurezza (limiti chiavi/valori)

### Fase 2: nx-store ✅
- [x] Store sled embedded
- [x] API get/set/delete/scan_prefix
- [x] Test unitari e integrazione

### Fase 3: nx-sync ✅
- [x] NodeId e Op/OpId
- [x] GCounter CRDT completo
- [x] Test proprietà CRDT (commutatività, associatività, idempotenza)
- [x] Serializzazione JSON

### Fase 4: nx-net ✅
- [x] Protocollo messaggi (Hello, PushOps, PullSince, Ping/Pong)
- [x] Framing length-prefixed
- [x] Versioning protocollo

### Fase 5: Documentazione e CI ✅
- [x] Test automatizzati
- [x] CI multi-OS (Ubuntu, macOS, Windows)
- [x] Clippy + rustfmt
- [x] WHITEPAPER.md allineato al codice
- [x] HOST_API.md
- [x] Esempi WASM (distributed_counter, distributed_chat)

> ⚠️ Nota: gli esempi di Fase 5 funzionavano solo localmente; la convergenza
> end-to-end tra peer è stata effettivamente cablata in Fase 6.5.

---

## Fasi Production-Ready

### Fase 6: Transport Security 🔒 ✅
**Obiettivo**: Comunicazioni sicure e autenticate tra nodi.

**TLS Base:**
- [x] TLS 1.3 per connessioni TCP
- [x] Certificati auto-generati per sviluppo (`rcgen`)
- [x] Supporto certificati custom per produzione
- [x] Forward secrecy (ECDHE automatico con TLS 1.3)
- [x] TLS wrapper: `TlsAcceptor` (server), `TlsConnector` (client)

**Mutual TLS (mTLS):**
- [x] Client deve presentare certificato
- [x] Server verifica certificato client
- [x] Supporto CA custom per verifica (`--tls-ca`)
- [x] Test: client senza cert → rifiutato
- [x] Test: client con cert invalido → rifiutato

**Identity & NodeID:**
- [x] NodeID derivato da chiave pubblica: `NodeId = hash(cert.public_key)` (Protocol identity: 16 bytes and Fingerprint/debug: 32 bytes)
- [x] Funzione `derive_node_id_from_cert(cert) -> NodeId`
- [x] Verifica durante handshake Hello: cert.pubkey → NodeId atteso
- [x] Mismatch NodeID → disconnect immediato

**Peer Verification:**
- [x] Verifica hostname/CN nel certificato
- [x] Allowlist opzionale di NodeID autorizzati
- [x] Connessione da NodeID non in lista → rifiutato (se allowlist attiva)

**CLI Flags:**
- [x] `--tls-cert <path>` - Certificato nodo
- [x] `--tls-key <path>` - Chiave privata nodo
- [x] `--tls-ca <path>` - CA per verificare peer
- [x] `--allowed-peers <id1,id2,...>` - Allowlist NodeID
- [x] `--tls-insecure` - Dev only, skip verify (warning)

**Test Security:**
- [x] Test: connessione TLS funziona tra 2 nodi
- [x] Test: connessione rifiutata senza certificato
- [x] Test: connessione rifiutata con cert scaduto/invalido
- [x] Test: mTLS - entrambi i peer autenticati
- [x] Test: NodeID mismatch → disconnect
- [x] Test: peer non in allowlist → rifiutato
- [x] Test: test per nuovi cli flags

**Librerie**: `rustls`, `tokio-rustls`, `rcgen`, `sha2`

**Matrice sicurezza raggiunta:**

| Attacco | Protetto |
|---------|----------|
| Eavesdropping | ✅ TLS |
| Tampering | ✅ TLS |
| Replay | ✅ TLS |
| MITM server | ✅ Cert verify |
| MITM client | ✅ mTLS |
| Rogue node | ✅ Allowlist |
| Spoofed NodeID | ✅ hash(pubkey) |

---

### Fase 6.5: End-to-End Sync Wiring 🔗
**Obiettivo**: chiudere i buchi nascosti tra guest WASM, SyncManager e datastore, in modo che le operazioni replicabili facciano davvero il giro completo tra peer. 
Include la ristrutturazione della host API per separare KV locale e CRDT replicato senza magie per-chiave.

**Runtime async**:
- [x] `Runtime::run_module` diventa `async` e gira dentro un `tokio::Runtime`.
- [x] CLI passa a `#[tokio::main]`; `real_main` diventa async.
- [x] `SyncManager` accessibile come `Arc<Mutex<SyncManager>>` (o handle clonabile).
- [x] `Runtime::start_sync` chiama davvero `SyncManager::start().await`.
- [x] `wasmtime` caricato con `add_to_linker_async` e `run.call_async` per non
      bloccare il tokio runtime durante le host call.

**Host API CRDT (nuove)**:
- [x] `crdt_gcounter_inc(key_ptr, key_len, delta: u64) -> i32`
      applica localmente, materializza il totale in sled ed emette Op via canale.
- [x] `crdt_gcounter_value(key_ptr, key_len, out_ptr, out_cap) -> i32`
      legge il totale corrente dal registry in memoria.
- [x] Wrapper SDK `nx_sdk::crdt::gcounter::{inc, value}`.

**Wiring end-to-end**:
- [x] `HostState` include handle al SyncManager (sender Op + accessor GCounter).
- [x] `apply_remote_op` aggiorna il GCounter **e** riscrive il totale in sled.
- [x] Materializzazione atomica: update GCounter → write sled in una transazione
      logica (anche solo sled batch ok).

**Cleanup del pregresso**:
- [x] Rimuovere flag CLI `--sync-prefix`.
- [x] Aggiornare messaggi di log e help.
- [x] Aggiornare `HOST_API.md` con la separazione `db_*` vs `crdt_*`.

**Examples migration**:
- [x] `examples/distributed_counter`: riscrittura con `nx_sdk::crdt::gcounter`.
- [x] `examples/distributed_chat`: marcato come "non-replicato (LWW locale)"
      o rimosso finché non abbiamo ORSet/RGA (Fase 14).
- [x] `examples/vote_tally_tls`: nuovo esempio con mTLS + allowlist + counter
      CRDT reale tra 3 nodi.

**Test**:
- [x] Test E2E: 2 nodi, A fa `gcounter::inc("visits", 1)`, dopo handshake
      e un round di PushOps B legge `gcounter::value("visits") == 1`.
- [x] Test E2E: 2 nodi, A e B incrementano in parallelo, convergono sullo stesso
      totale.
- [x] Test: nessun Op emesso quando sync è disabilitato.
- [x] Test: `apply_remote_op` idempotente (stesso Op 2x → nessun doppio conteggio).

**Criterio di chiusura**:
```bash
# Terminal A
nx run counter.wasm --listen 127.0.0.1:9000 --datastore-path ./data-a
# Terminal B
nx run counter.wasm --listen 127.0.0.1:9001 --peer 127.0.0.1:9000 \
    --datastore-path ./data-b
# Entrambi i nodi stampano lo stesso valore di gcounter::value("visits")
# entro pochi secondi.
```

> Nota: il wiring interno della Fase 6.5 è coperto dai test E2E su `SyncManager`, inclusi handshake, PushOps, convergenza e materializzazione sled. 
> Il criterio CLI qui sopra non è ancora completamente rispettato alla lettera perché `nx run` oggi esegue il guest una volta e poi termina: non ha ancora un lifecycle/settle mode che lasci tempo stabile a handshake, broadcast e apply remoto tra processi CLI. Inoltre il registry GCounter in memoria non viene ancora ricostruito dai valori materializzati in sled all'avvio. 
>Questi aspetti sono tracciati nella Fase 7.

---

### Fase 7: Graceful Lifecycle 🔄
**Obiettivo**: Shutdown pulito e recovery da crash

- [ ] Modalità long-running robusta per runtime con sync attivo.
- [ ] Hydration all'avvio: ricostruire il registry GCounter dai valori
      materializzati in sled.
- [ ] Modalità di settle per `nx run` con sync attivo: lasciare tempo a
      handshake, PushOps e apply remoto prima dell'exit, oppure sostituirla con
      lifecycle long-running.
- [ ] Smoke test CLI multi-process: due `nx run distributed_counter.wasm`
      convergono e stampano lo stesso valore entro pochi secondi.
- [ ] Signal handling (SIGTERM, SIGINT, SIGHUP)
- [ ] Graceful shutdown: completa ops in flight, chiudi connessioni
- [ ] Flush dello store prima di exit
- [ ] Timeout configurabile per shutdown (default 30s)
- [ ] Test: kill -TERM → nessuna corruzione dati
- [ ] Test: crash → restart → stato consistente

> Questi task completano il criterio CLI rimasto aperto dalla Fase 6.5 e lo
> portano dentro un lifecycle generale: loop di servizio, shutdown signal-aware,
> flush finale e gestione ordinata delle connessioni.

**Criteri**:
```bash
kill -TERM $PID  # Completa operazioni, esce con code 0
```

---

### Fase 8: Backpressure e Limiti ⚡
**Obiettivo**: Stabilità sotto carico

- [ ] Limite connessioni peer (default: 64)
- [ ] Limite ops in coda (default: 10000)
- [ ] Limite dimensione messaggio (default: 16 MiB)
- [ ] Rifiuto graceful quando sovraccarico
- [ ] Timeout lettura/scrittura socket (default: 30s)
- [ ] Test: 1000 connessioni simultanee → no crash

**Configurazione**:
```toml
[limits]
max_peers = 64
max_pending_ops = 10000
max_message_size = "16MiB"
socket_timeout_secs = 30
```

---

### Fase 9: Observability 📊
**Obiettivo**: Visibilità su cosa fa il runtime

**Logging strutturato**:
- [ ] Formato JSON per log
- [ ] Livelli configurabili (trace/debug/info/warn/error)
- [ ] Correlation ID per tracciare operazioni

**Metriche**:
- [ ] `numax_ops_total` - Operazioni processate
- [ ] `numax_peers_connected` - Peer attivi
- [ ] `numax_sync_latency_ms` - Latenza sync
- [ ] `numax_store_keys` - Chiavi nello store
- [ ] `numax_store_bytes` - Bytes usati
- [ ] Endpoint `/metrics` (Prometheus format)

**Health check**:
- [ ] Endpoint `/health` (liveness)
- [ ] Endpoint `/ready` (readiness)

**Librerie**: `tracing`, `tracing-subscriber`, `metrics`, `metrics-exporter-prometheus`

---

### Fase 10: Resilienza Rete 🌐
**Obiettivo**: Funzionamento robusto con rete instabile

- [ ] Reconnect automatico con backoff esponenziale
- [ ] Peer health tracking (mark dead dopo N timeout)
- [ ] Peer rotation (sostituisci peer morti)
- [ ] Anti-entropy periodico (pull ogni N secondi)
- [ ] Deduplicazione ops (bloom filter o set OpId)
- [ ] Test: rete intermittente (packet loss 10%)
- [ ] Test: nodo muore e torna → converge

---

### Fase 11: Serializzazione Dual-Mode 📦
**Obiettivo**: JSON per debug, bincode per produzione

**Motivazione**:
- JSON: leggibile, debuggabile, ispezionabile con tcpdump/wireshark
- bincode: compatto (~50% size), veloce (~10x faster parse)

**Task**:
- [ ] Aggiungere `bincode` a dipendenze
- [ ] Enum `SerializationFormat` con header di 1 byte nel wire
- [ ] Flag CLI `--debug-protocol`
- [ ] Negoziazione formato in Hello/HelloAck
- [ ] Test: roundtrip entrambi i formati
- [ ] Benchmark: JSON vs bincode (size, speed)

**Librerie**: `bincode`, `serde` (già presente)

---

### Fase 12: Host API Estese 🔌
**Obiettivo**: API complete per moduli WASM

**Database**:
- [ ] `db_scan`, `db_exists`, `db_keys`

**Tempo**:
- [ ] `time_now`, `time_monotonic`

**Crypto**:
- [ ] `random_bytes`, `hash_sha256`, `hash_blake3`

**Sistema**:
- [ ] `env_get`, `module_id`, `abort`

**Rete**:
- [ ] `net_node_id`, `net_peers`

**Librerie**: `sha2`, `blake3`, `getrandom`

---

### Fase 13: Test di Carico 🔥
**Obiettivo**: Verificare comportamento sotto stress

**Scenari**:
- [ ] Singolo nodo: 10k ops/sec per 1 ora
- [ ] 3 nodi: 1k ops/sec ciascuno, sync continuo
- [ ] 10 nodi: mesh completo, 100 ops/sec ciascuno
- [ ] Chaos: kill random nodo ogni 60s

**Metriche**: Throughput, latenza p50/p95/p99, RAM, CPU, tempo convergenza.

**Tool**: script custom o `criterion`.

---

### Fase 14: CRDT Completi 🧮
**Obiettivo**: Strutture dati per casi d'uso reali

| CRDT | Descrizione | Priorità |
|------|-------------|----------|
| **PNCounter** | Counter con increment/decrement | Alta |
| **LWW-Register** | Singolo valore, last-writer-wins | Alta |
| **ORSet** | Set con add/remove osservati | Alta |
| **LWW-Map** | Mappa chiave→valore con LWW | Media |
| **RGA** | Replicated Growable Array (liste ordinate) | Bassa |

Per ognuno: implementazione, test proprietà, OpKind, docs, esempio.

---

### Fase 15: Deployment & Docs 📦
**Obiettivo**: Pronto per utenti esterni

- [ ] Binari precompilati (Linux x86_64, ARM64, macOS, Windows)
- [ ] `cargo install numax`
- [ ] Tutorial: "Hello World distribuito in 5 minuti"
- [ ] Tutorial: "Deploy 3+ nodi con mTLS"
- [ ] Guida: configurazione produzione
- [ ] Guida: troubleshooting
- [ ] CHANGELOG.md
- [ ] CONTRIBUTING.md

---

## Riepilogo Fasi

| Fase | Nome | Stato | Priorità |
|------|------|-------|----------|
| 0-5 | Foundation | ✅ | - |
| 6 | Transport Security | ✅ | **P0** |
| 6.5 | End-to-End Sync Wiring | ✅* | **P0** |
| 7 | Graceful Lifecycle | ⏳ | **P0** |
| 8 | Backpressure | ⏳ | **P0** |
| 9 | Observability | ⏳ | **P1** |
| 10 | Resilienza Rete | ⏳ | **P1** |
| 11 | Serializzazione Dual | ⏳ | **P1** |
| 12 | Host API Estese | ⏳ | **P1** |
| 13 | Test di Carico | ⏳ | **P1** |
| 14 | CRDT Completi | ⏳ | **P2** |
| 15 | Deployment & Docs | ⏳ | **P2** |

**Legenda**:
- **P0**: Bloccante per produzione
- **P1**: Necessario per produzione sicura
- **P2**: Necessario per adoption

`✅*`: chiusa per wiring interno e test E2E `SyncManager`; il criterio CLI
letterale resta tracciato in Fase 7 come lifecycle/settle/hydration.

---

## Criteri Release v0.1.0 finale

- [x] Fasi 0-5 complete
- [x] Fase 6 (TLS) completa
- [x] Fase 6.5 (End-to-End Sync) completa
- [ ] Fase 7 (Graceful shutdown) completa
- [ ] Fase 8 (Backpressure) completa
- [ ] Fase 9 (Observability) almeno logging + health
- [ ] Fase 10 (Resilienza) almeno reconnect + dedup
- [ ] Fase 11 (Serializzazione) JSON + bincode funzionanti
- [ ] Fase 12 (Host API) almeno db_scan, time_now, random_bytes
- [ ] Fase 13 (Test carico) almeno scenario 3 nodi 1h
- [ ] Tutti i test passano
- [ ] Nessun warning clippy
- [ ] Documentazione base

---

## Criteri Release v0.1.0-alpha.1

- [x] Fasi 0-5 complete
- [x] Fase 6 (TLS) completa
- [x] Fase 6.5 wiring interno coperto da test E2E `SyncManager`
- [x] Esempi WASM di base presenti
- [x] `cargo test` passa fuori sandbox
- [x] `cargo clippy --all-targets --all-features -- -D warnings` passa
- [x] Limitazioni note documentate in roadmap

---

## 0.2.0:
> coming soon ...
