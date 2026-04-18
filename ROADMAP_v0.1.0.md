# Roadmap Numax v0.1.0

> **Obiettivo**: Runtime production-ready per workload non-critici
> **Stato**: In sviluppo

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
**Obiettivo**: chiudere i buchi nascosti tra guest WASM, SyncManager e
datastore, in modo che le operazioni replicabili facciano davvero il giro
completo tra peer. Include la ristrutturazione della host API per separare
KV locale e CRDT replicato senza magie per-chiave.

**Problema pre-6.5**:
1. `Runtime::start_sync` era uno stub, non avviava il listener.
2. `db_set` non emetteva Op verso il SyncManager.
3. `apply_remote_op` aggiornava solo lo stato in-memory, non toccava sled.
4. Semantica ambigua: il guest scriveva "valore assoluto" via `db_set`,
   la rete propagava "increment" → nessuna traduzione coerente.

**Design deciso**:
- **API separate nel SDK**:
  - `nx_sdk::db` → KV locale non-replicato (esistente).
  - `nx_sdk::crdt::gcounter` → counter CRDT replicato (nuovo).
- **Replicazione guidata dall'intent, non dalla chiave**:
  - Chi chiama `crdt::gcounter::inc(key)` sa che è replicato.
  - Chi chiama `db::set(key, value)` sa che è locale.
  - `--sync-prefix` e `SyncConfig::replicated_prefixes` vengono rimossi.
- **Namespace riservato nello sled**:
  - Lo stato materializzato dei CRDT vive sotto `__nx/crdt/gcounter/<key>`.
  - Non collide con le chiavi KV del guest.

**Runtime async**:
- [ ] `Runtime::run_module` diventa `async` e gira dentro un `tokio::Runtime`.
- [ ] CLI passa a `#[tokio::main]`; `real_main` diventa async.
- [ ] `SyncManager` accessibile come `Arc<Mutex<SyncManager>>` (o handle clonabile).
- [ ] `Runtime::start_sync` chiama davvero `SyncManager::start().await`.
- [ ] `wasmtime` caricato con `add_to_linker_async` e `run.call_async` per non
      bloccare il tokio runtime durante le host call.

**Host API CRDT (nuove)**:
- [ ] `crdt_gcounter_inc(key_ptr, key_len, delta: u64) -> i32`
      applica localmente, scrive totale in sled, emette Op via canale.
- [ ] `crdt_gcounter_value(key_ptr, key_len, out_ptr, out_cap) -> i32`
      legge il totale materializzato in sled.
- [ ] Wrapper SDK `nx_sdk::crdt::gcounter::{inc, value}`.

**Wiring end-to-end**:
- [ ] `HostState` include handle al SyncManager (sender Op + accessor GCounter).
- [ ] `apply_remote_op` aggiorna il GCounter **e** riscrive il totale in sled.
- [ ] Materializzazione atomica: update GCounter → write sled in una transazione
      logica (anche solo sled batch ok).

**Cleanup del pregresso**:
- [ ] Rimuovere `SyncConfig::replicated_prefixes` + `with_prefix` + `is_replicated`.
- [ ] Rimuovere flag CLI `--sync-prefix`.
- [ ] Aggiornare messaggi di log e help.
- [ ] Aggiornare `HOST_API.md` con la separazione `db_*` vs `crdt_*`.

**Examples migration**:
- [ ] `examples/distributed_counter`: riscrittura con `nx_sdk::crdt::gcounter`.
- [ ] `examples/distributed_chat`: marcato come "non-replicato (LWW locale)"
      o rimosso finché non abbiamo ORSet/RGA (Fase 14).
- [ ] `examples/vote_tally_tls`: nuovo esempio con mTLS + allowlist + counter
      CRDT reale tra 3 nodi.

**Test**:
- [ ] Test E2E: 2 nodi, A fa `gcounter::inc("visits", 1)`, dopo handshake
      e un round di PushOps B legge `gcounter::value("visits") == 1`.
- [ ] Test E2E: 2 nodi, A e B incrementano in parallelo, convergono sullo stesso
      totale.
- [ ] Test: nessun Op emesso quando sync è disabilitato.
- [ ] Test: `apply_remote_op` idempotente (stesso Op 2x → nessun doppio conteggio).

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

---

### Fase 7: Graceful Lifecycle 🔄
**Obiettivo**: Shutdown pulito e recovery da crash

- [ ] Signal handling (SIGTERM, SIGINT, SIGHUP)
- [ ] Graceful shutdown: completa ops in flight, chiudi connessioni
- [ ] Flush dello store prima di exit
- [ ] Timeout configurabile per shutdown (default 30s)
- [ ] Test: kill -TERM → nessuna corruzione dati
- [ ] Test: crash → restart → stato consistente

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
| 6.5 | End-to-End Sync Wiring | ⏳ | **P0** |
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

---

## Criteri Release v0.1.0

- [x] Fasi 0-5 complete
- [x] Fase 6 (TLS) completa
- [ ] Fase 6.5 (End-to-End Sync) completa
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

## 0.2.0:
> coming soon ...
