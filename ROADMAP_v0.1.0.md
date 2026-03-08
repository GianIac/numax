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
- [x] 38 test automatizzati
- [x] CI multi-OS (Ubuntu, macOS, Windows)
- [x] Clippy + rustfmt
- [x] WHITEPAPER.md allineato al codice
- [x] HOST_API.md
- [x] Esempi WASM (distributed_counter, distributed_chat)

---

## Fasi Production-Ready

### Fase 6: Transport Security 🔒
**Obiettivo**: Comunicazioni sicure e autenticate tra nodi (anti-MITM completo)

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
- [ ] NodeID derivato da chiave pubblica: `NodeId = hash(cert.public_key)`
- [ ] Funzione `derive_node_id_from_cert(cert) -> NodeId`
- [ ] Verifica durante handshake Hello: cert.pubkey → NodeId atteso
- [ ] Mismatch NodeID → disconnect immediato

**Peer Verification:**
- [x] Verifica hostname/CN nel certificato
- [ ] Allowlist opzionale di NodeID autorizzati
- [ ] Connessione da NodeID non in lista → rifiutato (se allowlist attiva)

**CLI Flags:**
- [ ] `--tls-cert <path>` - Certificato nodo
- [ ] `--tls-key <path>` - Chiave privata nodo
- [ ] `--tls-ca <path>` - CA per verificare peer
- [ ] `--allowed-peers <id1,id2,...>` - Allowlist NodeID
- [ ] `--tls-insecure` - Dev only, skip verify (warning)

**Test Security:**
- [x] Test: connessione TLS funziona tra 2 nodi
- [x] Test: connessione rifiutata senza certificato
- [ ] Test: connessione rifiutata con cert scaduto/invalido
- [x] Test: mTLS - entrambi i peer autenticati
- [ ] Test: NodeID mismatch → disconnect
- [ ] Test: peer non in allowlist → rifiutato

**Librerie**: `rustls`, `tokio-rustls`, `rcgen`, `sha2`

**CLI esempio**:
```bash
# Server con mTLS
nx run module.wasm --sync \
    --sync-listen 0.0.0.0:9000 \
    --tls-cert server.pem \
    --tls-key server-key.pem \
    --tls-ca ca.pem

# Client con mTLS
nx run module.wasm --sync \
    --sync-peers 10.0.0.1:9000 \
    --tls-cert client.pem \
    --tls-key client-key.pem \
    --tls-ca ca.pem

# Con allowlist (rete permissioned)
nx run module.wasm --sync \
    --tls-cert node.pem \
    --tls-key node-key.pem \
    --allowed-peers "abc123,def456"

# Dev mode (MAI in produzione!)
nx run module.wasm --sync --tls-insecure
```

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

**Implementazione**:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializationFormat {
    Json,    // Debug/sviluppo
    Bincode, // Produzione
}

impl Message {
    pub fn to_bytes(&self, format: SerializationFormat) -> Result<Vec<u8>> {
        match format {
            SerializationFormat::Json => serde_json::to_vec(self),
            SerializationFormat::Bincode => bincode::serialize(self),
        }
    }
    
    pub fn from_bytes(bytes: &[u8], format: SerializationFormat) -> Result<Self> {
        match format {
            SerializationFormat::Json => serde_json::from_slice(bytes),
            SerializationFormat::Bincode => bincode::deserialize(bytes),
        }
    }
}
```

**Protocollo wire aggiornato**:
```
┌──────────┬──────────────┬─────────────────────────────┐
│ Format   │ Length (4B)  │     Payload                 │
│ (1 byte) │ big-endian   │  (JSON or bincode)          │
│ 0=JSON   │              │                             │
│ 1=Bincode│              │                             │
└──────────┴──────────────┴─────────────────────────────┘
```

**CLI**:
```bash
# Default: bincode (produzione)
nx run module.wasm --sync

# Debug mode: JSON (ispezionabile)
nx run module.wasm --sync --debug-protocol
```

**Task**:
- [ ] Aggiungere `bincode` a dipendenze
- [ ] Enum `SerializationFormat`
- [ ] Byte header per formato
- [ ] Flag CLI `--debug-protocol`
- [ ] Negoziazione formato in Hello/HelloAck
- [ ] Test: roundtrip entrambi i formati
- [ ] Benchmark: JSON vs bincode (size, speed)

**Librerie**: `bincode`, `serde` (già presente)

---

### Fase 12: Host API Estese 🔌
**Obiettivo**: API complete per moduli WASM

**Database**:

| Funzione | Firma | Descrizione |
|----------|-------|-------------|
| `db_scan` | `(prefix_ptr, prefix_len, out_ptr, out_cap) -> i32` | Scansione per prefisso |
| `db_exists` | `(key_ptr, key_len) -> i32` | Verifica esistenza (1=sì, 0=no, <0=errore) |
| `db_keys` | `(prefix_ptr, prefix_len, out_ptr, out_cap) -> i32` | Lista chiavi con prefisso |

**Tempo**:

| Funzione | Firma | Descrizione |
|----------|-------|-------------|
| `time_now` | `() -> i64` | Unix timestamp millisecondi |
| `time_monotonic` | `() -> i64` | Clock monotono (per misurazioni) |

**Crypto**:

| Funzione | Firma | Descrizione |
|----------|-------|-------------|
| `random_bytes` | `(out_ptr, out_len) -> i32` | Bytes casuali sicuri |
| `hash_sha256` | `(data_ptr, data_len, out_ptr) -> i32` | SHA-256 (out: 32 bytes) |
| `hash_blake3` | `(data_ptr, data_len, out_ptr) -> i32` | BLAKE3 (out: 32 bytes) |

**Sistema**:

| Funzione | Firma | Descrizione |
|----------|-------|-------------|
| `env_get` | `(key_ptr, key_len, out_ptr, out_cap) -> i32` | Leggi variabile ambiente |
| `module_id` | `(out_ptr, out_cap) -> i32` | ID modulo corrente |
| `abort` | `(msg_ptr, msg_len) -> !` | Termina con errore |

**Rete (Futuro)**:

| Funzione | Firma | Descrizione |
|----------|-------|-------------|
| `net_node_id` | `(out_ptr, out_cap) -> i32` | Proprio NodeId |
| `net_peers` | `(out_ptr, out_cap) -> i32` | Lista peer connessi (JSON) |

**Task**:
- [ ] `db_scan` - Implementare in nx-core/host_api/db.rs
- [ ] `db_exists` - Implementare (ottimizzazione: no read value)
- [ ] `time_now` - Implementare in nx-core/host_api/time.rs
- [ ] `time_monotonic` - Implementare
- [ ] `random_bytes` - Implementare in nx-core/host_api/crypto.rs
- [ ] `hash_sha256` - Implementare
- [ ] `hash_blake3` - Implementare
- [ ] `env_get` - Implementare in nx-core/host_api/sys.rs
- [ ] `abort` - Implementare
- [ ] Aggiornare nx-sdk con wrapper
- [ ] Aggiornare HOST_API.md
- [ ] Test per ogni funzione

**Librerie**: `sha2`, `blake3`, `getrandom`

---

### Fase 13: Test di Carico 🔥
**Obiettivo**: Verificare comportamento sotto stress

**Scenari**:
- [ ] Singolo nodo: 10k ops/sec per 1 ora
- [ ] 3 nodi: 1k ops/sec ciascuno, sync continuo
- [ ] 10 nodi: mesh completo, 100 ops/sec ciascuno
- [ ] Chaos: kill random nodo ogni 60s

**Metriche**:
- Throughput (ops/sec sustained)
- Latenza p50, p95, p99
- Memoria usata
- CPU usata
- Tempo di convergenza dopo partition

**Tool**: script custom o `criterion` per benchmark

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

**Per ogni CRDT**:
- [ ] Implementazione
- [ ] Test proprietà (commutatività, associatività, idempotenza)
- [ ] OpKind corrispondente
- [ ] Documentazione
- [ ] Esempio d'uso

---

### Fase 15: Deployment & Docs 📦
**Obiettivo**: Pronto per utenti esterni

**Distribuzione**:
- [ ] Binari precompilati (Linux x86_64, ARM64, macOS, Windows)
- [ ] `cargo install numax`

**Documentazione**: (ancora da definire)
- [ ] Tutorial: "Hello World distribuito in 5 minuti"
- [ ] Tutorial: "Deploy 3+ nodi"
- [ ] Guida: configurazione produzione
- [ ] Guida: troubleshooting
- [ ] CHANGELOG.md
- [ ] CONTRIBUTING.md
- [ ] more ...

---

## Riepilogo Fasi

| Fase | Nome | Stato | Priorità |
|------|------|-------|----------|
| 0-5 | Foundation | ✅ | - |
| 6 | Transport Security | ⏳ | **P0** |
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
- [ ] Fase 6 (TLS) completa
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

**Target**: Production-ready per workload non-critici

---

## Dopo v0.1.0:
> coming soon ...
