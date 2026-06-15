---
title: Debug dei moduli WASM
description: Debug dell'esecuzione del modulo, chiamate Host API e comportamento della sync.
---

Questa guida copre gli strumenti e le tecniche per capire cosa sta facendo un modulo Numax, diagnosticare errori, ispezionare lo stato CRDT e osservare il comportamento della sync. Non c'è un debugger simbolico che si attacca al modulo WASM, ma il runtime espone abbastanza da rendere ogni problema diagnosticabile.

---

## Logging dal modulo

Il primo strumento è `nx_log!`. Ogni stringa che il modulo manda all'host finisce nel log del runtime.

```rust
use nx_sdk::{db, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    nx_log!("modulo avviato");

    match db::get("my_key").unwrap() {
        Some(v) => nx_log!("trovato: {:?}", v),
        None    => nx_log!("chiave non trovata"),
    }

    nx_log!("modulo terminato");
}
```

Di default il runtime stampa a `stderr` in formato testo. Per aumentare la verbosità:

```bash
nx run my_module.wasm --log-level debug
nx run my_module.wasm --log-level trace   # massima verbosità, include internals runtime
```

Per output strutturato (utile per grep, jq, log aggregators):

```bash
nx run my_module.wasm --log-level debug --log-format json
```

Il flag `-v` / `--verbose` è una scorciatoia per `--log-level debug`:

```bash
nx run my_module.wasm -v
```

**Precedenza log level:** CLI flag → variabile d'ambiente `NX_LOG_LEVEL` → file config → default (`info`).

---

## Leggere i codici di errore

Ogni funzione Host API restituisce un `i32`. L'SDK converte i codici negativi in `NxError`, ma se stai costruendo wrapper custom o debuggando comportamenti inattesi, questi sono i codici:

| Codice | Costante | Significato |
|---|---|---|
| `>= 0` | — | successo, valore = byte scritti |
| `-1` | `ERR_NOT_FOUND` | chiave non trovata |
| `-2` | `ERR_BUFFER_TOO_SMALL` | buffer troppo piccolo, l'SDK riprova automaticamente |
| `-3` | `ERR_INTERNAL` | errore interno runtime |
| `-4` | `ERR_RESERVED_KEY` | chiave nel prefisso riservato `__nx/` |
| `-5` | `ERR_SYNC_DISABLED` | operazione CRDT richiesta ma sync non abilitata |

Se vedi `ERR_INTERNAL` nei log, il runtime ha già stampato il dettaglio dell'errore su `stderr`. Cerca righe con `[nx-core]`.

---

## Diagnosticare errori comuni

### Il modulo non si avvia

```
[nx-cli] error: No entrypoint found (expected `run` or `_start`)
```

Il modulo non esporta `run`. Verifica:

```rust
// deve essere esattamente così
#[unsafe(no_mangle)]
pub extern "C" fn run() { ... }
```

E che `Cargo.toml` abbia:
```toml
[lib]
crate-type = ["cdylib"]
```

### Errore di link all'avvio

```
[nx-cli] error: import of `nx::something` was not found
```

Il modulo importa una funzione host che non esiste nel namespace `nx`. Controlla che stai usando una versione di `nx-sdk` compatibile con la versione del runtime.

Per vedere le capability disponibili direttamente dal modulo:

```rust
let caps = nx_sdk::system::host_capabilities().unwrap();
for cap in &caps {
    nx_log!("{}", cap);
}
```

### `ERR_RESERVED_KEY` su chiavi db

Il prefisso `__nx/` è riservato al runtime. Se una tua chiave inizia con quel prefisso, cambiala. Esempio corretto:

```rust
db::set("app:config:theme", b"dark").unwrap();  // ok
db::set("__nx/config", b"dark").unwrap();        // ERR_RESERVED_KEY
```

### `ERR_SYNC_DISABLED` sulle chiamate CRDT

Le funzioni CRDT richiedono che la sync sia abilitata con `--listen`. Se il modulo chiama `crdt_gcounter_inc` su un nodo standalone restituisce `-5`. Soluzione:

```bash
nx run my_module.wasm --listen 0.0.0.0:9000
```

Oppure, se vuoi gestire entrambi i casi nel modulo:

```rust
use nx_sdk::{crdt::gcounter, NxError};

match gcounter::inc("counter:visits", 1) {
    Ok(()) => {}
    Err(NxError::SyncDisabled) => nx_log!("sync non abilitata, skip CRDT"),
    Err(e) => nx_log!("errore: {}", e),
}
```

### Il modulo chiama `abort`

```
[nx-cli] error: guest abort: something went wrong
```

Il modulo ha chiamato `system::abort("something went wrong")`. Il runtime ha terminato il guest e riportato il messaggio. Cerca nel codice del modulo dove viene chiamato `abort` e quale condizione lo ha triggerato.

---

## Ispezionare lo stato CRDT dopo l'esecuzione

Il CLI ha flag `--print-*` che stampano il valore corrente di un CRDT dopo che il modulo è terminato e la finestra di sync è chiusa. Richiedono tutti `--listen`.

```bash
# GCounter
nx run my_module.wasm \
  --listen 0.0.0.0:9000 \
  --settle-for 2s \
  --print-gcounter counter:visits

# PNCounter
nx run my_module.wasm \
  --listen 0.0.0.0:9000 \
  --settle-for 2s \
  --print-pncounter inventory:sku-1

# LWW-Register
nx run my_module.wasm \
  --listen 0.0.0.0:9000 \
  --settle-for 2s \
  --print-lww-register status:user-1

# LWW-Map
nx run my_module.wasm \
  --listen 0.0.0.0:9000 \
  --settle-for 2s \
  --print-lww-map settings:svc-a

# ORSet
nx run my_module.wasm \
  --listen 0.0.0.0:9000 \
  --settle-for 2s \
  --print-orset tags:item-1

# RGA
nx run my_module.wasm \
  --listen 0.0.0.0:9000 \
  --settle-for 2s \
  --print-rga comments:doc-1
```

Output di esempio:

```
counter:visits = 42
inventory:sku-1 = 7
status:user-1 = online
settings:svc-a = {theme=dark, region=eu}
tags:item-1 = [blue, red]
comments:doc-1 = [primo commento, risposta]
```

---

## Ispezionare il protocollo di sync

Per rendere leggibili i messaggi wire tra nodi usa `--debug-protocol`. Questo fa usare JSON invece di bincode come formato di serializzazione:

```bash
# Nodo A
nx run my_module.wasm \
  --listen 0.0.0.0:9000 \
  --debug-protocol \
  --log-level debug

# Nodo B
nx run my_module.wasm \
  --listen 0.0.0.0:9001 \
  --peer 127.0.0.1:9000 \
  --debug-protocol \
  --log-level debug
```

Con `--log-level trace` il runtime logga ogni messaggio di rete ricevuto e inviato. I messaggi JSON sono leggibili direttamente nella console.

**Nota:** `--debug-protocol` non è compatibile con nodi che usano bincode. In un cluster misto usa lo stesso formato su tutti i nodi.

---

## Endpoint di osservabilità

Per un nodo a lunga esecuzione (`serve()` o con `--listen` senza `--settle-for`), abilita l'endpoint HTTP:

```bash
nx run my_module.wasm \
  --listen 0.0.0.0:9000 \
  --observability-listen 127.0.0.1:9100
```

Tre endpoint disponibili:

```bash
# liveness
curl http://127.0.0.1:9100/health
# -> ok

# readiness (503 finché il runtime non è pronto)
curl http://127.0.0.1:9100/ready
# -> ready

# metriche Prometheus
curl http://127.0.0.1:9100/metrics
```

Output metriche:

```
# HELP numax_ops_total Operations processed
# TYPE numax_ops_total counter
numax_ops_total 42
# HELP numax_peers_connected Active peers
# TYPE numax_peers_connected gauge
numax_peers_connected 2
# HELP numax_sync_latency_ms Last sync latency in milliseconds
numax_sync_latency_ms 3
# HELP numax_sync_errors_total Sync errors
numax_sync_errors_total 0
numax_peer_connects_total 5
numax_peer_disconnects_total 1
numax_broadcast_batches_total 12
numax_broadcast_ops_total 48
# HELP numax_store_keys Keys in the local store
numax_store_keys 156
# HELP numax_store_bytes Bytes used by local store keys and values
numax_store_bytes 8192
```

Le metriche sono in formato compatibile con Prometheus. Puoi scraperarle con un Prometheus locale e visualizzarle in Grafana.

---

## Ispezionare la configurazione effettiva

Prima di avviare, verifica che la configurazione sia quella che pensi:

```bash
# genera un file commentato con tutti i valori default
nx config init --output numax.toml

# valida un file esistente senza eseguire nulla
nx config validate --config numax.toml

# mostra la configurazione effettiva dopo aver applicato CLI + env + file + default
nx config show --config numax.toml --effective
```

La precedenza è: **CLI flags > variabili d'ambiente `NX_*` > file TOML > default**. Se un valore non è quello che ti aspetti, `config show --effective` ti dice esattamente quale sorgente ha vinto.

---

## Testare il comportamento di convergenza con nodi bounded

Per testare che due nodi convergano allo stesso stato senza tenerli in esecuzione indefinitamente:

```bash
# Nodo A - genera un incremento e aspetta che si propaghi
nx run my_module.wasm \
  --listen 0.0.0.0:9000 \
  --peer 127.0.0.1:9001 \
  --wait-before-run 500ms \
  --settle-for 2s \
  --print-gcounter counter:visits \
  --datastore-path ./node-a-data

# Nodo B - riceve e converge
nx run my_module.wasm \
  --listen 0.0.0.0:9001 \
  --peer 127.0.0.1:9000 \
  --wait-before-run 500ms \
  --settle-for 2s \
  --print-gcounter counter:visits \
  --datastore-path ./node-b-data
```

- `--wait-before-run` aspetta che i peer si connettano prima di eseguire il modulo
- `--settle-for` mantiene la sync attiva dopo l'esecuzione per propagare le op
- entrambi i nodi devono stampare lo stesso valore di `counter:visits` dopo la convergenza

Se i valori divergono, `--log-level debug` ti mostra quali op sono state ricevute e applicate su ciascun nodo.

---

## Checklist debug rapida

| Sintomo | Prima cosa da controllare |
|---|---|
| Modulo non si avvia | `#[unsafe(no_mangle)] pub extern "C" fn run()` presente? |
| `ERR_INTERNAL` nei log | Cerca `[nx-core]` in stderr |
| `ERR_RESERVED_KEY` | La chiave inizia con `__nx/`? |
| `ERR_SYNC_DISABLED` | Hai passato `--listen`? |
| CRDT non converge | `--log-level trace` su entrambi i nodi |
| Configurazione inattesa | `nx config show --effective` |
| Wire protocol illeggibile | Aggiungi `--debug-protocol` |
| Metriche non disponibili | Hai passato `--observability-listen`? |

---

## Correlati

- [Esecuzione WASM](/it/concepts/wasm-execution/) - sandbox, entry point e HostState
- [CRDT e stato](/it/concepts/crdt-and-state/) - come le op vengono applicate e propagate
- [Observability](/it/guides/observability/) - setup completo di metriche e health check
- [CLI reference](/it/reference/cli/) - tutti i flag disponibili