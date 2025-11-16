# Numax Roadmap v0.1.0
Versione iniziale. Soggetta a cambiamenti rapidi. Come il codice.

---

## FASE 0 — Bootstrap del Runtime: 
**Obiettivo:** eseguire un modulo WASM. Punto.

- Runtime minimale (`nx-core`)
  - carica e istanzia un modulo
  - supporta `run` oppure `_start`
  - host API: `host_log`
- CLI (`nx-cli`)
  - `nx run file.wasm`
- Esempio funzionante:
  - `hello_wasm`: stampa da guest → host

**Goal finale della fase:**  
`nx run examples/hello_wasm/target/.../hello.wasm` → `"Hello from Numax!"`

---

## FASE 1 — Runtime solido (ma ancora minimale)
**Obiettivo:** passare da “demo” a “runtime vero ma semplice”.

- Introduzione di `RuntimeConfig`
  - WASI on/off
  - limiti (memoria, timeout)
- WASI base tramite Wasmtime
- Errori leggibili (no wall of text)
- Log host coerenti: `[nx-core]`, `[guest]`

**Goal finale:** un modulo WASM può usare WASI semplice (stdout, args).

---

## FASE 2 — Store Locale Integrato
**Obiettivo:** lo stato torna vicino al calcolo.  
Senza store, Numax non è Numax.

- `nx-store` diventa una libreria vera
  - `open(path)`
  - `get/set/delete`
  - `scan_prefix`
- Stato del runtime contiene lo store
- Host API WASM:
  - `db_get`
  - `db_set`
  - `db_delete`
- `nx-sdk` lato guest:
  - wrapper Rust: `db::get("key")`, `db::set("k", b"...")`
- Esempio:
  - `kv_counter`: modulo che legge/incrementa/salva un contatore locale

**Goal finale:**  
Ogni modulo WASM ha un piccolo DB persistente, zero dipendenze esterne.

---

## FASE 3 — SDK + DX
**Obiettivo:** chi sviluppa moduli non deve toccare `extern "C"`.

- `nx-sdk` pubblicabile (ancora sperimentale)
  - `log!()`
  - `db::get/set/delete`
- Pulizia host API e naming
- Esempio:
  - `hello_sdk`: solo `nx-sdk`, nessuna API raw

**Goal finale:**  
Scrivere un modulo Numax = importare `nx-sdk` e andare.

---

## FASE 4 — Sync Distribuito (Prototipo)
**Obiettivo:** iniziare a far parlare due nodi.  
Non “magico”. Non completo. Solo il minimo per dimostrare il concetto.

- `nx-sync`
  - primo CRDT:
    - `GCounter` **oppure** `LWW-Register`
  - serializzazione operazioni
- `nx-net`
  - canale peer-to-peer semplice (TCP/QUIC)
  - scambio periodico di delta
- Integrazione con runtime
  - un prefisso di chiavi può essere replicato
- Esempio:
  - `sync_counter`: due processi convergono allo stesso valore

**Goal finale:**  
Due Numax separati, stesso modulo, stesso stato alla fine.

---

## FASE 5 — Ripulitura, Documenti, Tooling
**Obiettivo:** mettere ordine prima di costruire altro.

- `docs/`
  - WHITEPAPER aggiornato
  - ARCHITECTURE (runtime, store, sync)
  - HOST_API (specifica minima)
- CI:
  - `cargo fmt --check`
  - `cargo clippy -D warnings`
  - test basilari

**Goal finale:**  
Progetto coerente, comprensibile, compilabile ovunque.

---

## NOTE FINALI
- Ogni Fase deve produrre almeno **un esempio** eseguibile “end-to-end”.  
- Le feature difficili (CRDT complessi, gossip avanzato, permessi granulari, mobile/browser) arrivano con la versione 0.2.0 DOPO che l'asse principale funziona.  

Numax non cresce in larghezza ma in profondità.  
Si costruisce un pezzo utile per volta, completamente.

Fine roadmap. Torna al codice.
