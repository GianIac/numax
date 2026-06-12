---
title: nx-sdk
description: SDK guest per moduli WASM.
---

`nx-sdk` è la libreria che aggiungi al tuo modulo WASM per chiamare le funzioni host di Numax.
Gira dentro il binario `.wasm`, non dentro il runtime. È l'unico crate nel workspace
che ha come target `wasm32-unknown-unknown` e non ha dipendenze interne al workspace.

Avvolge ogni import FFI raw da `ffi.rs` in funzioni Rust sicure ed ergonomiche.
Non chiami mai `unsafe` direttamente dal codice del tuo modulo.

---

## Cosa non è

`nx-sdk` non ha async, networking, OS, filesystem. È `no_std` su wasm32.
La feature `std` esiste ma è disabilitata per default. L'unica cosa esterna che tocca
è l'host Numax tramite gli import WASM dal namespace `nx` in `ffi.rs`.

---

## Come aggiungerlo al tuo modulo

```toml
# Cargo.toml dentro il tuo crate modulo
[lib]
crate-type = ["cdylib"]

[dependencies]
nx-sdk = { path = "../crates/nx-sdk" }

[profile.release]
lto = true
opt-level = "z"
codegen-units = 1
panic = "abort"
```

L'entry point che Numax cerca:

```rust
#[unsafe(no_mangle)]
pub extern "C" fn run() {
    // logica del tuo modulo qui
}
```

---

## Layout modulo

```
src/
  lib.rs          re-export API pubblica, cfg no_std, re-export __alloc
  ffi.rs          import unsafe extern "C" raw dal namespace nx (privato)
  error.rs        enum NxError, alias Result<T>
  log.rs          log(), macro nx_log!
  db.rs           key/value store locale
  crypto.rs       random_bytes, hash_sha256, hash_blake3
  time.rs         now(), monotonic()
  net.rs          node_id(), peers()
  system.rs       env_get(), module_id(), host_capabilities(), event_emit(), abort()
  crdt/
    mod.rs
    gcounter.rs   inc(), value()
    pncounter.rs  inc(), dec(), value()
    lww_register.rs set(), get()
    lww_map.rs    set(), remove(), get(), contains(), entries()
    orset.rs      add(), remove(), contains(), elements()
    rga.rs        insert_after(), delete(), values()
```

`ffi.rs` è privato (`mod ffi` in `lib.rs`). Tutti gli altri moduli sono `pub mod`.

---

## Gestione errori

Ogni funzione SDK fallibile restituisce `Result<T>` che è `core::result::Result<T, NxError>`.

```rust
pub enum NxError {
    Internal,           // host ha restituito -3
    BufferTooSmall,     // cap di retry buffer superato
    NotFound,           // host ha restituito -1
    ReservedKey,        // chiave sotto il prefisso __nx/
    SyncDisabled,       // API richiede --listen, sync è off
    UnknownCode(i32),   // codice negativo inatteso
}
```

Il loop di retry buffer-too-small è gestito dentro l'SDK. Non lo vedi mai.
Quando l'host scrive più byte di quanti il buffer corrente ne tenga, l'SDK raddoppia il
buffer e riprova automaticamente fino a un cap (`MAX_SCAN_BUFFER = 1 MiB` per scan/keys,
`MAX_NET_BUFFER = 1 MiB` per net).

---

## log

```rust
use nx_sdk::log;
use nx_sdk::nx_log;

log("modulo avviato");
nx_log!("valore = {}", 42);
nx_log!("gira su Numax v{}", env!("CARGO_PKG_VERSION"));
```

`log(s)` chiama `host_log_v2`. È best-effort: se l'host restituisce un errore, la chiamata
viene ignorata silenziosamente. Usala liberamente.

`nx_log!` è una macro che formatta una stringa via `alloc::format!` e chiama `log`.
Funziona esattamente come `println!` ma passa per l'host.

---

## db

Key/value store locale. Non replicato. Ogni nodo ha la propria copia.

```rust
use nx_sdk::db;

// scrivi
db::set("user:1", b"alice")?;

// leggi
match db::get("user:1")? {
    Some(bytes) => { /* ... */ }
    None        => { /* chiave non trovata */ }
}

// verifica esistenza
if db::exists("user:1")? { /* ... */ }

// cancella
db::delete("user:1")?;

// scan completo per prefisso (paginato internamente, restituisce tutto)
let rows: Vec<(Vec<u8>, Vec<u8>)> = db::scan("user:")?;

// solo chiavi
let keys: Vec<Vec<u8>> = db::keys("user:")?;

// paginazione manuale con cursore offset (solo compatibilità - preferisci scan_page_after)
let page = db::scan_page("user:", 0, 64)?;

// paginazione manuale con cursore chiave (preferita per spazi di chiavi grandi)
let page = db::scan_page_after("user:", None, 64)?;           // prima pagina
let page = db::scan_page_after("user:", Some(last_key), 64)?; // pagina successiva

let kpage = db::keys_page("user:", 0, 64)?;
let kpage = db::keys_page_after("user:", None, 64)?;
```

`scan` e `keys` paginano automaticamente usando `scan_page_after` e `keys_page_after`
internamente con una page size di 64. Usali quando vuoi tutti i risultati in una volta.
Usa `scan_page_after` / `keys_page_after` direttamente quando vuoi paginazione esplicita.

Le chiavi sotto il prefisso `__nx/` sono riservate dal runtime. Accedervi restituisce `NxError::ReservedKey`.

---

## crdt

Operazioni CRDT. Richiedono che la sync sia abilitata (`--listen`). Restituiscono `NxError::SyncDisabled` altrimenti.

### gcounter

Contatore grow-only. Usa per totali che solo aumentano.

```rust
use nx_sdk::crdt::gcounter;

gcounter::inc("counter:visits", 1)?;
let total: u64 = gcounter::value("counter:visits")?;
```

### pncounter

Contatore positivo/negativo. Usa per stock, saldi, qualsiasi cosa si muova in entrambe le direzioni.

```rust
use nx_sdk::crdt::pncounter;

pncounter::inc("inventory:sku-1", 10)?;
pncounter::dec("inventory:sku-1", 3)?;
let available: i64 = pncounter::value("inventory:sku-1")?;
```

### lww_register

Registro last-writer-wins. Memorizza un singolo valore byte per chiave. L'ultimo timestamp vince.

```rust
use nx_sdk::crdt::lww_register;

lww_register::set("status:user-1", b"online")?;
let status: Option<Vec<u8>> = lww_register::get("status:user-1")?;
```

`get` restituisce `None` quando il registro non è mai stato impostato.

### lww_map

Mappa dove ogni campo è un LWW-register indipendente. Le rimozioni sono tombstonate.

```rust
use nx_sdk::crdt::lww_map;

lww_map::set("settings:svc-a", "theme", b"dark")?;
lww_map::remove("settings:svc-a", "region")?;
let val: Option<Vec<u8>>       = lww_map::get("settings:svc-a", "theme")?;
let exists: bool                = lww_map::contains("settings:svc-a", "theme")?;
let all: Vec<(String, Vec<u8>)> = lww_map::entries("settings:svc-a")?;
```

`entries` restituisce solo i campi visibili (non tombstoned).

### orset

Set observed-remove di stringhe. Gli add concorrenti non osservati da una remove restano visibili.

```rust
use nx_sdk::crdt::orset;

orset::add("tags:item-1", "blue")?;
orset::remove("tags:item-1", "blue")?;
let has_blue: bool        = orset::contains("tags:item-1", "blue")?;
let all_tags: Vec<String> = orset::elements("tags:item-1")?;
```

### rga

Sequenza ordinata di valori byte. Gli insert generano id stabili. I delete tombstonano per id.

```rust
use nx_sdk::crdt::rga;

let id       = rga::insert_after("comments:doc-1", None, b"primo commento")?;
let reply_id = rga::insert_after("comments:doc-1", Some(&id), b"risposta")?;
rga::delete("comments:doc-1", &reply_id)?;
let visible: Vec<Vec<u8>> = rga::values("comments:doc-1")?;
```

`insert_after(key, parent_id, value)`:
- `parent_id = None` inserisce in testa.
- `parent_id = Some(id)` inserisce dopo l'elemento con quell'id.
- Restituisce l'id del nuovo elemento come `String`.

---

## time

```rust
use nx_sdk::time;

let now_ms:     u64 = time::now();        // timestamp Unix in ms
let elapsed_ms: u64 = time::monotonic();  // ms monotoni dall'avvio del runtime
```

`now()` usa il wall clock dell'host. `monotonic()` è adatto per misurare il tempo trascorso,
non per timestamp persistiti.

---

## crypto

```rust
use nx_sdk::crypto;

let nonce: Vec<u8> = crypto::random_bytes(16)?;
let sha: [u8; 32]  = crypto::hash_sha256(b"payload")?;
let b3: [u8; 32]   = crypto::hash_blake3(b"payload")?;
```

`random_bytes(n)` riempie un buffer con `n` byte casuali crittograficamente sicuri dall'host.
Massimo: 1 MiB per chiamata.

---

## net

Richiede che la sync sia abilitata. Restituisce `NxError::SyncDisabled` altrimenti.

```rust
use nx_sdk::net;

let id: String          = net::node_id()?;
let peers: Vec<net::Peer> = net::peers()?;

for peer in &peers {
    nx_log!("peer addr={} node_id={}", peer.addr, peer.node_id);
}
```

`Peer` ha due campi: `addr: String` e `node_id: String`.

---

## system

```rust
use nx_sdk::system;

// Leggi una variabile NX_* o NUMAX_* dall'host
let val: Option<Vec<u8>> = system::env_get("NX_MY_VAR")?;

// Identificatore del modulo impostato dal runtime
let id: String = system::module_id()?;

// Lista capability host disponibili (separate da newline)
let caps: Vec<String> = system::host_capabilities()?;

// Emetti un evento con nome al runtime
system::event_emit("my.event", b"payload")?;

// Termina con un messaggio visibile nel log host
system::abort("qualcosa è andato storto");
```

`abort` è `-> !`. Chiama `ffi::abort` e poi gira in loop su `core::hint::spin_loop()`.
L'host converte la chiamata FFI in un trap Wasmtime che termina il guest.

---

## ffi.rs

`ffi.rs` è l'unico file che dichiara gli import host raw unsafe. I moduli wrapper pubblici contengono
i call site unsafe e traducono i codici di ritorno host in valori `Result`:

```rust
#[link(wasm_import_module = "nx")]
unsafe extern "C" {
    pub fn db_get(key_ptr: u32, key_len: u32, out_ptr: u32, out_cap: u32) -> i32;
    pub fn db_set(key_ptr: u32, key_len: u32, val_ptr: u32, val_len: u32) -> i32;
    // ...
    pub fn host_log_v2(msg_ptr: u32, msg_len: u32) -> i32;
    pub fn host_log(msg_ptr: u32, msg_len: u32);  // legacy, mantenuto per compatibilità
}
```

`host_log` è mantenuto per retrocompatibilità con esempi guest più vecchi. Il nuovo codice usa
`host_log_v2` tramite `log()`.

Tutti gli argomenti puntatore sono `u32` (offset nella memoria lineare WASM). L'SDK gestisce
il casting dai riferimenti Rust. La convenzione è: stringhe e slice come coppie `(ptr, len)`,
buffer di output come `(out_ptr, out_cap)`.

---

## Come aggiungere un nuovo wrapper SDK (guida per developer)

1. Aggiungi la dichiarazione FFI raw a `ffi.rs`.
2. Aggiungi il wrapper sicuro nel modulo appropriato (`db.rs`, `crypto.rs`, ecc.).
3. Gestisci tutti i codici di ritorno: `0` per successo, codici negativi a varianti `NxError`.
4. Se l'output è a lunghezza variabile, usa il pattern retry con buffer raddoppiato da `db.rs`/`net.rs`.
5. Re-esporta da `lib.rs` se appartiene all'API pubblica top-level.
6. Aggiungi l'implementazione host in `nx-core/src/host_api/`.
7. Registrala in `nx-core/src/runtime.rs` tramite `add_to_linker`.

---

## no_std e alloc

`lib.rs` inizia con:

```rust
#![cfg_attr(target_arch = "wasm32", no_std)]

pub extern crate alloc as __alloc;
```

Su `wasm32`, il crate è `no_std` e usa `alloc` per `Vec`, `String`, `format!`.
Su nativo (es. per unit test o doc examples), `std` è disponibile normalmente.

`__alloc` è re-esportato così macro come `nx_log!` possono riferirsi a `$crate::__alloc::format!`
senza assumere se il consumatore ha `std` o meno.

---

## Correlati

Leggi questa pagina insieme ai docs host/runtime:

- [Host API](/numax/it/reference/host-api/) - le funzioni host che questo SDK avvolge
- [Il tuo primo modulo](/numax/it/getting-started/your-first-module/) - esempio end-to-end con l'SDK
- [Crate nx-core](/numax/it/reference/crates/nx-core/) - implementa le funzioni dichiarate in `ffi.rs`
- [Panoramica crate](/numax/it/reference/crates/) - dove `nx-sdk` si inserisce nello stack
