---
title: Stato KV locale
description: Guida passo-passo allo store key/value locale di Numax.
---

Questa guida ti porta a costruire un modulo WASM che usa lo store key/value locale di Numax. Partiamo da zero: setup del progetto, operazioni base, paginazione, e un esempio reale di contatore persistente. Tutto gira su un singolo nodo, offline, senza sync.

---

## Cosa costruiamo

Alla fine di questa guida avrai:

1. un modulo Rust compilato in `.wasm` che legge e scrive nello store locale
2. capito il pattern di lettura/scrittura/scan della Host API
3. un contatore persistente che sopravvive ai riavvii
4. la base per qualsiasi modulo che usa stato locale

---

## Prerequisiti

- Rust con target `wasm32-unknown-unknown` installato
- `nx` CLI installata ([Installazione](/numax/it/getting-started/installation/))

```bash
rustup target add wasm32-unknown-unknown
```

---

## Step 1 - crea il progetto

```bash
cargo new --lib my_kv_store
cd my_kv_store
```

Modifica `Cargo.toml`:

```toml
[package]
name = "my_kv_store"
version = "0.1.0"
edition = "2024"

[lib]
crate-type = ["cdylib"]

[dependencies]
nx-sdk = { path = "/path/to/numax/crates/nx-sdk" }

[profile.release]
lto = true
opt-level = "z"
codegen-units = 1
panic = "abort"

[workspace]
```

Se hai clonato il repo Numax puoi usare il path relativo. Altrimenti, quando `nx-sdk` sarà su crates.io, sarà sufficiente `nx-sdk = "0.1"`.

---

## Step 2 - scrivi il modulo

Sostituisci il contenuto di `src/lib.rs`:

```rust
use nx_sdk::{db, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    // scrive una coppia chiave/valore
    db::set("hello", b"world").unwrap();
    nx_log!("scritto: hello = world");

    // legge
    match db::get("hello").unwrap() {
        Some(bytes) => {
            let s = core::str::from_utf8(&bytes).unwrap_or("?");
            nx_log!("letto: hello = {}", s);
        }
        None => nx_log!("chiave non trovata"),
    }

    // verifica esistenza
    let exists = db::exists("hello").unwrap();
    nx_log!("exists: {}", exists); // true

    // cancella
    db::delete("hello").unwrap();
    nx_log!("dopo delete: {:?}", db::get("hello").unwrap()); // None
}
```

---

## Step 3 - compila e avvia

```bash
cargo build --target wasm32-unknown-unknown --release

nx run target/wasm32-unknown-unknown/release/my_kv_store.wasm
```

Output atteso:

```
scritto: hello = world
letto: hello = world
exists: true
dopo delete: None
```

Lo store è persistente. Se esegui di nuovo, il modulo riparte da zero perché `delete` ha rimosso la chiave nella run precedente. Se rimuovi il `delete`, alla seconda esecuzione `get("hello")` restituirà `Some(b"world")`.

---

## Step 4 - un contatore persistente

Questo è il pattern più comune: leggere un valore, modificarlo, riscriverlo. Il contatore sopravvive tra una run e l'altra.

```rust
use nx_sdk::{db, nx_log};

const KEY: &str = "counter";

fn parse_u64(bytes: &[u8]) -> u64 {
    core::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    // leggi il valore corrente (0 se non esiste)
    let current = match db::get(KEY).unwrap() {
        Some(v) => parse_u64(&v),
        None => 0,
    };

    // incrementa
    let next = current.saturating_add(1);

    // persisti come stringa ASCII
    let s = nx_sdk::__alloc::format!("{}", next);
    db::set(KEY, s.as_bytes()).unwrap();

    nx_log!("counter = {}", next);
}
```

Esegui più volte:

```bash
nx run target/wasm32-unknown-unknown/release/my_kv_store.wasm
# counter = 1
nx run target/wasm32-unknown-unknown/release/my_kv_store.wasm
# counter = 2
nx run target/wasm32-unknown-unknown/release/my_kv_store.wasm
# counter = 3
```

Il valore è nella directory dati del runtime (default `./nx-data`). Cancellala per azzerare il contatore.

---

## Step 5 - scan per prefisso

Lo store supporta la scansione per prefisso. Utile per record che condividono un namespace.

```rust
use nx_sdk::{db, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    // popola alcune chiavi
    db::set("user:1", b"alice").unwrap();
    db::set("user:2", b"bob").unwrap();
    db::set("user:3", b"carol").unwrap();
    db::set("session:abc", b"active").unwrap();

    // scan completo per prefisso "user:" - restituisce tutto
    let users = db::scan("user:").unwrap();
    nx_log!("utenti trovati: {}", users.len()); // 3

    for (key, value) in &users {
        let k = core::str::from_utf8(key).unwrap_or("?");
        let v = core::str::from_utf8(value).unwrap_or("?");
        nx_log!("  {} = {}", k, v);
    }

    // solo le chiavi
    let keys = db::keys("user:").unwrap();
    nx_log!("chiavi: {:?}", keys.len()); // 3

    // "session:" non appare nello scan di "user:"
    let sessions = db::scan("session:").unwrap();
    nx_log!("sessioni: {}", sessions.len()); // 1
}
```

`db::scan` e `db::keys` paginano automaticamente internamente (page size 64). Per dataset grandi o controllo esplicito della paginazione usa `scan_page_after`:

```rust
// prima pagina
let page = db::scan_page_after("user:", None, 10).unwrap();

// pagina successiva
let last_key = page.last().map(|(k, _)| k.clone());
let page2 = db::scan_page_after("user:", last_key.as_deref(), 10).unwrap();
```

Il cursore evita di rileggere le chiavi già viste. Non è uno snapshot: se modifichi il dataset durante la scansione, le nuove chiavi possono apparire o meno a seconda dell'ordine lessicografico.

---

## Step 6 - chiavi composte

Il pattern più utile per strutturare lo store è usare chiavi composte con separatori. Scegli un separatore che non appare nei tuoi dati (`:` è comune).

```rust
use nx_sdk::{db, nx_log};

fn set_user(id: u32, name: &str) {
    let key = nx_sdk::__alloc::format!("user:{}", id);
    db::set(&key, name.as_bytes()).unwrap();
}

fn get_user(id: u32) -> Option<nx_sdk::__alloc::string::String> {
    let key = nx_sdk::__alloc::format!("user:{}", id);
    db::get(&key)
        .unwrap()
        .map(|b| nx_sdk::__alloc::string::String::from_utf8_lossy(&b).into_owned())
}

fn set_score(game: &str, user_id: u32, score: u64) {
    let key = nx_sdk::__alloc::format!("score:{}:{}", game, user_id);
    let val = nx_sdk::__alloc::format!("{}", score);
    db::set(&key, val.as_bytes()).unwrap();
}

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    set_user(1, "alice");
    set_user(2, "bob");
    set_score("chess", 1, 1500);
    set_score("chess", 2, 1200);

    nx_log!("user 1: {:?}", get_user(1));

    // tutti gli utenti
    let users = db::scan("user:").unwrap();
    nx_log!("utenti totali: {}", users.len());

    // tutti i punteggi di chess
    let scores = db::scan("score:chess:").unwrap();
    nx_log!("punteggi chess: {}", scores.len());
}
```

---

## Errori comuni

**La chiave inizia con `__nx/`.**
Quelle chiavi sono riservate al runtime. Se scrivi `db::set("__nx/something", ...)` ricevi `NxError::ReservedKey`. Usa sempre prefissi propri dell'applicazione.

**Il modulo non ha l'export `run`.**
Se dimentichi `#[unsafe(no_mangle)]` o `pub extern "C"`, Wasmtime non trova l'entry point e il runtime restituisce un errore. Controlla che il nome esportato sia esattamente `run`.

**Il target non è `wasm32-unknown-unknown`.**
`cargo build --release` senza `--target` compila per il tuo sistema operativo, non per WASM. Il file `.wasm` non viene prodotto.

**Dimentichi `[workspace]` nel `Cargo.toml`.**
Se l'esempio è dentro una directory che contiene un workspace Cargo padre, senza `[workspace]` nel `Cargo.toml` del modulo Cargo tenterà di aggiungere il crate al workspace sbagliato. Aggiungi sempre `[workspace]` per isolare il modulo.

---

## Directory dati

Per default il runtime salva lo store in `./nx-data`. Puoi cambiarlo:

```bash
nx run my_kv_store.wasm --datastore-path ./my-data
```

O nel file di configurazione:

```toml
[storage]
datastore_path = "./my-data"
```

Ogni directory store appartiene a un singolo nodo. Non condividere la stessa directory tra più processi Numax in esecuzione contemporaneamente.

---

## Prossimi passi

- Leggi [CRDT e stato](/numax/it/concepts/crdt-and-state/) per capire quando usare stato CRDT replicato invece dello store KV locale
- Guarda l'esempio [`kv_counter`](https://github.com/GianIac/numax/tree/main/examples/kv_counter) nel repository
- Guarda l'esempio [`kv_sdk_roundtrip`](https://github.com/GianIac/numax/tree/main/examples/kv_sdk_roundtrip) per vedere tutte le API dello store in azione
- Leggi [nx-sdk db](/numax/it/reference/crates/nx-sdk/) per il riferimento completo delle funzioni
