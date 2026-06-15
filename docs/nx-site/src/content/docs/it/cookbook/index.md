---
title: Cookbook
description: Ricette brevi copia-incolla.
---

Questo cookbook è una prima versione. Contiene ricette piccole, pratiche e copiabili per usare le API Numax più comuni.

Nel tempo verranno aggiunte molte altre ricette. Se vuoi proporne una, una Pull Request è benvenuta: esempi reali, snippet piccoli e casi d'uso concreti sono perfetti per questa sezione.

---

## Loggare dal modulo

Usa `nx_log!` quando vuoi capire cosa sta facendo il guest.

```rust
use nx_sdk::nx_log;

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    nx_log!("module started");
    nx_log!("value = {}", 42);
}
```

---

## Salvare e leggere stato KV locale

`db::*` usa lo store locale del nodo. Non replica tra peer.

```rust
use nx_sdk::{db, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    db::set("app:greeting", b"hello").unwrap();

    match db::get("app:greeting").unwrap() {
        Some(bytes) => nx_log!("value = {}", core::str::from_utf8(&bytes).unwrap_or("?")),
        None => nx_log!("missing"),
    }
}
```

---

## Contatore locale persistente

Pattern classico: leggi, modifica, riscrivi.

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
    let current = db::get(KEY).unwrap().map_or(0, |v| parse_u64(&v));
    let next = current.saturating_add(1);
    let value = nx_sdk::__alloc::format!("{}", next);

    db::set(KEY, value.as_bytes()).unwrap();
    nx_log!("counter = {}", next);
}
```

---

## Scansionare chiavi per prefisso

Usa prefissi stabili per creare piccoli namespace nello store locale.

```rust
use nx_sdk::{db, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    db::set("user:1", b"alice").unwrap();
    db::set("user:2", b"bob").unwrap();
    db::set("session:1", b"active").unwrap();

    for (key, value) in db::scan("user:").unwrap() {
        let key = core::str::from_utf8(&key).unwrap_or("?");
        let value = core::str::from_utf8(&value).unwrap_or("?");
        nx_log!("{} = {}", key, value);
    }
}
```

---

## Incrementare un GCounter replicato

Le API CRDT richiedono sync abilitata sul runtime (`--listen`). Ogni nodo incrementa il proprio slot; i peer convergono quando si scambiano le op.

```rust
use nx_sdk::{crdt::gcounter, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    gcounter::inc("counter:visits", 1).unwrap();
    let value = gcounter::value("counter:visits").unwrap();
    nx_log!("visits = {}", value);
}
```

---

## Stato utente con LWW-Register

Usa un LWW-Register quando vuoi un solo valore corrente per chiave.

```rust
use nx_sdk::{crdt::lww_register, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    lww_register::set("status:user-1", b"online").unwrap();

    if let Some(value) = lww_register::get("status:user-1").unwrap() {
        nx_log!("status = {}", core::str::from_utf8(&value).unwrap_or("?"));
    }
}
```

---

## Tag con ORSet

Usa ORSet per set replicati dove add e remove possono avvenire su nodi diversi.

```rust
use nx_sdk::{crdt::orset, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    orset::add("tags:item-1", "blue").unwrap();
    orset::add("tags:item-1", "urgent").unwrap();

    let tags = orset::elements("tags:item-1").unwrap();
    nx_log!("tags = {:?}", tags);
}
```

---

## Gestire sync disabilitata

Utile se lo stesso modulo può girare sia standalone sia con sync.

```rust
use nx_sdk::{crdt::gcounter, nx_log, NxError};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    match gcounter::inc("counter:visits", 1) {
        Ok(()) => nx_log!("replicated counter updated"),
        Err(NxError::SyncDisabled) => nx_log!("sync disabled, skipping CRDT update"),
        Err(e) => nx_log!("error: {}", e),
    }
}
```

---

## Leggere una variabile runtime

Il runtime espone solo variabili con prefisso `NX_` o `NUMAX_`.

```rust
use nx_sdk::{nx_log, system};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    match system::env_get("NX_REGION").unwrap() {
        Some(bytes) => nx_log!("region = {}", core::str::from_utf8(&bytes).unwrap_or("?")),
        None => nx_log!("NX_REGION not set"),
    }
}
```

---

## Stampare le capability host

Comodo per debug e compatibilità tra runtime e SDK.

```rust
use nx_sdk::{nx_log, system};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    for capability in system::host_capabilities().unwrap() {
        nx_log!("capability: {}", capability);
    }
}
```

---

## Hash SHA-256

Le funzioni crypto vengono eseguite dall'host.

```rust
use nx_sdk::{crypto, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    let digest = crypto::hash_sha256(b"hello numax").unwrap();
    nx_log!("sha256 first byte = {}", digest[0]);
}
```

---

## Correlati

- [Stato KV locale](/numax/it/guides/build-a-kv-store/) - guida completa allo store locale
- [CRDT e stato](/numax/it/concepts/crdt-and-state/) - convergenza e tipi CRDT
- [Debug dei moduli WASM](/numax/it/guides/debugging-wasm-modules/) - log, errori e sync
- [nx-sdk](/numax/it/reference/crates/nx-sdk/) - wrapper guest-side
