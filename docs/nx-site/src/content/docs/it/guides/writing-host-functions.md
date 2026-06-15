---
title: Scrivere host function
description: Estendere il runtime con nuove API host.
---

Questa guida spiega come aggiungere una nuova host function a Numax: una funzione che il runtime espone ai moduli guest WASM sotto il namespace `nx`.

È una guida avanzata per chi vuole aggiungere una nuova API host al runtime e poi renderla comoda da usare dal modulo guest tramite `nx-sdk`. Una host function vive in `nx-core`; l'SDK espone solo il wrapper guest-side. Per seguirla serve quindi un clone locale del repository e modifiche sia a `nx-core` sia a `nx-sdk`.

Il pattern mostrato qui è quello più comune in Numax: una funzione che legge byte dalla memoria guest, scrive byte in un buffer guest e restituisce un codice `i32`.

---

## Come funzionano le host function

Quando un modulo WASM importa `nx::my_function`, Wasmtime cerca `my_function` nel linker registrato per il namespace `nx`. Se la trova, chiama la closure Rust registrata nel runtime.

Per le funzioni che scambiano dati dinamici, il percorso tipico è:

```
guest: nx_sdk::text::upper(input)
  │
  ├── nx-sdk/src/ffi.rs
  │   unsafe extern "C" { fn string_upper(ptr, len, out_ptr, out_cap) -> i32; }
  │
  ├── memoria lineare WASM
  │
  └── nx-core/src/host_api/text.rs
      string_upper_impl(caller, ptr, len, out_ptr, out_cap) -> i32
          legge input dalla memoria guest
          esegue il lavoro lato host
          scrive output nella memoria guest
          restituisce byte count o codice errore
```

Non tutte le host function hanno questa forma. `time_now()` e `time_monotonic()` restituiscono `u64`, `host_log` legacy restituisce `()`, e `abort` genera un trap Wasmtime. La forma byte-in/byte-out però è quella giusta per la maggior parte delle API che devono passare stringhe, liste o payload binari.

---

## Step 1 - Scegli dove metterla

Le host function sono raggruppate per responsabilità in `crates/nx-core/src/host_api/`:

| File | Contiene |
|---|---|
| `db.rs` | operazioni database |
| `crdt.rs` | operazioni CRDT |
| `system.rs` | env, module id, capabilities, abort, events |
| `time.rs` | `time_now`, `time_monotonic` |
| `crypto.rs` | `random_bytes`, `hash_sha256`, `hash_blake3` |
| `log.rs` | `host_log`, `host_log_v2` |
| `net.rs` | `net_node_id`, `net_peers` |

Aggiungi un nuovo file se la funzione introduce una nuova responsabilità. Aggiungi a un file esistente se si adatta naturalmente.

Per questa guida aggiungiamo una funzione minimale: `nx::string_upper`, che prende una stringa UTF-8 dalla memoria guest e la riscrive in maiuscolo.

---

## Step 2 - Scrivi l'implementazione host

Crea `crates/nx-core/src/host_api/text.rs`:

```rust
use anyhow::Result;
use wasmtime::{Caller, Linker, Memory};

use crate::runtime::HostState;

const ERR_BUF_TOO_SMALL: i32 = -2;
const ERR_INTERNAL: i32 = -3;
const MAX_INPUT_LEN: u32 = 64 * 1024;
const MAX_OUT_CAP: u32 = 1024 * 1024;

fn get_memory(caller: &mut Caller<'_, HostState>) -> Option<Memory> {
    match caller.get_export("memory") {
        Some(wasmtime::Extern::Memory(mem)) => Some(mem),
        _ => None,
    }
}

fn string_upper_impl(
    mut caller: Caller<'_, HostState>,
    in_ptr: u32,
    in_len: u32,
    out_ptr: u32,
    out_cap: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(memory) => memory,
        None => {
            eprintln!("[nx-core] string_upper: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    if in_len > MAX_INPUT_LEN {
        eprintln!("[nx-core] string_upper: input too large: {in_len}");
        return ERR_INTERNAL;
    }

    if out_cap > MAX_OUT_CAP {
        eprintln!("[nx-core] string_upper: output cap too large: {out_cap}");
        return ERR_INTERNAL;
    }

    let mut input = vec![0u8; in_len as usize];
    if let Err(e) = memory.read(&mut caller, in_ptr as usize, &mut input) {
        eprintln!("[nx-core] string_upper: failed to read input: {e}");
        return ERR_INTERNAL;
    }

    let input = match std::str::from_utf8(&input) {
        Ok(input) => input,
        Err(e) => {
            eprintln!("[nx-core] string_upper: input is not UTF-8: {e}");
            return ERR_INTERNAL;
        }
    };

    let result = input.to_uppercase();
    let result = result.as_bytes();

    if result.len() > out_cap as usize {
        return ERR_BUF_TOO_SMALL;
    }

    if let Err(e) = memory.write(&mut caller, out_ptr as usize, result) {
        eprintln!("[nx-core] string_upper: failed to write output: {e}");
        return ERR_INTERNAL;
    }

    result.len() as i32
}

pub fn add_to_linker(linker: &mut Linker<HostState>) -> Result<()> {
    linker.func_wrap(
        "nx",
        "string_upper",
        |caller: Caller<'_, HostState>,
         in_ptr: u32,
         in_len: u32,
         out_ptr: u32,
         out_cap: u32|
         -> i32 { string_upper_impl(caller, in_ptr, in_len, out_ptr, out_cap) },
    )?;

    Ok(())
}
```

I punti importanti sono:

- prendere la `memory` esportata dal guest;
- validare ogni lunghezza prima di leggere;
- leggere dalla memoria guest con `memory.read`;
- scrivere nella memoria guest solo se `out_cap` è sufficiente;
- restituire `ERR_BUF_TOO_SMALL` quando l'SDK può riprovare con un buffer più grande;
- non fare `panic!` nei percorsi di errore.

---

## Step 3 - Registra il modulo host API

Aggiungi il nuovo modulo a `crates/nx-core/src/host_api/mod.rs`:

```rust
pub mod crdt;
pub mod crypto;
pub mod db;
pub mod log;
pub mod net;
pub mod system;
pub mod text;
pub mod time;
```

---

## Step 4 - Collega al linker

In `crates/nx-core/src/runtime.rs`, nel blocco dove il runtime registra le altre host API, aggiungi:

```rust
host_api::text::add_to_linker(&mut linker)?;
```

Tutte le funzioni esportate al guest devono passare da qui o da un `add_to_linker` chiamato da qui.

---

## Step 5 - Aggiungi la capability

In `crates/nx-core/src/host_api/system.rs`, aggiungi `"string_upper"` a `HOST_CAPABILITIES`.

Questo permette ai moduli guest di chiamare `system::host_capabilities()` e scoprire che la funzione è disponibile nel runtime.

---

## Step 6 - Aggiungi l'import FFI nello SDK

In `crates/nx-sdk/src/ffi.rs`, aggiungi la funzione dentro il blocco esistente:

```rust
#[link(wasm_import_module = "nx")]
unsafe extern "C" {
    // ... import esistenti ...

    pub fn string_upper(
        in_ptr: u32,
        in_len: u32,
        out_ptr: u32,
        out_cap: u32,
    ) -> i32;
}
```

Il nome deve corrispondere esattamente a quello registrato nel linker: `string_upper`.

---

## Step 7 - Aggiungi il wrapper SDK

Crea `crates/nx-sdk/src/text.rs`:

```rust
use crate::__alloc::{string::String, vec};
use crate::{ffi, NxError, Result};

const ERR_BUF_TOO_SMALL: i32 = -2;
const ERR_INTERNAL: i32 = -3;
const MAX_RETRY_CAP: usize = 1024 * 1024;

/// Converts a UTF-8 string to uppercase using the Numax host runtime.
pub fn upper(input: &str) -> Result<String> {
    let input = input.as_bytes();
    let mut cap = input.len().saturating_add(64).max(64);

    loop {
        let mut out = vec![0u8; cap];
        let rc = unsafe {
            ffi::string_upper(
                input.as_ptr() as u32,
                input.len() as u32,
                out.as_mut_ptr() as u32,
                out.len() as u32,
            )
        };

        match rc {
            n if n >= 0 => {
                out.truncate(n as usize);
                return String::from_utf8(out).map_err(|_| NxError::Internal);
            }
            ERR_BUF_TOO_SMALL => {
                cap = cap.saturating_mul(2);
                if cap > MAX_RETRY_CAP {
                    return Err(NxError::BufferTooSmall);
                }
            }
            ERR_INTERNAL => return Err(NxError::Internal),
            code => return Err(NxError::UnknownCode(code)),
        }
    }
}
```

Registra il modulo in `crates/nx-sdk/src/lib.rs`:

```rust
pub mod text;
```

---

## Step 8 - Usa la funzione da un modulo guest

```rust
use nx_sdk::{nx_log, text};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    match text::upper("hello numax") {
        Ok(value) => nx_log!("upper: {}", value),
        Err(e) => nx_log!("error: {}", e),
    }
}
```

Output atteso:

```text
upper: HELLO NUMAX
```

---

## Regole ABI pratiche

Per le host function byte-in/byte-out, usa queste convenzioni:

**Input dinamici** come coppie `(ptr: u32, len: u32)`. L'host legge `len` byte dalla memoria lineare guest all'offset `ptr`.

**Output dinamici** come `(out_ptr: u32, out_cap: u32)`. L'host scrive al massimo `out_cap` byte nella memoria guest. Se il risultato non entra, restituisce `ERR_BUF_TOO_SMALL` (`-2`) e l'SDK riprova con un buffer più grande.

**Valore di ritorno** di solito `i32`:

| Codice | Significato |
|---|---|
| `>= 0` | successo; per output dinamici è il numero di byte scritti |
| `-1` | not found, usato soprattutto da `db_get` |
| `-2` | buffer output troppo piccolo |
| `-3` | errore interno |
| `-4` | chiave riservata `__nx/` |
| `-5` | sync disabilitata |

Questa è una convenzione, non una legge universale. Alcune API hanno una firma più semplice perché non passano buffer dinamici.

---

## Host function asincrone

Se la funzione deve fare `await`, usa `func_wrap_async`.

Esempio minimale:

```rust
pub fn add_to_linker(linker: &mut Linker<HostState>) -> Result<()> {
    linker.func_wrap_async(
        "nx",
        "my_async_function",
        |caller: Caller<'_, HostState>,
         (in_ptr, in_len, out_ptr, out_cap): (u32, u32, u32, u32)| {
            Box::new(my_async_function_impl(
                caller,
                in_ptr,
                in_len,
                out_ptr,
                out_cap,
            ))
        },
    )?;

    Ok(())
}

async fn my_async_function_impl(
    mut caller: Caller<'_, HostState>,
    in_ptr: u32,
    in_len: u32,
    out_ptr: u32,
    out_cap: u32,
) -> i32 {
    // può usare .await qui
    0
}
```

Le funzioni CRDT e alcune funzioni network usano questo pattern perché interagiscono con stato condiviso o canali async.

---

## Cosa non fare

**Non accedere a risorse host globali saltando `HostState`.** `HostState` è il punto in cui il runtime collega store, sync handle, metriche e configurazione dell'invocazione.

**Non scrivere più byte di `out_cap`.** Anche se la memoria guest contiene spazio valido oltre quel buffer, il contratto ABI dice che l'host può scrivere solo nella regione dichiarata dal guest.

**Non usare `std::process::exit`.** Una host function non deve terminare il processo runtime. Se deve interrompere il guest, restituisci un errore o usa un trap Wasmtime esplicito, come fa `abort`.

**Non registrare lo stesso nome due volte.** Wasmtime restituirà un errore quando costruisci il linker.

---

## Correlati

- [Esecuzione WASM](/numax/it/concepts/wasm-execution/) - come funzionano linker e `HostState`
- [Host API reference](/numax/it/reference/host-api/) - le host function esposte dal runtime
- [Crate nx-core](/numax/it/reference/crates/nx-core/) - internals di runtime e host API
- [Crate nx-sdk](/numax/it/reference/crates/nx-sdk/) - wrapper guest-side sopra gli import raw
