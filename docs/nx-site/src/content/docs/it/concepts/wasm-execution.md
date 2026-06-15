---
title: Esecuzione WASM
description: Come i moduli WebAssembly vengono eseguiti dentro Numax.
---

Un nodo Numax esegue un modulo WASM per invocazione. Il modulo è un binario portabile compilato per il target `wasm32-unknown-unknown` o `wasm32-wasip1`. Il runtime lo carica, lo valida, linka la Host API, lo istanzia e chiama l'entry point. Questa pagina spiega esattamente cosa avviene ad ogni passo.

---

## Cosa è un modulo WASM in Numax

Un modulo Numax è un binario WebAssembly standard con un solo requisito: deve esportare una funzione chiamata `run` o `_start` con la firma `() -> ()`.

```rust
#[unsafe(no_mangle)]
pub extern "C" fn run() {
    // logica applicativa
}
```

Il modulo è compilato con `crate-type = ["cdylib"]` e costruito per `wasm32-unknown-unknown`. È un binario autocontenuto: nessun dynamic linking, nessun accesso implicito al sistema operativo, nessun socket di rete. L'unico modo in cui può interagire con il mondo esterno è chiamare le funzioni che l'host espone esplicitamente.

---

## La sandbox

La sandbox WASM è strutturale, non configurata. Il modulo gira dentro un `Store` Wasmtime creato fresco per ogni invocazione di `run_module`. La sandbox impone:

- **Isolamento memoria** - il modulo ha accesso solo alla propria memoria lineare. Non può leggere né scrivere la memoria dell'host o di altri moduli.
- **Nessun I/O implicito** - nessun accesso al filesystem, nessuno stack di rete, nessuna chiamata OS, a meno che l'host non li fornisca esplicitamente via WASI o la Host API.
- **Import controllati** - il modulo può chiamare solo le funzioni registrate nel `Linker` prima dell'istanziazione. Qualsiasi import non registrato causa un link error al momento dell'istanziazione, prima che qualsiasi codice giri.
- **Cap memoria opzionale** - `RuntimeConfig.max_memory_bytes` imposta un limite per invocazione imposto dai `StoreLimits` di Wasmtime. Se il modulo tenta di far crescere la propria memoria lineare oltre questo limite, la grow fallisce.

```rust
// da runtime.rs - limiti per invocazione
let mut limits_builder = StoreLimitsBuilder::new();
if let Some(max_bytes) = self.config.max_memory_bytes {
    limits_builder = limits_builder.memory_size(max_bytes as usize);
}
let limits = limits_builder.build();
// ...
store.limiter(|state| &mut state.limits);
```

---

## Compilazione e caching

Quando viene chiamato `run_module(wasm_bytes)`:

1. Viene calcolato l'hash blake3 dei byte.
2. Viene controllata la module cache (`Mutex<HashMap<[u8; 32], Module>>`). Se esiste un modulo compilato per questo hash, viene riusato.
3. Altrimenti, `Module::new(&engine, wasm_bytes)` compila e valida il binario. Wasmtime esegue la validazione strutturale e di tipo completa in questo momento. I moduli non validi vengono rifiutati prima dell'istanziazione.
4. Il modulo compilato viene inserito nella cache sotto il suo hash.

La cache vive per tutta la lifetime del `Runtime`. Eseguire lo stesso binario mille volte costa una sola compilazione. Binari diversi con lo stesso hash (impossibile con blake3) condividerebbero un modulo, ma in pratica ogni binario distinto ha la propria voce in cache.

---

## La Host API: cosa può importare il modulo

Ogni funzione host disponibile al modulo è registrata in `Runtime::new` tramite le funzioni `add_to_linker` in `nx-core/src/host_api/`. Il namespace è sempre `"nx"`. Il modulo importa da questo namespace usando le dichiarazioni FFI in `ffi.rs`.

Tutte le 41 funzioni host registrate all'avvio:

| Gruppo | Funzioni |
|---|---|
| **db** | `db_get`, `db_set`, `db_delete`, `db_exists`, `db_scan`, `db_scan_after`, `db_keys`, `db_keys_after` |
| **crdt** | `crdt_gcounter_inc`, `crdt_gcounter_value`, `crdt_pncounter_inc`, `crdt_pncounter_dec`, `crdt_pncounter_value`, `crdt_lww_set`, `crdt_lww_get`, `crdt_lww_map_set`, `crdt_lww_map_remove`, `crdt_lww_map_get`, `crdt_lww_map_contains`, `crdt_lww_map_entries`, `crdt_orset_add`, `crdt_orset_remove`, `crdt_orset_contains`, `crdt_orset_elements`, `crdt_rga_insert`, `crdt_rga_delete`, `crdt_rga_values` |
| **log** | `host_log`, `host_log_v2` |
| **time** | `time_now`, `time_monotonic` |
| **crypto** | `random_bytes`, `hash_sha256`, `hash_blake3` |
| **system** | `env_get`, `module_id`, `host_capabilities`, `event_emit`, `abort` |
| **net** | `net_node_id`, `net_peers` |

Se il modulo tenta di importare una funzione non in questa lista, l'istanziazione fallisce con un link error. Non c'è modo di chiamare funzioni host non elencate.

Un modulo può interrogare la lista completa delle funzioni disponibili a runtime:

```rust
let caps: Vec<String> = system::host_capabilities()?;
```

---

## Memoria lineare e ABI

La memoria lineare WASM è un array piatto di byte. Il modulo e l'host condividono l'accesso: il modulo scrive dati al suo interno, poi passa puntatori e lunghezze alle funzioni host. Le funzioni host leggono da essa e scrivono i risultati al suo interno.

La maggior parte delle funzioni host segue la stessa convenzione ABI:

```
input:   (ptr: u32, len: u32)  ->  l'host legge len byte dalla memoria lineare all'offset ptr
output:  (out_ptr: u32, out_cap: u32)  ->  l'host scrive il risultato nella memoria lineare a out_ptr, fino a out_cap byte
return:  i32  ->  byte count/status in caso di successo, codice errore negativo in caso di fallimento
```

Ci sono alcune eccezioni: `time_now` e `time_monotonic` restituiscono direttamente `u64`,
il legacy `host_log` restituisce `()`, e `abort` genera un trap invece di ritornare normalmente.

L'host valida ogni accesso ai puntatori prima di toccare la memoria. Se il range richiesto cade fuori dalla memoria lineare corrente del modulo, la chiamata restituisce `ERR_INTERNAL`. Se il buffer di output è troppo piccolo per il risultato, la chiamata restituisce `ERR_BUF_TOO_SMALL` e l'SDK riprova con un buffer più grande.

**Limiti input imposti dall'host:**

| Risorsa | Limite |
|---|---|
| Lunghezza chiave | 8 KiB |
| Lunghezza valore | 1 MiB |
| Capacità buffer output | 1 MiB |
| Limite scan per pagina | 1024 voci |
| Lunghezza nome evento | 128 byte |
| Payload evento | 64 KiB |
| Messaggio abort | 8 KiB |

Questi limiti vengono controllati ad ogni chiamata, prima di qualsiasi operazione su store o rete. Un modulo che invia una chiave sovradimensionata riceve `ERR_INTERNAL` immediatamente.

---

## WASI

Quando `RuntimeConfig.enable_wasi` è `true` (il default), il runtime linka anche le funzioni WASI preview1 nel modulo. WASI dà al modulo accesso a:

- I/O standard (`stdin`, `stdout`, `stderr`)
- argomenti riga di comando

Numax non eredita esplicitamente handle filesystem o variabili d'ambiente WASI. L'accesso all'ambiente runtime passa da `env_get`, che espone solo variabili consentite `NX_*` e `NUMAX_*`. Il contesto WASI è costruito con `WasiCtx::builder().inherit_stdio().inherit_args().build_p1()`, che è il minimo necessario per supportare moduli che usano `println!` o `eprintln!`.

Per disabilitare WASI completamente (per moduli di logica pura che non hanno bisogno di stdio), imposta `enable_wasi: false` in `RuntimeConfig`. Il modulo verrà linkato senza funzioni WASI e qualsiasi import dal namespace `wasi_snapshot_preview1` causerà un link error.

---

## HostState: cosa vede il modulo

Ogni invocazione crea un `HostState` e lo attacca al `Store` Wasmtime. Questo è il ponte tra le chiamate del modulo e lo stato del runtime.

```rust
pub struct HostState {
    pub wasi:        Option<p1::WasiP1Ctx>,  // None quando WASI disabilitato
    pub store:       Arc<NxStore>,           // condiviso con Runtime e SyncManager
    pub sync_handle: Option<SyncHandle>,     // None quando sync disabilitata
    pub module_id:   Arc<str>,               // impostato da RuntimeConfig.module_id
    pub limits:      wasmtime::StoreLimits,  // cap memoria per invocazione
}
```

`store` e `sync_handle` sono clone `Arc` dal `Runtime`. Il database sled è condiviso: le scritture dal modulo sono immediatamente visibili al sync manager. `sync_handle` è `None` quando il runtime è stato avviato senza `--listen`, ecco perché le funzioni host CRDT lo controllano e restituiscono `ERR_SYNC_DISABLED` quando è assente.

---

## L'entry point

Dopo l'istanziazione, il runtime cerca l'entry point in quest'ordine:

1. `run` - preferito, entry point esplicito Numax
2. `_start` - fallback per moduli compilati con WASI

```rust
let run = instance
    .get_typed_func::<(), ()>(&mut store, "run")
    .or_else(|_| instance.get_typed_func::<(), ()>(&mut store, "_start"))
    .map_err(|e| anyhow!("No entrypoint found (expected `run` or `_start`): {e}"))?;

run.call_async(&mut store, ()).await?;
```

La funzione deve non prendere argomenti e non restituire nulla. Se né `run` né `_start` sono esportati, `run_module` restituisce un errore e il modulo non viene eseguito.

La chiamata usa le API async di Wasmtime perché alcune funzioni host sono registrate con `func_wrap_async`. Le funzioni host possono sospendere il modulo mentre aspettano lavoro runtime senza bloccare il thread tokio.

---

## Cosa succede su `abort`

Se il modulo chiama `system::abort("messaggio")`, la funzione host restituisce un `Error` Wasmtime, che causa un trap immediato. Il trap si propaga su attraverso `run.call_async`, e `run_module` restituisce `Err(...)` al chiamante. Il messaggio appare nel log host.

```rust
// da host_api/system.rs
fn abort_impl(...) -> Result<(), Error> {
    let msg = /* leggi dalla memoria guest */;
    Err(Error::msg(format!("guest abort: {msg}")))
}
```

Il modulo non ha la possibilità di eseguire codice di cleanup dopo `abort`. Lo `Store` viene droppato, che droppa `HostState`, che droppa il clone di `SyncHandle`. Le operazioni CRDT in volo che erano già state inviate al sync manager non vengono rollback.

---

## L'esecuzione è sincrona dal punto di vista del modulo

Il modulo chiama le funzioni host in modo sincrono. Dal suo punto di vista, `db::set("key", b"value")` è una chiamata bloccante che restituisce un codice risultato. Non ci sono callback, nessun future, nessuna macchina async visibile al guest.

La macchina async vive interamente sul lato host. Quando una funzione host deve attendere qualcosa (es. spingere un'op nel canale del sync manager), Wasmtime sospende la fiber che esegue il modulo, permette allo scheduler tokio di girare, e riprende la fiber quando l'operazione è completata. Il modulo non osserva mai questo.

---

## Scrivere un modulo: esempio minimale

```rust
// Cargo.toml
// [lib]
// crate-type = ["cdylib"]
// [dependencies]
// nx-sdk = { path = "../../crates/nx-sdk" }

use nx_sdk::{db, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    nx_log!("modulo avviato");

    match db::get("counter") {
        Ok(Some(bytes)) => {
            let n = u64::from_le_bytes(bytes.try_into().unwrap_or([0u8; 8]));
            let next = n + 1;
            db::set("counter", &next.to_le_bytes()).unwrap();
            nx_log!("counter = {}", next);
        }
        Ok(None) => {
            db::set("counter", &1u64.to_le_bytes()).unwrap();
            nx_log!("counter = 1");
        }
        Err(e) => nx_log!("errore: {}", e),
    }
}
```

Build:

```bash
cargo build --target wasm32-unknown-unknown --release
nx run target/wasm32-unknown-unknown/release/my_module.wasm
```

---

## Correlati

- [Modello runtime](/numax/it/concepts/runtime-model/) - lifecycle e filosofia
- [Host API](/numax/it/reference/host-api/) - riferimento completo funzioni
- [Crate nx-sdk](/numax/it/reference/crates/nx-sdk/) - l'SDK lato guest
- [Crate nx-core](/numax/it/reference/crates/nx-core/) - internals Runtime e HostState
