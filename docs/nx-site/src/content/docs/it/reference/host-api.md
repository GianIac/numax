---
title: Host API
description: Riferimento per le funzioni host esposte ai moduli WASM.
---

Questo documento descrive le funzioni host disponibili per i moduli WASM in esecuzione nel runtime Numax.

Tutte le funzioni vengono importate dal namespace `nx`. La maggior parte degli utenti
dovrebbe chiamarle tramite `nx-sdk`; l'ABI raw è documentato qui in modo che gli autori
di moduli possano capire il contratto wire e le future binding SDK possano restare consistenti.

## Contenuti

- [Convenzioni ABI](#convenzioni-abi)
- [Codici di Errore](#codici-di-errore)
- [Limiti](#limiti)
- [Database API](#database-api)
- [CRDT API](#crdt-api)
- [Time API](#time-api)
- [Crypto API](#crypto-api)
- [System API](#system-api)
- [Network API](#network-api)
- [Logging API](#logging-api)
- [Roadmap](#roadmap)

---

## Convenzioni ABI

### Namespace

Tutte le import usano il namespace `nx`.

### Memoria

Stringhe, chiavi e array di byte vengono passati come coppie `(ptr, len)` nella
memoria lineare del guest. Le funzioni di output scrivono in buffer `(out_ptr, out_cap)`
e restituiscono il numero di byte scritti.

### Valori di Ritorno

Se non diversamente specificato:

- `0` indica successo per le funzioni command-style.
- `> 0` indica il numero di byte scritti per le funzioni read-style.
- valori negativi sono codici di errore elencati in [Codici di Errore](#codici-di-errore).

### Codifica Interi

Gli output binari strutturati usano campi interi little-endian salvo diversa indicazione esplicita.

### Chiavi Riservate

Le chiavi sotto il prefisso riservato dal runtime `__nx/` non sono visibili ai moduli guest.
Le Database API rifiutano l'accesso diretto a chiavi/prefissi riservati.

---

## Codici di Errore

| Codice | Costante | Significato |
|--------|----------|-------------|
| `0` | `OK` | Successo |
| `-1` | `ERR_NOT_FOUND` | Chiave/valore/risorsa non trovata |
| `-2` | `ERR_BUFFER_TOO_SMALL` | Buffer di output troppo piccolo |
| `-3` | `ERR_INTERNAL` | Errore host/runtime |
| `-4` | `ERR_RESERVED_KEY` | Chiave o prefisso usa un namespace riservato al runtime |
| `-5` | `ERR_SYNC_DISABLED` | API dipendente dalla sync chiamata senza sync abilitata |

---

## Limiti

| Risorsa | Limite |
|---------|--------|
| Lunghezza chiave | 1024 byte |
| Lunghezza valore | 1 MiB |
| Buffer di output | 10 MiB |
| Richiesta `random_bytes` | 1 MiB |
| Messaggio di log | 8 KiB |

L'SDK rispecchia dove possibile i limiti lato host più importanti, così le
allocazioni guest sovradimensionate possono fallire prima di chiamare l'host.

---

## Database API

Le funzioni database operano sul key/value store embedded locale. Non replicano da sole.
Usa la CRDT API per lo stato replicato.

### Sommario

| Funzione | Scopo | Stato |
|----------|-------|-------|
| `db_get` | Legge un valore per chiave | Implementata |
| `db_set` | Scrive un valore per chiave | Implementata |
| `db_delete` | Cancella una chiave | Implementata |
| `db_exists` | Verifica l'esistenza di una chiave | Implementata |
| `db_scan` | Scan per prefisso con cursore offset | Implementata, compatibilità |
| `db_scan_after` | Scan per prefisso con cursore chiave | Implementata, preferita |
| `db_keys` | Lista chiavi per prefisso con cursore offset | Implementata, compatibilità |
| `db_keys_after` | Lista chiavi per prefisso con cursore chiave | Implementata, preferita |

### `db_get`

```text
fn db_get(key_ptr: u32, key_len: u32, out_ptr: u32, out_cap: u32) -> i32
```

Restituisce la lunghezza del valore in caso di successo, `ERR_NOT_FOUND` se assente, o
`ERR_BUFFER_TOO_SMALL` se `out_cap` è insufficiente.

SDK:

```rust
use nx_sdk::db;

let value = db::get("user:1")?;
```

### `db_set`

```text
fn db_set(key_ptr: u32, key_len: u32, val_ptr: u32, val_len: u32) -> i32
```

Restituisce `0` in caso di successo.

SDK:

```rust
use nx_sdk::db;

db::set("user:1", b"alice")?;
```

### `db_delete`

```text
fn db_delete(key_ptr: u32, key_len: u32) -> i32
```

Restituisce `0` in caso di successo, anche se la chiave non esisteva.

### `db_exists`

```text
fn db_exists(key_ptr: u32, key_len: u32) -> i32
```

Restituisce:

| Valore | Significato |
|--------|-------------|
| `1` | Chiave presente |
| `0` | Chiave assente |
| `< 0` | Errore |

SDK:

```rust
use nx_sdk::db;

if db::exists("user:1")? {
    // presente
}
```

### `db_scan`

```text
fn db_scan(
    prefix_ptr: u32,
    prefix_len: u32,
    cursor: u64,
    limit: u32,
    out_ptr: u32,
    out_cap: u32
) -> i32
```

Restituisce una pagina limitata di coppie chiave/valore che corrispondono a `prefix`.

`cursor` è un offset logico di riga tra le righe visibili. Questa API è mantenuta per
compatibilità; i nuovi moduli dovrebbero preferire `db_scan_after` perché i cursori
offset possono spostarsi quando lo store cambia durante la paginazione.

Codifica output:

```text
u32 row_count
repeat row_count times:
  u32 key_len
  u32 value_len
  u8[key_len] key
  u8[value_len] value
```

### `db_scan_after`

```text
fn db_scan_after(
    prefix_ptr: u32,
    prefix_len: u32,
    start_after_ptr: u32,
    start_after_len: u32,
    limit: u32,
    out_ptr: u32,
    out_cap: u32
) -> i32
```

API di scan preferita per spazi di chiavi grandi. `start_after_len = 0` parte dalla
prima chiave visibile. Altrimenti, `start_after` deve essere una chiave sotto `prefix`.

La codifica output è la stessa di `db_scan`.

SDK:

```rust
use nx_sdk::db;

let rows = db::scan("user:")?;
```

### `db_keys`

```text
fn db_keys(
    prefix_ptr: u32,
    prefix_len: u32,
    cursor: u64,
    limit: u32,
    out_ptr: u32,
    out_cap: u32
) -> i32
```

Lista le chiavi corrispondenti a `prefix` usando un cursore offset. Mantenuta per
compatibilità; i nuovi moduli dovrebbero preferire `db_keys_after`.

Codifica output:

```text
u32 key_count
repeat key_count times:
  u32 key_len
  u8[key_len] key
```

### `db_keys_after`

```text
fn db_keys_after(
    prefix_ptr: u32,
    prefix_len: u32,
    start_after_ptr: u32,
    start_after_len: u32,
    limit: u32,
    out_ptr: u32,
    out_cap: u32
) -> i32
```

API di listing chiavi preferita per spazi di chiavi grandi. `start_after_len = 0`
parte dalla prima chiave visibile. La codifica output è la stessa di `db_keys`.

SDK:

```rust
use nx_sdk::db;

let keys = db::keys("user:")?;
```

---

## CRDT API

Le funzioni CRDT operano sullo stato replicato gestito dal sync manager del runtime.
Richiedono che la sync sia abilitata; altrimenti restituiscono `ERR_SYNC_DISABLED`.

Lo stato CRDT è:

- tenuto in un registry in-memory mentre il runtime è attivo;
- persistito come stato CRDT durevole e metadati dell'op-log;
- materializzato nello store locale per restart/rilettura;
- trasmesso ai peer tramite il layer di sync;
- riparato tramite reconnect e anti-entropy quando i peer perdono i push.

### Regola di Completamento per Nuovi CRDT

Una host API CRDT si considera completa solo quando ha:

- implementazione in `nx-sync`;
- supporto a `OpKind` e serializzazione;
- integrazione stato durevole/op-log in `nx-core`;
- host API e wrapper SDK;
- test unitari/property;
- test E2E SyncManager;
- esempio distribuito con README e run riproducibile a 2-3 nodi.

### Sommario

| CRDT | Funzioni | Stato |
|------|----------|-------|
| GCounter | `crdt_gcounter_inc`, `crdt_gcounter_value` | Implementato |
| PNCounter | `crdt_pncounter_inc`, `crdt_pncounter_dec`, `crdt_pncounter_value` | Implementato |
| LWW-Register | `crdt_lww_set`, `crdt_lww_get` | Implementato |
| ORSet | `crdt_orset_add`, `crdt_orset_remove`, `crdt_orset_contains`, `crdt_orset_elements` | Implementato |
| LWW-Map | `crdt_lww_map_set`, `crdt_lww_map_remove`, `crdt_lww_map_get`, `crdt_lww_map_contains`, `crdt_lww_map_entries` | Implementato |
| RGA | `crdt_rga_insert`, `crdt_rga_delete`, `crdt_rga_values` | Implementato |

### GCounter

Un contatore grow-only. Ogni nodo possiede il proprio slot positivo. Il valore convergente
è la somma di tutti gli slot dei nodi.

Usalo per totali che aumentano soltanto: visite, eventi emessi, job completati.

#### `crdt_gcounter_inc`

```text
fn crdt_gcounter_inc(key_ptr: u32, key_len: u32, delta: u64) -> i32
```

Restituisce `0` in caso di successo.

SDK:

```rust
use nx_sdk::crdt::gcounter;

gcounter::inc("counter:visits", 1)?;
```

#### `crdt_gcounter_value`

```text
fn crdt_gcounter_value(
    key_ptr: u32,
    key_len: u32,
    out_ptr: u32,
    out_cap: u32
) -> i32
```

Scrive un `u64` little-endian da 8 byte. Restituisce `8` in caso di successo.

SDK:

```rust
use nx_sdk::crdt::gcounter;

let visits = gcounter::value("counter:visits")?;
```

### PNCounter

Un contatore positivo/negativo. Supporta incrementi e decrementi convergendo comunque
senza coordinamento. Internamente si comporta come due contatori grow-only: uno per gli
slot positivi e uno per quelli negativi.

Usalo per stock, saldi, voti con movimento su/giù, o qualsiasi valore che debba muoversi
in entrambe le direzioni tollerando la consistenza eventuale.

#### `crdt_pncounter_inc`

```text
fn crdt_pncounter_inc(key_ptr: u32, key_len: u32, delta: u64) -> i32
```

Restituisce `0` in caso di successo.

SDK:

```rust
use nx_sdk::crdt::pncounter;

pncounter::inc("inventory:sku-1", 10)?;
```

#### `crdt_pncounter_dec`

```text
fn crdt_pncounter_dec(key_ptr: u32, key_len: u32, delta: u64) -> i32
```

Restituisce `0` in caso di successo.

SDK:

```rust
use nx_sdk::crdt::pncounter;

pncounter::dec("inventory:sku-1", 3)?;
```

#### `crdt_pncounter_value`

```text
fn crdt_pncounter_value(
    key_ptr: u32,
    key_len: u32,
    out_ptr: u32,
    out_cap: u32
) -> i32
```

Scrive un `i64` signed little-endian da 8 byte. Restituisce `8` in caso di successo.

SDK:

```rust
use nx_sdk::crdt::pncounter;

let available = pncounter::value("inventory:sku-1")?;
```

### LWW-Register

Un registro last-writer-wins memorizza un singolo valore byte per chiave. Ogni scrittura
viene taggata dall'host con un timestamp e il `NodeId` locale; i timestamp maggiori vincono,
e i timestamp uguali vengono risolti deterministicamente per `NodeId`.

L'host mantiene le scritture locali monotone rispetto allo stato attualmente osservato del
registro, così le scritture ripetute dallo stesso modulo possono comunque sostituire valori
locali precedenti anche quando avvengono nello stesso millisecondo.

Usalo per stati, etichette, valori di configurazione, modalità selezionate, o qualsiasi
stato a valore singolo dove "l'ultimo valore noto vince" è la policy di conflitto corretta.

#### `crdt_lww_set`

```text
fn crdt_lww_set(
    key_ptr: u32,
    key_len: u32,
    value_ptr: u32,
    value_len: u32
) -> i32
```

Restituisce `0` in caso di successo. I valori sono limitati a 1 MiB.

SDK:

```rust
use nx_sdk::crdt::lww_register;

lww_register::set("status:user-1", b"online")?;
```

#### `crdt_lww_get`

```text
fn crdt_lww_get(
    key_ptr: u32,
    key_len: u32,
    out_ptr: u32,
    out_cap: u32
) -> i32
```

Scrive il valore vincente corrente in `out_ptr` e restituisce il numero di byte scritti.
Restituisce `ERR_NOT_FOUND` quando il registro non ha valore e `ERR_BUF_TOO_SMALL` quando
`out_cap` non è abbastanza grande.

SDK:

```rust
use nx_sdk::crdt::lww_register;

let status = lww_register::get("status:user-1")?;
```

### LWW-Map

Una mappa chiave-valore dove ogni campo è un registro last-writer-wins indipendente.
Le rimozioni sono memorizzate come tombstone, così le vecchie scritture non possono
resuscitare campi cancellati dopo reconnect o replay di anti-entropy.

Usala per impostazioni replicate, feature flag, metadati di servizio e piccoli documenti
di configurazione.

#### `crdt_lww_map_set`

```text
fn crdt_lww_map_set(
    key_ptr: u32,
    key_len: u32,
    field_ptr: u32,
    field_len: u32,
    value_ptr: u32,
    value_len: u32
) -> i32
```

Imposta un campo. L'host assegna il timestamp e il NodeId locale dello scrittore.

SDK:

```rust
use nx_sdk::crdt::lww_map;

lww_map::set("settings:service-a", "theme", b"dark")?;
```

#### `crdt_lww_map_remove`

```text
fn crdt_lww_map_remove(
    key_ptr: u32,
    key_len: u32,
    field_ptr: u32,
    field_len: u32
) -> i32
```

Rimuove un campo scrivendo un tombstone.

SDK:

```rust
use nx_sdk::crdt::lww_map;

lww_map::remove("settings:service-a", "region")?;
```

#### `crdt_lww_map_get`

```text
fn crdt_lww_map_get(
    key_ptr: u32,
    key_len: u32,
    field_ptr: u32,
    field_len: u32,
    out_ptr: u32,
    out_cap: u32
) -> i32
```

Scrive il valore visibile del campo e restituisce il numero di byte scritti. Restituisce
`ERR_NOT_FOUND` quando la mappa o il campo è assente, inclusi i campi con tombstone.

SDK:

```rust
use nx_sdk::crdt::lww_map;

let value = lww_map::get("settings:service-a", "theme")?;
```

#### `crdt_lww_map_contains`

```text
fn crdt_lww_map_contains(
    key_ptr: u32,
    key_len: u32,
    field_ptr: u32,
    field_len: u32
) -> i32
```

Restituisce `1` quando il campo ha un valore visibile e `0` quando è assente o con tombstone.

SDK:

```rust
use nx_sdk::crdt::lww_map;

let has_theme = lww_map::contains("settings:service-a", "theme")?;
```

#### `crdt_lww_map_entries`

```text
fn crdt_lww_map_entries(
    key_ptr: u32,
    key_len: u32,
    out_ptr: u32,
    out_cap: u32
) -> i32
```

Scrive le voci visibili in ordine deterministico per campo e restituisce il numero di byte
scritti. I tombstone non vengono restituiti. La codifica output raw è:

```text
u32 entry_count
repeat entry_count times:
  u32 field_len
  u8[field_len] utf8_field
  u32 value_len
  u8[value_len] value
```

SDK:

```rust
use nx_sdk::crdt::lww_map;

let entries = lww_map::entries("settings:service-a")?;
```

### ORSet

Un ORSet memorizza elementi stringa visibili per chiave. Ogni add crea un add-tag univoco.
Una remove porta gli add-tag osservati localmente per quell'elemento, così gli add
concorrenti non osservati dalla remove rimangono visibili dopo il merge.

Usalo per tag, etichette, set di feature, membership, o qualsiasi set di stringhe dove
add/remove devono convergere senza coordinamento.

#### `crdt_orset_add`

```text
fn crdt_orset_add(
    key_ptr: u32,
    key_len: u32,
    element_ptr: u32,
    element_len: u32
) -> i32
```

Restituisce `0` in caso di successo. L'host genera l'add-tag dall'`OpId` locale.

SDK:

```rust
use nx_sdk::crdt::orset;

orset::add("tags:item-1", "blue")?;
```

#### `crdt_orset_remove`

```text
fn crdt_orset_remove(
    key_ptr: u32,
    key_len: u32,
    element_ptr: u32,
    element_len: u32
) -> i32
```

Restituisce `0` in caso di successo. Rimuovere un elemento che non ha add-tag osservati
localmente è un no-op.

SDK:

```rust
use nx_sdk::crdt::orset;

orset::remove("tags:item-1", "blue")?;
```

#### `crdt_orset_contains`

```text
fn crdt_orset_contains(
    key_ptr: u32,
    key_len: u32,
    element_ptr: u32,
    element_len: u32
) -> i32
```

Restituisce `1` quando l'elemento è visibile e `0` quando è assente.

SDK:

```rust
use nx_sdk::crdt::orset;

let has_blue = orset::contains("tags:item-1", "blue")?;
```

#### `crdt_orset_elements`

```text
fn crdt_orset_elements(
    key_ptr: u32,
    key_len: u32,
    out_ptr: u32,
    out_cap: u32
) -> i32
```

Scrive gli elementi visibili in ordine deterministico e restituisce il numero di byte
scritti. La codifica output raw è:

```text
u32 element_count
repeat element_count times:
  u32 element_len
  u8[element_len] utf8_element
```

SDK:

```rust
use nx_sdk::crdt::orset;

let tags = orset::elements("tags:item-1")?;
```

### RGA

Un RGA memorizza una sequenza ordinata di valori byte per chiave. Gli insert creano id
elemento stabili e puntano opzionalmente a un id elemento padre. I delete mettono un
tombstone su un id elemento, così i figli inseriti dopo un elemento cancellato rimangono
visibili e ordinati.

Usalo per commenti ordinati, building block di testo/lista collaborativi, log di workflow,
o qualsiasi sequenza append/insert-after che debba convergere senza coordinamento.

#### `crdt_rga_insert`

```text
fn crdt_rga_insert(
    key_ptr: u32,
    key_len: u32,
    parent_ptr: u32,
    parent_len: u32,
    value_ptr: u32,
    value_len: u32,
    out_id_ptr: u32,
    out_id_cap: u32
) -> i32
```

Inserisce `value` dopo `parent`. Usa `parent_len = 0` per inserire in testa. L'host genera
l'id elemento dall'`OpId` locale, lo scrive in `out_id_ptr` e restituisce il numero di byte
dell'id scritti.

SDK:

```rust
use nx_sdk::crdt::rga;

let id = rga::insert_after("comments:doc-1", None, b"primo commento")?;
let reply_id = rga::insert_after("comments:doc-1", Some(&id), b"risposta")?;
```

#### `crdt_rga_delete`

```text
fn crdt_rga_delete(
    key_ptr: u32,
    key_len: u32,
    id_ptr: u32,
    id_len: u32
) -> i32
```

Mette un tombstone sull'elemento identificato da `id` e restituisce `0` in caso di successo.

SDK:

```rust
use nx_sdk::crdt::rga;

rga::delete("comments:doc-1", &reply_id)?;
```

#### `crdt_rga_values`

```text
fn crdt_rga_values(
    key_ptr: u32,
    key_len: u32,
    out_ptr: u32,
    out_cap: u32
) -> i32
```

Scrive i valori visibili in ordine di sequenza deterministico e restituisce il numero di
byte scritti. Gli elementi con tombstone non vengono restituiti. La codifica output raw è:

```text
u32 value_count
repeat value_count times:
  u32 value_len
  u8[value_len] value
```

SDK:

```rust
use nx_sdk::crdt::rga;

let comments = rga::values("comments:doc-1")?;
```

### Esempi CRDT Distribuiti

| CRDT | Esempio | Note |
|------|---------|------|
| PNCounter | `examples/distributed_inventory` | Incremento/decremento inventario |
| LWW-Register | `examples/distributed_status` | Valore di stato singolo |
| ORSet | `examples/distributed_tags` | Tag observed-remove |
| LWW-Map | `examples/distributed_settings` | Impostazioni per campo |
| RGA | `examples/distributed_comments` | Commenti ordinati |

---

## Time API

### `time_now`

```text
fn time_now() -> u64
```

Restituisce il timestamp Unix corrente in millisecondi.

SDK:

```rust
use nx_sdk::time;

let now_ms = time::now();
```

### `time_monotonic`

```text
fn time_monotonic() -> u64
```

Restituisce millisecondi monotoni relativi al processo runtime. Usalo per misurare
il tempo trascorso, non per timestamp wall-clock persistiti.

SDK:

```rust
use nx_sdk::time;

let start = time::monotonic();
let elapsed_ms = time::monotonic() - start;
```

---

## Crypto API

Le Crypto API espongono primitive di casualità e hashing fornite dall'host.

### `random_bytes`

```text
fn random_bytes(out_ptr: u32, out_len: u32) -> i32
```

Riempie `out_ptr` con byte casuali crittograficamente sicuri. Restituisce il numero
di byte scritti.

SDK:

```rust
use nx_sdk::crypto;

let nonce = crypto::random_bytes(16)?;
```

### `hash_sha256`

```text
fn hash_sha256(input_ptr: u32, input_len: u32, out_ptr: u32, out_cap: u32) -> i32
```

Calcola un digest SHA-256 da 32 byte. Restituisce `32` in caso di successo.

### `hash_blake3`

```text
fn hash_blake3(input_ptr: u32, input_len: u32, out_ptr: u32, out_cap: u32) -> i32
```

Calcola un digest BLAKE3 da 32 byte. Restituisce `32` in caso di successo.

SDK:

```rust
use nx_sdk::crypto;

let sha = crypto::hash_sha256(b"payload")?;
let b3 = crypto::hash_blake3(b"payload")?;
```

---

## System API

### `env_get`

```text
fn env_get(key_ptr: u32, key_len: u32, out_ptr: u32, out_cap: u32) -> i32
```

Legge una variabile d'ambiente host consentita. La policy attuale espone solo
variabili uppercase il cui nome inizia con `NX_` o `NUMAX_`.

### `module_id`

```text
fn module_id(out_ptr: u32, out_cap: u32) -> i32
```

Restituisce l'identificatore del modulo corrente fornito dal runtime.

### `host_capabilities`

```text
fn host_capabilities(out_ptr: u32, out_cap: u32) -> i32
```

Restituisce i nomi delle capability UTF-8 separati da `\n`.

### `event_emit`

```text
fn event_emit(name_ptr: u32, name_len: u32, payload_ptr: u32, payload_len: u32) -> i32
```

Emette un evento con nome al runtime. I nomi degli eventi devono essere nomi ASCII
non vuoti che usano lettere, cifre, `_`, `-`, `.` o `:`.

### `abort`

```text
fn abort(msg_ptr: u32, msg_len: u32)
```

Termina l'esecuzione del guest con un messaggio di errore visibile all'host.
L'host trasforma questa chiamata in un trap Wasmtime.

---

## Network API

Le Network API espongono l'introspezione del runtime di sync. Richiedono che la sync
sia abilitata.

### `net_node_id`

```text
fn net_node_id(out_ptr: u32, out_cap: u32) -> i32
```

Restituisce il `NodeId` di sync locale.

### `net_peers`

```text
fn net_peers(out_ptr: u32, out_cap: u32) -> i32
```

Restituisce i peer di sync attualmente connessi.

Codifica output:

```text
u32 peer_count
repeat peer_count times:
  u32 addr_len
  u32 node_id_len
  u8[addr_len] addr
  u8[node_id_len] node_id
```

SDK:

```rust
use nx_sdk::net;

let node_id = net::node_id()?;
let peers = net::peers()?;
```

---

## Logging API

### `host_log_v2`

```text
fn host_log_v2(msg_ptr: u32, msg_len: u32) -> i32
```

Scrive un messaggio di log del guest nello stream di log dell'host. Restituisce `0` in
caso di successo o `ERR_INTERNAL` se il messaggio non può essere letto dalla memoria guest.

SDK:

```rust
use nx_sdk::log;

log("Hello from WASM!");
```

---

## Esempio Completo

```rust
use nx_sdk::{db, log};

#[no_mangle]
pub extern "C" fn run() {
    log("Modulo avviato...");

    db::set("counter", b"0").unwrap();

    if let Ok(Some(value)) = db::get("counter") {
        log(&format!("Counter: {:?}", value));
    }

    db::delete("counter").unwrap();
    log("Modulo completato!");
}
```

---

## Roadmap

Questa sezione è solo un tracker compatto della superficie API. Il roadmap autorevole
del progetto vive in [Roadmap](/it/roadmap/).

### Implementate

- Database: `db_get`, `db_set`, `db_delete`, `db_exists`, `db_scan`,
  `db_scan_after`, `db_keys`, `db_keys_after`
- CRDT: `crdt_gcounter_inc`, `crdt_gcounter_value`, `crdt_pncounter_inc`,
  `crdt_pncounter_dec`, `crdt_pncounter_value`, `crdt_lww_set`,
  `crdt_lww_get`, `crdt_lww_map_set`, `crdt_lww_map_remove`,
  `crdt_lww_map_get`, `crdt_lww_map_contains`, `crdt_lww_map_entries`,
  `crdt_orset_add`, `crdt_orset_remove`, `crdt_orset_contains`,
  `crdt_orset_elements`, `crdt_rga_insert`, `crdt_rga_delete`,
  `crdt_rga_values`
- Time: `time_now`, `time_monotonic`
- Crypto: `random_bytes`, `hash_sha256`, `hash_blake3`
- System: `env_get`, `module_id`, `abort`, `host_capabilities`, `event_emit`
- Introspezione Network: `net_node_id`, `net_peers`

### Pianificate

- Callback/eventi di messaggistica network: `on_peer_connect`, `on_peer_disconnect`,
  `on_message`, `on_timer`
- Le API HTTP/client opzionali rimangono fuori scope finché un modello di capability
  non è formalizzato.