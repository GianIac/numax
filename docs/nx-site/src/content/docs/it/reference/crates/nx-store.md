---
title: nx-store
description: Astrazione del key/value store embedded.
---

`nx-store` è un wrapper sottile e tipizzato su [sled](https://github.com/spacejam/sled),
un database key/value embedded scritto in puro Rust. È l'unico crate nel workspace
che tocca lo storage persistente direttamente. Tutto il resto che ha bisogno di durabilità
passa attraverso di esso.

Non ha async, networking, logica CRDT, né dipendenze dal resto del workspace.
Viene aperto una volta in `nx-core::Runtime::new` e condiviso tramite `Arc<Store>` con
il sync manager e ogni `HostState` prodotto da `run_module`.

---

## Responsabilità

| Responsabilità | Dove |
|---|---|
| Apertura o creazione del database sled | `store.rs` `Store::open` |
| Lettura, scrittura, cancellazione, exists su chiave singola | `store.rs` operazioni base |
| Batch atomico multi-chiave (set + delete) | `store.rs` `Store::apply_batch` |
| Scan per prefisso con cursore offset | `store.rs` `scan_prefix_page` |
| Scan per prefisso con cursore chiave (preferito) | `store.rs` `scan_prefix_page_after` |
| Listing chiavi con cursore offset | `store.rs` `keys_prefix_page` |
| Listing chiavi con cursore chiave (preferito) | `store.rs` `keys_prefix_page_after` |
| Flush esplicito su disco | `store.rs` `Store::flush` |
| Statistiche store (conteggio chiavi, byte totali) | `store.rs` `Store::stats` |
| Tipi errore | `error.rs` `StoreError` |

---

## Store

`Store` è la struct pubblica unica. Avvolge `sled::Db` e implementa `Clone`
perché gli handle sled sono economici da clonare (reference-countano il database sottostante).

```rust
pub struct Store {
    db: sled::Db,  // Clone è economico: sled::Db è un Arc internamente
}
```

### Apertura

```rust
let store = Store::open("./nx-data")?;
```

`Store::open` crea la directory con `fs::create_dir_all` se non esiste.
Se il percorso esiste ma non è una directory, restituisce `StoreError::NotADirectory`.

La directory dello store è di proprietà di un singolo nodo. Ogni nodo deve usare il proprio percorso.

### Operazioni base

```rust
// scrivi
store.set(b"user:1", b"alice")?;

// leggi
let val: Option<Vec<u8>> = store.get(b"user:1")?;

// verifica esistenza
let found: bool = store.exists(b"user:1")?;

// cancella (no-op se la chiave non esiste)
store.delete(b"user:1")?;

// flush esplicito (chiamato allo shutdown)
store.flush()?;
```

`set` non fa flush su disco immediatamente. sled fornisce crash safety tramite il proprio
write-ahead log, ma la chiamata esplicita a `flush` in `Runtime::shutdown_inner` garantisce
che tutte le scritture siano durevoli prima che il processo esca.

### Batch atomico

```rust
store.apply_batch(
    &[(b"new_key", b"new_value")],  // set
    &[b"old_key"],                  // delete
)?;
```

`apply_batch` avvolge set e delete in un singolo `sled::Batch` e lo applica atomicamente.
Usato dal sync manager per persistere stato CRDT e voci dell'op-log insieme.

### Statistiche

```rust
let stats: StoreStats = store.stats()?;
// stats.keys  -> numero totale di chiavi
// stats.bytes -> byte totali (lunghezze chiave + valore, sommate)
```

`stats()` itera l'intero database. Viene usato dall'endpoint di osservabilità.
Non chiamarlo in un hot path.

---

## Prefix scan

Lo store espone quattro varianti di scan. Due includono i valori, due restituiscono solo le chiavi.
Ogni coppia ha una versione con cursore offset (per compatibilità) e una con cursore chiave (preferita).

### Perché i cursori chiave sono preferiti

I cursori offset contano le righe visibili dalla posizione 0 a ogni chiamata. Se una chiave viene
inserita prima dell'offset corrente tra due chiamate paginate, l'offset si sposta e una riga
può essere restituita due volte o saltata del tutto.

I cursori chiave usano `db.range(start_after..)` e saltano finché la chiave non è strettamente
maggiore del cursore. Gli inserimenti prima del cursore non influenzano il risultato.

### scan_prefix_page (cursore offset)

```rust
let page: Vec<(Vec<u8>, Vec<u8>)> = store.scan_prefix_page(
    b"app:",         // prefisso
    0,               // cursore offset (indice riga)
    64,              // dimensione pagina
    Some(b"__nx/"),  // prefisso escluso (passa None se non serve)
)?;
```

### scan_prefix_page_after (cursore chiave, preferito)

```rust
// prima pagina
let page = store.scan_prefix_page_after(b"app:", None, 64, None)?;

// pagina successiva: passa l'ultima chiave della pagina precedente
let last_key = page.last().map(|(k, _)| k.clone());
let page = store.scan_prefix_page_after(b"app:", last_key.as_deref(), 64, None)?;
```

### keys_prefix_page (cursore offset)

```rust
let keys: Vec<Vec<u8>> = store.keys_prefix_page(b"app:", 0, 64, None)?;
```

### keys_prefix_page_after (cursore chiave, preferito)

```rust
let keys = store.keys_prefix_page_after(b"app:", None, 64, None)?;
let keys = store.keys_prefix_page_after(b"app:", last_key.as_deref(), 64, None)?;
```

### excluded_prefix

Tutti e quattro i metodi di scan accettano `excluded_prefix: Option<&[u8]>`.
Il layer host API passa `Some(b"__nx/")` per nascondere le chiavi riservate dal runtime
al codice guest. Passa `None` quando stai scansionando chiavi interne dall'interno di `nx-core`.

---

## Prefisso chiave riservato

Le chiavi sotto `__nx/` sono usate dal runtime per il proprio stato (NodeId, persistenza CRDT, op-log).
L'host API in `nx-core` passa `excluded_prefix = Some(b"__nx/")` a tutte le chiamate scan
così i moduli guest non le vedono mai, e rifiuta le chiamate dirette `db_get`/`db_set`/`db_delete`
su chiavi riservate con codice errore `-4` (`ERR_RESERVED_KEY`).

`nx-store` stesso non fa rispettare questa regola. L'enforcement è in `nx-core/src/host_api/db.rs`.

---

## Tipi errore

```rust
pub enum StoreError {
    Sled(sled::Error),
    Io(std::io::Error),
    NotADirectory(String),
}
```

`Sled` avvolge qualsiasi errore del motore sled.
`Io` avvolge errori filesystem dalla creazione directory.
`NotADirectory` viene restituito quando il percorso esiste ma è un file.

---

## Copertura test

I test si trovano in `lib.rs` (`#[cfg(test)]`), oltre a test di integrazione in `tests/`.
Tutti i test usano `tempfile::tempdir()` per l'isolamento.

| Test | Cosa copre |
|---|---|
| `test_set_and_get` | roundtrip base scrittura/lettura |
| `test_get_nonexistent` | chiave mancante restituisce None |
| `test_exists` | exists() prima del set, dopo il set, dopo il delete |
| `test_overwrite` | il secondo set sostituisce il primo valore |
| `test_delete` | delete fa restituire None alla chiave |
| `test_multiple_keys` | chiavi indipendenti non interferiscono |
| `test_apply_batch_sets_and_deletes_atomically` | batch imposta nuova chiave e rimuove quella vecchia |
| `test_scan_prefix_page_paginates_visible_keys` | cursore offset pagina correttamente |
| `test_scan_prefix_page_excludes_reserved_prefix` | chiave `__nx/` è nascosta quando esclusa |
| `test_scan_prefix_page_after_uses_key_cursor` | cursore chiave pagina correttamente |
| `test_scan_prefix_page_after_does_not_shift_when_key_is_inserted_before_cursor` | cursore chiave è stabile contro gli inserimenti |
| `test_keys_prefix_page_paginates_visible_keys` | cursore offset solo chiavi |
| `test_keys_prefix_page_excludes_reserved_prefix` | chiave `__nx/` nascosta nel scan solo chiavi |
| `test_keys_prefix_page_after_uses_key_cursor` | cursore chiave solo chiavi |
| `test_stats_counts_keys_and_bytes` | stats conta chiavi e lunghezze byte correttamente |

```bash
cargo test -p nx-store
```

---

## Correlati

Leggi questa pagina insieme ai docs runtime e storage esposti all'utente:

- [Panoramica crate](/numax/it/reference/crates/) - dove `nx-store` si inserisce nel grafo delle dipendenze
- [Crate nx-core](/numax/it/reference/crates/nx-core/) - apre e condivide lo `Store`
- [Host API](/numax/it/reference/host-api/) - funzioni `db_*` che chiamano lo store passando da `nx-core`
- [Configurazione](/numax/it/reference/configuration/) - `[storage].datastore_path` che diventa il percorso dello store
