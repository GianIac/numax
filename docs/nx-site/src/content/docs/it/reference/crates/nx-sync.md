---
title: nx-sync
description: Tipi CRDT, operazioni e identificatori nodo.
---

`nx-sync` è il nucleo di logica pura del modello di replica Numax.
Possiede strutture dati CRDT, tipi di operazione, identificatori nodo e serializzazione.
Non ha I/O, async, networking, persistenza. Non dipende da nessun altro crate nel workspace.

Questo è l'unico crate su cui puoi ragionare, testare e verificare in isolamento.
Se un merge CRDT è sbagliato, il bug è qui. Se un'operazione è malformata, il bug è qui.
Tutto il resto sposta solo le op in giro.

---

## Responsabilità

| Responsabilità | Dove |
|---|---|
| Tipo `NodeId` e generazione | `node_id.rs` |
| Tipo `OpId` (UUID v4) | `op.rs` |
| Enum `OpKind` (una variante per operazione) | `op.rs` |
| Struct `Op` e metodi builder | `op.rs` |
| Serializzazione JSON per le op | `op.rs` `to_json`, `to_bytes`, `from_json`, `from_bytes` |
| Stato e merge GCounter | `crdt/gcounter.rs` |
| Stato e merge PNCounter | `crdt/pncounter.rs` |
| Stato e merge LWW-Register | `crdt/lww_register.rs` |
| Stato e merge LWW-Map | `crdt/lww_map.rs` |
| Stato e merge ORSet | `crdt/orset.rs` |
| Stato e merge RGA | `crdt/rga.rs` |
| Tipi errore | `error.rs` |

---

## NodeId

`NodeId` è un newtype su `String`. Identifica un nodo nel cluster.

```rust
// da stringa fissa (test, config)
let id = NodeId::new("node-a");

// UUID v4 casuale (usato da Runtime::new al primo avvio senza TLS)
let id = NodeId::generate();

id.as_str()   // &str
id.to_string() // String via Display
```

`NodeId` implementa `Hash`, `Eq`, `Serialize`, `Deserialize`, `From<&str>`, `From<String>`.

In modalità TLS, il NodeId non è casuale. Viene derivato dal SHA-256 dei byte
`SubjectPublicKeyInfo` del certificato X.509 del nodo: primi 16 byte dell'hash,
codificati come stringa hex lowercase da 32 caratteri.
Questa derivazione vive in `nx-net/src/tls.rs` (`derive_protocol_node_id_from_cert`).
`nx-sync` stesso memorizza e confronta i NodeId solo come stringhe opache.

---

## OpId

`OpId` è un newtype su `String`. Ogni operazione ha un id globalmente unico.

```rust
let id = OpId::generate();  // UUID v4
let id = OpId::new("custom-id");  // da stringa (test, replay)
id.as_str()  // &str
```

`OpId` implementa `Hash`, `Eq`, `Serialize`, `Deserialize`, `Display`.

Gli OpId sono usati per la deduplicazione nel sync manager. Il set seen-ops in `nx-core`
traccia gli OpId per evitare di applicare la stessa op due volte. `nx-sync` stesso non
lo fa rispettare. Il commento in `GCounter::apply_op` è esplicito: i chiamanti devono
deduplicare prima di chiamare.

Per ORSet e RGA, l'OpId è anche usato come tag elemento o id elemento:
`Op::orset_add_with_op_id_tag` e `Op::rga_insert_with_op_id` generano l'identità
dell'elemento dall'id dell'op stessa, così gli id sono stabili senza un'allocazione separata.

---

## OpKind

`OpKind` è un enum con una variante per tipo di operazione. Ogni variante porta
la chiave e i dati necessari ad applicare l'operazione al CRDT corrispondente.

```rust
pub enum OpKind {
    GCounterIncrement  { key: String, increment: u64 },
    PNCounterIncrement { key: String, increment: u64 },
    PNCounterDecrement { key: String, decrement: u64 },
    LwwRegisterSet     { key: String, value: Vec<u8>, timestamp_ms: u64 },
    LwwMapSet          { key: String, field: String, value: Vec<u8>, timestamp_ms: u64 },
    LwwMapRemove       { key: String, field: String, timestamp_ms: u64 },
    ORSetAdd           { key: String, element: String, tag: String },
    ORSetRemove        { key: String, element: String, observed_tags: Vec<String> },
    RgaInsert          { key: String, id: String, parent: Option<String>, value: Vec<u8> },
    RgaDelete          { key: String, id: String },
}
```

Il metodo `apply_op` di ogni CRDT fa match sulle proprie varianti e restituisce `Ok(false)`
per tutte le altre. Questo è il modo in cui una singola op viene instradata al CRDT giusto
senza una dispatch table.

---

## Op

`Op` è l'operazione completa, pronta per il wire.

```rust
pub struct Op {
    pub id:     OpId,    // globalmente unico
    pub origin: NodeId,  // nodo che ha generato questa op
    pub kind:   OpKind,  // tipo e payload
}
```

### Metodi builder

```rust
Op::gcounter_increment(origin, "counter:visits", 1)
Op::pncounter_increment(origin, "inventory:sku-1", 10)
Op::pncounter_decrement(origin, "inventory:sku-1", 3)
Op::lww_register_set(origin, "status:user-1", b"online".to_vec(), timestamp_ms)
Op::lww_map_set(origin, "settings:svc-a", "theme", b"dark".to_vec(), timestamp_ms)
Op::lww_map_remove(origin, "settings:svc-a", "region", timestamp_ms)
Op::orset_add(origin, "tags:item-1", "blue", "tag-esplicito")
Op::orset_add_with_op_id_tag(origin, "tags:item-1", "blue")   // tag = op.id
Op::orset_remove(origin, "tags:item-1", "blue", observed_tags)
Op::rga_insert(origin, "comments:doc-1", "elem-id", Some("parent-id"), b"testo")
Op::rga_insert_with_op_id(origin, "comments:doc-1", Some("parent-id"), b"testo")  // id = op.id
Op::rga_delete(origin, "comments:doc-1", "elem-id")
```

### Serializzazione

```rust
let json: String   = op.to_json()?;
let bytes: Vec<u8> = op.to_bytes()?;   // byte JSON
let op = Op::from_json(&json)?;
let op = Op::from_bytes(&bytes)?;
```

Sia `to_bytes` che `from_bytes` usano JSON sotto (via `serde_json`).
La serializzazione bincode per il trasporto wire è gestita in `nx-net` usando serde derive direttamente.
I due percorsi di serializzazione sono indipendenti e sono entrambi testati.

---

## Proprietà CRDT

Tutte le implementazioni CRDT in questo crate soddisfano le tre proprietà richieste per
il merge conflict-free:

| Proprietà | Significato | Come verificata |
|---|---|---|
| Commutatività | `merge(a, b) == merge(b, a)` | test: `merge_commutativity` in ogni CRDT |
| Associatività | `merge(merge(a,b),c) == merge(a,merge(b,c))` | test: `merge_associativity` in ogni CRDT |
| Idempotenza | `merge(a, a) == a` | test: `merge_idempotency` in ogni CRDT |

Queste proprietà garantiscono che qualsiasi nodo possa ricevere op in qualsiasi ordine,
qualsiasi numero di volte, e convergere allo stesso stato di ogni altro nodo.

---

## GCounter

Contatore grow-only. Ogni nodo possiede il proprio slot `u64`. Il valore convergente è la somma.

```
stato: HashMap<NodeId, u64>
merge: prendi il max di ogni slot
valore: somma di tutti gli slot
```

```rust
let mut c = GCounter::new();
c.increment(&node, 5);
let total: u64 = c.value();
let node_slot: u64 = c.value_for(&node);

c.merge(&other);
let merged = c.merged_with(&other);
let changed: bool = c.apply_op(&op)?;

let json = c.to_json()?;
let c2 = GCounter::from_json(&json)?;
```

I valori degli slot usano `saturating_add` per prevenire overflow. Il merge prende `max` per slot,
quindi uno stato vecchio da un nodo riavviato non può mai decrementare il contatore di un altro nodo.

`apply_op` non è idempotente. Applicare lo stesso `OpId` due volte incrementa due volte.
Il sync manager deduplica per OpId prima di chiamare.

---

## PNCounter

Contatore positivo/negativo. Internamente due `GCounter`: uno per gli incrementi, uno per i decrementi.
Il valore convergente è `sum(positive) - sum(negative)`, clamped a `i64`.

```
stato: positive: GCounter, negative: GCounter
merge: merge entrambi i GCounter indipendentemente
valore: positive.sum() - negative.sum()  (clamped a i64)
```

```rust
let mut c = PNCounter::new();
c.increment(&node, 10);
c.decrement(&node, 3);
let v: i64 = c.value();           // 7
let pos: u64 = c.positive_for(&node);
let neg: u64 = c.negative_for(&node);
```

L'overflow dello slot satura: se `u64::MAX` viene incrementato di 1, lo slot rimane `u64::MAX`.
Il valore finale `i64` viene clampato a `[i64::MIN, i64::MAX]` tramite `clamp_i128_to_i64`.

---

## LwwRegister

Registro last-writer-wins. Memorizza una singola tripla `(value, timestamp_ms, writer)`.

```
stato: value: Vec<u8>, timestamp_ms: u64, writer: NodeId
merge: mantieni l'entry che vince l'ordinamento LWW
```

**Ordine di risoluzione conflitti:**
1. `timestamp_ms` maggiore vince.
2. Timestamp uguali: la stringa `NodeId` lessicograficamente maggiore vince.

Questo è deterministico e non richiede coordinamento. Entrambi i lati di un merge
scelgono sempre lo stesso vincitore.

```rust
let mut r = LwwRegister::new(b"online".to_vec(), 100, NodeId::new("node-a"));
let changed: bool = r.merge(&other);
let changed: bool = r.assign(b"away".to_vec(), 200, NodeId::new("node-b"));

r.value()        // &[u8]
r.value_bytes()  // Vec<u8>
r.timestamp_ms() // u64
r.writer()       // &NodeId
```

`assign` applica lo stesso ordinamento LWW di `merge`. Viene usato dall'host API
quando il guest chiama `crdt_lww_set` per garantire che le scritture locali siano
monotone rispetto allo stato attualmente osservato.

---

## LwwMap

Una mappa dove ogni campo segue la semantica LWW-register. Le rimozioni sono memorizzate come
tombstone (una `LwwMapRemove` op con un timestamp, applicata come entry vincente con valore `None`).

```
stato: BTreeMap<campo, LwwMapEntry { value: Option<Vec<u8>>, timestamp_ms, writer }>
merge: LWW per campo
```

I campi tombstonati non vengono restituiti da `entries()`. Un add vecchio riapplicato dopo una
remove non può resuscitare un campo tombstonato se il timestamp della remove è maggiore.

```rust
lww_map.set("theme", b"dark", timestamp_ms, writer);
lww_map.remove("region", timestamp_ms, writer);
let val: Option<&[u8]>          = lww_map.get("theme");
let exists: bool                 = lww_map.contains("theme");
let all: Vec<(String, Vec<u8>)>  = lww_map.entries(); // solo visibili
lww_map.merge(&other);
```

---

## ORSet

Set observed-remove di stringhe. Lo stato è due mappe: `adds` e `removes`,
entrambe `BTreeMap<elemento, BTreeSet<tag>>`.

```
stato: adds: BTreeMap<String, BTreeSet<String>>,
       removes: BTreeMap<String, BTreeSet<String>>
merge: unione di entrambe le mappe
visibile: un elemento è visibile sse ha almeno un add-tag non in removes
```

Una remove porta gli add-tag osservati localmente per quell'elemento. Gli add concorrenti
che hanno usato tag diversi e non sono stati osservati restano visibili dopo il merge.

```rust
let mut s = ORSet::new();
s.add("blue", "tag-1");
s.add("blue", "tag-2");
let observed = s.remove("blue");   // restituisce ["tag-1", "tag-2"]
s.apply_remove("blue", observed);
let visible: bool        = s.contains("blue");
let all: Vec<String>     = s.elements(); // ordinato (ordinamento BTreeMap)
let tags: Vec<String>    = s.observed_tags("blue");
s.merge(&other);
```

`elements()` restituisce elementi in ordine deterministico BTree. `add` restituisce `false`
se il tag era già presente (idempotente per lo stesso tag).

---

## RGA

Replicated Growable Array. Sequenza ordinata di valori byte. Ogni elemento ha un `String` id stabile
e un id padre opzionale. I delete sono memorizzati in un set di tombstone per id; i figli degli elementi cancellati restano visibili.

```
stato: elements: BTreeMap<RgaElementId, RgaElement { id, parent, value }>
       tombstones: BTreeSet<RgaElementId>
insert: aggiunge un element id globalmente unico con padre opzionale
delete: aggiunge l'element id al set di tombstone
values: elementi visibili in ordine di sequenza, tombstone esclusi
```

```rust
let inserted = rga.insert("elem-1", None::<String>, b"primo");      // insert in testa
let inserted = rga.insert("elem-2", Some("elem-1"), b"risposta");   // insert dopo il primo
rga.delete("elem-2");

let visible: Vec<Vec<u8>> = rga.values();
rga.merge(&other);
```

Il merge RGA risolve gli insert concorrenti nella stessa posizione padre in modo deterministico
usando l'id elemento come tiebreaker, garantendo che tutti i nodi convergano alla stessa sequenza.

---

## Serializzazione

Tutte le struct CRDT e il tipo `Op` derivano `Serialize`/`Deserialize` via serde.

Per le op, `nx-sync` usa JSON (`serde_json`) per i propri helper `to_json`/`from_json`.
Il trasporto wire in `nx-net` usa bincode o JSON via serde derive direttamente.
I due percorsi di serializzazione sono indipendenti e sono entrambi testati.

La persistenza dello stato per i CRDT (durevole su `nx-store`) passa anch'essa per JSON,
gestita dal sync manager in `nx-core`.

---

## Tipi errore

```rust
pub enum SyncError {
    Serialization(serde_json::Error),  // fallimento parse/serialize JSON
    UnknownNode(String),
    DuplicateOp(String),
    InvalidOp(String),
}
```

---

## Come aggiungere un nuovo CRDT (guida per developer)

1. Aggiungi la struct di stato in un nuovo file `crdt/my_crdt.rs`.
2. Implementa `new`, `apply_op`, `merge`, `merged_with`, `to_json`, `from_json`.
3. Aggiungi le nuove varianti `OpKind` a `op.rs`.
4. Aggiungi i metodi builder a `Op` in `op.rs`.
5. Aggiungi `pub mod my_crdt` a `crdt/mod.rs` e re-esporta da `lib.rs`.
6. Aggiungi il CRDT al registry in `nx-core/src/sync_manager.rs`.
7. Aggiungi le funzioni host API in `nx-core/src/host_api/crdt.rs`.
8. Aggiungi i wrapper SDK in `nx-sdk/src/crdt/my_crdt.rs`.
9. Scrivi i test: commutatività, associatività, idempotenza, `apply_op`, roundtrip JSON.
10. Scrivi un test E2E in `sync_manager.rs`.
11. Scrivi un esempio distribuito in `examples/distributed_my_crdt/`.
12. Verifica la regola di completamento dalla doc Host API prima di marcarlo come fatto.

---

## Copertura test

I test vivono dentro ogni file sorgente nei blocchi `#[cfg(test)]`.

| Gruppo test | Cosa copre |
|---|---|
| `node_id` | generazione unica, from-string, roundtrip serde |
| `op` | tutti i costruttori builder, roundtrip JSON/bytes per ogni OpKind |
| `gcounter` | init zero, increment, nodi multipli, merge (max), commutatività, associatività, idempotenza, apply_op, roundtrip JSON, saturazione overflow |
| `pncounter` | init zero, inc+dec, valore negativo, nodi multipli, merge (max slot), commutatività, associatività, idempotenza, apply op inc/dec, ignora wrong op kind, saturazione overflow, clamping i64 |
| `lww_register` | new, merge più recente vince, merge più vecchio ignorato, tiebreaker NodeId timestamp uguale, ordinamento assign, commutatività, associatività, idempotenza, roundtrip JSON |
| `lww_map` | set, remove, get, contains, entries (tombstone esclusi), merge, proprietà |
| `orset` | vuoto, add visibile, tag duplicato idempotente, remove nasconde tag, remove di unseen, add concorrente sopravvive observed-remove, remove full-observe nasconde, commutatività, associatività, idempotenza, roundtrip JSON |
| `rga` | insert in testa, insert dopo, delete, ordine values, merge, proprietà |

```bash
cargo test -p nx-sync
```

---

## Correlati

Leggi questa pagina insieme ai layer che creano, persistono e trasportano le operazioni sync:

- [Panoramica crate](/numax/it/reference/crates/) - dove `nx-sync` si inserisce nel grafo delle dipendenze
- [Crate nx-core](/numax/it/reference/crates/nx-core/) - il `SyncManager` che applica, deduplica e persiste le op
- [Crate nx-net](/numax/it/reference/crates/nx-net/) - trasporto wire per `Op`
- [Host API](/numax/it/reference/host-api/) - funzioni guest-facing che creano op CRDT
