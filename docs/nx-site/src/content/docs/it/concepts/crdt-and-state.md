---
title: CRDT e stato
description: Come lo stato replicato converge tra i nodi Numax.
---

Questa pagina spiega cosa sono i CRDT, perché esistono, come li usa Numax, e a cosa serve ciascun tipo. Alla fine saprai esattamente quale usare e perché la convergenza è garantita senza coordinamento.

---

## Il problema che i CRDT risolvono

Immagina due nodi, ciascuno che esegue il tuo modulo. Entrambi offline. Entrambi accettano scritture. Quando si riconnettono, i loro stati sono diversi.

Con un database tradizionale, devi scegliere un vincitore, eseguire una procedura di merge, o rifiutare una delle scritture. Qualcuno deve coordinare. Il coordinamento richiede connettività. E se la rete è inaffidabile, la tua applicazione è inaffidabile.

I CRDT eliminano questo problema completamente. Un **Conflict-free Replicated Data Type** è una struttura dati progettata affinché due copie qualsiasi, indipendentemente da quanto siano diverse, possano sempre essere unite in un risultato coerente e in automatico senza chiedere il permesso a nessuno, senza un coordinatore e senza un lock.

---

## Le tre proprietà che lo rendono possibile

Ogni CRDT in Numax soddisfa tre proprietà matematiche:

**Commutatività.** L'ordine in cui applichi gli aggiornamenti non conta.

```
merge(A, B) == merge(B, A)
```

Il Nodo 1 riceve l'op dal Nodo 2, poi quella dal Nodo 3.
Il Nodo 2 riceve l'op dal Nodo 3, poi quella dal Nodo 1.
Convergono allo stesso stato.

**Associatività.** Come raggruppi i merge non conta.

```
merge(merge(A, B), C) == merge(A, merge(B, C))
```

Puoi fare merge in batch, in parallelo, incrementalmente. Il risultato è sempre lo stesso.

**Idempotenza.** Mergiare lo stesso stato due volte non cambia nulla.

```
merge(A, A) == A
```

Se lo stesso stato viene mergiato due volte, lo stato non si corrompe. L'applicazione delle operazioni è gestita separatamente: le op replicate vengono deduplicate per `OpId` prima di essere applicate.

Queste tre proprietà insieme significano: **qualsiasi nodo può mergiare stato replicato in qualsiasi ordine e convergere allo stesso stato di ogni altro nodo che ha visto gli stessi aggiornamenti.**

Tutte e tre sono testate esplicitamente in `nx-sync`:

```bash
cargo test -p nx-sync
# test_gcounter_merge_commutativity
# test_gcounter_merge_associativity
# test_gcounter_merge_idempotency
```

---

## Come le op scorrono nel sistema

Quando un modulo guest chiama una funzione host CRDT, ecco cosa succede:

```
guest chiama crdt_gcounter_inc("visits", 1)
       │
       ▼
host valida input, legge NodeId da HostState
       │
       ▼
Host API applica op allo stato CRDT in-memory
       │
       ├── persiste stato CRDT aggiornato / valore materializzato in sled (sotto __nx/)
       │
       └── accoda op per broadcast
              │
              ▼
         broadcast loop registra metadata seen-op + op-log
              │
              ▼
         nx-net invia PushOps a ogni peer
              │
              ▼
         il peer riceve PushOps, controlla OpId contro il set seen-ops
              │
              ├── se già visto: scarta
              │
              └── se nuovo: applica allo stato CRDT in-memory del peer
                            persiste stato aggiornato e metadata op-log
                            segna OpId come visto
```

Il modulo non aspetta mai i peer ma l'unica cosa che aspetta è il push nel canale del sync manager. La propagazione ai peer avviene asincronamente in background.

---

## Op-log e deduplicazione

Ogni operazione CRDT è un `Op`:

```rust
pub struct Op {
    pub id:     OpId,    // UUID v4, globalmente unico
    pub origin: NodeId,  // nodo che ha generato questa op
    pub kind:   OpKind,  // quale CRDT, quale chiave, quali dati
}
```

L'op-log è una lista limitata di valori `Op` persistiti in sled sotto `__nx/crdt/op-log/`. Serve per anti-entropy e catch-up tra peer. Lo stato CRDT viene anche persistito sotto chiavi per tipo `__nx/crdt/state/...`, con alcuni valori materializzati mantenuti sotto prefissi storici `__nx/crdt/...` per compatibilità. Al riavvio, il runtime idrata il registry CRDT in-memory da quelle chiavi di stato durevole, quindi lo stato CRDT sopravvive ai riavvii anche se l'op-log è limitato.

Il set seen-ops è un `HashSet<OpId>` limitato (cappato a `seen_ops_limit`, default 100.000). Prima di applicare un'op remota, il sync manager controlla se il suo `OpId` è già nel set. Se sì: scarta. Se no: applica e aggiungi al set.

Questo è il meccanismo che rende pratico il delivery delle operazioni: le proprietà matematiche di merge coprono la convergenza dello stato, e il seen-ops set evita di applicare più volte delta operazionali non idempotenti.

---

## I sei CRDT

Numax include sei tipi CRDT. Ciascuno risolve un problema specifico. Eccoli senza ambiguità su quando usare quale.

---

### GCounter - il contatore grow-only

**Usalo per:** totali che possono solo aumentare. Visite di pagina, conteggi eventi, like, task completati, byte trasferiti.

**Come funziona:** ogni nodo possiede il proprio slot `u64`. Un nodo può solo incrementare il proprio slot. Il totale è la somma di tutti gli slot. Il merge prende il massimo di ogni slot.

```
Slot Nodo A: 10
Slot Nodo B:  7
             ---
Totale:       17
```

Se lo slot del Nodo A sul Nodo B mostra 8 ma il Nodo A ha effettivamente 10, il merge userà 10. Uno slot non può mai diminuire. Questo rende gli incrementi concorrenti sicuri senza coordinamento.

```rust
use nx_sdk::crdt::gcounter;

gcounter::inc("counter:visits", 1)?;
let total: u64 = gcounter::value("counter:visits")?;
```

---

### PNCounter - il contatore che va in entrambe le direzioni

**Usalo per:** qualsiasi cosa che aumenta e diminuisce. Livelli di inventario, saldi di conto, sessioni attive, letture di temperatura come delta.

**Come funziona:** due GCounter internamente. Uno traccia gli incrementi (`P`), uno traccia i decrementi (`N`). Il valore visibile è `P - N`. Il merge gestisce ogni GCounter indipendentemente.

```
Slot locali Nodo A: P[A]=10, N[A]=3  ->  valore = 7
Slot locali Nodo B: P[B]=5,  N[B]=8  ->  valore = -3
Dopo merge:
  slot P = { A: 10, B: 5  } -> somma P = 15
  slot N = { A: 3,  B: 8  } -> somma N = 11
  valore = 15 - 11 = 4
```

L'overflow è gestito con `saturating_add` e il valore finale `i64` è clampato a `[i64::MIN, i64::MAX]`.

```rust
use nx_sdk::crdt::pncounter;

pncounter::inc("inventory:sku-1", 10)?;
pncounter::dec("inventory:sku-1", 3)?;
let available: i64 = pncounter::value("inventory:sku-1")?;
```

---

### LWW-Register - l'ultimo che scrive vince

**Usalo per:** un singolo valore per chiave dove l'ultima scrittura deve vincere. Stato utente, impostazioni di configurazione, posizione corrente, feature flag.

**Come funziona:** il registro memorizza `(value, timestamp_ms, writer_node_id)`. Quando due scritture sono in conflitto, quella con il timestamp più alto vince. Se i timestamp sono uguali, vince il NodeId lessicograficamente maggiore. È deterministico: entrambi i lati scelgono sempre lo stesso vincitore.

```
Nodo A scrive "online" a t=100
Nodo B scrive "away"   a t=150
Dopo merge: "away"  (t=150 vince)

Nodo A scrive "online" a t=100
Nodo B scrive "away"   a t=100
Dopo merge: vince il nodo con NodeId lessicograficamente maggiore
```

Il tiebreaker è il motivo per cui i clock non devono essere perfettamente sincronizzati. Anche se due scritture avvengono nello stesso millisecondo, il risultato è deterministico.

```rust
use nx_sdk::crdt::lww_register;

lww_register::set("status:user-1", b"online")?;
let status: Option<Vec<u8>> = lww_register::get("status:user-1")?;
```

---

### LWW-Map - una mappa di LWW-register indipendenti

**Usalo per:** un insieme di impostazioni o proprietà con nome dove ogni campo si evolve indipendentemente. Configurazione servizio, preferenze utente, mappe di metadati.

**Come funziona:** ogni campo nella mappa è il proprio LWW-register. I campi vengono mergiati indipendentemente. Le rimozioni sono tombstone: una `remove` al timestamp `t` vince su qualsiasi `set` al timestamp `< t`, ma perde contro un `set` al timestamp `> t`. Un campo rimosso può essere resuscitato da una scrittura successiva.

```
Nodo A: { "theme": "dark" a t=100 }
Nodo B: { "theme": "light" a t=200, "region": "eu" a t=100 }
Dopo merge: { "theme": "light", "region": "eu" }

Nodo A: { "theme": "dark" a t=100 }
Nodo B: rimuove "theme" a t=200
Dopo merge: "theme" è sparito (tombstonato)
```

```rust
use nx_sdk::crdt::lww_map;

lww_map::set("settings:svc-a", "theme", b"dark")?;
lww_map::remove("settings:svc-a", "region")?;
let val: Option<Vec<u8>>        = lww_map::get("settings:svc-a", "theme")?;
let all: Vec<(String, Vec<u8>)>  = lww_map::entries("settings:svc-a")?;
```

---

### ORSet - il set che gestisce le rimozioni concorrenti correttamente

**Usalo per:** set di stringhe dove gli elementi possono essere aggiunti e rimossi concorrentemente da nodi diversi. Tag attivi, dispositivi connessi, opzioni selezionate, ruoli utente.

**Perché non un semplice flag?** Un semplice flag booleano "presente/rimosso" si rompe sotto operazioni concorrenti. Se il Nodo A aggiunge "blue" e il Nodo B rimuove "blue" nello stesso momento, e la rimozione vince, l'aggiunta del Nodo A viene persa silenziosamente. Non è corretto.

**Come funziona:** ogni `add` porta un tag unico (l'OpId). Una `remove` porta l'insieme dei tag che ha osservato per quell'elemento. Il merge fa l'unione di add e remove. Un elemento è visibile se ha almeno un add-tag che non è stato rimosso.

```
Nodo A aggiunge "blue" con tag "op-1"
Nodo B aggiunge "blue" con tag "op-2"
Nodo B rimuove "blue", osservando solo "op-1"
Dopo merge: "blue" è ancora visibile (il tag "op-2" non è stato rimosso)

Nodo A aggiunge "blue" con tag "op-1"
Nodo A rimuove "blue", osservando "op-1"
Dopo merge: "blue" è sparito (tutti i tag rimossi)
```

Gli add concorrenti di nodi diversi sopravvivono sempre a una remove che non li ha osservati. Questo è l'"observed-remove" nel nome.

```rust
use nx_sdk::crdt::orset;

orset::add("tags:item-1", "blue")?;
orset::remove("tags:item-1", "blue")?;
let has_blue: bool        = orset::contains("tags:item-1", "blue")?;
let all_tags: Vec<String> = orset::elements("tags:item-1")?;
```

---

### RGA - la sequenza ordinata

**Usalo per:** liste ordinate dove gli inserimenti concorrenti nella stessa posizione devono convergere allo stesso ordine. Documenti collaborativi, code ordinate, thread di commenti, voci di log.

**Come funziona:** ogni elemento ha un `id` stabile globalmente unico e un `parent_id` opzionale (l'elemento dopo cui è stato inserito). La sequenza viene ricostruita seguendo i link padre. Quando due inserimenti avvengono nella stessa posizione (stesso padre), l'elemento con l'`id` lessicograficamente più piccolo viene prima. È deterministico indipendentemente dall'ordine di arrivo.

I delete sono tombstone: l'id dell'elemento viene aggiunto a un set di id eliminati. L'elemento rimane nella struttura dati (così i link padre restano validi) ma è invisibile in `values()`. I figli degli elementi cancellati rimangono visibili.

```
insert "a" in testa -> id="op-1"
insert "b" dopo "op-1" -> id="op-2"
insert "c" dopo "op-2" -> id="op-3"
values: ["a", "b", "c"]

delete "op-2"
values: ["a", "c"]
ordered_ids (inclusi tombstone): ["op-1", "op-2", "op-3"]
```

```rust
use nx_sdk::crdt::rga;

let id1 = rga::insert_after("comments:doc-1", None, b"primo commento")?;
let id2 = rga::insert_after("comments:doc-1", Some(&id1), b"risposta")?;
rga::delete("comments:doc-1", &id2)?;
let visible: Vec<Vec<u8>> = rga::values("comments:doc-1")?;
```

---

## Quale usare

| Situazione | CRDT |
|---|---|
| Contare cose, non si rimuovono mai | GCounter |
| Contare cose, può salire e scendere | PNCounter |
| Memorizzare un valore, l'ultima scrittura vince | LWW-Register |
| Memorizzare molti valori con nome, ognuno indipendente | LWW-Map |
| Mantenere un set, add/remove concorrenti | ORSet |
| Mantenere una lista ordinata, inserimenti concorrenti | RGA |

In caso di dubbio: se i valori nella tua struttura dati sono indipendenti l'uno dall'altro, LWW è quasi sempre la risposta giusta. Se hai bisogno di una collezione dove l'appartenenza conta sotto operazioni concorrenti, ORSet. Se l'ordine conta, RGA.

---

## Cosa i CRDT non risolvono

I CRDT garantiscono la convergenza dello stato, non la correttezza della logica applicativa.

Se il tuo modulo legge un valore GCounter e prende una decisione di business basata su di esso, quella decisione è aggiornata solo quanto l'ultima volta che la sync ha girato. I CRDT non ti danno linearizzabilità o consistenza forte. Ti danno **consistenza eventuale**: dato tempo sufficiente e connettività, tutti i nodi convergono.

Se hai bisogno di garanzie forti come "questo valore deve essere esattamente X prima che io proceda", i CRDT non sono il primitivo giusto. Usa lo store locale per quello, combinato con qualsiasi meccanismo di coordinamento la tua applicazione necessiti.

---

## Correlati

- [Modello runtime](/numax/it/concepts/runtime-model/) - come le op CRDT scorrono nel sistema
- [Protocollo gossip](/numax/it/concepts/gossip-protocol/) - come le op si propagano tra i nodi
- [Crate nx-sync](/numax/it/reference/crates/nx-sync/) - implementazioni CRDT e proprietà matematiche
- [Host API](/numax/it/reference/host-api/) - tutte le 19 funzioni host CRDT
- [Wrapper CRDT nx-sdk](/numax/it/reference/crates/nx-sdk/) - l'API lato guest
