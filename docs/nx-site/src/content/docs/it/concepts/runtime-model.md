---
title: Runtime-model
description: Come Numax esegue moduli, gestisce lo stato locale e la filosofia del modello runtime.
---

Il Runtime-model di Numax è costruito su una premessa: **un'applicazione distribuita non dovrebbe aver bisogno di un'infrastruttura distribuita per funzionare**.

Ogni capacità di cui un modulo ha bisogno: compute, stato, sincronizzazione vive nello stesso processo, sullo stesso nodo. Non c'è un database remoto da interrogare, nessun broker attraverso cui instradare, nessun orchestratore da consultare. Il nodo è autosufficiente.

---

## Tre componenti, e solo tre

Numax integra tre cose, e deliberatamente nient'altro:

```
 ┌──────────────────────────────────────────┐
 │           Modulo WASM (guest)            │
 │        compilato con nx-sdk              │
 └─────────────────┬────────────────────────┘
                   │  Host API (namespace "nx")
                   ▼
 ┌──────────────────────────────────────────┐
 │              nx-core (host)              │
 │  ┌──────────┐  ┌──────────┐  ┌────────┐ │
 │  │ Wasmtime │  │ Host API │  │  WASI  │ │
 │  └──────────┘  └────┬─────┘  └────────┘ │
 └───────────────────┬─┼────────────────────┘
                     │ │
          ┌──────────┘ └──────────┐
          ▼                       ▼
 ┌────────────────┐     ┌──────────────────────┐
 │   nx-store     │     │  nx-sync + nx-net     │
 │  sled (locale) │◄────┤  CRDT + gossip + TLS  │
 └────────────────┘     └──────────────────────┘
```

**1. Esecuzione** - un modulo WASM gira in una sandbox. Non ha accesso al filesystem, alla rete o alle risorse di sistema se non quelle che l'host espone esplicitamente tramite il namespace `nx`. L'isolamento è strutturale, non configurato.

**2. Stato locale** - un database sled embedded vive dentro il runtime. Letture e scritture sono locali, senza hop di rete, e sled offre garanzie transazionali per le operazioni singole. Non c'è una connessione da aprire. Lo store è semplicemente lì.

**3. Sincronizzazione distribuita** - quando la sync è abilitata, una porzione dello stato viene replicata tra i nodi usando CRDT e un protocollo gossip. Questo è opt-in: un nodo senza `--listen` funziona perfettamente come nodo standalone. Quando la sync è attiva, la convergenza è garantita matematicamente dalle proprietà CRDT, non da lock o consensus.

---

## Il confine host/guest

Il modulo e il runtime vivono in mondi diversi. Il modulo è un binario `.wasm`: un'unità di calcolo portabile, sandboxata, indipendente dall'architettura. Il runtime è un processo Rust nativo. Comunicano attraverso la Host API.

Ogni funzione Host API segue la stessa convenzione:

- puntatori e lunghezze vengono passati come offset `u32` nella memoria lineare WASM
- i codici di ritorno sono `i32`: valori non negativi indicano successo, valori negativi portano codici di errore
- il guest gestisce gli errori deterministicamente

```
Codice guest (Rust, via nx-sdk)
    │
    │  wrapper sicuro (es. db::set("key", b"value"))
    ▼
Chiamata FFI  (unsafe extern "C" da ffi.rs)
    │
    │  confine memoria lineare WASM
    ▼
Funzione host (Rust in nx-core)
    │
    │  scrive su sled / invia al SyncManager / legge dal clock
    ▼
Codice di ritorno (i32) al guest
```

| Codice | Significato |
|---|---|
| `>= 0` | successo; a seconda della funzione il valore è un byte count, un boolean/status value, o zero |
| `-1` | chiave non trovata |
| `-2` | buffer output troppo piccolo, riprova |
| `-3` | errore interno |
| `-4` | chiave riservata (prefisso `__nx/`) |
| `-5` | sync disabilitata su questo runtime |

L'SDK gestisce automaticamente il loop di retry `-2` aumentando il buffer di output e riprovando.
Il codice del modulo non deve gestire a mano la dimensione dei buffer.

---

## Lifecycle del modulo

Un nodo Numax segue un lifecycle fisso:

```
Runtime::new(config)          apre store, costruisce engine, registra host API, crea SyncManager se la sync è configurata
  start_observability()       opzionalmente fa bind dell'endpoint HTTP metriche
  start_sync()                opzionalmente fa bind del listener TCP e dial dei peer configurati
  wait_before_run(durata)     opzionalmente ritenta le connessioni ai peer prima di eseguire
  run_module(wasm_bytes)      compila, istanzia, chiama run() o _start()
  settle_for(durata)          opzionalmente mantiene la sync attiva per una finestra limitata
    OPPURE serve()            se la sync è attiva e non c'è settle window, resta vivo fino a segnale OS
  shutdown_with_timeout()     ferma sync, flush store, chiude connessioni
```

Il modulo stesso è stateless dal punto di vista del runtime. Riceve il controllo, chiama le funzioni Host API secondo necessità, e ritorna. 
Ciò che persiste è nello store e nel registry CRDT, non nel modulo.

I moduli compilati sono cachati per hash blake3 dei loro byte. Eseguire lo stesso modulo più volte non paga di nuovo il costo di compilazione.

---

## Stato locale: lo store

Il key/value store locale è un database sled embedded. Si apre da una directory su disco e persiste tra i restart e ogni nodo possiede la propria directory store.

Le chiavi sono slice di byte arbitrari e i valori sono slice di byte arbitrari.

Il runtime riserva il prefisso chiave `__nx/` per il proprio stato interno: NodeId, valori CRDT materializzati, voci dell'op-log. Il codice guest che tenta di leggere o scrivere una chiave riservata riceve `ERR_RESERVED_KEY`.

Allo shutdown, il runtime chiama un `flush()` esplicito per garantire che tutte le scritture raggiungano il disco prima che il processo esca.

---

## Sincronizzazione distribuita: CRDT

Quando la sync è abilitata, i moduli guest possono operare su strutture dati CRDT tramite la Host API. Una chiamata operazione CRDT dal guest:

1. viene applicata immediatamente allo stato CRDT in-memory
2. persiste lo stato CRDT aggiornato / valore materializzato in sled
3. accoda l'op generata per il broadcast
4. viene registrata nel set seen-op e nell'op-log dal broadcast loop prima dell'invio in rete
5. viene deduplicata per OpId quando ricevuta da un peer

Il sync manager in `nx-core` possiede il registry CRDT. Fa da ponte tra la host API, lo store locale e il layer di rete. Non espone un linguaggio di query: il guest chiama funzioni tipizzate (`crdt_gcounter_inc`, `crdt_lww_set`, ecc.) e il runtime si occupa del resto.

CRDT disponibili:

| Tipo | Usare per |
|---|---|
| GCounter | totali che crescono solo |
| PNCounter | contatori che possono diminuire |
| LWW-Register | valori singoli last-writer-wins |
| LWW-Map | mappe dove ogni campo è LWW indipendente |
| ORSet | set con add e remove concorrenti |
| RGA | sequenze ordinate |

I CRDT soddisfano tre proprietà che rendono possibile la convergenza distribuita senza coordinamento:

- **Commutatività** — `merge(A, B) == merge(B, A)`: l'ordine di arrivo non conta
- **Associatività** — `merge(merge(A, B), C) == merge(A, merge(B, C))`: il raggruppamento non conta
- **Idempotenza** — `merge(A, A) == A`: ricevere la stessa op due volte non corrompe lo stato

Un nodo può andare offline, ricevere operazioni in qualsiasi ordine, riconnettersi dopo giorni, e convergere comunque allo stesso stato di ogni altro nodo. È una proprietà matematica.

---

## NodeId e identità

Ogni nodo Numax ha un `NodeId`: una stringa opaca che lo identifica univocamente.

Senza TLS, il runtime genera un UUID v4 casuale al primo avvio e lo persiste sotto `__nx/runtime/node_id`. La stessa identità viene riusata ad ogni avvio successivo dalla stessa directory store.

Con TLS, il NodeId viene derivato dal SHA-256 dei byte `SubjectPublicKeyInfo` del certificato X.509 del nodo: primi 16 byte dell'hash, codificati come stringa hex lowercase da 32 caratteri. L'identità di un nodo è la sua chiave. Non può essere falsificata senza la chiave privata.

Il NodeId viene usato:
- come chiave slot nello stato CRDT (lo slot GCounter di ogni nodo è indicizzato per NodeId)
- come campo `origin` in ogni `Op`
- per la verifica identità peer mTLS e l'enforcement dell'allowlist

---

## Cosa il runtime non è

Capire il modello significa anche capire cosa è deliberatamente assente.

**Nessun coordinatore centrale.** Non c'è leader election, nessun nodo primario, nessun round di consensus. I nodi sono peer. Qualsiasi nodo può accettare scritture. La convergenza viene dalle proprietà CRDT, non da un coordinatore. Ma questo credo fosse già chiaro se sei qui.

**Nessuno stato remoto.** Lo store è embedded, non un servizio. Non apri una connessione. Non c'è un hop di rete tra compute e stato.

**Nessun linguaggio di configurazione runtime.** Un nodo è configurato con un file TOML e flag CLI. Un binario, un file di configurazione, un modulo `.wasm`.

**Nessun networking implicito.** Un nodo senza `--listen` non apre nessuna porta, non si connette a nessun peer, non replica nulla. La sync è completamente opt-in.

---

## La filosofia

Il modello runtime di Numax fa un trade deliberato:

Rinuncia alla generalità e non cerca di essere un toolkit universale per sistemi distribuiti, ma in cambio offre un modello coerente e minimale dove le parti difficili (isolamento, persistenza, convergenza) sono risolte strutturalmente.

L'obiettivo non è eliminare la complessità dei sistemi distribuiti. Quella complessità è reale e non può essere ignorata. L'obiettivo è eliminare la complessità *auto-imposta*: i layer di tool, configurazioni e infrastruttura che si accumulano non perché il problema lo richieda, ma perché è quello che l'ecosistema esistente assume si userà.

Un nodo Numax è un singolo processo. Spedisci un file `.wasm`. Lo esegui. Il resto è già lì.

---

## Correlati

- [CRDT e stato](/numax/it/concepts/crdt-and-state/) - approfondimento sul modello CRDT
- [Esecuzione WASM](/numax/it/concepts/wasm-execution/) - come i moduli vengono compilati e istanziati
- [Protocollo gossip](/numax/it/concepts/gossip-protocol/) - come le operazioni si propagano tra i nodi
- [Host API](/numax/it/reference/host-api/) - ogni funzione che il guest può chiamare
- [Crate nx-core](/numax/it/reference/crates/nx-core/) - internals di `Runtime` e `SyncManager`
