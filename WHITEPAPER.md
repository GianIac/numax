# Numax Runtime - Whitepaper Tecnico (ITA)

> **Nota**
> Questo whitepaper è allineato a **v0.1.0-alpha.1**, la prima preview tecnica pubblica di Numax.
> Rispetto alle bozze precedenti, gran parte dei `TODO` è stata risolta sulla base del codice presente nel repository. Ciò che rimane aperto è esplicitamente etichettato come *(Planned)* e tracciato nella roadmap.
>
> **Label di stato (coerenti con il codice):**
> - **(Implemented)**: presente nel codice, funzionante e coperto da test.
> - **(Prototype)**: presente in forma parziale; wiring interno o cammini critici già verificati, ma non ancora production-ready.
> - **(Planned)**: previsto in roadmap, non ancora implementato.
>
> **Riferimento di versione**: `v0.1.0-alpha.1` - preview tecnica, API e wire format possono cambiare prima della v0.1.0 stabile.
>
> > 📍 **Roadmap di riferimento:** le fasi citate in questo documento (Fase 7, Fase 8, …) sono definite in [`ROADMAP_v0.1.0.md`](./ROADMAP_v0.1.0.md).
> Ogni volta che leggi *Fase N*, puoi consultare la roadmap per dettagli, criteri di completamento e stato di avanzamento :)

---

## 1. Executive Summary

### 1.1 Il problema

Costruire applicazioni distribuite, oggi, è sproporzionatamente complesso rispetto a ciò che la maggior parte di queste applicazioni fa davvero.

Per qualsiasi logica anche solo parzialmente distribuita, lo sviluppatore si trova a dover comporre:

- container e orchestratori,
- database esterni,
- sistemi di sincronizzazione costruiti ad hoc,
- ambienti di esecuzione molto diversi tra loro (browser, server, edge, IoT),
- catene di dipendenze, permessi, versioni, configurazioni.

Il risultato è quasi sempre lo stesso: un ecosistema fragile, difficile da comprendere end-to-end, costoso da mantenere e poco portabile.

### 1.2 Numax

Numax è un **runtime portabile scritto in Rust** progettato per eseguire applicazioni distribuite in modo semplice, sicuro e coerente tra ambienti diversi.

L'architettura si fonda su tre e solo tre componenti:

1. **Esecuzione di moduli WebAssembly in sandbox isolata** *(Implemented)*
   Wasmtime come motore, WASI preview1 come base I/O, host API minimale e controllata sotto namespace `nx`.

2. **Datastore key/value locale embedded** *(Implemented)*
   Basato su `sled`. Lo stato vive vicino al calcolo: bassa latenza, nessuna dipendenza esterna, funzionamento offline nativo.

3. **Sincronizzazione distribuita dello stato tramite CRDT + gossip** *(Prototype)*
   Replica automatica tra nodi, senza coordinamento centralizzato, senza lock distribuiti, con convergenza garantita dalle proprietà matematiche dei CRDT.

Numax non è un container, non è un orchestratore, non è un database distribuito. È un runtime: l'unità minima necessaria per eseguire logica distribuita portando con sé stato e sincronizzazione.

### 1.3 Il cuore tecnologico

I principi che guidano ogni scelta tecnica:

- **Semplicità architetturale come principio guida.** Il runtime integra solo ciò che è davvero necessario: compute, stato locale, sincronizzazione. Tutto il resto resta opzionale o esterno.

- **Stato e codice nello stesso ambiente.** Il datastore non è un servizio remoto: è parte del runtime. Latenza nulla tra calcolo e stato, coerenza locale ACID, resilienza offline come default.

- **WASM come unità di calcolo portabile.** Un singolo artefatto `.wasm` può girare su server, edge, browser, dispositivi embedded, senza branching condizionale e senza codebase multiple.

- **CRDT al posto di lock o transazioni distribuite.** La sincronizzazione non richiede coordinamento centralizzato: i CRDT garantiscono convergenza automatica anche con latenze, partizioni e aggiornamenti concorrenti.

- **Funzionamento offline come caratteristica nativa.** Ogni nodo è autosufficiente. Quando rientra in rete, riconcilia attraverso CRDT senza conflitti e senza codice applicativo aggiuntivo.

Numax non pretende di eliminare la complessità del dominio distribuito: la **incorpora nel runtime in modo sistematico**, e rifiuta la complessità auto-imposta che oggi domina lo sviluppo di sistemi distribuiti.

---

## 2. Contesto

### 2.1 Complessità necessaria vs complessità auto-imposta

Costruire sistemi distribuiti comporta una quota irriducibile di complessità. Una buona parte di quella che vediamo oggi nei nostri stack, però, non viene dal problema: viene dagli strumenti.

**Complessità necessaria** - intrinseca al dominio:

- la rete è inaffidabile, introduce ritardi, disconnessioni, partizioni;
- più nodi possono aggiornare lo stesso stato in parallelo;
- i client possono andare offline e rientrare in momenti arbitrari;
- gli ambienti di esecuzione sono eterogenei (browser, server, mobile, IoT).

Questi aspetti **non si possono evitare**: richiedono modelli dati e meccanismi di sincronizzazione robusti.

**Complessità auto-imposta** - aggiunta dagli strumenti, non dal problema:

- orchestratori complessi anche per applicazioni piccole;
- catene di dipendenze tra servizi e infrastrutture esterne;
- configurazione frammentata su decine di file (YAML, operator custom, chart);
- stato delegato a database remoti anche quando una replica locale sarebbe più efficiente;
- toolchain differenziate per ambiente (dev, browser, edge, IoT).

Questa complessità è **largamente evitabile**: nasce dall'accumulo di tecnologie general-purpose applicate a contesti per cui non sono indispensabili.

### 2.2 Opportunità

WebAssembly e i CRDT, presi insieme, rendono possibile ripensare la base su cui costruiamo sistemi distribuiti:

- esecuzione realmente portabile tra architetture e ambienti,
- isolamento sandbox per default,
- sincronizzazione dello stato fondata su proprietà matematiche dimostrabili,
- runtime leggeri, indipendenti da una specifica infrastruttura.

Numax nasce in questo spazio.

---

## 3. Principi di design di Numax

### 3.1 I tre elementi principali

Numax integra tre componenti, e solo tre:

1. esecuzione di moduli WASM in sandbox *(Implemented)*
2. datastore locale sempre disponibile *(Implemented)*
3. sincronizzazione distribuita dello stato *(Prototype)*

Tutto il resto appartiene ai livelli superiori o a tool esterni. Il runtime resta intenzionalmente minimo: questa è una scelta, non una mancanza.

### 3.2 Portabilità radicale

Un modulo WASM deve poter girare senza modifiche:

- on premise,
- in cloud,
- su nodi edge,
- su dispositivi embedded,
- nel browser.

Questo riduce a zero le configurazioni specifiche per ambiente, le dipendenze da piattaforma e il branching condizionale nel codice applicativo.

### 3.3 Stato vicino al calcolo

Il runtime assume che lo stato debba:

- essere **locale**, per garantire velocità e resilienza;
- essere **replicabile**, per garantire distribuzione *(Prototype)*.

Il datastore è quindi integrato nel runtime e non dipende da componenti esterni. Il calcolo non viaggia verso lo stato: lo stato è già lì.

### 3.4 Sincronizzazione senza conflitti

La replica si basa su modelli CRDT che permettono:

- aggiornamenti concorrenti senza lock;
- consistenza eventuale dimostrabile;
- assenza di conflitti che richiedano risoluzione manuale.

La rete è considerata fallibile per natura. Disconnessioni, latenze e rientri sono condizioni **normali**, non eccezionali.

---

## 4. Panoramica su Numax

### 4.1 Componenti principali

Numax è composto da sei crate Rust organizzati in un workspace:

| Crate | Stato | Responsabilità |
|-------|-------|----------------|
| **nx-core** | *Implemented* | Runtime WASM (Wasmtime), sandboxing, host API |
| **nx-store** | *Implemented* | Datastore key/value locale persistente (sled) |
| **nx-sync** | *Implemented* | Strutture dati CRDT, operazioni, identità nodi, SyncManager |
| **nx-net** | *Implemented* | Networking TCP, protocollo messaggi, TLS 1.3 + mTLS |
| **nx-sdk** | *Implemented* | SDK per sviluppare moduli WASM guest |
| **nx-cli** | *Implemented* | Interfaccia a linea di comando |

La separazione mantiene responsabilità chiare e permette ai componenti di evolvere in modo indipendente.

### 4.2 Ambienti supportati

Numax è progettato per girare su:

- server (x86_64, ARM64),
- nodi edge,
- browser (tramite WASM),
- mobile (tramite integrazione nativa),
- IoT (ARM / RISC-V).

La CI verifica oggi la compilazione e l'esecuzione dei test su:

- Ubuntu (x86_64),
- macOS (x86_64, ARM64),
- Windows (x86_64).

### 4.3 Modello di esecuzione e dati

- **Compute**: un nodo Numax esegue moduli WASM in sandbox, esponendo un set limitato di host API. *(Implemented)*
- **State**: ogni nodo mantiene uno store key/value locale persistente basato su sled. *(Implemented)*
- **Sync**: una parte dello stato può essere replicata tra nodi tramite CRDT + gossip. *(Prototype)*
- **Consistency**: il sistema mira a convergenza eventuale (eventual consistency); in assenza di nuove scritture e con connettività sufficiente, tutti i nodi convergono allo stesso stato. *(Prototype)*
- **Rete fallibile**: disconnessioni e rientri sono condizioni normali; la roadmap include meccanismi espliciti per recuperare delta mancanti (anti-entropy). *(Planned, Fase 10)*

### 4.4 Security model & threat model

**Assunzioni:**

- la rete è potenzialmente ostile (osservazione, MITM, packet injection, route hijack);
- i nodi possono essere offline o intermittenti;
- alcuni peer possono essere malevoli o non affidabili.

**Obiettivi di sicurezza:**

- Isolamento del compute (sandbox WASM). *(Implemented)*
- Confidenzialità e integrità delle comunicazioni tra nodi. *(Implemented - TLS 1.3)*
- Autenticazione mutua dei peer. *(Implemented - mTLS, NodeID derivato dall'hash della chiave pubblica del certificato)*
- Membership controllata via allowlist di peer trust. *(Implemented)*
- Resilienza completa del canale rispetto a tutti gli scenari (replay, downgrade, certificate pinning evoluto, rotazione automatica). *(Prototype / Planned)*

**Guardrail implementati:**

| Risorsa | Limite |
|---------|--------|
| Lunghezza chiave | 1024 byte |
| Lunghezza valore | 1 MB |
| Buffer di output | 10 MB |

Tutti gli input provenienti dal guest sono validati prima di essere processati.

**Fuori scope (oggi):**

- bug logici nel modulo applicativo;
- data poisoning se si accettano peer non trusted senza policy;
- compromissione host-level (la perdita della chiave privata di un nodo richiede revoca/rotazione esterna).

---

## 5. Architettura del Sistema

Panoramica ad alto livello dei componenti e delle loro interazioni.

```
┌─────────────────────────────────────────────────────────────┐
│                      WASM Module (Guest)                    │
│                    (compiled with nx-sdk)                   │
└──────────────────────────┬──────────────────────────────────┘
                           │ Host API calls (namespace "nx")
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                       nx-core (Host)                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  Wasmtime   │  │  Host API   │  │    WASI (preview1)  │  │
│  │   Engine    │  │ db_*, log,  │  │    stdio, args      │  │
│  │             │  │ crdt_*      │  │                     │  │
│  └─────────────┘  └──────┬──────┘  └─────────────────────┘  │
└──────────────────────────┼──────────────────────────────────┘
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
   ┌────────────┐   ┌────────────┐   ┌────────────────────┐
   │  nx-store  │   │  nx-sync   │   │      nx-net        │
   │   (sled)   │◄──┤ SyncMgr +  ├──►│  TCP + TLS 1.3     │
   │            │   │   CRDTs    │   │  (mTLS, allowlist) │
   └────────────┘   └────────────┘   └────────────────────┘
                          ▲
                          │ async runtime (tokio)
                          ▼
                  Peer nodes (gossip, K-fanout)
```

Il runtime host è interamente **async, basato su tokio**: l'esecuzione del modulo WASM, l'I/O di rete, le operazioni sullo store e la propagazione delle operazioni CRDT sono coordinate dallo stesso scheduler asincrono. Il **SyncManager** è il punto di raccordo tra host API CRDT, store locale e rete: riceve le operazioni generate dal guest, le materializza su sled e le propaga ai peer attivi.

### 5.1 Numax Core - Runtime WASM *(Implemented)*

**Responsabilità principali:**

- caricare ed eseguire moduli WASM,
- gestire la sandbox con isolamento rigoroso,
- esporre le host functions verso il modulo guest,
- integrare WASI preview1 come base standard per I/O.

**Tecnologie:**

- implementazione in Rust,
- **Wasmtime** come motore WASM,
- WASI preview1 come interfaccia di sistema,
- runtime asincrono **tokio** per l'host.

**Caratteristiche:**

- isolamento rigoroso: il guest non può accedere a risorse non esplicitamente concesse;
- nessun accesso implicito al filesystem;
- avvio rapido (tipicamente sotto la decina di millisecondi);
- memory-safety garantita da Rust e dal modello WASM.

**Convenzione dei return code:**

Le funzioni host restituiscono interi con semantica precisa:

| Codice | Costante | Significato |
|--------|----------|-------------|
| `>= 0` | - | Successo (per `db_get`: lunghezza del valore letto) |
| `0` | `OK` | Successo (per `db_set`, `db_delete`, host API CRDT) |
| `-1` | `ERR_NOT_FOUND` | Chiave non trovata |
| `-2` | `ERR_BUFFER_TOO_SMALL` | Buffer output troppo piccolo, riprovare con buffer più grande |
| `-3` | `ERR_INTERNAL` | Errore interno del runtime |
| `-4` | `ERR_RESERVED_KEY` | Tentativo di usare una chiave riservata al runtime |
| `-5` | `ERR_SYNC_DISABLED` | Operazione di sync richiesta ma sync non abilitato |

Questa convenzione permette al guest di gestire gli errori in modo deterministico, senza eccezioni o panic.

**Limiti di sicurezza (guardrail):**

| Risorsa | Limite |
|---------|--------|
| Lunghezza chiave | 1024 byte |
| Lunghezza valore | 1 MB |
| Buffer di output | 10 MB |

### 5.2 Numax Store - Datastore locale *(Implemented)*

Numax Store fornisce un key/value store persistente locale per ogni istanza di runtime.

**Implementazione:**

Il datastore è basato su **sled**, un embedded database scritto in Rust che offre:

- persistenza su disco,
- operazioni atomiche,
- prestazioni elevate per workload misti read/write,
- nessuna configurazione esterna richiesta.

**API (lato Rust):**

```rust
impl Store {
    pub fn open(path: &Path) -> Result<Self, StoreError>
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError>
    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<(), StoreError>
    pub fn delete(&self, key: &[u8]) -> Result<(), StoreError>
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError>
}
```

**API (lato guest WASM, tramite nx-sdk):**

```rust
use nx_sdk::db;

let value: Option<Vec<u8>> = db::get("my_key")?;
db::set("my_key", b"my_value")?;
db::delete("my_key")?;
```

L'SDK gestisce automaticamente la serializzazione, i buffer e i retry in caso di `ERR_BUFFER_TOO_SMALL`.

**Proprietà:**

- ACID locale per singole operazioni;
- get/set/delete atomiche;
- nessun lock esplicito richiesto dal chiamante;
- dati persistenti tra riavvii del runtime.

### 5.3 Numax Sync - Replica distribuita *(Implemented core, Prototype end-to-end)*

Numax Sync è responsabile della replica dello stato tra nodi. Le primitive fondamentali sono implementate e coperte da test (incluso il wiring end-to-end del SyncManager). Il ciclo CLI multi-process completo è tracciato come Fase 7 della roadmap.

**Componenti:**

**NodeId** *(Implemented)* - identifica univocamente un nodo. In modalità TLS, il NodeId è **derivato deterministicamente dall'hash della chiave pubblica del certificato**: l'identità di un nodo è la sua chiave.

**Op e OpId** *(Implemented)* - operazioni CRDT serializzabili e trasportabili tra nodi.

```rust
pub struct Op {
    pub id: OpId,           // Identificatore univoco dell'operazione
    pub origin: NodeId,     // Nodo che ha generato l'operazione
    pub timestamp: u64,     // Timestamp logico
    pub kind: OpKind,       // Tipo di operazione (es. GCounterIncrement)
}
```

Le operazioni sono serializzabili per il trasporto via rete (oggi JSON length-prefixed; dual-mode JSON/bincode previsto in Fase 11).

**GCounter (Grow-only Counter)** *(Implemented)* - primo CRDT implementato, contatore distribuito che supporta solo incrementi. Ogni nodo possiede il proprio "slot" e può incrementare solo quello.

```rust
pub struct GCounter {
    counts: HashMap<String, u64>,  // NodeId -> valore locale
}
```

Il valore totale è la somma di tutti gli slot: `value() = Σ counts[node]`.

**Proprietà CRDT garantite e verificate:**

1. **Commutatività** - `merge(A, B) == merge(B, A)`
2. **Associatività** - `merge(merge(A, B), C) == merge(A, merge(B, C))`
3. **Idempotenza** - `merge(A, A) == A`

Verificate da test dedicati nella suite di `nx-sync`.

**Operazione di merge:**

```rust
pub fn merge(&mut self, other: &GCounter) {
    for (node, &value) in &other.counts {
        let entry = self.counts.entry(node.clone()).or_insert(0);
        *entry = (*entry).max(value); // Prende il massimo per slot
    }
}
```

**Protezione overflow:** gli incrementi usano `saturating_add` per saturare a `u64::MAX` invece di andare in overflow.

**SyncManager** *(Implemented)* - componente async che integra CRDT, store e rete:

- riceve operazioni dal guest via host API CRDT,
- materializza lo stato CRDT su sled,
- propaga le operazioni ai peer attivi tramite nx-net,
- coperto da test E2E end-to-end.

**Hydration** *(Planned, Fase 7)* - la ricostruzione dello stato GCounter da sled all'avvio del nodo non è ancora implementata. Oggi lo stato è ricostruito durante l'esecuzione tramite operazioni; la persistenza dello stato CRDT materializzato è presente ma il replay all'avvio è in roadmap.

**CRDT pianificati (Fase 14):**

| Tipo | Descrizione | Stato |
|------|-------------|-------|
| PNCounter | Counter con incrementi e decrementi | *Planned* |
| LWW-Register | Registro last-writer-wins | *Planned* |
| ORSet | Set con add/remove osservati | *Planned* |
| LWW-Map | Mappa con semantica LWW | *Planned* |
| RGA | Replicated Growable Array (sequenze) | *Planned* |

### 5.4 Numax Net - Networking *(Implemented base, Prototype resilienza)*

Numax Net gestisce la comunicazione tra nodi per la sincronizzazione dello stato.

**Architettura:** peer-to-peer. Ogni nodo può comunicare direttamente con altri nodi senza un server centrale. Trasporto TCP, **TLS 1.3 con mTLS** disponibile e raccomandato.

**Protocollo messaggi:** length-prefixed JSON (4 byte big-endian per la lunghezza, payload JSON):

```
┌──────────────┬─────────────────────────────┐
│ Length (4B)  │     JSON Payload            │
│ big-endian   │                             │
└──────────────┴─────────────────────────────┘
```

**Tipi di messaggio implementati:**

| Messaggio | Direzione | Descrizione |
|-----------|-----------|-------------|
| `Hello` | Client → Server | Handshake iniziale con NodeId e versione protocollo |
| `HelloAck` | Server → Client | Conferma handshake |
| `PushOps` | Bidirezionale | Invia batch di operazioni CRDT |
| `PushOpsAck` | Bidirezionale | Conferma ricezione operazioni |
| `PullSince` | Client → Server | Richiede operazioni dopo un certo OpId |
| `Ping` | Bidirezionale | Keepalive |
| `Pong` | Bidirezionale | Risposta a Ping |

**Versioning del protocollo:** numero di versione (`PROTOCOL_VERSION = 1`) scambiato durante l'handshake. Permette evoluzione retrocompatibile e rilevamento di incompatibilità.

**Stato corrente:**

- canale TCP + TLS 1.3 + mTLS *(Implemented)*;
- handshake, push/pull e keepalive *(Implemented)*;
- gossip peer-to-peer con fanout K: architettura definita, integrazione completa in corso *(Prototype)*;
- resilienza rete piena (reconnect con backoff esponenziale, anti-entropy automatica, dedup ops) *(Planned, Fase 10)*;
- backpressure e limiti di connessione *(Planned, Fase 8)*.

### 5.5 Sicurezza del canale *(Implemented)*

Numax assume una rete ostile: il trasporto può essere osservato, alterato o reindirizzato. Le comunicazioni tra nodi avvengono per default su canali cifrati e autenticati.

**Garantito oggi:**

- **Confidenzialità** - TLS 1.3 cifra tutto il traffico tra nodi.
- **Integrità** - TLS 1.3 protegge da manipolazioni del payload.
- **Autenticazione mutua** - mTLS: ogni nodo presenta un certificato e verifica quello del peer.
- **Identità verificabile** - il `NodeId` è derivato dall'hash della chiave pubblica del certificato. L'identità di un nodo non può essere falsificata senza la sua chiave privata.
- **Membership controllata** - allowlist esplicita dei NodeId/peer accettati.
- **Forward Secrecy** - fornita dai cipher suite di TLS 1.3.

**Flag CLI dedicati:** `--tls-cert`, `--tls-key`, `--tls-ca`, `--allowed-peers`, `--tls-insecure` (quest'ultimo solo per sviluppo locale).

**Fuori scope per v0.1.0-alpha.1:**

- rotazione automatica dei certificati;
- certificate pinning evoluto;
- hardening completo del canale per tutti gli scenari operativi (oggetto di lavoro nelle fasi successive).

### 5.6 Numax SDK *(Implemented)*

L'SDK fornisce un'interfaccia ergonomica per sviluppare moduli WASM guest.

**Moduli disponibili:**

| Modulo | Funzionalità |
|--------|--------------|
| `nx_sdk::db` | Accesso al datastore (get, set, delete) |
| `nx_sdk::log` | Logging strutturato verso l'host |
| `nx_sdk::crdt::gcounter` | API ergonomica per incrementare e leggere GCounter distribuiti |

**Esempio: contatore distribuito**

```rust
use nx_sdk::{log, crdt::gcounter};

#[no_mangle]
pub extern "C" fn run() {
    log::info("Modulo avviato");

    if let Err(e) = gcounter::inc("visits", 1) {
        log::error(&format!("Errore inc: {:?}", e));
        return;
    }

    match gcounter::value("visits") {
        Ok(v) => log::info(&format!("visits = {}", v)),
        Err(e) => log::error(&format!("Errore read: {:?}", e)),
    }
}
```

**Gestione automatica dei buffer:** l'SDK gestisce trasparentemente il caso `ERR_BUFFER_TOO_SMALL`, riallocando e ripetendo la chiamata, così il developer non deve mai pensare alla dimensione dei buffer.

### 5.7 Numax CLI *(Implemented)*

La CLI è l'interfaccia principale per eseguire un nodo Numax.

```bash
# Esegue un modulo WASM (single-shot oggi; long-running in Fase 7)
nx run <module.wasm>

# Esegue con directory dati custom
nx run <module.wasm> --data-dir ./my-data

# Esegue con sync abilitato e TLS/mTLS
nx run <module.wasm> --sync \
    --sync-listen 0.0.0.0:9000 \
    --sync-peers 192.168.1.10:9000,192.168.1.11:9000 \
    --sync-keys "counter:,votes:" \
    --tls-cert ./certs/node.crt \
    --tls-key  ./certs/node.key \
    --tls-ca   ./certs/ca.crt \
    --allowed-peers ./peers.allow
```

**Opzioni principali:**

| Flag | Descrizione |
|------|-------------|
| `--data-dir` | Directory per i dati persistenti |
| `--sync` | Abilita sincronizzazione |
| `--sync-listen` | Indirizzo su cui accettare connessioni peer |
| `--sync-peers` | Lista di peer iniziali (comma-separated) |
| `--sync-keys` | Prefissi delle chiavi da sincronizzare |
| `--tls-cert` / `--tls-key` / `--tls-ca` | Materiale TLS per mTLS |
| `--allowed-peers` | Allowlist dei NodeID peer accettati |
| `--tls-insecure` | Disattiva TLS (solo dev locale) |

### 5.8 Topologia: epidemic gossip *(Prototype)*

Numax non assume una topologia ad anello (es. `n1→n2→n3→…`): sarebbe fragile, la caduta di un nodo spezzerebbe la catena.

Il modello è **peer-to-peer a gossip**:

- ogni nodo mantiene connessioni attive verso un sottoinsieme di peer (fanout **K**);
- gli aggiornamenti (operazioni CRDT) si propagano in modo "epidemico": un nodo invia l'update ai suoi peer, i peer lo inoltrano ad altri, fino a coprire la rete;
- ogni operazione ha un identificatore univoco (`OpId`) per **deduplicare** e prevenire loop.

L'approccio scala meglio del full-mesh e resta resiliente in presenza di disconnessioni temporanee. L'integrazione completa del fanout dinamico è in corso.

### 5.9 Resilienza: nodo down, rete intermittente, rientro *(Planned - Fase 10)*

La rete è considerata fallibile per natura. Le contromisure progettate:

Quando un peer diventa irraggiungibile:

- timeout e retry con **backoff esponenziale**;
- marcatura del peer come down e rimozione dal set attivo;
- selezione di un nuovo peer dal discovery per mantenere il fanout **K**.

Quando un nodo rientra:

- ristabilimento delle connessioni con i peer noti;
- meccanismo di **anti-entropy** (`PullSince`) per recuperare gli update mancanti;
- convergenza allo stesso stato grazie alle proprietà dei CRDT.

---

## 6. Modello di Programmazione

### 6.1 Moduli WASM come unità di calcolo *(Implemented)*

Un'applicazione Numax è composta da uno o più moduli WASM che:

- eseguono logica applicativa pura,
- leggono/scrivono sul datastore locale tramite host API,
- pubblicano e ricevono aggiornamenti CRDT via host API dedicate,
- (futuro) effettueranno chiamate HTTP se esplicitamente permesso.

Il modulo deve esporre una funzione `run` con firma:

```rust
#[no_mangle]
pub extern "C" fn run() {
    // logica applicativa
}
```

### 6.2 Host API esposte ai moduli

**Namespace import:** `"nx"`. Tutte le funzioni host sono importate dal namespace `"nx"`; l'SDK fornisce wrapper type-safe.

**Database** - *(Implemented)*

| Funzione | Firma | Stato |
|----------|-------|-------|
| `db_get` | `(key_ptr: u32, key_len: u32, out_ptr: u32, out_cap: u32) -> i32` | *Implemented* |
| `db_set` | `(key_ptr: u32, key_len: u32, val_ptr: u32, val_len: u32) -> i32` | *Implemented* |
| `db_delete` | `(key_ptr: u32, key_len: u32) -> i32` | *Implemented* |
| `db_scan` | scansione per prefisso | *Planned (Fase 12)* |

**Logging** - *(Implemented)*

| Funzione | Firma | Stato |
|----------|-------|-------|
| `host_log_v2` | `(level: u32, msg_ptr: u32, msg_len: u32) -> ()` | *Implemented* |

Livelli di log: 0 = trace, 1 = debug, 2 = info, 3 = warn, 4 = error.

**CRDT** - *(Implemented)*

| Funzione | Firma | Stato |
|----------|-------|-------|
| `crdt_gcounter_inc` | `(key_ptr: u32, key_len: u32, delta: u64) -> i32` | *Implemented* |
| `crdt_gcounter_value` | `(key_ptr: u32, key_len: u32, out_ptr: u32, out_cap: u32) -> i32` | *Implemented* |

Le operazioni di incremento sono materializzate su sled e propagate ai peer tramite SyncManager. In assenza di sync abilitato, le funzioni restituiscono `ERR_SYNC_DISABLED` (-5).

**Host API estese** - *(Planned, Fase 12)*

| Funzione | Descrizione | Stato |
|----------|-------------|-------|
| `time_now` | Timestamp monotonico/UTC | *Planned* |
| `random_bytes` | Sorgente di entropia controllata | *Planned* |
| `hash_*` | Famiglia di funzioni di hashing | *Planned* |
| `env_get` | Lettura variabili d'ambiente filtrate | *Planned* |
| `http_fetch` | HTTP request con whitelist | *Planned* |

### 6.3 Configurazione e Deploy *(Planned - Fase 15)*

Il deploy consisterà nell'invio di un file `.wasm` e di una configurazione minimale. Esempio (formato in definizione):

```toml
[module]
name = "cart_handler"
path = "cart_handler.wasm"

[permissions]
db = true
network = ["https://api.example.com"]

[sync]
enabled = true
keys = ["cart:", "user:"]
```

---

## 7. Test Suite

Il progetto include una test suite automatizzata che copre runtime, store, CRDT, networking e flussi end-to-end.

**Copertura attuale:** oltre **38 test** tra unit, integration ed end-to-end, distribuiti tra i crate del workspace (nx-core, nx-store, nx-sync, nx-net) e i flussi E2E del SyncManager.

**Test CRDT specifici** - verificano esplicitamente le proprietà matematiche:

- `test_gcounter_merge_commutativity` - `merge(A, B) == merge(B, A)`
- `test_gcounter_merge_associativity` - `(A⊕B)⊕C == A⊕(B⊕C)`
- `test_gcounter_merge_idempotency` - `A⊕A == A`
- `test_gcounter_overflow_protection` - saturazione invece di overflow

**Test E2E SyncManager** - verificano che un'operazione generata dal guest WASM venga materializzata su sled e propagata correttamente ai peer.

**CI/CD:** GitHub Actions esegue la pipeline su ogni push/PR su Ubuntu, macOS, Windows. Job principali:

1. `check` - verifica compilazione
2. `fmt` - verifica formattazione codice
3. `clippy` - linter Rust
4. `test` - esecuzione test suite completa
5. `build-wasm` - compilazione esempi WASM

**Test di carico** - *Planned, Fase 13*.

---

## 8. Casi d'Uso

I casi d'uso sotto sono **concretamente realizzabili oggi** con le primitive di v0.1.0-alpha.1. Non descrivono visioni: descrivono ciò che il runtime sa già fare, o saprà fare appena chiuse le ultime fasi della preview.

### 8.1 Contatori e metriche distribuite (esempio: `distributed_counter`)

**Problema.** Servono contatori globali - visite, eventi, like, throttle counter - su nodi geograficamente distribuiti, senza un singolo punto di centralizzazione.

**Perché Numax.** Un GCounter CRDT garantisce che ogni nodo possa incrementare il proprio slot localmente, senza coordinamento, e che i totali convergano automaticamente. Nessun database centrale, nessun lock distribuito.

**Cosa serve oggi.** Un modulo WASM che chiama `crdt_gcounter_inc` / `crdt_gcounter_value` via SDK; più istanze `nx run` con `--sync` e mTLS attivi. È esattamente ciò che fa l'esempio `distributed_counter` nel repo.

### 8.2 Voto, polling, tally distribuito (esempio: `vote_tally_tls`)

**Problema.** Aggregare conteggi (voti, segnalazioni, scelte) provenienti da nodi indipendenti - tipicamente in ambienti dove la fiducia tra nodi va verificata e il canale non può essere considerato sicuro.

**Perché Numax.** GCounter per i conteggi + mTLS con allowlist per garantire che **solo i peer autorizzati** possano contribuire. L'identità del peer è la sua chiave (NodeID = hash della public key): un peer non autorizzato non può iniettare voti spacciandosi per un altro.

**Cosa serve oggi.** Modulo WASM che incrementa il contatore corrispondente alla scelta votata + nodi `nx run` con TLS, certificati per ciascun nodo e `--allowed-peers`. È esattamente lo scenario dell'esempio `vote_tally_tls`.

### 8.3 Edge computing con stato locale e riconciliazione

**Problema.** Su nodi edge (gateway industriali, store fisici, veicoli, infrastrutture remote) si vuole eseguire logica applicativa **vicino al dato**, mantenendo lo stato anche se la connessione verso il "centro" è intermittente.

**Perché Numax.** Lo stato vive nello store sled del nodo edge, sempre disponibile localmente. Le operazioni vengono replicate ai peer (altri nodi edge o nodi di backhaul) appena la rete è disponibile. Le proprietà CRDT garantiscono che, una volta riconnesso, il nodo non perda né duplichi nulla.

Il calcolo è portabile: lo stesso modulo `.wasm` gira su gateway ARM, su server cloud per il rollup centrale e - in prospettiva - su browser per dashboard locali.

### 8.4 Applicazioni offline-first e collaborative

**Problema.** Applicazioni che devono funzionare senza connessione (note collaborative, configurazioni distribuite, applicazioni di campo, dispositivi marittimi/aerei/rurali) e riconciliarsi quando rientrano in rete, senza imporre risoluzione manuale dei conflitti.

**Perché Numax.** È esattamente lo sweet spot dei CRDT: ogni nodo opera localmente sul proprio store, le modifiche si propagano in modo opportunistico, la convergenza è garantita matematicamente. Quando arriveranno PNCounter, LWW-Register, ORSet, LWW-Map e RGA (Fase 14), il modello coprirà la maggior parte dei pattern offline-first reali.

L'esempio `distributed_chat` (oggi in modalità local-only) rappresenta l'ossatura di questo caso d'uso.

---

## 9. Dove si colloca Numax

Numax è facile da descrivere per **differenza**.

- **Non è Kubernetes.** Non orchestra container, non gestisce workload su cluster, non esiste un control plane. Numax è un singolo binario che esegue moduli WASM. Se Kubernetes risponde a "come orchestro centinaia di servizi?", Numax risponde a una domanda precedente: "perché ho bisogno di centinaia di servizi per fare una cosa distribuita?".

- **Non è Redis (né un database distribuito).** Non è un servizio remoto a cui si fanno query. Lo stato non vive "da qualche parte nella rete": vive **dentro il runtime**, accanto al codice che lo usa. La replica avviene tra runtime peer, non tra client e server. (ps: amo redis)

- **Non è Deno né un altro runtime "edge JavaScript".** Quei runtime portano un linguaggio ovunque. Numax porta **logica + stato + sincronizzazione** ovunque, in un singolo modello coerente. Il linguaggio è un dettaglio: l'unità è il modulo WASM, agnostico rispetto al sorgente.

- **Non è un framework CRDT.** Yjs, Automerge e simili sono librerie eccellenti, ma sono librerie: lasciano allo sviluppatore l'onere di progettare trasporto, persistenza, identità, sandbox. Numax incorpora tutto questo in un runtime e fornisce le strutture CRDT come **primitive di prima classe** accessibili dal guest tramite host API.

**Cosa è Numax, allora.** Un **runtime portabile minimale** che combina, in un unico processo, i tre elementi necessari per costruire applicazioni distribuite: compute isolato (WASM), stato locale (sled), sincronizzazione senza coordinamento (CRDT + gossip).

L'idea è semplice e radicale: **eseguire applicazioni distribuite senza costruire un'infrastruttura distribuita**.

### Una nota sull'era AI

Numax stesso è stato scritto in parte usando l'AI. Sarebbe ipocrita fingere il contrario, e non è il punto: l'AI oggi è uno strumento di lavoro, esattamente come lo sono un compilatore, un debugger o un editor. La domanda interessante non è se si usa l'AI per costruire software, ma cosa si costruisce.

E qui Numax fa qualcosa di diverso da gran parte del sw che nasce in questa "stagione". Non genera, non predice, non classifica. Non è un'altra interfaccia sopra un modello. Risolve un problema strutturale, quello di chi costruisce software distribuito che esiste a prescindere dall'AI e che, semmai, l'AI rende più urgente: i sistemi di oggi devono coordinare modelli, dati e calcolo su edge, cloud e device, in modo affidabile e portabile.

Numax non è AI. È una delle cose su cui l'AI può, comodamente, girarci sopra.

**Numax è un runtime per chi vuole costruire sistemi distribuiti senza costruire un'infrastruttura distribuita.**

---

## 10. Limitazioni

v0.1.0-alpha.1 è una preview tecnica. Ne riconosciamo i limiti, esplicitamente:

- **`nx run` esegue il guest una sola volta e termina.** La modalità long-running con lifecycle gestito è in Fase 7.
- **L'hydration del GCounter da sled all'avvio non è ancora implementata.** I dati persistono, ma il replay automatico dello stato CRDT al boot è in roadmap (Fase 7).
- **Gossip a fanout K e resilienza rete completa sono in corso.** L'architettura è definita; reconnect con backoff, anti-entropy automatica e dedup ops sono in Fase 10.
- **TLS/mTLS è implementato, ma non ancora hardened per tutti gli scenari.** Lo è abbastanza per scenari controllati (dev, lab, deployment definiti); l'hardening completo (rotazione, pinning evoluto, scenari ostili estremi) prosegue.
- **Osservabilità minimale.** Logging strutturato avanzato, metriche Prometheus e health check sono in Fase 9.
- **Wire format e Host API possono cambiare.** Prima della v0.1.0 stabile, ci aspettiamo modifiche non retrocompatibili. La serializzazione dual-mode JSON/bincode è prevista in Fase 11.
- **CRDT disponibili limitati al GCounter.** PNCounter, LWW-Register, ORSet, LWW-Map e RGA arriveranno con la Fase 14.
- **Non sostituisce orchestratori complessi.** Non è progettato per gestire cluster estesi o deployment ad alta scalabilità con scheduling avanzato.
- **Non ottimizzato per workload CPU-bound.** Il focus è I/O e coordinamento, non calcolo intensivo.
- **I modelli dati devono essere compatibili con i CRDT.** Pattern basati su lock o transazioni distribuite forti non si adattano direttamente.

Questi limiti non sono debolezze nascoste: sono il **perimetro onesto** di una preview che vuole far vedere la traiettoria, non vendere un prodotto finito.

---

## 11. Conclusioni

Numax propone un runtime unificato che combina:

- esecuzione sicura e portabile tramite WebAssembly,
- datastore locale integrato per uno stato vicino al calcolo,
- sincronizzazione distribuita basata su CRDT e gossip,
- identità dei nodi e canale cifrato fondati su mTLS.

L'obiettivo non è replicare l'ecosistema esistente, ma **ridurre la complessità auto-imposta** che oggi domina lo sviluppo di sistemi distribuiti, mantenendo intatto il controllo sulla complessità necessaria del proprio dominio.

v0.1.0-alpha.1 è una preview tecnica. Ciò che contiene è reale, testato, funzionante: runtime WASM, store sled, GCounter CRDT, SyncManager async, networking TCP, TLS 1.3 + mTLS con identità derivata dalla chiave, host API stabili per database, log e CRDT, CI multi-OS, esempi end-to-end.

Ciò che ancora manca è dichiarato esplicitamente e tracciato in roadmap. Le iterazioni successive ne affineranno dettagli, esempi pratici, confronti e risultati sperimentali.

**v0.1.0-alpha.1 è solo l'inizio.** Ma è un inizio costruito sul codice, non sulle promesse.

In conclusione io adoro il software e adoro numax.

---

## Appendice A - Struttura del Repository

```
numax/
├── Cargo.toml              # Workspace manifest
├── crates/
│   ├── nx-core/            # Runtime WASM + Host API
│   ├── nx-store/           # Datastore locale (sled)
│   ├── nx-sync/            # CRDT, operazioni, SyncManager
│   ├── nx-net/             # Networking, protocollo, TLS/mTLS
│   ├── nx-sdk/             # SDK per guest WASM
│   └── nx-cli/             # CLI
├── examples/
│   ├── distributed_counter/
│   ├── distributed_chat/
│   └── vote_tally_tls/
├── docs/
│   └── HOST_API.md
├── WHITEPAPER.md
├── ROADMAP_v0.1.0.md
└── LICENSE
```

---

## Appendice B - Riferimenti

- WebAssembly: https://webassembly.org/
- WASI: https://wasi.dev/
- Wasmtime: https://wasmtime.dev/
- sled: https://sled.rs/
- CRDT: Shapiro et al., *A comprehensive study of Convergent and Commutative Replicated Data Types*
