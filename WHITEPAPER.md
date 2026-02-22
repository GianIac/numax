# Numax Runtime - Whitepaper Tecnico (Versione 0.1 ITA)

> **Nota V0.1.0**  
> Questo documento è una base di partenza. Alcune sezioni contengono `TODO:` per indicare parti da approfondire in iterazioni successive.
>
> **Label di stato (coerenza col codice):**
> - **(Implemented)**: presente nel codice attuale
> - **(Prototype)**: presente in forma iniziale, crate scaffolding, comportamento parziale
> - **(Planned)**: previsto da roadmap, non ancora stabile/implementato
>
> **Ultimo aggiornamento codice**: 2026-02-22

---

## 1. Executive Summary

### 1.1 The Problem

L'idea di numax nasce da una riflessione fatta riguardo lo sviluppo di applicazioni distribuite, che risulta essere spesso eccessivamente complesse anche per quanto riguarda logiche semplici.

Spesso si ricorre a:

- container e orchestratori,
- db esterni,
- sistemi di sincronizzazione ad hoc,
- differenze significative tra ambienti (browser, server, edge, IoT),
- catene di dipendenze, permessi, versioni, configurazioni.

Il risultato è spesso un ecosistema:

- fragile,
- difficile da comprendere end-to-end,
- costoso da mantenere,
- poco portabile tra ambienti diversi.

### 1.2 Numax

Numax è un runtime portabile progettato per eseguire applicazioni distribuite in modo semplice, sicuro e coerente su qualsiasi ambiente. 
La sua architettura è semplice ed è composta da 3 blocchi:

1. **WebAssembly (WASM)** *(Implemented)*
   Il runtime esegue moduli WASM in sandbox isolata, con un set controllato di host API.
   Questo garantisce portabilità tra piattaforme, avvii rapidi e sicurezza memory-safe.

2. **Datastore key/value locale embedded** *(Implemented)*
   Ogni istanza del runtime include un datastore locale persistente e sempre disponibile.
   Lo stato vive vicino al calcolo, riducendo latenza, dipendenze esterne e permettendo il funzionamento offline.

3. **Sincronizzazione distribuita dello stato** *(Prototype)*
   Il runtime replica automaticamente lo stato tra nodi tramite CRDT (Conflict-free replicated data type) e gossip.
   Il protocollo gossip gestisce propagazione, resilienza e comunicazione tra nodi anche con rete intermittente.

## 1.3 Il cuore tecnologico

I concetti chiave di numax sono:

- **Semplicità architetturale come principio guida**
  Il runtime integra solo ciò che è davvero necessario come compute, stato locale o sincronizzazione.
  Tutto il resto rimane opzionale. Questo riduce drasticamente la quantità di infrastruttura da configurare, mantenere e sopratutto capire.

- **Stato e codice nello stesso ambiente**
  Il datastore locale è parte integrante del runtime.
  Il calcolo non è separato dallo stato tramite un database remoto: vive nello stesso luogo, con benefici in termini di latenza, coerenza e resilienza offline.

- **WASM come unità di calcolo portabile**
  Il modulo WASM è l'unico artefatto necessario per distribuire logica applicativa.
  Lo stesso modulo può essere eseguito su un po' ovunque senza modifiche, evitando codebase multiple o branching condizionale.

- **CRDT invece di lock o transazioni distribuite**
  La sincronizzazione dello stato non richiede coordinamento centralizzato:
  i CRDT garantiscono convergenza automatica tra nodi anche in presenza di latenze, disconnessioni o aggiornamenti concorrenti.

- **Funzionamento offline come caratteristica nativa**
  Ogni nodo mantiene una copia locale dello stato e continua a funzionare autonomamente.
  Quando torna online, il runtime esegue la riconciliazione tramite CRDT, senza conflitti e senza codice applicativo aggiuntivo.

In sintesi: l'obbiettivo è costruire applicazioni distribuite senza dipendere da una infrastruttura complessa, mantenendo al tempo stesso portabilità, resilienza e coerenza dei dati.

Numax non elimina la complessità del dominio distribuito: la gestisce in modo sistematico, incorporandola nel runtime.

L'obiettivo è ridurre drasticamente la complessità auto-imposta fornendo: un runtime portabile unificato basato su WebAssembly, uno store locale integrato vicino al calcolo e una sincronizzazione automatica basata su CRDT.

In questo modo, lo sviluppatore mantiene il controllo sulla complessità necessaria del proprio dominio, senza dover pagare il costo dell'infrastruttura distribuita tradizionale.

---

## 2. Contesto

### 2.1 Complessità Necessaria vs Complessità Auto-Imposta

Per fare chiarezza, la progettazione di sistemi distribuiti comporta una parte di complessità che è intrinseca al dominio e non può essere eliminata. Tuttavia, l'ecosistema tecnologico moderno introduce spesso complessità aggiuntiva non strettamente necessaria.
Questa sezione chiarisce questa distinzione.

**Complessità Necessaria:**

È la complessità che deriva dalle proprietà naturali dei sistemi distribuiti:
* la rete è inaffidabile e introduce ritardi, disconnessioni e partizioni
* più nodi possono aggiornare lo stesso stato in parallelo
* i client possono trovarsi offline e riconnettersi in momenti diversi
* gli ambienti di esecuzione sono eterogenei (browser, server, mobile, IoT)

> Questi aspetti non possono essere evitati: richiedono modelli dati e meccanismi di sincronizzazione robusti.

**Complessità Auto-Imposta:**

È la complessità aggiunta dagli strumenti moderni e dal toolchain, non dal problema:
* orchestratori complessi spesso anche per applicazioni piccole
* dipendenze multiple tra servizi e infrastrutture esterne
* configurazioni distribuite in molti file (YAML, operator custom, chart)
* stato delegato spesso a DB remoti anche quando sarebbe più efficiente mantenerlo localmente
* tool differenziati per ambiente (dev, browser, edge, IoT)

> Questa complessità è spesso evitabile: nasce dalla stratificazione di tecnologie general-purpose applicate anche in scenari in cui non sono strettamente necessarie.

### 2.2 Opportunità

L'emergere di WebAssembly e di modelli di sincronizzazione come i CRDT apre la possibilità di ripensare la base su cui costruiamo sistemi distribuiti:

- esecuzione più portabile,
- maggiore isolamento,
- sincronizzazione dello stato basata su proprietà matematiche,
- runtime più leggeri e indipendenti dall'infrastruttura specifica.

Numax nasce in questo spazio.

---

## 3. Principi di Design di Numax

Questo paragrafo definisce cosa Numax è e cosa non è.

### 3.1 I tre elementi principali

Numax integra solo tre componenti fondamentali:

1. esecuzione di moduli WASM in sandbox *(Implemented)*
2. datastore locale sempre disponibile *(Implemented)*
3. sincronizzazione distribuita dello stato *(Prototype)*

Qualsiasi altra funzionalità appartiene ai livelli superiori o a tool esterni.
Il runtime rimane intenzionalmente minimo.

### 3.2 Portabilità radicale

Un modulo WASM deve poter girare senza modifiche:

- on premise
- in cloud
- su edge nodes
- su dispositivi embedded
- nel browser

Questo approccio riduce configurazioni specifiche per ambiente, dipendenze da piattaforma e branching condizionale nel codice applicativo.

### 3.3 Stato vicino al calcolo

Il runtime assume che lo stato debba:

- essere locale per garantire velocità e resilienza
- essere replicabile per garantire distribuzione *(Prototype)*

Per questo il datastore è integrato nel runtime e non dipende da componenti esterni.

### 3.4 Sincronizzazione senza conflitti

La sincronizzazione dello stato usa modelli CRDT che consentono:

- aggiornamenti concorrenti
- consistenza eventuale
- assenza di conflitti manuali

La rete è considerata fallibile per natura.
Il runtime gestisce disconnessioni, latenze e rientri come condizioni normali, non eccezionali.

---

## 4. Panoramica su Numax

### 4.1 Componenti principali

Numax è composto da sei crate Rust organizzati in un workspace:

| Crate | Stato | Responsabilità |
|-------|-------|----------------|
| **nx-core** | *Implemented* | Runtime WASM, sandboxing, host API |
| **nx-store** | *Implemented* | Datastore key/value locale persistente |
| **nx-sync** | *Prototype* | Strutture dati CRDT, operazioni, identità nodi |
| **nx-net** | *Prototype* | Networking TCP, protocollo messaggi, gossip |
| **nx-sdk** | *Implemented* | SDK per sviluppare moduli WASM guest |
| **nx-cli** | *Implemented* | Interfaccia a linea di comando |

Questa separazione mantiene responsabilità chiare e permette di evolvere i componenti in modo indipendente.

### 4.2 Ambienti supportati

Numax è progettato per girare su:

- server (x86_64, ARM64),
- edge nodes,
- browser (tramite WASM),
- mobile (tramite integrazione nativa),
- IoT (ARM / RISC-V).

La CI attuale verifica la compilazione su:
- Ubuntu (x86_64)
- macOS (x86_64, ARM64)
- Windows (x86_64)

### 4.3 Modello di esecuzione e dati (overview)
- **Compute**: un nodo Numax esegue moduli **WASM** in sandbox, esponendo un set limitato di Host API. *(Implemented)*
- **State**: ogni nodo mantiene uno **store key/value locale** persistente basato su sled. *(Implemented)*
- **Sync**: una parte dello stato può essere **replicata** tra nodi tramite **CRDT + gossip**. *(Prototype)*
- **Consistency**: il sistema mira a **convergenza eventuale** (eventual consistency): in assenza di nuove scritture e con connettività sufficiente, tutti i nodi convergono allo stesso stato. *(Prototype)*
- **Rete fallibile**: disconnessioni e rientri sono condizioni normali; Numax include meccanismi per recuperare delta mancanti. *(Prototype)*

### 4.4 Security model & threat model
**Assunzioni:**
- La rete è potenzialmente **ostile** (osservazione, MITM, packet injection, route hijack). *(Planned)*
- I nodi possono essere **offline** o intermittenti oppure alcuni peer possono essere **malevoli** o non affidabili. *(Planned)*

**Obiettivi di sicurezza:**
- Isolamento del compute (sandbox WASM). *(Implemented)*
- Confidenzialità/integrità delle comunicazioni tra nodi. *(Planned)*
- Autenticazione dei peer (evitare MITM). *(Planned)*
- (valutare) Policy di membership (permissioned vs open). *(Planned)*

**Guardrail implementati:**
- Limiti su lunghezza chiavi: 8 KiB
- Limiti su lunghezza valori: 1 MiB
- Limiti su buffer di output: 1 MiB
- Validazione di tutti gli input dal guest prima di processarli

**Fuori scope e idee future:**
- Bug logici nel modulo applicativo.
- Data poisoning se si accettano peer non trusted senza policy.
- Compromissione host-level (se un nodo perde la chiave privata, serve revoca/rotazione).

---

## 5. Architettura del Sistema

Di seguito una panoramica ad alto livello dei componenti principali di Numax e delle loro interazioni.

```
┌─────────────────────────────────────────────────────────────┐
│                      WASM Module (Guest)                     │
│                    (compiled with nx-sdk)                    │
└──────────────────────────┬──────────────────────────────────┘
                           │ Host API calls (namespace "nx")
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                       nx-core (Host)                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  Wasmtime   │  │  Host API   │  │    WASI (preview1)  │  │
│  │   Engine    │  │  db_*, log  │  │    stdio, args      │  │
│  └─────────────┘  └──────┬──────┘  └─────────────────────┘  │
└──────────────────────────┼──────────────────────────────────┘
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
   ┌────────────┐   ┌────────────┐   ┌────────────┐
   │  nx-store  │   │  nx-sync   │   │   nx-net   │
   │   (sled)   │   │  (CRDTs)   │   │   (TCP)    │
   └────────────┘   └────────────┘   └────────────┘
```

### 5.1 Numax Core - Runtime WASM *(Implemented)*

**Responsabilità principali:**

- caricare ed eseguire moduli WASM,
- gestire la sandbox con isolamento rigoroso,
- esporre le host functions verso il modulo guest,
- integrare WASI preview1 come base standard per I/O.

**Tecnologie:**

- Implementazione in Rust,
- **Wasmtime** come motore WASM,
- WASI preview1 come interfaccia di sistema.

**Caratteristiche:**

- isolamento rigoroso: il guest non può accedere a risorse non esplicitamente concesse,
- nessun accesso implicito al filesystem,
- avvio rapido (< 5 ms tipico),
- sicurezza memory-safe garantita da Rust e dal modello WASM.

**Host API esposte (namespace `nx`):**

Il modulo guest importa funzioni dal namespace `"nx"`. Attualmente sono disponibili:

| Funzione | Stato | Descrizione |
|----------|-------|-------------|
| `db_get` | *Implemented* | Legge un valore dal datastore locale |
| `db_set` | *Implemented* | Scrive un valore nel datastore locale |
| `db_delete` | *Implemented* | Elimina una chiave dal datastore |
| `host_log_v2` | *Implemented* | Scrive un messaggio di log con livello |
| `db_scan` | *Planned* | Scansione per prefisso |

**Convenzione dei return code:**

Le funzioni host restituiscono interi con semantica precisa:

| Codice | Costante | Significato |
|--------|----------|-------------|
| `>= 0` | - | Successo (per `db_get`: lunghezza del valore letto) |
| `0` | `OK` | Successo (per `db_set`, `db_delete`) |
| `-1` | `ERR_NOT_FOUND` | Chiave non trovata |
| `-2` | `ERR_BUF_TOO_SMALL` | Buffer output troppo piccolo, riprovare con buffer più grande |
| `-3` | `ERR_INTERNAL` | Errore interno del runtime |

Questa convenzione permette al guest di gestire gli errori in modo deterministico senza eccezioni o panic.

**Limiti di sicurezza (guardrail):**

| Risorsa | Limite | Note |
|---------|--------|------|
| Lunghezza chiave | 8 KiB | Previene allocazioni eccessive |
| Lunghezza valore | 1 MiB | Bilancia utilità e sicurezza |
| Buffer output | 1 MiB | Limita memoria copiata verso il guest |

Questi limiti proteggono l'host da comportamenti patologici o malevoli del guest.

### 5.2 Numax Store - Datastore Locale *(Implemented)*

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

// Lettura
let value: Option<Vec<u8>> = db::get("my_key")?;

// Scrittura
db::set("my_key", b"my_value")?;

// Eliminazione
db::delete("my_key")?;
```

L'SDK gestisce automaticamente la serializzazione, i buffer e i retry in caso di `ERR_BUF_TOO_SMALL`.

**Proprietà:**

- ACID locale per singole operazioni,
- operazioni atomiche get/set/delete,
- nessun lock esplicito richiesto dal chiamante,
- dati persistenti tra riavvii del runtime.

### 5.3 Numax Sync - Replica Distribuita *(Prototype)*

Numax Sync è responsabile della replica dello stato tra nodi.
La versione attuale fornisce le primitive fondamentali; l'integrazione completa con nx-net è in sviluppo.

**Componenti implementati:**

**NodeId** *(Implemented)*

Identifica univocamente un nodo nella rete.
Può essere generato casualmente (UUID) o assegnato esplicitamente.

```rust
let node = NodeId::new("node-alpha");
let random_node = NodeId::generate(); // UUID v4
```

**Op e OpId** *(Implemented)*

Rappresentano operazioni CRDT serializzabili e trasportabili tra nodi.

```rust
pub struct Op {
    pub id: OpId,           // Identificatore univoco dell'operazione
    pub origin: NodeId,     // Nodo che ha generato l'operazione
    pub timestamp: u64,     // Timestamp logico
    pub kind: OpKind,       // Tipo di operazione (es. GCounterIncrement)
}
```

Le operazioni sono serializzabili in JSON e binario per il trasporto via rete.

**GCounter (Grow-only Counter)** *(Implemented)*

Il primo CRDT implementato è un contatore distribuito che supporta solo incrementi.
Ogni nodo mantiene il proprio "slot" e può incrementare solo quello.

Struttura interna:
```rust
pub struct GCounter {
    counts: HashMap<String, u64>,  // NodeId -> valore locale
}
```

Il valore totale è la somma di tutti gli slot: `value() = Σ counts[node]`.

**Proprietà CRDT garantite:**

I CRDT (Conflict-free Replicated Data Types) garantiscono convergenza automatica grazie a tre proprietà matematiche:

1. **Commutatività**: `merge(A, B) == merge(B, A)`
   L'ordine in cui i nodi ricevono gli aggiornamenti non influisce sul risultato finale.

2. **Associatività**: `merge(merge(A, B), C) == merge(A, merge(B, C))`
   Non importa come vengono raggruppati i merge.

3. **Idempotenza**: `merge(A, A) == A`
   Applicare lo stesso aggiornamento più volte non cambia lo stato.

Queste proprietà sono verificate dalla test suite con test dedicati.

**Operazione di merge:**

```rust
pub fn merge(&mut self, other: &GCounter) {
    for (node, &value) in &other.counts {
        let entry = self.counts.entry(node.clone()).or_insert(0);
        *entry = (*entry).max(value);  // Prende il massimo
    }
}
```

Il merge prende il valore massimo per ogni slot.
Questo garantisce che:
- nessun incremento venga perso,
- incrementi duplicati non vengano contati due volte,
- l'ordine di ricezione non influenzi il risultato.

**Protezione overflow:**

Gli incrementi usano `saturating_add` per prevenire overflow:
```rust
*entry = entry.saturating_add(delta);  // Satura a u64::MAX invece di overflow
```

**CRDT pianificati:**

| Tipo | Descrizione | Stato |
|------|-------------|-------|
| PNCounter | Counter con incrementi e decrementi | *Planned* |
| LWW-Register | Registro last-writer-wins | *Planned* |
| ORSet | Set con add/remove osservati | *Planned* |
| LWW-Map | Mappa con semantica LWW | *Planned* |

### 5.4 Numax Net - Networking *(Prototype)*

Numax Net gestisce la comunicazione tra nodi per la sincronizzazione dello stato.

**Architettura:**

La rete è peer-to-peer: ogni nodo può comunicare direttamente con altri nodi senza un server centrale.
La versione attuale usa TCP come trasporto; TLS è pianificato per le versioni successive.

**Protocollo messaggi:**

I messaggi sono serializzati in JSON con un prefisso di lunghezza (4 byte big-endian):

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
| `PullSince` | Client → Server | Richiedi operazioni dopo un certo OpId |
| `Ping` | Bidirezionale | Keepalive |
| `Pong` | Bidirezionale | Risposta a Ping |

**Struttura dei messaggi:**

```rust
pub enum MessageKind {
    Hello { node_id: NodeId, version: u32 },
    HelloAck { node_id: NodeId, version: u32 },
    PushOps { ops: Vec<Op> },
    PushOpsAck { received_count: usize },
    PullSince { since_op_id: Option<String> },
    Ping,
    Pong,
}
```

**Versioning del protocollo:**

Il protocollo include un numero di versione (`PROTOCOL_VERSION = 1`) scambiato durante l'handshake.
Questo permette evoluzione retrocompatibile e rilevamento di incompatibilità.

### 5.5 Numax SDK *(Implemented)*

L'SDK fornisce un'interfaccia ergonomica per sviluppare moduli WASM guest.

**Moduli disponibili:**

| Modulo | Funzionalità |
|--------|--------------|
| `nx_sdk::db` | Accesso al datastore (get, set, delete) |
| `nx_sdk::log` | Logging verso l'host |

**Esempio completo:**

```rust
use nx_sdk::{db, log};

#[no_mangle]
pub extern "C" fn run() {
    log("Modulo avviato");
    
    // Scrivi un valore
    if let Err(e) = db::set("visits", b"1") {
        log(&format!("Errore scrittura: {:?}", e));
        return;
    }
    
    // Leggi il valore
    match db::get("visits") {
        Ok(Some(value)) => {
            log(&format!("Valore letto: {} bytes", value.len()));
        }
        Ok(None) => {
            log("Chiave non trovata");
        }
        Err(e) => {
            log(&format!("Errore lettura: {:?}", e));
        }
    }
    
    log("Modulo completato");
}
```

**Gestione automatica dei buffer:**

L'SDK gestisce automaticamente il caso `ERR_BUF_TOO_SMALL`:

1. Prima chiamata con buffer di dimensione stimata
2. Se il buffer è troppo piccolo, rialloca con la dimensione corretta
3. Seconda chiamata con buffer adeguato

Questo nasconde la complessità della gestione memoria al developer.

### 5.6 Numax CLI *(Implemented)*

La CLI fornisce l'interfaccia principale per interagire con il runtime.

**Comandi disponibili:**

```bash
# Esegue un modulo WASM
nx run <module.wasm>

# Esegue con directory dati custom
nx run <module.wasm> --data-dir ./my-data

# Esegue con sync abilitato (prototype)
nx run <module.wasm> --sync \
    --sync-listen 0.0.0.0:9000 \
    --sync-peers 192.168.1.10:9000,192.168.1.11:9000 \
    --sync-keys "counter:,votes:"
```

**Opzioni:**

| Flag | Descrizione |
|------|-------------|
| `--data-dir` | Directory per dati persistenti |
| `--sync` | Abilita sincronizzazione |
| `--sync-listen` | Indirizzo su cui ascoltare per connessioni peer |
| `--sync-peers` | Lista di peer iniziali (comma-separated) |
| `--sync-keys` | Prefissi delle chiavi da sincronizzare |

### 5.7 Topologia: epidemic gossip *(Prototype)*

Numax Net non assume una topologia ad anello (es. `n1→n2→n3→…`) perché sarebbe fragile: la caduta di un nodo può spezzare la catena.

Utilizza invece un modello **peer-to-peer a gossip** in cui:
- ogni nodo mantiene connessioni attive verso un **sottoinsieme** di peer (fanout **K**);
- gli aggiornamenti (operazioni CRDT) vengono propagati in modo "epidemico": un nodo invia l'update ai suoi peer, i peer lo inoltrano ad altri peer, fino a coprire la rete;
- ogni operazione ha un **identificatore univoco** (`OpId`) così i nodi possono **deduplicare** e prevenire loop.

Questo approccio scala meglio del full-mesh (tutti connessi con tutti) e rimane resiliente anche in presenza di disconnessioni temporanee.

### 5.8 Resilienza: nodo down, rete intermittente, rientro *(Planned)*

La rete è considerata fallibile per natura: nodi possono spegnersi, perdere connettività o rientrare.

Quando un peer diventa irraggiungibile:
- il nodo applica timeout e retry con **backoff esponenziale**;
- marca il peer come **down** e lo rimuove dal set attivo;
- seleziona un nuovo peer dal discovery per mantenere il fanout **K**.

Quando un nodo rientra:
- ristabilisce connessioni con i peer noti;
- esegue un meccanismo di **anti-entropy** (`PullSince`) per recuperare gli update mancanti;
- converge allo stesso stato grazie alle proprietà dei CRDT.

### 5.9 Sicurezza del canale *(Planned)*

Numax assume una rete ostile: il trasporto può essere osservato, alterato o reindirizzato.
Per questo, tutte le comunicazioni tra nodi dovranno avvenire su canali cifrati e autenticati.

Obiettivi:
- **Confidenzialità**: terzi non possono leggere il traffico.
- **Integrità**: terzi non possono modificare i messaggi.
- **Autenticazione**: un nodo parla solo con peer che dimostrano la propria identità.
- **Forward Secrecy**: compromissione futura di una chiave non decifra traffico passato.

Implementazione prevista: **TLS 1.3** con mutual authentication o protocollo equivalente.

---

## 6. Modello di Programmazione

### 6.1 Moduli WASM come unità di calcolo *(Implemented)*

Un'applicazione Numax è composta da uno o più moduli WASM che:

* eseguono logica applicativa pura,
* leggono/scrivono sul datastore locale tramite host API,
* (futuro) pubblicano e ricevono aggiornamenti via Sync,
* (futuro) effettuano chiamate HTTP se esplicitamente permesso.

Il modulo deve esporre una funzione `run` con firma:
```rust
#[no_mangle]
pub extern "C" fn run() {
    // logica applicativa
}
```

### 6.2 API Host esposte ai moduli

**Namespace import:** `"nx"`

Tutte le funzioni host sono importate dal namespace `"nx"`.
Il modulo WASM dichiara le import e l'SDK fornisce wrapper type-safe.

**Database:**

| Funzione | Firma | Stato |
|----------|-------|-------|
| `db_get` | `(key_ptr: u32, key_len: u32, out_ptr: u32, out_cap: u32) -> i32` | *Implemented* |
| `db_set` | `(key_ptr: u32, key_len: u32, val_ptr: u32, val_len: u32) -> i32` | *Implemented* |
| `db_delete` | `(key_ptr: u32, key_len: u32) -> i32` | *Implemented* |

**Logging:**

| Funzione | Firma | Stato |
|----------|-------|-------|
| `host_log_v2` | `(level: u32, msg_ptr: u32, msg_len: u32) -> ()` | *Implemented* |

Livelli di log:
- 0 = trace
- 1 = debug
- 2 = info
- 3 = warn
- 4 = error

**Sync (pianificate):**

| Funzione | Descrizione | Stato |
|----------|-------------|-------|
| `sync_publish` | Pubblica un'operazione CRDT | *Planned* |
| `sync_on_update` | Registra callback per aggiornamenti | *Planned* |

**Networking (pianificate):**

| Funzione | Descrizione | Stato |
|----------|-------------|-------|
| `http_fetch` | HTTP request (con whitelist) | *Planned* |

### 6.3 Configurazione e Deploy *(Planned)*

Il deploy consiste nell'invio di un file `.wasm` e una configurazione minimale.

Esempio di configurazione (formato in definizione):

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

Il progetto include una test suite automatizzata che verifica il corretto funzionamento di tutti i componenti.

**Copertura attuale:**

| Crate | Test | Note |
|-------|------|------|
| nx-core | 2 | Config sync |
| nx-net | 7 | Messaggi, peer, roundtrip |
| nx-store | 10 | 5 unit + 5 integration |
| nx-sync | 19 | CRDT properties, serialization |
| **Totale** | **38** | |

**Test CRDT specifici:**

La test suite verifica esplicitamente le proprietà matematiche dei CRDT:

- `test_gcounter_merge_commutativity` - Verifica merge(A,B) == merge(B,A)
- `test_gcounter_merge_associativity` - Verifica (A⊕B)⊕C == A⊕(B⊕C)
- `test_gcounter_merge_idempotency` - Verifica A⊕A == A
- `test_gcounter_overflow_protection` - Verifica saturazione invece di overflow

**CI/CD:**

GitHub Actions esegue la test suite su ogni push/PR:
- Ubuntu latest (x86_64)
- macOS latest (x86_64/ARM64)
- Windows latest (x86_64)

Job eseguiti:
1. `check` - Verifica compilazione
2. `fmt` - Verifica formattazione codice
3. `clippy` - Linter Rust
4. `test` - Esegue test suite completa
5. `build-wasm` - Compila esempi WASM

---

## 8. Casi d'Uso
//TODO

---

## 9. Dove si colloca Numax

Numax si posiziona in uno spazio specifico dell'ecosistema:

**Runtime portabile con stato locale integrato e sincronizzazione nativa.**

//TODO

**Numax è il runtime per chi vuole costruire sistemi distribuiti senza costruire un'infrastruttura distribuita.**

---

## 10. Limitazioni

La prima versione presenta alcune limitazioni:

- **Non sostituisce orchestratori complessi.**
  Non è progettato per gestire cluster estesi o deployment ad alta scalabilità.

- **Non ottimizzato per workload CPU-bound.**
  Il focus è su I/O e coordinamento, non su calcolo intensivo.

- **I modelli dati devono essere compatibili con i CRDT.**
  Pattern basati su lock o transazioni forti non si adattano direttamente.

- **Debugging e osservabilità sono iniziali.**
  Strumenti più avanzati arriveranno nelle versioni successive.

- **TLS/mTLS non ancora implementato.**
  Le comunicazioni in chiaro sono accettabili solo per sviluppo locale.

---

## 11. Conclusioni

Numax propone un runtime unificato che combina:
* esecuzione sicura e portabile tramite WebAssembly,
* datastore locale integrato per uno stato vicino al calcolo,
* sincronizzazione distribuita basata su CRDT e gossip.

L'obiettivo non è replicare l'ecosistema esistente, ma ridurre la complessità necessaria per costruire applicazioni distribuite moderne.

Questo whitepaper rappresenta una base di partenza concettuale e tecnica.
Le iterazioni successive ne affineranno dettagli, esempi pratici, confronti e risultati sperimentali.

---

## Appendice A: Struttura del Repository

```
numax/
├── Cargo.toml              # Workspace manifest
├── crates/
│   ├── nx-core/            # Runtime WASM + Host API
│   ├── nx-store/           # Datastore locale (sled)
│   ├── nx-sync/            # CRDT e operazioni
│   ├── nx-net/             # Networking e messaggi
│   ├── nx-sdk/             # SDK per guest WASM
│   └── nx-cli/             # CLI
├── examples/
│   ├── distributed_counter/
│   └── distributed_chat/
├── docs/
│   └── HOST_API.md
├── WHITEPAPER.md
├── ROADMAP_v0.1.0.md
└── LICENSE
```

---

## Appendice B: Riferimenti

- WebAssembly: https://webassembly.org/
- WASI: https://wasi.dev/
- Wasmtime: https://wasmtime.dev/
- sled: https://sled.rs/
- CRDT: Shapiro et al., "A comprehensive study of Convergent and Commutative Replicated Data Types"
