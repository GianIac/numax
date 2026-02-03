# Numax Runtime - Whitepaper Tecnico (Versione 0.1 ITA)

> **Nota V0.1.0  
> Questo documento è una base di partenza. Alcune sezioni contengono `TODO:` per indicare parti da approfondire in iterazioni successive.

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

1. **WebAssembly (WASM)**
   Il runtime esegue moduli WASM in sandbox isolata, con un set controllato di host API.
   Questo garantisce portabilità tra piattaforme, avvii rapidi e sicurezza memory-safe.

2. **Datastore key/value locale embedded**
   Ogni istanza del runtime include un datastore locale persistente e sempre disponibile.
   Lo stato vive vicino al calcolo, riducendo latenza, dipendenze esterne e permettendo il funzionamento offline.

3. **Sincronizzazione distribuita dello stato**
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
  Il modulo WASM è l’unico artefatto necessario per distribuire logica applicativa.
  Lo stesso modulo può essere eseguito su un po' ovunque senza modifiche, evitando codebase multiple o branching condizionale.

- **CRDT invece di lock o transazioni distribuite**
  La sincronizzazione dello stato non richiede coordinamento centralizzato:
  i CRDT garantiscono convergenza automatica tra nodi anche in presenza di latenze, disconnessioni o aggiornamenti concorrenti.

- **Funzionamento offline come caratteristica nativa**
  Ogni nodo mantiene una copia locale dello stato e continua a funzionare autonomamente.
  Quando torna online, il runtime esegue la riconciliazione tramite CRDT, senza conflitti e senza codice applicativo aggiuntivo.

In sintesi: l'obbiettivo è costruire applicazioni distribuite senza dipendere da una infrastruttura complessa, mantenendo al tempo stesso portabilità, resilienza e coerenza dei dati.

Numax non elimina la complessità del dominio distribuito: la gestisce in modo sistematico, incorporandola nel runtime.

L’obiettivo è ridurre drasticamente la complessità auto-imposta fornendo: un runtime portabile unificato basato su WebAssembly, uno store locale integrato vicino al calcolo e una sincronizzazione basata che gestisce automaticamente la concorrenza.

In questo modo, lo sviluppatore mantiene il controllo sulla complessità necessaria del proprio dominio, senza dover pagare il costo dell’infrastruttura distribuita tradizionale.

---

## 2. Contesto

### 2.1 Complessità Necessaria vs Complessità Auto-Imposta

Per fare chiarezza, la progettazione di sistemi distribuiti comporta una parte di complessità che è intrinseca al dominio e non può essere eliminata. Tuttavia, l’ecosistema tecnologico moderno introduce spesso un livello aggiuntivo di complessità che non deriva dal problema, ma dagli strumenti utilizzati per affrontarlo.
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

L’emergere di WebAssembly e di modelli di sincronizzazione come i CRDT apre la possibilità di ripensare la base su cui costruiamo sistemi distribuiti:

- esecuzione più portabile,
- maggiore isolamento,
- sincronizzazione dello stato basata su proprietà matematiche,
- runtime più leggeri e indipendenti dall’infrastruttura specifica.

Numax nasce in questo spazio.

---

## 3. Principi di Design di Numax

Questo paragrafo definisce cosa Numax è e cosa non è.

### 3.1 I tre elementi principali

Numax integra solo tre componenti fondamentali:

1. esecuzione di moduli WASM in sandbox
2. datastore locale sempre disponibile
3. sincronizzazione distribuita dello stato

Qualsiasi altra funzionalità appartiene ai livelli superiori o a tool esterni
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
- essere replicabile per garantire distribuzione

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

Numax è composto da tre moduli core:

1. **Numax Core** - runtime WASM + sandboxing + host API.
2. **Numax Store** - datastore key/value locale.
3. **Numax Sync** - replica dello stato tramite CRDT + protocollo gossip.

### 4.2 Ambienti supportati

Numax è progettato per girare su:

- server (x86_64),
- edge nodes,
- browser (tramite WASM nel WASM),
- mobile (tramite integrazione nativa),
- IoT (ARM / RISC-V).

> TODO: in futuro verranno elencati target specifici previsti per la prima versione (es. Linux server, Raspberry Pi, ecc.).

### 4.3 Modello di esecuzione e dati (overview)
- **Compute**: un nodo Numax esegue moduli **WASM** in sandbox, esponendo un set limitato di Host API.
- **State**: ogni nodo mantiene uno **store key/value locale** persistente.
- **Sync**: una parte dello stato può essere **replicata** tra nodi tramite **CRDT + gossip**.
- **Consistency**: il sistema mira a **convergenza eventuale** (eventual consistency): in assenza di nuove scritture e con connettività sufficiente, tutti i nodi convergono allo stesso stato.
- **Rete fallibile**: disconnessioni e rientri sono condizioni normali; Numax include meccanismi per recuperare delta mancanti.

---

## 5. Architettura del Sistema

Di seguito una panoramica ad alto livello dei tre componenti principali di Numax e delle loro interazioni.

// TODO: add img 

### 5.1 Numax Core - Runtime WASM

**Responsabilità principali:**

- caricare ed eseguire moduli WASM,
- gestire la sandbox,
- esporre le host functions verso il modulo,
- integrare WASI come base standard.

**Tecnologie:**

- implementazione in Rust,
- Wasmtime o WasmEdge come motore WASM,
- WASI come interfaccia di sistema.

**Caratteristiche:**

- isolamento rigoroso,
- nessun accesso implicito al filesystem,
- avvio rapido (< 5 ms),
- sicurezza memory-safe.

> TODO: dettagliare le host functions principali esposte (DB, sync, rete).

### 5.2 Numax Store - Datastore Locale

Numax Store fornisce un key/value store persistente locale per ogni istanza di runtime.

**API tipiche (lato modulo WASM):**

- `db_get(key)`
- `db_set(key, value)`
- `db_delete(key)`
- `db_scan(prefix)`

**Proprietà:**

- ACID locale,
- operazioni atomiche,
- prestazioni elevate,
- nessuna configurazione esterna.

Implementazione possibile:

- motore embedded in Rust (ex. `sled`),
- o LSM-tree custom.

### 5.3 Numax Sync - Replica Distribuita

Numax Sync è responsabile della replica dello stato tra nodi.

**Tecnologie concettuali:**

- CRDT (Conflict-free Replicated Data Types),
- protocolli gossip per la propagazione,
- TLS per la sicurezza dei canali.

**Proprietà:**

- consistenza eventuale,
- resilienza alla disconnessione,
- risoluzione automatica dei conflitti,
- sincronizzazione incrementale (deltas).

> TODO: specificare il tipo di CRDT utilizzati (es. OR-Set, LWW-Register, Map, ecc.).

### 5.4 Numax Net (rete) e Numax CLI

- **Numax Net**: gestisce networking, TLS, discovery peer-to-peer, gossip.
- **Numax CLI**: fornisce comandi per:
  - esecuzione moduli,
  - introspezione,
  - gestione peer.

Esempi di comandi:

```
nx run module.wasm
nx inspect module.wasm
nx peers
```
### 5.5 Topologia: epidemic gossip!

Numax Net non assume una topologia ad anello (es. `n1→n2→n3→…`) perché sarebbe fragile: la caduta di un nodo può spezzare la catena.

Ma utilizza un modello **peer-to-peer a gossip** in cui:
- ogni nodo mantiene connessioni attive verso un **sottoinsieme** di peer (fanout **K**);
- gli aggiornamenti (delta CRDT) vengono propagati in modo “epidemico”: un nodo invia l’update ai suoi peer, i peer lo inoltrano ad altri peer, fino a coprire la rete;
- ogni update ha un **identificatore** (es. `op_id`) e/o una versione logica, così i nodi possono **deduplicare** e prevenire loop.

Questo approccio scala meglio del full-mesh (tutti connessi con tutti) e rimane resiliente anche in presenza di disconnessioni temporanee.

### 5.6 Resilienza: nodo down, rete intermittente, rientro

La rete è considerata fallibile per natura: nodi possono spegnersi, perdere connettività o rientrare.

Quando un peer diventa irraggiungibile:
- il nodo applica timeout e retry con **backoff**;
- marca il peer come **down** e lo rimuove dal set attivo;
- seleziona un nuovo peer dal discovery per mantenere il fanout **K** (evitando che la mesh si “assottigli”).

Quando un nodo rientra:
- ristabilisce connessioni sicure (TLS/mTLS o equivalente);
- esegue un meccanismo di **anti-entropy** (pull periodico) per recuperare gli update mancanti;
- converge allo stesso stato grazie alle proprietà dei CRDT (commutativit��/merge-safe).

### 5.7 Sicurezza del canale (anti‑MITM) e identità dei nodi

Numax assume una rete ostile: il trasporto può essere osservato, alterato o reindirizzato (attacchi MITM, DNS/route hijack, Wi‑Fi malevolo).  
Per questo, **tutte le comunicazioni tra nodi avvengono su canali cifrati e autenticati**.

- **Confidenzialità**: terzi non possono leggere il traffico.
- **Integrità**: terzi non possono modificare i messaggi senza essere rilevati.
- **Autenticazione**: un nodo parla solo con peer che dimostrano la propria identità crittografica.
- **Forward Secrecy**: la compromissione futura di una chiave non decifra traffico passato.

Numax Net stabilisce connessioni usando **TLS 1.3 in modalità mutual authentication (mTLS)** (o protocollo equivalente basato su key exchange autenticato). (non ho ancora preso una decisone definitiva ... sto studiando)
La protezione contro MITM non deriva dalla sola cifratura, ma dal fatto che **il peer deve presentare una credenziale valida** (certificato o chiave pubblica attesa) durante l’handshake.

In altre parole:
- se un attaccante si inserisce “in mezzo” ma **non possiede una credenziale valida**, l’handshake fallisce;
- se il trasporto viene manomesso, i messaggi non verificano e la sessione viene terminata.

Ogni nodo possiede una coppia di chiavi (private/public). L’identità del nodo (**NodeID**) è derivata dalla sua chiave pubblica (es. hash).  
La verifica dell’identità può seguire due modelli:

1. **Rete permissioned (consigliato per cluster/edge gestiti)**  
   I nodi sono ammessi tramite una CA/registry: i certificati sono emessi e revocati da un’autorità o da un meccanismo di governance.

2. **Rete permissionless (sperimentale)**  
   L’identità è la chiave pubblica; le policy di trust (allowlist, reputazione, stake/slashing) determinano con chi parlare.

Se un nodo viene compromesso e l’attaccante ottiene la sua chiave privata, il traffico può risultare “valido” dal punto di vista TLS.  
Per questo Numax prevede un meccanismo di **revoca/quarantena** (denylist/CRL/registry) e rotazione chiavi: un NodeID compromesso può essere escluso rapidamente dalla rete.

---

## 6. Modello di Programmazione

### 6.1 Moduli WASM come unità di calcolo

L'idea è che un’applicazione è composta da uno o più moduli WASM che:

* eseguono logica applicativa,
* leggono/scrivono sul datastore locale,
* pubblicano e ricevono aggiornamenti via Sync,
* effettuano chiamate HTTP se necessario.

### 6.2 API Host esposte ai moduli

TODO: Descrizione allienata al codice attuale

**Datastore:**
* db_get(key)
* db_set(key, value)
* db_delete(key)
* db_scan(prefix)

**Sync:**
* sync_on_update(prefix, callback)
* sync_publish(op)

**Networking:**
* http_fetch(url)

**Eventi:**
* event_emit(topic, payload)
* event_subscribe(topic, callback)

TODO: definire meglio la semantica delle callback e eventuali limiti (timeout, dimensioni payload, ecc.).

### 6.3 Configurazione e Deploy
TO DO: aggiungere più info + esmepio reale 
Il deploy consiste nell’invio di un file .wasm e una configurazione minimale.
Esempio:

```
[name]
module = "cart_handler.wasm"

[permissions]
db = true
network = ["https://api.example.com"]

[sync]
prefix = "cart:"
```

Questa configurazione definisce:
* modulo WASM da eseguire,
* permessi sul datastore,
* domini di rete autorizzati,
* prefissi di chiavi soggette a sincronizzazione.

---

## 7. Casi d'Uso

### // TODO

> TODO: aggiungere esempi concreti (es. applicazione scsritta usando numax).

---

## 8. Dove si colloca Numax

Questa sezione chiarisce la posizione di Numax all'interno dell’ecosistema moderno, confrontandolo con le principali categorie di piattaforme utilizzate oggi per eseguire applicazioni distribuite.

### // TODO

Numax combina compute, stato e sync in un singolo runtime, fornendo un ambiente più alto livello per applicazioni distribuite.

### 8.X Quindi dove si colloca Numax?

Numax si posiziona in uno spazio specifico che oggi è quasi vuoto:

**runtime portabile con stato locale integrato e sincronizzazione nativa.** (magari lo scrivi meglio)

Riducendo la complessità dell’infrastruttura senza rinunciare a distribuzione, portabilità e sicurezza.

In breve:

**Numax è il runtime per chi vuole costruire sistemi distribuiti senza costruire un'infrastruttura distribuita.**

---

## 9. Limitazioni

Numax è pensato per semplificare lo sviluppo di applicazioni distribuite, ma la prima versione presenta alcune limitazioni da considerare.

- Non sostituisce orchestratori complessi. 
  Non è progettato per gestire cluster estesi o deployment ad alta scalabilità.

- Non è ottimizzato (per ora) per workload pesanti o CPU-bound. 

- I modelli dati devono essere compatibili con i CRDT. 
  Pattern basati su lock o transazioni forti non si adattano direttamente.

- Debugging e osservabilità sono iniziali. 
  Strumenti più avanzati arriveranno nelle versioni successive.

- Supporto a piattaforme aggiuntive (mobile, RISC-V) sarà introdotto progressivamente.

Queste limitazioni riguardano la prima release e volendo verranno ridotte nel corso dello sviluppo.

---

## 10. Conclusioni

Numax propone un runtime unificato che combina:
* esecuzione sicura e portabile tramite WebAssembly,
* datastore locale integrato per uno stato vicino al calcolo,
* sincronizzazione distribuita basata su CRDT e gossip.
L’obiettivo non è replicare l’ecosistema esistente, ma ridurre la complessità necessaria per costruire applicazioni distribuite moderne.
Questo whitepaper rappresenta una base di partenza concettuale e tecnica.

Le iterazioni successive ne affineranno dettagli, esempi pratici, confronti e risultati sperimentali.
