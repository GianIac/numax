# Numax Runtime — Whitepaper Tecnico (Versione 0.1 ITA)

> **Nota V0.1.0  
> Questo documento è una base di partenza. Alcune sezioni contengono `TODO:` per indicare parti da approfondire in iterazioni successive.

---

## 1. Executive Summary

### 1.1 Problema

Lo sviluppo di applicazioni distribuite moderne è diventato eccessivamente complesso e per far girare logica relativamente semplice si ricorre a:

- container e orchestratori,
- database esterni per ogni tipo di stato,
- sistemi di sincronizzazione ad hoc,
- differenze significative tra ambienti (browser, server, edge, IoT),
- catene di dipendenze, permessi, versioni, configurazioni.

Il risultato è spesso un ecosistema:

- fragile,
- difficile da comprendere end-to-end,
- costoso da mantenere,
- poco portabile tra ambienti diversi.

### 1.2 Soluzione proposta: Numax

Numax è un runtime portabile progettato per eseguire applicazioni distribuite in modo semplice, sicuro e coerente su qualsiasi ambiente. 
Integra tre componenti fondamentali:

1. **Esecuzione di moduli WebAssembly (WASM)**
   Il runtime esegue moduli WASM in sandbox isolata, con un set controllato di host API.
   Questo garantisce portabilità tra piattaforme, avvii rapidi e sicurezza memory-safe.

2. **Datastore key/value locale embedded**
   Ogni istanza del runtime include un datastore locale persistente e sempre disponibile.
   Lo stato vive vicino al calcolo, riducendo latenza, dipendenze esterne e permettendo il funzionamento offline.

3. **Sincronizzazione distribuita dello stato basata su CRDT + gossip**
   Il runtime replica automaticamente lo stato tra nodi tramite CRDT, evitando conflitti senza lock o transazioni distribuite.
   Il protocollo gossip gestisce propagazione, resilienza e comunicazione tra nodi anche con rete intermittente.

## 1.3 Concetti Chiave

I concetti chiave che lo rendono utile sono:

- **Semplicità architetturale come principio guida**
  Il runtime integra solo ciò che è davvero necessario (compute, stato locale, sincronizzazione).
  Tutto il resto rimane opzionale. Questo riduce drasticamente la quantità di infrastruttura da configurare, mantenere e capire.

- **Stato e codice nello stesso ambiente**
  In Numax il datastore locale è parte integrante del runtime.
  Il calcolo non è separato dallo stato tramite un database remoto: vive nello stesso luogo, con benefici in termini di latenza, coerenza e resilienza offline.

- **WASM come unità di calcolo portabile**
  Il modulo WASM è l’unico artefatto necessario per distribuire logica applicativa.
  Lo stesso modulo può essere eseguito su server, edge, browser, mobile e IoT senza modifiche, evitando codebase multiple o branching condizionale.

- **CRDT invece di lock o transazioni distribuite**
  La sincronizzazione dello stato non richiede coordinamento centralizzato:
  i CRDT garantiscono convergenza automatica tra nodi anche in presenza di latenze, disconnessioni o aggiornamenti concorrenti.

- **Funzionamento offline come caratteristica nativa**
  Ogni nodo mantiene una copia locale dello stato e continua a funzionare autonomamente.
  Quando torna online, il runtime esegue la riconciliazione tramite CRDT, senza conflitti e senza codice applicativo aggiuntivo.

In sintesi: Numax rende possibile costruire applicazioni distribuite senza dipendere da una infrastruttura complessa, mantenendo al tempo stesso portabilità, resilienza e coerenza dei dati.


### 1.4 Obiettivo

Numax non elimina la complessità del dominio distribuito: la gestisce in modo sistematico, incorporandola nel runtime.

L’obiettivo è ridurre drasticamente la complessità auto-imposta fornendo:

* un runtime portabile unificato basato su WebAssembly,
* uno store locale integrato vicino al calcolo,
* sincronizzazione basata su CRDT che gestisce automaticamente la concorrenza.

In questo modo, lo sviluppatore mantiene il controllo sulla complessità necessaria del proprio dominio, senza dover pagare il costo dell’infrastruttura distribuita tradizionale.

---

## 2. Contesto e Problema

### 2.1 L’ecosistema attuale

Negli ultimi anni, lo sviluppo di sistemi distribuiti ha fatto emergere un pattern ricorrente:

- microservizi containerizzati,
- orchestrazione centralizzata,
- database esterni condivisi,
- sistemi di messaggistica/eventi,
- strumenti di osservabilità e gestione sempre più complessi.

Questa architettura funziona, ma ha un costo: **la complessità operativa diventa una dipendenza strutturale**.

## 2.2 Sintomi della complessità

La complessità dell’ecosistema attuale emerge come una serie di frizioni durante sviluppo, deploy e manutenzione.
Spesso il sistema è difficile da comprendere nella sua interezza: la logica applicativa si disperde tra livelli di configurazione con YAML, Helm chart, operator custom che influenzano il comportamento ma sono onerosi da mantenere.

Questa frammentazione rallenta l’ingresso di nuovi sviluppatori: prima di scrivere codice è necessario capire infrastruttura, permessi e convenzioni operative. La complessità vive più nel contesto che nell’applicazione stessa.

Replicare ambienti coerenti (dev, staging, produzione) diventa difficile: differenze minime in servizi, configurazioni o variabili generano comportamenti divergenti difficili da diagnosticare.

A questo si aggiunge un **coupling nascosto** verso componenti infrastrutturali come database remoti, sistemi di messaggistica, ingress, sidecar che vincola le applicazioni alla topologia del cloud più di quanto appaia.

Infine, molte architetture moderne sono pensate per il cloud centrale e risultano poco portabili su edge, browser, mobile o IoT. Il codice deve adattarsi all’ambiente, moltiplicando i percorsi di esecuzione e ampliando la superficie d’errore.

Nel complesso, questi sintomi mostrano un modello potente ma spesso più complesso del necessario per molti casi d’uso.


### 2.3 Complessità Necessaria vs Complessità Auto-Imposta

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
* orchestratori complessi anche per applicazioni piccole
* dipendenze multiple tra servizi e infrastrutture esterne
* configurazioni distribuite in molti file (YAML, operator custom, chart)
* stato delegato a DB remoti anche quando sarebbe più efficiente mantenerlo localmente
* tool differenziati per ambiente (dev, browser, edge, IoT)

> Questa complessità è spesso evitabile: nasce dalla stratificazione di tecnologie general-purpose applicate anche in scenari in cui non sono strettamente necessarie.

### 2.4 Opportunità

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

---

## 6. Modello di Programmazione

### 6.1 Moduli WASM come unità di calcolo

L'idea è che un’applicazione è composta da uno o più moduli WASM che:

* eseguono logica applicativa,
* leggono/scrivono sul datastore locale,
* pubblicano e ricevono aggiornamenti via Sync,
* effettuano chiamate HTTP se necessario.

### 6.2 API Host esposte ai moduli

TODO: Descrizione 

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
TO DO: aggiungere più info
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

### 7.1 Applicazioni offline-first

Scenario: applicazioni che devono funzionare anche senza connettività continua.
Numax offre:
* datastore locale per lo stato,
* sincronizzazione eventuale via CRDT + gossip,
* stessa logica che può girare nel browser, su mobile e su edge.

### 7.2 Edge Functions

Scenario: esecuzione di funzioni vicino all’utente, con latenze ridotte.
Numax fornisce:
* moduli WASM dal cold start molto rapido,
* stato persistente vicino al calcolo,
* un modello di deployment semplificato (WASM + config).

### 7.3 IoT e dispositivi a risorse limitate

Scenario: dispositivi embedded con risorse limitate e connettività intermittente.
Numax contribuisce con:
* runtime leggero e sicuro,
* datastore locale integrato,
* sincronizzazione eventuale quando il dispositivo è online.

### 7.4 Microservizi minimali

Scenario: servizi piccoli, indipendenti, che non richiedono l’intero stack container + orchestratore.

Numax permette:
* di eseguire logica applicativa senza container,
* di gestire stato e sync dentro il runtime,
* di ridurre dipendenze infrastrutturali.

> TODO: aggiungere esempi concreti (es. carrello e-commerce, note condivise, IoT sensor hub, ecc.).

---

## 8. Dove si colloca Numax

Questa sezione chiarisce la posizione di Numax all'interno dell’ecosistema moderno, confrontandolo con le principali categorie di piattaforme utilizzate oggi per eseguire applicazioni distribuite.

### 8.1 Container e Kubernetes

Kubernetes offre un ecosistema maturo, strumenti diffusi e un alto livello di automazione nella gestione dei cluster.
Tuttavia opera a un livello molto diverso da Numax.

Differenze principali:

- Numax non è un orchestratore ma un runtime leggero
- molte applicazioni che non richiedono container o orchestrazione complessa possono funzionare interamente dentro Numax
- l'obiettivo non è gestire cluster ma ridurre la complessità operativa

Kubernetes rimane ideale per workload complessi e altamente scalabili; Numax mira a casi d'uso più semplici e distribuiti, dove la leggerezza è un vantaggio.

### 8.2 Serverless tradizionale

Le piattaforme serverless astraggono l’infrastruttura e forniscono scalabilità automatica, ma introducono vincoli forti:

- il calcolo è spesso stateless
- lo stato è delegato a servizi esterni
- forte dipendenza dal vendor

Numax adotta un modello diverso:

- portabile e self-hosted
- stato locale integrato e sincronizzazione nativa
- nessun lock-in con un provider specifico

Dove il serverless separa calcolo e dati, Numax li riporta nello stesso luogo.

### 8.3 Altri runtime WASM ed edge platforms

Esistono runtime WASM focalizzati su edge computing (es. Wasmtime, WasmEdge, Cloudflare Workers, Fastly Compute) e ciascuno ottimizza un aspetto diverso: performance, sandboxing, deploy veloce.

La differenza sostanziale rispetto a Numax:

- questi runtime eseguono solo calcolo
- non includono uno store locale persistente
- non offrono sincronizzazione distribuita dello stato
- non forniscono un modello dati basato su CRDT

Numax combina compute, stato e sync in un singolo runtime, fornendo un ambiente più alto livello per applicazioni distribuite.

### 8.4 Quindi dove si colloca Numax?

Numax si posiziona in uno spazio specifico che oggi è quasi vuoto:

**runtime portabile con stato locale integrato e sincronizzazione nativa.**

Riducendo la complessità dell’infrastruttura senza rinunciare a distribuzione, portabilità e sicurezza.

In breve:

**Numax è il runtime per chi vuole costruire sistemi distribuiti senza costruire un'infrastruttura distribuita.**

---

## 9. Limitazioni

Numax è pensato per semplificare lo sviluppo di applicazioni distribuite, ma la prima versione presenta alcune limitazioni da considerare.

- Non sostituisce orchestratori complessi. 
  Non è progettato per gestire cluster estesi o deployment ad alta scalabilità.

- Non è ottimizzato per workload pesanti o CPU-bound. 
  Il focus è su applicazioni leggere, offline-first e distribuite.

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
