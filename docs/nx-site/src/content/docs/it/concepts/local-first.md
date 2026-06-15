---
title: Local-first
description: Perché Numax tiene lo stato vicino al codice.
---

Il Local-first è un vincolo di design che plasma tutto: dove vive lo stato, come viene trattata la rete, cosa succede quando si perde la connettività, e quali garanzie può dare la tua applicazione senza chiedere il permesso a un server remoto.

Questa pagina spiega cosa significa local-first nel contesto di Numax e perché è importante.

---

## L'assunzione di default che la maggior parte del software fa

La maggior parte delle applicazioni è costruita su un'assunzione remote-first. Lo stato autorevole vive su un server. Il client lo richiede, lo modifica, e lo rimanda indietro. Il server decide cosa è vero.

Questo funziona bene quando:

- la rete è veloce e affidabile o la latenza non conta
- il server è sempre raggiungibile
- sei contento di instradare ogni operazione attraverso un'infrastruttura centralizzata :) 

In pratica, nessuna di queste condizioni vale in modo costante. Le reti falliscono. I server hanno downtime la latenza conta per i nodi edge, i dispositivi mobili, i sensori IoT, e tutto ciò che gira lontano da un datacenter. E l'infrastruttura centralizzata ha un costo: operativo, finanziario, e architetturale.

Il modello remote-first spinge la complessità verso l'esterno. La logica applicativa rimane semplice, ma solo perché le parti difficili come disponibilità, consistenza, partizionamento, vengono delegate a un'infrastruttura che qualcun altro gestisce. Quella delega ha un prezzo ...

---

## Cosa significa local-first

Local-first significa che l'applicazione funziona correttamente con i dati che ha localmente, senza richiedere un round-trip a un sistema remoto per ogni operazione.

La descrizione canonica che adoro viene dal [laboratorio di ricerca Ink & Switch](https://www.inkandswitch.com/local-first/):

> Nel software local-first, la copia primaria dei dati vive sul tuo dispositivo locale.

Per Numax, questo si traduce così:

- lo store dello stato è embedded nel processo runtime, non un servizio remoto
- letture e scritture vanno a sled, su disco, sulla stessa macchina del codice
- la rete è usata per la sincronizzazione, non per l'accesso
- un nodo che non si è mai connesso a nessun peer può comunque leggere e scrivere stato locale
- le API CRDT replicate sono local-first una volta abilitata la sync: la sync è il layer di replica, non il layer di storage

Il nodo non degrada semplicemente in modo graceful quando è offline. Per il proprio stato locale, l'offline non è una condizione di errore. È la baseline.

---

## Come Numax lo implementa

Ogni nodo Numax possiede i propri dati:

```
┌─────────────────────────────────────┐
│           Nodo Numax                │
│                                     │
│  ┌──────────────┐  ┌─────────────┐  │
│  │ Modulo WASM  │  │  nx-store   │  │
│  │  (compute)   │◄─┤  (sled)     │  │
│  └──────────────┘  └─────────────┘  │
│                                     │
│  ┌──────────────────────────────┐   │
│  │  nx-sync + nx-net (opzionale)│   │
│  │  sync CRDT con peer noti     │   │
│  └──────────────────────────────┘   │
└─────────────────────────────────────┘
```

Il modulo legge e scrive attraverso uno store embedded. Non c'è una connessione remota da aprire, nessuna query da inviare sulla rete, e nessun acknowledgment da aspettare da un sistema remoto prima che lo stato locale possa avanzare.

La sync è opt-in. Un nodo avviato senza `--listen` non si connette a niente. Funziona e basta. Quando la sync è abilitata, i CRDT gestiscono la convergenza: lo stato locale e quello remoto vengono uniti usando proprietà matematiche che garantiscono la consistenza senza coordinamento.

Questo è il punto chiave: **la sincronizzazione è disaccoppiata dall'accesso**. Non devi essere connesso per leggere o scrivere. Devi essere connesso per propagare i cambiamenti agli altri nodi. Queste sono due cose diverse, e trattarle come la stessa cosa è la fonte di gran parte della fragilità nei sistemi remote-first.

---

## I tradeoff

Local-first non è gratuito. Sposta la complessità dall'infrastruttura al modello dei dati. Capire i tradeoff è importante.

**Cosa guadagni:**

- **Disponibilità.** Il nodo funziona indipendentemente dallo stato della rete. Un gateway edge che perde il suo uplink per un'ora accetta comunque scritture e serve letture. Quando si riconnette, sincronizza.
- **Latenza.** Letture e scritture locali evitano round-trip di rete. Il percorso dello stato resta vicino alla computazione.
- **Resilienza.** Non c'è un single point of failure centrale. Se un nodo si interrompe, gli altri continuano a operare indipendentemente.
- **Semplicità di deployment.** Un nodo Numax è un binario, un file di configurazione, un modulo `.wasm`. Non c'è un database remoto da provisionare, nessuna connection string da gestire, nessuna dipendenza cloud da mantenere.

**Cosa perdi:**

- **Consistenza forte.** Non puoi avere un "valore corrente" globalmente concordato senza coordinamento. I CRDT ti danno consistenza eventuale: tutti i nodi convergono dato tempo e connettività sufficienti, ma in qualsiasi momento nodi diversi potrebbero vedere valori diversi.
- **Linearizzabilità.** Se hai bisogno di "questo valore deve essere esattamente X prima che io proceda, e nessun altro nodo può cambiarlo mentre sto decidendo", local-first non può dartelo senza un protocollo di coordinamento sopra.
- **Mutazioni conflict-free per tutte le forme di dati.** I CRDT coprono pattern specifici: contatori, registri, set, mappe, sequenze. Se il tuo modello dati richiede pattern che non mappano a questi allora il local-first con CRDT non è il fit giusto.

---

## Quando local-first è la scelta giusta

Local-first è adatto per sistemi dove:

- **le scritture avvengono indipendentemente su più nodi** e devono convergere in seguito
- **l'operazione offline è un requisito reale**, non solo un nice-to-have
- **la latenza conta**, perché i dati sono vicini alla computazione
- **il target di deployment è eterogeneo**: server, edge, IoT, mobile
- **il modello dati si adatta ai CRDT**: contatori, presenza, tag, impostazioni, contenuto ordinato

---

## Quando non è la scelta giusta

Local-first con CRDT non è appropriato quando:

- **l'ordinamento stretto delle operazioni è richiesto** e deve essere globalmente imposto
- **la logica applicativa richiede la lettura di un valore globalmente consistente prima di scrivere**
- **i dati devono essere immediatamente visibili a tutti i nodi dopo una scrittura** senza tolleranza per il ritardo di propagazione
- **il modello dati è fondamentalmente relazionale** con vincoli cross-entity complessi che i CRDT non possono esprimere

Questi non sono bug. Sono il perimetro onesto del modello. Sapere dove non si applica è importante quanto sapere dove si applica.

---

## Local-first e la filosofia Numax

Il vincolo local-first è ciò che rende coerente il modello a tre componenti di Numax.

Se lo stato fosse remoto, avresti bisogno di un round-trip di rete per ogni lettura. L'isolamento sandbox WASM non significherebbe nulla se ogni operazione richiedesse I/O esterno. Il modello CRDT sarebbe una curiosità piuttosto che una parte portante del design.

Perché lo stato è locale, il modulo gira a piena velocità. Perché la sync è separata dall'accesso, il nodo sopravvive alle disconnessioni. Perché i CRDT gestiscono la convergenza, la riconnessione è automatica e corretta.

Il risultato è un nodo genuinamente autosufficiente. Non degradato, non cachato, non una read replica. È il nodo primario, per i propri dati, sempre.

---

## Correlati

- [Modello runtime](/numax/it/concepts/runtime-model/) - lifecycle e i tre componenti
- [CRDT e stato](/numax/it/concepts/crdt-and-state/) - come funziona la convergenza senza coordinamento
- [Protocollo gossip](/numax/it/concepts/gossip-protocol/) - come la sincronizzazione si propaga tra i nodi
- [Ink & Switch: Local-first Software](https://www.inkandswitch.com/local-first/) - la ricerca originale che ha dato nome a questo approccio
