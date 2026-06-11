---
title: Introduzione
description: Cos'è Numax e quando usarlo.
---

## Cos'è Numax

Numax è un runtime per applicazioni distribuite, scritto in Rust.

Tre cose, solo tre:

1. **Esegue moduli WebAssembly in una sandbox.**
   Scrivi un modulo in qualsiasi linguaggio che compila in WASM. Numax lo carica,
   collega la host API, e chiama `run()`. Il modulo non può toccare nulla al di fuori
   di quello che l'host espone esplicitamente.

2. **Tiene lo stato locale.**
   Ogni nodo ha un datastore key/value embedded (sled) su disco.
   Lo stato vive vicino al codice, non su un server remoto.
   Il nodo resta utile anche quando la rete non c'è.

3. **Sincronizza lo stato tra nodi con CRDT e gossip.**
   Quando i nodi sono connessi, si scambiano operazioni e convergono automaticamente.
   Non scrivi codice di riconciliazione. Non risolvi conflitti a mano.
   Ci pensano le strutture dati.

Questo è il modello. Tu scrivi la logica. Numax gestisce l'ambiente.

---

## Perché esiste

Costruire software distribuito oggi di solito significa:
container, orchestratori, database remoti, un layer di sync separato,
e tre toolchain diverse a seconda di dove gira il codice.

Tutto quel peso esiste per risolvere problemi che vengono dall'architettura stessa,
non dal problema originale che stavi cercando di risolvere.

Numax prova una strada diversa: tieni il runtime piccolo, tieni lo stato locale,
lascia che i CRDT gestiscano la convergenza. Le parti difficili dei sistemi distribuiti
non spariscono, ma smetti di pagare per quelle di cui non avevi effettivamente bisogno.

---

## Quando usarlo

Numax è adatto quando:

- hai **più nodi o dispositivi che condividono stato** e devono restare in sync
- hai bisogno che il sistema **continui a funzionare quando la rete è assente o lenta**
- vuoi **distribuire logica come modulo WASM** e lasciare al runtime storage e sync
- vuoi qualcosa **autonomo** - niente database esterno, niente servizio di coordinamento

Esempi concreti: sistemi di inventario edge, app offline-capable, reti di sensori,
strumenti collaborativi, propagazione di configurazione tra nodi, stato multiplayer leggero.

---

## Quando non usarlo (per ora)

- **Strong consistency o transazioni tra nodi** - i CRDT convergono alla fine, non immediatamente.
  È nel roadmap.
- **Modelli di dati che non si adattano a semantiche CRDT** - grow-only, last-writer-wins, operazioni su set.
  Altre primitive sono in arrivo.
- **Database general-purpose con query ricche** - non è quello che fa Numax.
- **Carichi di produzione critici** - Numax è a `v0.1.x`, testato e usabile, ma ancora agli inizi.
  I limiti rimanenti sono documentati nel [Roadmap](/it/roadmap/).

Questi sono limiti attuali, non permanenti. Se qualcosa ti blocca,
[apri una issue](https://github.com/GianIac/numax/issues/new) - è esattamente così che si definiscono le priorità.

---

## Cosa c'è dentro

| Componente | Cosa fa |
|---|---|
| `nx` | La CLI - `nx run module.wasm`, `nx config validate`, `nx config show` |
| `nx-core` | Il motore runtime - caricamento WASM, host API, datastore, sync |
| `nx-sdk` | L'SDK guest - libreria Rust per scrivere moduli WASM |
| `nx-site` | Questo sito di documentazione |
| `examples/` | Sette esempi funzionanti, ognuno sotto ~100 righe |

L'esecuzione WASM è gestita da [Wasmtime](https://wasmtime.dev/).
Il datastore locale usa [sled](https://github.com/spacejam/sled).
La sync usa gossip con anti-entropy periodica per il recupero.

---

## Stato attuale

`v0.1.x` - prima release line stabile, pensata per carichi controllati e non critici.

Funziona. Gli esempi girano. I due nodi convergono.
I limiti rimanenti sono documentati nel [Roadmap](/it/roadmap/).

---

## Da dove continuare

- Non hai mai toccato Numax - [Quickstart: 5 minuti](/it/getting-started/quickstart-5-min/)
- Vuoi scrivere un modulo - [Il tuo primo modulo](/it/getting-started/your-first-module/)
- Parole come CRDT o gossip sono nuove - [Fondamenti](/it/concepts/foundations/)
- Vuoi capire la visione completa - [Whitepaper](/it/whitepaper/)
- Vuoi vedere dove sta andando il progetto - [Roadmap](/it/roadmap/)