---
title: Fondamenta
description: Glossario semplice per capire le idee dietro Numax.
---

Non serve essere esperti di sistemi distribuiti per iniziare con Numax.
Questa pagina spiega le parole principali che troverai nella documentazione,
negli esempi e nella roadmap.

## WebAssembly

WebAssembly, spesso abbreviato in **WASM**, e un formato binario portabile per
eseguire codice in sandbox.

Per Numax l'idea importante e semplice:

- scrivi un modulo in un linguaggio come Rust;
- lo compili in `.wasm`;
- Numax lo carica, lo esegue e decide quali funzioni host puo chiamare.

Puoi pensare a WASM come a un formato per piccoli programmi portabili. Il modulo
guest non ha accesso libero a filesystem, rete o processo. Puo fare solo quello
che il runtime host espone.

Link utili:

- [MDN: WebAssembly](https://developer.mozilla.org/en-US/docs/WebAssembly)
- [MDN: WebAssembly concepts](https://developer.mozilla.org/en-US/docs/WebAssembly/Concepts)
- [Bytecode Alliance](https://bytecodealliance.org/)

## Runtime

Un **runtime** e il programma che esegue il tuo modulo e gli fornisce i servizi
attorno.

Numax usa **Wasmtime** come motore di esecuzione WebAssembly.

In Numax, il runtime:

- carica e valida un modulo WASM;
- espone funzioni Host API come database, tempo, crypto e helper CRDT;
- possiede il datastore locale;
- avvia networking e sincronizzazione quando configurati;
- chiude tutto in modo ordinato.

Il tuo modulo contiene la logica applicativa. Numax e l'ambiente in cui gira.

Link utili:

- [Wasmtime documentation](https://docs.wasmtime.dev/)

## Host API

La **Host API** e il contratto tra codice guest e Numax.

Un modulo WASM non puo chiamare direttamente funzioni Rust dentro Numax. Invece,
Numax espone funzioni esplicite come:

- `db_set`
- `db_get`
- `gcounter_inc`
- `time_now`
- `random_bytes`

L'SDK le incapsula in funzioni Rust piu comode, cosi un modulo guest puo usare
`db::set(...)` o `gcounter::inc(...)`.

Approfondisci:

- [Numax Host API](/numax/it/reference/host-api/)
- [Import ed export WebAssembly su MDN](https://developer.mozilla.org/en-US/docs/WebAssembly/Guides/Understanding_the_text_format#imports_and_exports)

## Local-first

**Local-first** significa che il nodo locale resta utile anche quando la rete e
lenta, assente o rotta.

Invece di chiedere ogni operazione a un server remoto, l'app scrive prima in
locale e sincronizza dopo. Per questo Numax mantiene lo stato in uno store locale
embedded e usa CRDT per far convergere i nodi.

Link utile:

- [Ink & Switch: Local-first Software](https://www.inkandswitch.com/local-first/)

## CRDT

Un **CRDT** e una struttura dati pensata per sistemi replicati.

La promessa pratica e:

- piu nodi possono aggiornare la propria copia locale in modo indipendente;
- gli aggiornamenti possono arrivare in ordini diversi;
- quando i nodi hanno visto gli stessi aggiornamenti, convergono allo stesso valore.

Questo evita molto codice manuale di risoluzione conflitti. Numax oggi include
CRDT come GCounter, PNCounter, LWW-Register, ORSet, LWW-Map e RGA.

Link utili:

- [CRDT resources](https://crdt.tech/resources)
- [Conflict-free Replicated Data Types paper](https://arxiv.org/abs/1805.06358)

## Gossip

**Gossip** e uno stile di comunicazione in cui i nodi diffondono informazioni
parlando con altri peer nel tempo.

Invece di avere un coordinatore centrale, i nodi si scambiano operazioni con i
peer connessi. Numax usa questa idea insieme a reconnect e anti-entropy, cosi le
operazioni perse possono essere recuperate.

Link utili:

- [Gossip protocol overview](https://en.wikipedia.org/wiki/Gossip_protocol)
- [Epidemic Algorithms for Replicated Database Maintenance](https://paperswelove.org/papers/epidemic-algorithms-for-replicated-database-mainte-9283e904/)

## Anti-entropy

**Anti-entropy** e il ciclo di riparazione.

La sincronizzazione normale invia le operazioni appena accadono. Anti-entropy
chiede periodicamente a un peer le operazioni, cosi un nodo puo recuperare
messaggi persi, broadcast mancati o reconnect.

In breve: push e il percorso veloce, anti-entropy e la rete di sicurezza.

Link utili:

- [Dynamo: Amazon's highly available key-value store](https://www.amazon.science/publications/dynamo-amazons-highly-available-key-value-store)
- [CRDT resources](https://crdt.tech/resources)

## mTLS

**mTLS** significa mutual TLS. Entrambe le parti di una connessione presentano un
certificato, cosi ogni peer puo verificare l'altro.

Numax lo usa per cluster permissioned. L'identita di un nodo puo essere derivata
dal certificato, e i peer possono essere limitati con una allowlist.

Link utili:

- [Apache APISIX: What is mutual TLS?](https://apisix.apache.org/learning-center/what-is-mutual-tls/)
- [Mozilla: Transport Layer Security](https://developer.mozilla.org/en-US/docs/Web/Security/Transport_Layer_Security)

## Component Model

Il **WebAssembly Component Model** e la direzione futura per interfacce WASM piu
ricche e multi-linguaggio.

Oggi Numax espone una Host API in stile legacy. La roadmap porta verso WIT e
Component Model, cosi Rust, Go, JavaScript, Python e altri linguaggi guest
potranno condividere una ABI piu chiara.

Link utili:

- [WebAssembly Component Model concepts](https://component-model.bytecodealliance.org/design/component-model-concepts.html)
- [Why the Component Model?](https://component-model.bytecodealliance.org/design/why-component-model.html)

## Come leggere la documentazione Numax

Se stai iniziando, segui questo ordine:

1. Leggi questa pagina.
2. Esegui la [Quickstart](/numax/it/getting-started/quickstart-5-min/).
3. Apri un esempio distribuito e leggi il modulo guest.
4. Leggi [CRDT e stato](/numax/it/concepts/crdt-and-state/) quando vuoi capire la convergenza.
5. Leggi [Host API](/numax/it/reference/host-api/) quando inizi a scrivere un modulo tuo.
