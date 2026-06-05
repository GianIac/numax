---
title: Numax
description: Runtime portabile per applicazioni distribuite local-first. Moduli WebAssembly, stato locale, sincronizzazione CRDT.
template: splash
hero:
  tagline: Esegui moduli WebAssembly. Tieni lo stato locale. Lascia che i CRDT facciano il resto.
  actions:
    - text: Quickstart in 5 minuti
      link: /it/getting-started/quickstart-5-min/
      icon: right-arrow
      variant: primary
    - text: Leggi il whitepaper
      link: /it/whitepaper/
      icon: document
      variant: secondary
    - text: GitHub
      link: https://github.com/GianIac/numax
      icon: external
      variant: minimal
---
## Cos'e Numax

Numax e un runtime piccolo e portabile per applicazioni distribuite.
Scrivi un modulo WebAssembly. Numax lo esegue in sandbox, gli mette a
disposizione uno store key/value locale e sincronizza lo stato tra nodi usando
CRDT e gossip.


## Tre cose, solo tre

1. **Esecuzione WebAssembly in sandbox.**
   Il tuo codice gira isolato, su qualunque host capace di eseguire WASM.
   Oggi il percorso principale e Rust; altri linguaggi guest arriveranno con il
   Component Model.

2. **Datastore embedded locale.**
   Lo stato vive vicino al codice, dentro uno store key/value persistente basato
   su sled.

3. **Sincronizzazione CRDT via gossip.**
   I peer si scambiano operazioni e convergono automaticamente.
   Non devi scrivere codice di riconciliazione.

Se termini come WASM, CRDT, gossip o local-first ti sono nuovi, parti da
[**Fondamenta**](/it/concepts/foundations/). E il glossario breve che avremmo
voluto trovare prima di leggere qualsiasi documentazione su un runtime
distribuito.

## Cosa puoi costruire oggi

Il runtime include sette esempi piccoli, spesso sotto le 100 righe.
Servono a dimostrare che il modello funziona, a darti codice da copiare e a
rendere concreta ogni primitiva di replica. Considerali **punti di partenza,
non il limite**.

- **[`distributed_counter`](https://github.com/GianIac/numax/tree/main/examples/distributed_counter)** - contatore visite replicato.
- **[`distributed_inventory`](https://github.com/GianIac/numax/tree/main/examples/distributed_inventory)** - carichi, vendite e resi su uno SKU condiviso.
- **[`distributed_status`](https://github.com/GianIac/numax/tree/main/examples/distributed_status)** - stato di servizio con last-writer-wins.
- **[`distributed_settings`](https://github.com/GianIac/numax/tree/main/examples/distributed_settings)** - mappa di configurazione replicata.
- **[`distributed_tags`](https://github.com/GianIac/numax/tree/main/examples/distributed_tags)** - set collaborativo di tag con observed-remove.
- **[`distributed_comments`](https://github.com/GianIac/numax/tree/main/examples/distributed_comments)** - commenti ordinati e testo collaborativo.
- **[`vote_tally_tls`](https://github.com/GianIac/numax/tree/main/examples/vote_tally_tls)** - contatore a tre nodi con mTLS.

### Cosa ci puoi fare davvero

Con gli stessi blocchi di base si possono gia immaginare parecchie cose:

- **App che ti seguono tra dispositivi** - note, liste e tracker che funzionano offline e poi si sincronizzano.
- **Oggetti costruiti insieme in tempo reale** - documenti, lavagne, piccoli giochi multiplayer.
- **Software che continua a lavorare quando la rete no** - punti vendita, inventory edge, eventi, festival.
- **Sensori e dispositivi che parlano tra loro** - case, serre, stazioni meteo, flotte.
- **Configurazioni e feature flag che si propagano da soli** - cambi una volta, ogni nodo riceve lo stato.
- **...e molte cose a cui onestamente non abbiamo ancora pensato.**

L'ultimo punto e il piu interessante. Se la tua idea suona come
*"tante persone o tanti dispositivi condividono qualcosa che alla fine deve
restare coerente"*,
Numax e probabilmente uno degli strumenti piu semplici da provare oggi.

### Hai costruito qualcosa? Faccelo vedere.

Serio, strano, incompleto: va bene tutto.
Se lo hai scritto con Numax, ci piacerebbe vederlo nello [**Showcase**](/it/showcase/).

## Dove stiamo andando

La linea `0.1.x` procede a piccoli passi stabili verso **`v0.2.0`**:
la versione che vorremmo consigliare senza note a margine, anche per scenari
piu critici.
La direzione e questa:

- **Peer discovery dinamica** - mDNS, DNS-SRV, file-watch, poi membership SWIM e gossip K-fanout adattivo. Basta `--peer 1.2.3.4` ovunque.
- **Moduli reattivi** - event loop long-running con `on_remote_op`, `on_tick`, `on_peer_connected`, `on_message` e handler HTTP. `run()` one-shot resta, ma non sara l'unico modello.
- **Hot reload** - sostituire un modulo live senza perdere connessioni peer o stato CRDT.
- **Sicurezza capability-based** - policy per modulo con capability deny-by-default e quote su risorse come CPU, memoria, ops/s e bytes/s. Pensato per il multi-tenant.
- **Operabilita** - snapshot, restore, replay, diff, compaction dell'op-log e modalita deterministica per debug riproducibile.
- **Dashboard e TUI built-in** - viste dedicate a cluster, CRDT, flusso operazioni, salute della convergenza, throughput e moduli, piu una TUI `nx top` per ambienti solo SSH.
- **Component Model + WIT** - ABI stabile e multi-linguaggio. Guest Rust, Go con TinyGo, JavaScript e Python che convergono sugli stessi CRDT.

Il piano completo, versione per versione, e nella
[**Roadmap**](/it/roadmap/). Le PR alla roadmap sono benvenute quanto le PR al
codice.

## Provalo. Rompilo. Raccontaci cosa succede.

Numax e nel momento in cui feedback mirati possono ancora cambiare molto la
forma del progetto.

- **[Quickstart in 5 minuti](/it/getting-started/quickstart-5-min/)** - clona, compila, avvia due nodi che convergono.
- **[Il tuo primo modulo](/it/getting-started/your-first-module/)** - il modulo WASM piu piccolo ma interessante che puoi scrivere.
- **[Apri una issue](https://github.com/GianIac/numax/issues/new)** - bug, opinioni di design, anche piccole sorprese.
- **[Metti una star al repo](https://github.com/GianIac/numax)** - oggi e il segnale piu semplice che questa idea merita di essere spinta ancora.

Apache 2.0. Una porta sola, per ora aperta.
