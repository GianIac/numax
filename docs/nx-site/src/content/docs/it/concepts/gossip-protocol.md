---
title: Protocollo gossip
description: Come Numax muove operazioni tra peer.
---

Questa pagina spiega cosa significa gossip in Numax, cosa fa già oggi il layer di sync, e cosa arriverà nelle release dedicate alla peer discovery.

La versione breve: **oggi Numax usa peer configurati, broadcast diretto e anti-entropy periodica.** Nelle prossime release questo diventerà peer discovery dinamica con membership stile SWIM e gossip K-fanout.

---

## Cos'è un protocollo gossip

Un protocollo gossip è un modo per diffondere informazioni in un sistema distribuito senza richiedere un coordinatore centrale.

Invece di mandare ogni aggiornamento attraverso un leader, ogni nodo parla con alcuni peer. Quei peer parlano con altri peer. Con il tempo, l'informazione si propaga nel cluster.

Per Numax, l'informazione da propagare è soprattutto una operazione CRDT:

```rust
pub struct Op {
    pub id:     OpId,
    pub origin: NodeId,
    pub kind:   OpKind,
}
```

Ogni operazione ha un `OpId` globalmente unico, il nodo che l'ha prodotta, e il cambiamento CRDT vero e proprio. I peer usano `OpId` per deduplicare messaggi già visti.

---

## Cosa esiste oggi

L'implementazione attuale è volutamente semplice e deterministica.

Numax non ha ancora peer discovery dinamica. Un nodo conosce i peer configurati all'avvio o aggiunti esplicitamente tramite API runtime. Quando una operazione viene prodotta localmente, il sync manager la accoda e la invia ai peer attualmente connessi.

```
chiamata host CRDT locale
       |
       v
op accodata nel SyncManager
       |
       v
broadcast loop raggruppa le op
       |
       v
PushOps inviato ai peer connessi
       |
       v
il peer applica le op non viste e persiste lo stato
```

Questo non è ancora SWIM e non è ancora K-fanout. È un full broadcast verso i peer connessi, limitato da `max_peers`, con batching e backpressure attraverso la coda delle operazioni.

---

## Messaggi wire

La comunicazione tra peer è gestita da `nx-net`. Il protocollo wire attuale definisce questi messaggi:

| Messaggio | Scopo |
|---|---|
| `Hello` | Avvia l'handshake, dichiara node id, versione protocollo e formati di serializzazione supportati. |
| `HelloAck` | Accetta l'handshake e sceglie il formato di serializzazione. |
| `PushOps` | Invia una o più operazioni CRDT a un peer. |
| `PushOpsAck` | Messaggio di acknowledge per operazioni ricevute. Il path di sync attuale non lo usa come frontiera causale. |
| `PullSince` | Chiede a un peer le operazioni mantenute. Oggi viene di solito inviato con `None`. |
| `Ping` / `Pong` | Tipi di messaggio per keepalive. Un `Ping` ricevuto riceve risposta con `Pong`. |

La versione protocollo è attualmente `2`. I peer negoziano `Bincode` o `Json`, con `Bincode` come default di produzione e `Json` disponibile per interoperabilità e debug.

---

## Handshake e identità

Quando un nodo si connette a un peer, il primo scambio è:

```
client -> server: Hello(node_id, version, supported_formats, preferred_format)
server -> client: HelloAck(node_id, version, selected_format)
```

Dopo questo scambio, entrambi i lati conoscono il `NodeId` del peer e il formato di serializzazione selezionato.

Se TLS è abilitato, il `NodeId` dichiarato viene verificato contro il certificato del peer. Questo impedisce a un nodo di dichiarare una identità che non corrisponde al certificato. Eventuali allowlist possono restringere ulteriormente quali peer id sono accettati.

---

## Percorso di broadcast

Le scritture CRDT locali vengono applicate prima localmente. Poi l'operazione corrispondente viene accodata per la propagazione di rete.

Il broadcast loop svuota quella coda, raggruppa le operazioni in batch, registra metadata seen-op e op-log, e invia un messaggio `PushOps` tramite `nx-net`.

I limiti importanti sono:

| Limite | Default attuale |
|---|---|
| Peer connessi massimi | `nx_net::DEFAULT_MAX_PEERS` |
| Op locali in coda | `10.000` |
| Entry op-log mantenute | `10.000` |
| `OpId` visti mantenuti | `100.000` |
| Socket timeout | `nx_net::DEFAULT_SOCKET_TIMEOUT` |

Se un peer è disconnesso, non riceve il push immediato. Per questo esiste anti-entropy.

---

## Anti-entropy

Anti-entropy è il loop di riparazione.

Ogni `anti_entropy_interval` secondi, un nodo chiede a ogni peer configurato e connesso le operazioni mantenute usando `PullSince`.

Oggi la richiesta è conservativa: chiede l'op-log limitato invece di basarsi su un singolo "ultimo op id visto" come frontiera causale. Questo conta perché ricevere una operazione più nuova non prova che tutte le operazioni precedenti siano arrivate.

Il lato ricevente deduplica per `OpId`, applica solo le operazioni non viste, e persiste lo stato CRDT risultante.

```
node A perde op-7 durante una disconnessione temporanea
       |
       v
node A si riconnette
       |
       v
anti-entropy invia PullSince(None)
       |
       v
node B restituisce le op mantenute
       |
       v
node A applica solo gli OpId non visti
```

L'op-log è limitato, quindi anti-entropy è un meccanismo pratico di catch-up, non un archivio storico infinito.

---

## Salute peer e reconnect

I peer configurati hanno un piccolo stato di salute:

| Stato | Significato |
|---|---|
| `Healthy` | Il peer configurato è connesso o si è connesso con successo di recente. |
| `Suspect` | Un tentativo di connessione è fallito, ma il peer non ha ancora superato la soglia di fallimenti. |
| `Dead` | I fallimenti consecutivi hanno raggiunto `peer_dead_after_failures`. |

Il reconnect usa exponential backoff:

| Setting | Default attuale |
|---|---|
| Primo delay di reconnect | `500ms` |
| Delay massimo di reconnect | `30s` |
| Dead dopo fallimenti | `3` |
| Intervallo anti-entropy | `30s` |

Questo è tracking semplice per peer configurati. Non è ancora un protocollo di membership completo.

---

## Cosa non è ancora implementato

La release line attuale **non** fornisce ancora:

- peer discovery automatica,
- membership SWIM,
- failure detection stile Lifeguard,
- failure detection phi-accrual,
- disseminazione K-fanout,
- rate gossip adattivo,
- NAT traversal,
- metadata di frontiera causale per pull incrementali precisi.

Se trovi "gossip" nelle docs attuali, leggilo come il layer di sync che propaga e ripara operazioni CRDT tra peer conosciuti. Il protocollo gossip più formale è pianificato nel lavoro di peer discovery.

---

## Cosa arriverà

La peer discovery è pianificata in due passaggi.

### v0.1.5 - Peer Discovery: Foundations

Questa release introduce l'astrazione di discovery e i primi backend.

Lavoro pianificato:

- trait `PeerDiscovery` con `discover()`, `announce()` e `watch()`.
- `StaticDiscovery`, che preserva il comportamento attuale con peer configurati.
- bootstrap discovery: entrare tramite un indirizzo noto e imparare gli altri peer.
- discovery mDNS per LAN e sviluppo.
- discovery DNS-SRV per ambienti che pubblicano già service record.
- file-watch discovery per orchestrator e setup stile Kubernetes.

L'obiettivo è smettere di far elencare manualmente a ogni nodo tutti gli altri nodi.

### v0.1.6 - Peer Discovery: SWIM & Gossip K-fanout

Qui il protocollo diventa un vero protocollo di cluster dinamico.

Split pianificato:

| Canale | Responsabilità |
|---|---|
| Membership | Vista stile SWIM / Lifeguard di chi è nel cluster. |
| Failure detection | Suspicion e dead-peer detection senza dipendere solo da indirizzi configurati. |
| Data dissemination | Gossip K-fanout per operazioni CRDT. |

K-fanout significa che un nodo non manda ogni aggiornamento a ogni peer. Invece manda ogni aggiornamento a `K` peer selezionati. Quei peer lo inoltrano oltre. Con un buon valore di `K`, il cluster ottiene propagazione veloce senza trasformare ogni operazione in un full-cluster broadcast.

Il default pianificato è basato sulla dimensione del cluster:

```text
K = ceil(log2(N) + c)
```

dove `N` è la dimensione nota del cluster e `c` è una piccola costante di sicurezza.

La roadmap include anche fanout adattivo basato su load e RTT, backpressure controllata, anti-entropy periodica come percorso di riparazione, e randomness seedabile per rendere riproducibili i test del gossip.

---

## Perché servono sia gossip sia anti-entropy

Gossip è il percorso veloce. Diffonde rapidamente le nuove operazioni.

Anti-entropy è il percorso di riparazione. Recupera i nodi che erano offline, partizionati, lenti o sfortunati.

Numax ha bisogno di entrambi perché i sistemi local-first devono tollerare disconnessioni temporanee. I CRDT rendono sicuro il merge. Gossip muove velocemente le operazioni. Anti-entropy rende recuperabili le operazioni perse.

---

## Correlati

- [CRDT e stato](/numax/it/concepts/crdt-and-state/) - il modello dati che rende sicura la convergenza
- [Modello runtime](/numax/it/concepts/runtime-model/) - come interagiscono moduli, host API e sync
- [Crate nx-net](/numax/it/reference/crates/nx-net/) - trasporto peer e messaggi wire
- [Crate nx-sync](/numax/it/reference/crates/nx-sync/) - operazioni, CRDT e deduplicazione
- [Roadmap](/numax/it/roadmap/) - release pianificate per peer discovery
