---
title: nx-net
description: Networking tra peer, messaggi wire e TLS.
---

`nx-net` possiede tutto ciò che sta sotto il layer sync: connessioni TCP, handshake TLS/mTLS,
framing messaggi wire, negoziazione del formato di serializzazione, gestione degli slot peer,
e lo shutdown cooperativo di tutti i task di rete. Espone gli eventi verso l'alto a `nx-core`
tramite un canale async.

Dipende da `nx-sync` per i tipi `Op` e `NodeId`. Non dipende da `nx-core` o `nx-store`.

---

## Responsabilità

| Responsabilità | Dove |
|---|---|
| Listener TCP e gestione connessioni in ingresso | `node.rs` - `Node::start_listener`, `handle_incoming` |
| Connessioni in uscita e handshake | `node.rs` - `Node::connect_to_peer` |
| Framing messaggi wire (length-prefixed) | `node.rs` - `read_message`, `write_message` |
| Negoziazione formato di serializzazione | `node.rs` - `negotiate_serialization_format` |
| Acceptor e connector TLS/mTLS | `tls.rs` - `TlsConfig::accept_stream`, `TlsConfig::connect_stream` |
| Binding NodeId al certificato TLS | `tls.rs` - `derive_protocol_node_id_from_cert` |
| Enforcement slot peer (semaforo) | `node.rs` - `connection_slots`, `ensure_peer_slot_available` |
| Broadcast e invio op mirato | `node.rs` - `Node::broadcast_ops`, `Node::send_ops_to_addr` |
| Richieste pull anti-entropy | `node.rs` - `Node::send_pull_since_to_addr` |
| Shutdown cooperativo via watch channel | `node.rs` - `Node::shutdown`, `shutdown_tx` |
| Tipi messaggi wire e encode/decode | `message.rs` - `Message`, `MessageKind` |
| Tracking stato peer | `node.rs` - `PeerConnection`, `peer.rs` - `PeerInfo`, `PeerState` |
| Tipi errore | `error.rs` - `NetError` |

---

## Node

`Node` è la struct principale. Il sync manager in `nx-core` ne crea una, la avvia,
e consuma gli eventi da essa.

```rust
pub struct Node {
    config:           NodeConfig,
    peers:            Arc<RwLock<HashMap<String, PeerConnection>>>,
    event_tx:         mpsc::Sender<NodeEvent>,
    event_rx:         Option<mpsc::Receiver<NodeEvent>>,
    shutdown_tx:      watch::Sender<bool>,
    connection_slots: Arc<Semaphore>,
    tasks:            Arc<Mutex<Vec<JoinHandle<()>>>>,
}
```

### NodeConfig

```rust
NodeConfig::new(node_id, "0.0.0.0:9000")
    .with_peers(vec!["127.0.0.1:9001".into()])
    .with_tls(tls_config)
    .with_max_peers(64)
    .with_max_message_size(16 * 1024 * 1024)
    .with_socket_timeout(Duration::from_secs(30))
    .with_serialization_format(SerializationFormat::Bincode)
    .with_event_channel_capacity(1024)
```

### Lifecycle Node

```
Node::new(config)
  └── take_event_receiver()     prendi il canale eventi prima di avviare
  └── start_listener()          bind TCP, spawn listener task, restituisce SocketAddr bound
  └── connect_to_peer(addr)     dial, TLS, handshake, registra, spawn read loop
      ...in esecuzione...
  └── broadcast_ops(ops)        invia ops a tutti i peer connessi
  └── send_ops_to_addr(addr, ops)
  └── send_pull_since_to_addr(addr, since_op_id)
  └── shutdown()                invia true su shutdown_tx, attende i task (3s grace), svuota peers
```

### NodeEvent

Eventi emessi al sync manager tramite `mpsc::Sender<NodeEvent>`:

```rust
pub enum NodeEvent {
    OpsReceived     { from: NodeId, ops: Vec<Op> },
    PullRequested   { from: NodeId, addr: String, since_op_id: Option<String> },
    PeerConnected   { node_id: NodeId, addr: String, peers_connected: usize },
    PeerDisconnected{ node_id: NodeId, addr: String, peers_connected: usize },
}
```

`take_event_receiver()` deve essere chiamato una volta prima di `start_listener`.
Il receiver viene spostato fuori dal `Node` così il sync manager ne diventa proprietario.

---

## Flusso connessione

### Uscente (dialer)

```
connect_to_peer(addr)
  1. acquisisce slot semaforo (PeerLimitReached se pieno)
  2. TCP connect con socket_timeout
  3. TLS handshake (se configurato)
  4. cattura bytes DER del certificato peer
  5. invia Hello { node_id, version, supported_formats, preferred_format }
  6. riceve HelloAck { node_id, version, selected_format }
  7. valida version == PROTOCOL_VERSION (2)
  8. se TLS e non insecure: deriva NodeId dal cert peer, verifica == node_id dichiarato
  9. se allowlist configurata: verifica peer_node_id in allowed_peers
  10. inserisce PeerConnection nella mappa peers
  11. emette evento PeerConnected
  12. spawn task read_loop
```

### Entrante (listener)

```
handle_incoming(stream, addr, context)
  1. TLS accept (se configurato), cattura peer_cert
  2. riceve Hello
  3. valida protocol version
  4. negotiate_serialization_format
  5. TLS identity binding (uguale al dialer)
  6. invia HelloAck { node_id, version, selected_format }
  7. inserisce PeerConnection nella mappa peers
  8. emette evento PeerConnected
  9. esegue read_loop inline (non spawn - task già spawnato dal listener)
```

---

## Formato wire

Ogni messaggio è framed come:

```
[4 byte BE lunghezza][1 byte formato][byte payload]
```

- La lunghezza è il totale di `byte formato + payload`, codificato come `u32` big-endian.
- Byte formato: `0x01` = JSON, `0x02` = bincode.
- Il payload è la struct `Message` serializzata.

`PROTOCOL_VERSION = 2`. Il mismatch di versione durante l'handshake causa disconnessione immediata.

### Varianti MessageKind

| Variante | Direzione | Scopo |
|---|---|---|
| `Hello` | dialer -> listener | Apre l'handshake: identità nodo, versione, formati supportati |
| `HelloAck` | listener -> dialer | Accetta handshake: formato selezionato confermato |
| `PushOps` | entrambi | Trasporta un batch di op CRDT |
| `PushOpsAck` | entrambi | Conferma conteggio ricezione |
| `PullSince` | entrambi | Richiede op da un op id noto (anti-entropy) |
| `Ping` / `Pong` | entrambi | Keepalive |

### Negoziazione formato serializzazione

Quando un nodo bincode si connette a un nodo debug JSON-only:

```
dialer invia: supported_formats = [Bincode, Json], preferred = Bincode
listener sceglie: primo formato nella lista del dialer che il listener supporta
risultato: Json (perché il listener supporta solo Json)
HelloAck selected_format = Json
```

Un nodo `--debug-protocol` (solo JSON) negozia sempre JSON con qualsiasi peer.
Un nodo standard pubblica entrambi e preferisce bincode.

---

## TLS e mTLS

TLS è opzionale. Quando `TlsConfig` è fornito:

- Uscente: `TlsConfig::connect_stream(tcp, server_name)` via `tokio-rustls`.
- Entrante: `TlsConfig::accept_stream(tcp)`.
- Entrambi i lati estraggono il certificato DER del peer dalla sessione TLS completata.

### TlsConfig

```rust
pub struct TlsConfig {
    pub cert_path:     Option<String>,  // cert PEM di questo nodo
    pub key_path:      Option<String>,  // chiave PEM di questo nodo
    pub ca_path:       Option<String>,  // cert CA per verifica peer (abilita mTLS)
    pub allowed_peers: Option<HashSet<String>>, // allowlist opzionale di NodeId
    pub insecure:      bool,            // salta verifica cert (solo dev)
}
```

### Binding NodeId

Dopo l'handshake TLS, il nodo verifica il NodeId dichiarato in `Hello`/`HelloAck`
contro l'identità derivata dal certificato X.509 del peer:

```
derive_protocol_node_id_from_cert(peer_cert_der)
  -> SHA-256 dei byte SubjectPublicKeyInfo
  -> primi 16 byte dell'hash
  -> 32 caratteri hex lowercase
  -> NodeId(hex_prefix)
```

Se il NodeId dichiarato non corrisponde a quello derivato dal cert, la connessione viene rifiutata
con `NetError::TlsError("node_id mismatch ...")`.

### Enforcement allowlist

Quando `TlsConfig.allowed_peers` è impostato, il NodeId derivato dal cert viene verificato
contro il set. Un peer non nell'allowlist viene rifiutato dopo l'handshake TLS,
prima che le op vengano scambiate.

### Utility per i test

`TestPki` in `tls.rs` genera una CA in-memory + due cert nodo per l'uso nei test:

```rust
let pki = TestPki::generate().unwrap();
let node1_cfg = pki.node1_config(); // TlsConfig per nodo 1
let node2_cfg = pki.node2_config(); // TlsConfig per nodo 2
```

---

## Gestione slot peer

La capacità peer è imposta con un `tokio::sync::Semaphore` inizializzato a `max_peers`.

- Entrante: `try_acquire_owned()` al momento dell'accept - il permit è tenuto in `PeerConnection._slot`.
  Se il semaforo è esaurito, la connessione viene droppata prima del costo TLS/handshake.
- Uscente: `try_acquire_owned()` prima del TCP connect - fallisce subito con `PeerLimitReached`.

Il permit viene droppato quando la `PeerConnection` viene rimossa dalla mappa peers
(su disconnect o shutdown).

---

## Shutdown cooperativo

Lo shutdown usa un canale `tokio::sync::watch`. `shutdown_tx` è un `watch::Sender<bool>`.
Tutti i task in background si iscrivono con `shutdown_tx.subscribe()` e selezionano su `shutdown_rx.changed()`.

```
Node::shutdown()
  1. shutdown_tx.send(true)
  2. raccoglie tutti i JoinHandle dal Vec tasks
  3. per ogni task: timeout(3s, task).await
     - se il task non finisce in 3s: task.abort()
  4. peers.clear() -> droppa tutte le PeerConnection -> droppa tutti i permit semaforo
```

I read loop controllano il segnale di shutdown a ogni iterazione via `tokio::select!`.
Il listener loop lo controlla tra una accept e l'altra.
Questo evita di aspettare i socket timeout durante uno shutdown pulito.

---

## Tipi errore

```rust
pub enum NetError {
    Io(std::io::Error),
    Serialization(serde_json::Error),
    BincodeSerialization(Box<bincode::ErrorKind>),
    ConnectionFailed(String),
    PeerDisconnected(String),
    InvalidMessage(String),
    MessageTooLarge { len: usize, limit: usize },
    Timeout,
    ChannelClosed,
    TlsError(String),
    PeerNotAllowed(String),
    PeerLimitReached(usize),
    NodeIdMismatch { expected: String, got: String },
}
```

---

## Default

| Costante | Valore | Descrizione |
|---|---|---|
| `DEFAULT_MAX_PEERS` | 64 | Massimo peer simultanei |
| `DEFAULT_MAX_MESSAGE_SIZE` | 16 MiB | Dimensione massima messaggio wire |
| `DEFAULT_SOCKET_TIMEOUT` | 30s | Timeout lettura/scrittura per operazione |
| `DEFAULT_EVENT_CHANNEL_CAPACITY` | 1024 | Buffer del canale eventi |
| `TASK_SHUTDOWN_GRACE` | 3s | Grace period cooperativo per task |

---

## Copertura test

I test si trovano in `node.rs` e `message.rs` (`#[cfg(test)]`), oltre a test di integrazione in `tests/`.

| Test | Cosa copre |
|---|---|
| `test_node_config` | valori default e builder |
| `node_config_allows_custom_peer_limit` | `with_max_peers` |
| `negotiation_prefers_local_format_when_peer_supports_it` | bincode-bincode -> bincode |
| `negotiation_falls_back_to_json_for_debug_peer` | nodo bincode + peer solo-json -> json |
| `negotiation_rejects_empty_peer_formats` | nessun formato comune -> None |
| `peer_slot_limit_rejects_new_peer_when_full` | semaforo esaurito -> PeerLimitReached |
| `peer_slot_limit_allows_replacing_same_addr` | ri-connessione allo stesso addr è permessa |
| `mark_peer_failed_returns_updated_connected_count` | transizione stato peer + conteggio |
| `track_task_prunes_finished_handles_before_push` | lista task resta pulita |
| `is_connected_addr_tracks_connected_state` | solo peer Connected restituiscono true |
| `read_message_rejects_payload_over_configured_limit` | MessageTooLarge |
| `read_message_times_out_waiting_for_length` | Timeout su lettura bloccata |
| `connect_to_peer_times_out_during_handshake` | Timeout durante handshake plain |
| `connect_to_peer_times_out_during_tls_handshake` | Timeout durante handshake TLS |
| `connect_to_peer_rejects_protocol_version_mismatch` | versione vecchia in HelloAck |
| `incoming_rejects_protocol_version_mismatch` | versione vecchia in Hello |
| `incoming_idle_handshake_consumes_peer_slot` | slot tenuto prima che handshake completi |
| `incoming_idle_tls_handshake_releases_peer_slot_after_timeout` | slot rilasciato dopo timeout |
| `active_peer_shutdown_does_not_wait_for_socket_timeout` | timing shutdown cooperativo |
| `incoming_ping_gets_pong_response` | keepalive Ping/Pong |
| `bincode_node_negotiates_json_with_debug_peer` | negoziazione cross-format E2E |
| `message_roundtrip_json` / `message_roundtrip_bincode` | roundtrip encode/decode |
| `rejects_unknown_serialization_format` | byte formato sconosciuto -> InvalidMessage |

```bash
cargo test -p nx-net
```

---

## Correlati

Leggi questa pagina insieme ai docs del modello sync e runtime:

- [Crate nx-sync](/numax/it/reference/crates/nx-sync/) - tipi `Op` e `NodeId` usati dal protocollo wire
- [Crate nx-core](/numax/it/reference/crates/nx-core/) - il sync manager che guida `Node`
- [Configurazione](/numax/it/reference/configuration/) - campi TLS e limiti che diventano `NodeConfig`
- [Panoramica crate](/numax/it/reference/crates/) - dove `nx-net` si inserisce nel grafo delle dipendenze
