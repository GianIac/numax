use std::collections::HashMap;
use std::sync::Arc;

use nx_sync::{NodeId, Op};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, mpsc};
use tracing::{debug, error, info, warn};

use crate::error::{NetError, NetResult};
use crate::message::{Message, MessageKind, PROTOCOL_VERSION};
use crate::peer::{PeerInfo, PeerState};

/// Configurazione del nodo.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// NodeId di questo nodo.
    pub node_id: NodeId,

    /// Indirizzo su cui ascoltare (es. "0.0.0.0:9000").
    pub listen_addr: String,

    /// Peer iniziali a cui connettersi.
    pub initial_peers: Vec<String>,
}

impl NodeConfig {
    pub fn new(node_id: NodeId, listen_addr: impl Into<String>) -> Self {
        Self {
            node_id,
            listen_addr: listen_addr.into(),
            initial_peers: Vec::new(),
        }
    }

    pub fn with_peers(mut self, peers: Vec<String>) -> Self {
        self.initial_peers = peers;
        self
    }
}

/// Evento in uscita dal nodo (per il runtime).
#[derive(Debug, Clone)]
pub enum NodeEvent {
    /// Ricevute nuove operazioni da un peer.
    OpsReceived { from: NodeId, ops: Vec<Op> },

    /// Peer connesso.
    PeerConnected { node_id: NodeId },

    /// Peer disconnesso.
    PeerDisconnected { node_id: NodeId },
}

#[allow(dead_code)]
/// Stato interno di una connessione peer.
struct PeerConnection {
    info: PeerInfo,
    state: PeerState,
    writer: Option<tokio::io::WriteHalf<TcpStream>>,
}

/// Nodo di rete.
pub struct Node {
    config: NodeConfig,
    peers: Arc<RwLock<HashMap<String, PeerConnection>>>,
    event_tx: mpsc::Sender<NodeEvent>,
    event_rx: Option<mpsc::Receiver<NodeEvent>>,
}

impl Node {
    /// Crea un nuovo nodo.
    pub fn new(config: NodeConfig) -> Self {
        let (event_tx, event_rx) = mpsc::channel(100);

        Self {
            config,
            peers: Arc::new(RwLock::new(HashMap::new())),
            event_tx,
            event_rx: Some(event_rx),
        }
    }

    /// Prende il receiver degli eventi (può essere chiamato una sola volta).
    pub fn take_event_receiver(&mut self) -> Option<mpsc::Receiver<NodeEvent>> {
        self.event_rx.take()
    }

    /// Avvia il listener TCP.
    pub async fn start_listener(&self) -> NetResult<()> {
        let listener = TcpListener::bind(&self.config.listen_addr).await?;
        info!(addr = %self.config.listen_addr, "listening");

        let peers = Arc::clone(&self.peers);
        let node_id = self.config.node_id.clone();
        let event_tx = self.event_tx.clone();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        info!(%addr, "incoming connection");
                        let peers = Arc::clone(&peers);
                        let node_id = node_id.clone();
                        let event_tx = event_tx.clone();

                        tokio::spawn(async move {
                            if let Err(e) = handle_incoming(stream, node_id, peers, event_tx).await
                            {
                                error!(%addr, error = %e, "connection error");
                            }
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "accept error");
                    }
                }
            }
        });

        Ok(())
    }

    /// Connetti a un peer.
    pub async fn connect_to_peer(&self, addr: &str) -> NetResult<()> {
        info!(%addr, "connecting to peer");

        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| NetError::ConnectionFailed(format!("{}: {}", addr, e)))?;

        let (mut reader, mut writer) = tokio::io::split(stream);

        // Invia HELLO
        let hello = Message::hello(self.config.node_id.clone());
        write_message(&mut writer, &hello).await?;

        // Attendi HELLO_ACK
        let response = read_message(&mut reader).await?;
        let peer_node_id = match response.kind {
            MessageKind::HelloAck { node_id, version } => {
                if version != PROTOCOL_VERSION {
                    warn!(
                        their_version = version,
                        our_version = PROTOCOL_VERSION,
                        "protocol version mismatch"
                    );
                }
                info!(peer = %node_id, "handshake complete");
                node_id
            }
            _ => {
                return Err(NetError::InvalidMessage("expected HelloAck".into()));
            }
        };

        // Salva connessione
        {
            let mut peers = self.peers.write().await;
            peers.insert(
                addr.to_string(),
                PeerConnection {
                    info: PeerInfo::new(addr).with_node_id(peer_node_id.clone()),
                    state: PeerState::Connected,
                    writer: Some(writer),
                },
            );
        }

        // Notifica evento
        let _ = self
            .event_tx
            .send(NodeEvent::PeerConnected {
                node_id: peer_node_id.clone(),
            })
            .await;

        // Avvia loop di lettura
        let peers = Arc::clone(&self.peers);
        let event_tx = self.event_tx.clone();
        let addr_owned = addr.to_string();

        tokio::spawn(async move {
            if let Err(e) = read_loop(reader, peer_node_id.clone(), event_tx.clone()).await {
                debug!(peer = %peer_node_id, error = %e, "read loop ended");
            }

            // Cleanup
            let mut peers = peers.write().await;
            peers.remove(&addr_owned);

            let _ = event_tx
                .send(NodeEvent::PeerDisconnected {
                    node_id: peer_node_id,
                })
                .await;
        });

        Ok(())
    }

    /// Invia operazioni a tutti i peer connessi.
    pub async fn broadcast_ops(&self, ops: Vec<Op>) -> NetResult<()> {
        let msg = Message::push_ops(ops);
        let bytes = msg.to_bytes()?;

        let mut peers = self.peers.write().await;

        for (addr, conn) in peers.iter_mut() {
            if conn.state == PeerState::Connected
                && let Some(ref mut writer) = conn.writer
                && let Err(e) = writer.write_all(&bytes).await
            {
                warn!(%addr, error = %e, "failed to send ops");
                conn.state = PeerState::Failed;
            }
        }

        Ok(())
    }

    /// Restituisce il numero di peer connessi.
    pub async fn connected_peer_count(&self) -> usize {
        let peers = self.peers.read().await;
        peers
            .values()
            .filter(|c| c.state == PeerState::Connected)
            .count()
    }
}

/// Gestisce una connessione in ingresso.
async fn handle_incoming(
    stream: TcpStream,
    our_node_id: NodeId,
    _peers: Arc<RwLock<HashMap<String, PeerConnection>>>,
    event_tx: mpsc::Sender<NodeEvent>,
) -> NetResult<()> {
    let (mut reader, mut writer) = tokio::io::split(stream);

    // Attendi HELLO
    let msg = read_message(&mut reader).await?;
    let peer_node_id = match msg.kind {
        MessageKind::Hello { node_id, version } => {
            if version != PROTOCOL_VERSION {
                warn!(their_version = version, "protocol mismatch");
            }
            node_id
        }
        _ => {
            return Err(NetError::InvalidMessage("expected Hello".into()));
        }
    };

    // Rispondi con HELLO_ACK
    let ack = Message::hello_ack(our_node_id);
    write_message(&mut writer, &ack).await?;

    info!(peer = %peer_node_id, "incoming peer connected");

    // Notifica
    let _ = event_tx
        .send(NodeEvent::PeerConnected {
            node_id: peer_node_id.clone(),
        })
        .await;

    // Loop lettura
    read_loop(reader, peer_node_id.clone(), event_tx.clone()).await?;

    let _ = event_tx
        .send(NodeEvent::PeerDisconnected {
            node_id: peer_node_id,
        })
        .await;

    Ok(())
}

/// Loop di lettura messaggi da un peer.
async fn read_loop(
    mut reader: tokio::io::ReadHalf<TcpStream>,
    peer_node_id: NodeId,
    event_tx: mpsc::Sender<NodeEvent>,
) -> NetResult<()> {
    loop {
        let msg = match read_message(&mut reader).await {
            Ok(m) => m,
            Err(NetError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                debug!(peer = %peer_node_id, "peer disconnected");
                break;
            }
            Err(e) => return Err(e),
        };

        match msg.kind {
            MessageKind::PushOps { ops } => {
                debug!(peer = %peer_node_id, count = ops.len(), "received ops");
                let _ = event_tx
                    .send(NodeEvent::OpsReceived {
                        from: peer_node_id.clone(),
                        ops,
                    })
                    .await;
            }
            MessageKind::Ping => {
                debug!(peer = %peer_node_id, "received ping");
                // TODO: rispondere con Pong (serve writer)
            }
            _ => {
                debug!(peer = %peer_node_id, kind = ?msg.kind, "unhandled message");
            }
        }
    }

    Ok(())
}

/// Scrive un messaggio su uno stream.
async fn write_message<W: AsyncWriteExt + Unpin>(writer: &mut W, msg: &Message) -> NetResult<()> {
    let bytes = msg.to_bytes()?;
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

/// Legge un messaggio da uno stream.
async fn read_message<R: AsyncReadExt + Unpin>(reader: &mut R) -> NetResult<Message> {
    // Leggi lunghezza (4 bytes)
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    // Sanity check
    if len > 10 * 1024 * 1024 {
        return Err(NetError::InvalidMessage("message too large".into()));
    }

    // Leggi payload
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;

    Message::from_bytes(&buf).map_err(|e| e.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_node_config() {
        let config = NodeConfig::new(NodeId::new("test"), "127.0.0.1:9000")
            .with_peers(vec!["127.0.0.1:9001".into()]);

        assert_eq!(config.listen_addr, "127.0.0.1:9000");
        assert_eq!(config.initial_peers.len(), 1);
    }
}
