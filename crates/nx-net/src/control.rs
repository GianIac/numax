use serde::{Deserialize, Serialize};

use crate::{NetError, NetResult};
use nx_sync::NodeId;
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc, oneshot};

pub struct ControlIncoming {
    pub from: NodeId,
    pub verification: crate::PeerIdentityVerification,
    pub request: ControlRequest,
    pub respond_to: oneshot::Sender<ControlReply>,
}

pub(crate) struct ControlService {
    pub(crate) incoming: Arc<Semaphore>,
    pub(crate) outgoing: Arc<Semaphore>,
    pub(crate) events: mpsc::Sender<ControlIncoming>,
}

impl ControlService {
    pub(crate) fn new(capacity: usize) -> (Self, mpsc::Receiver<ControlIncoming>) {
        let capacity = capacity.min(MAX_CONTROL_REQUESTS);
        let (events, receiver) = mpsc::channel(capacity.max(1));
        (
            Self {
                incoming: Arc::new(Semaphore::new(capacity)),
                outgoing: Arc::new(Semaphore::new(capacity)),
                events,
            },
            receiver,
        )
    }
}

pub const MAX_CONTROL_FRAME: usize = 16 * 1024;
pub const MAX_CONTROL_RECORDS: usize = 4;
pub const MAX_CONTROL_REQUESTS: usize = 64;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    wincode::SchemaRead,
    wincode::SchemaWrite,
)]
pub struct MembershipVersion {
    pub epoch: u64,
    pub incarnation: u64,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    wincode::SchemaRead,
    wincode::SchemaWrite,
)]
pub enum MembershipState {
    Alive,
    Suspect,
    Dead,
    Left,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
pub struct MembershipRecord {
    pub member: String,
    pub version: MembershipVersion,
    pub state: MembershipState,
    pub endpoint: Option<String>,
    pub reporter: String,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
pub enum ControlKind {
    Probe {
        target: String,
        version: MembershipVersion,
    },
    Indirect {
        target: String,
        version: MembershipVersion,
    },
    Exchange {
        after: Option<String>,
    },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
pub struct ControlRequest {
    pub request_id: u64,
    pub owner: MembershipRecord,
    pub updates: Vec<MembershipRecord>,
    pub kind: ControlKind,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
pub struct ControlReply {
    pub request_id: u64,
    pub owner: MembershipRecord,
    pub ack: bool,
    pub nack: bool,
    pub records: Vec<MembershipRecord>,
    pub next: Option<String>,
}

fn invalid() -> NetError {
    NetError::InvalidMessage("invalid or excessive membership control metadata".into())
}

fn valid_id(member: &str) -> bool {
    !member.is_empty() && member.len() <= 256 && !member.chars().any(char::is_control)
}

impl MembershipRecord {
    pub(crate) fn validate(&self) -> NetResult<()> {
        if !valid_id(&self.member)
            || !valid_id(&self.reporter)
            || self.version.epoch == 0
            || self.endpoint.as_ref().is_some_and(|endpoint| {
                endpoint.is_empty()
                    || endpoint.len() > 512
                    || endpoint.chars().any(char::is_control)
            })
        {
            return Err(invalid());
        }
        Ok(())
    }

    fn validate_owner(&self) -> NetResult<()> {
        self.validate()?;
        if self.reporter != self.member
            || !matches!(self.state, MembershipState::Alive | MembershipState::Left)
        {
            return Err(invalid());
        }
        Ok(())
    }
}

impl ControlRequest {
    pub(crate) fn validate(&self) -> NetResult<()> {
        self.owner.validate_owner()?;
        if self.request_id == 0 || self.updates.len() > MAX_CONTROL_RECORDS {
            return Err(invalid());
        }
        for record in &self.updates {
            record.validate()?;
        }
        match &self.kind {
            ControlKind::Probe { target, version } | ControlKind::Indirect { target, version }
                if !valid_id(target) || version.epoch == 0 =>
            {
                return Err(invalid());
            }
            ControlKind::Exchange { after: Some(after) } if !valid_id(after) => {
                return Err(invalid());
            }
            _ => {}
        }
        Ok(())
    }
}

impl ControlReply {
    pub(crate) fn validate(&self) -> NetResult<()> {
        self.owner.validate_owner()?;
        if self.request_id == 0
            || self.ack && self.nack
            || self.records.len() > MAX_CONTROL_RECORDS
            || self.next.as_ref().is_some_and(|next| !valid_id(next))
        {
            return Err(invalid());
        }
        for record in &self.records {
            record.validate()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{WireMessage, WireMessageKind};
    use crate::{PROTOCOL_VERSION, SerializationFormat};
    use nx_sync::NodeId;

    fn owner() -> MembershipRecord {
        MembershipRecord {
            member: "owner".into(),
            reporter: "owner".into(),
            version: MembershipVersion {
                epoch: 1,
                incarnation: 0,
            },
            state: MembershipState::Alive,
            endpoint: Some("127.0.0.1:9000".into()),
        }
    }

    #[test]
    fn control_codec_bounds_and_roundtrips_both_formats() {
        let request = ControlRequest {
            request_id: 1,
            owner: owner(),
            updates: vec![],
            kind: ControlKind::Exchange { after: None },
        };
        for format in [SerializationFormat::Json, SerializationFormat::Bincode] {
            let message = WireMessage {
                kind: WireMessageKind::ControlHello {
                    node_id: NodeId::new("owner"),
                    protocol_version: PROTOCOL_VERSION,
                    supported_formats: vec![format],
                    preferred_format: format,
                    request: request.clone(),
                },
            };
            let bytes = message.to_bytes_with_format(format).unwrap();
            assert_eq!(
                WireMessage::from_bytes_with_format(&bytes[4..]).unwrap().1,
                message
            );
            let mut bad = request.clone();
            bad.updates = vec![owner(); MAX_CONTROL_RECORDS + 1];
            assert!(bad.validate().is_err());
            bad = request.clone();
            bad.owner.reporter = "other".into();
            assert!(bad.validate().is_err());
            bad = request.clone();
            bad.request_id = 0;
            assert!(bad.validate().is_err());
        }
    }

    #[tokio::test]
    async fn control_rpc_rejects_uncorrelated_versions_and_codecs() {
        use crate::node::{read_wire_message_with_format, write_message};
        use std::time::Duration;
        for format in [SerializationFormat::Json, SerializationFormat::Bincode] {
            for invalid in ["request_id", "codec", "protocol"] {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let endpoint = listener.local_addr().unwrap().to_string();
                let client = crate::Node::new(
                    crate::NodeConfig::new(NodeId::new("owner"), "127.0.0.1:0")
                        .with_serialization_format(format),
                );
                let request = ControlRequest {
                    request_id: 1,
                    owner: owner(),
                    updates: vec![],
                    kind: ControlKind::Exchange { after: None },
                };
                let serve = async {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    read_wire_message_with_format(
                        &mut stream,
                        MAX_CONTROL_FRAME,
                        Duration::from_secs(2),
                    )
                    .await
                    .unwrap();
                    let mut peer = owner();
                    peer.member = "server".into();
                    peer.reporter = "server".into();
                    let response = WireMessage {
                        kind: WireMessageKind::ControlAck {
                            node_id: NodeId::new("server"),
                            protocol_version: if invalid == "protocol" {
                                PROTOCOL_VERSION - 1
                            } else {
                                PROTOCOL_VERSION
                            },
                            selected_format: if invalid == "codec" {
                                match format {
                                    SerializationFormat::Json => SerializationFormat::Bincode,
                                    SerializationFormat::Bincode => SerializationFormat::Json,
                                }
                            } else {
                                format
                            },
                            reply: ControlReply {
                                request_id: if invalid == "request_id" { 2 } else { 1 },
                                owner: peer,
                                ack: true,
                                nack: false,
                                records: vec![],
                                next: None,
                            },
                        },
                    };
                    write_message(&mut stream, &response, format, Duration::from_secs(2))
                        .await
                        .unwrap();
                };
                let (result, ()) = tokio::join!(
                    client.control_request(&endpoint, request, Duration::from_secs(2)),
                    serve
                );
                assert!(result.is_err(), "{format:?}: accepted invalid {invalid}");
                assert_eq!(client.connected_peer_count().await, 0);
                client.shutdown().await;
            }
        }
    }

    #[tokio::test]
    async fn control_rpc_saturation_is_explicit_and_releases_capacity() {
        use std::time::Duration;
        for format in [SerializationFormat::Json, SerializationFormat::Bincode] {
            let client = crate::Node::new(
                crate::NodeConfig::new(NodeId::new("owner"), "127.0.0.1:0")
                    .with_serialization_format(format),
            );
            let mut server = crate::Node::new(
                crate::NodeConfig::new(NodeId::new("server"), "127.0.0.1:0")
                    .with_max_peers(1)
                    .with_serialization_format(format),
            );
            let mut incoming = server.take_control_receiver().unwrap();
            let endpoint = server.start_listener().await.unwrap().to_string();
            let request = ControlRequest {
                request_id: 1,
                owner: owner(),
                updates: vec![],
                kind: ControlKind::Exchange { after: None },
            };
            let first = client.control_request(&endpoint, request.clone(), Duration::from_secs(2));
            tokio::pin!(first);
            let held = tokio::select! {
                event = incoming.recv() => event.unwrap(),
                result = &mut first => panic!("request ended before service response: {result:?}"),
            };
            let refused = client
                .control_request(&endpoint, request, Duration::from_secs(2))
                .await;
            assert!(matches!(
                refused,
                Err(NetError::Wire(crate::WireError::RateLimited { .. }))
            ));
            let mut peer = owner();
            peer.member = "server".into();
            peer.reporter = "server".into();
            held.respond_to
                .send(ControlReply {
                    request_id: 1,
                    owner: peer,
                    ack: false,
                    nack: false,
                    records: vec![],
                    next: None,
                })
                .unwrap();
            assert!(first.await.is_ok());
            assert_eq!(server.connected_peer_count().await, 0);
            server.shutdown().await;
            client.shutdown().await;
        }
    }

    #[tokio::test]
    async fn control_rpc_real_sockets_and_tls_leave_data_registry_untouched() {
        use rustls::pki_types::{CertificateDer, pem::PemObject};
        let pki = crate::TestPki::generate().unwrap();
        for format in [SerializationFormat::Json, SerializationFormat::Bincode] {
            for secure in [false, true] {
                let mut client_config =
                    crate::NodeConfig::new(NodeId::new("client"), "127.0.0.1:0")
                        .with_serialization_format(format);
                let mut server_config =
                    crate::NodeConfig::new(NodeId::new("server"), "127.0.0.1:0")
                        .with_serialization_format(format);
                if secure {
                    let client_tls = pki.node2_config();
                    let server_tls = pki.node1_config();
                    client_config.node_id = crate::tls::derive_protocol_node_id_from_cert(
                        &CertificateDer::from_pem_slice(pki.node2_cert.as_bytes()).unwrap(),
                    )
                    .unwrap();
                    server_config.node_id = crate::tls::derive_protocol_node_id_from_cert(
                        &CertificateDer::from_pem_slice(pki.node1_cert.as_bytes()).unwrap(),
                    )
                    .unwrap();
                    client_config.tls = Some(client_tls);
                    server_config.tls = Some(server_tls);
                }
                let client_id = client_config.node_id.clone();
                let server_id = server_config.node_id.clone();
                let client = crate::Node::new(client_config);
                let mut server = crate::Node::new(server_config);
                let mut incoming = server.take_control_receiver().unwrap();
                let bound = server.start_listener().await.unwrap();
                let address = if secure {
                    format!("localhost:{}", bound.port())
                } else {
                    bound.to_string()
                };
                let mut request_owner = owner();
                request_owner.member = client_id.to_string();
                request_owner.reporter = request_owner.member.clone();
                let request = ControlRequest {
                    request_id: 42,
                    owner: request_owner,
                    updates: vec![],
                    kind: ControlKind::Exchange { after: None },
                };
                let serve = async {
                    let event = incoming.recv().await.unwrap();
                    assert_eq!(event.from, client_id);
                    assert_eq!(
                        event.verification,
                        if secure {
                            crate::PeerIdentityVerification::CertificateBound
                        } else {
                            crate::PeerIdentityVerification::Unverified
                        }
                    );
                    let mut response_owner = owner();
                    response_owner.member = server_id.to_string();
                    response_owner.reporter = response_owner.member.clone();
                    event
                        .respond_to
                        .send(ControlReply {
                            request_id: event.request.request_id,
                            owner: response_owner,
                            ack: false,
                            nack: false,
                            records: vec![],
                            next: None,
                        })
                        .unwrap();
                };
                let (result, served) = tokio::join!(
                    client.control_request(&address, request, std::time::Duration::from_secs(2)),
                    tokio::time::timeout(std::time::Duration::from_secs(3), serve)
                );
                assert!(
                    result.is_ok(),
                    "secure={secure}, format={format:?}: {result:?}"
                );
                served.unwrap();
                assert_eq!(result.unwrap().1.request_id, 42);
                assert_eq!(client.connected_peer_count().await, 0);
                assert_eq!(server.connected_peer_count().await, 0);
                if secure {
                    let forged = crate::Node::new(
                        crate::NodeConfig::new(NodeId::new("owner"), "127.0.0.1:0")
                            .with_tls(pki.node2_config())
                            .with_serialization_format(format),
                    );
                    let refused = forged
                        .control_request(
                            &address,
                            ControlRequest {
                                request_id: 43,
                                owner: owner(),
                                updates: vec![],
                                kind: ControlKind::Exchange { after: None },
                            },
                            std::time::Duration::from_secs(2),
                        )
                        .await;
                    assert!(refused.is_err(), "TLS identity mismatch admitted");
                    assert!(matches!(
                        incoming.try_recv(),
                        Err(mpsc::error::TryRecvError::Empty)
                    ));
                    forged.shutdown().await;
                }
                client.shutdown().await;
                server.shutdown().await;
            }
        }
    }
}
