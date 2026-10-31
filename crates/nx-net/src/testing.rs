//! Codec boundary for deterministic transport tests, not an alternative protocol.
use crate::message::{WireMessage, WireMessageKind};
use crate::{
    ControlReply, ControlRequest, NetError, NetResult, PROTOCOL_VERSION, SerializationFormat,
};
use nx_sync::{HistoryMessage, NodeId};

pub fn encode_history(message: HistoryMessage, format: SerializationFormat) -> NetResult<Vec<u8>> {
    WireMessage {
        kind: WireMessageKind::History { message },
    }
    .to_bytes_with_format(format)
}

pub fn decode_history(frame: &[u8]) -> NetResult<HistoryMessage> {
    let (_, message) = WireMessage::from_bytes_with_format(payload(frame)?)?;
    match message.kind {
        WireMessageKind::History { message } => Ok(message),
        _ => Err(NetError::InvalidMessage("expected history frame".into())),
    }
}

pub fn control_request(
    from: NodeId,
    request: ControlRequest,
    format: SerializationFormat,
) -> NetResult<(usize, ControlRequest)> {
    let frame = WireMessage {
        kind: WireMessageKind::ControlHello {
            node_id: from.clone(),
            protocol_version: PROTOCOL_VERSION,
            supported_formats: vec![format],
            preferred_format: format,
            request,
        },
    }
    .to_bytes_with_format(format)?;
    crate::message::validate_payload_len(frame.len().saturating_sub(4), crate::MAX_CONTROL_FRAME)?;
    let (_, decoded) = WireMessage::from_bytes_with_format(payload(&frame)?)?;
    match decoded.kind {
        WireMessageKind::ControlHello {
            node_id, request, ..
        } if node_id == from && request.owner.member == from.as_str() => Ok((frame.len(), request)),
        _ => Err(NetError::InvalidMessage("unexpected control owner".into())),
    }
}

pub fn control_reply(
    from: NodeId,
    request_id: u64,
    reply: ControlReply,
    format: SerializationFormat,
) -> NetResult<(usize, ControlReply)> {
    let frame = WireMessage {
        kind: WireMessageKind::ControlAck {
            node_id: from.clone(),
            protocol_version: PROTOCOL_VERSION,
            selected_format: format,
            reply,
        },
    }
    .to_bytes_with_format(format)?;
    crate::message::validate_payload_len(frame.len().saturating_sub(4), crate::MAX_CONTROL_FRAME)?;
    let (_, decoded) = WireMessage::from_bytes_with_format(payload(&frame)?)?;
    match decoded.kind {
        WireMessageKind::ControlAck { node_id, reply, .. }
            if node_id == from
                && reply.owner.member == from.as_str()
                && reply.request_id == request_id =>
        {
            Ok((frame.len(), reply))
        }
        _ => Err(NetError::InvalidMessage(
            "uncorrelated control reply".into(),
        )),
    }
}

fn payload(frame: &[u8]) -> NetResult<&[u8]> {
    let header: [u8; 4] = frame
        .get(..4)
        .ok_or_else(|| NetError::InvalidMessage("truncated frame".into()))?
        .try_into()
        .map_err(|_| NetError::InvalidMessage("invalid frame header".into()))?;
    if u32::from_be_bytes(header) as usize != frame.len() - 4 {
        return Err(NetError::InvalidMessage("invalid frame length".into()));
    }
    Ok(&frame[4..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn real_codec_boundary_rejects_malformed_and_uncorrelated_frames() {
        for format in [SerializationFormat::Json, SerializationFormat::Bincode] {
            let message = HistoryMessage::SummaryRequest { after_origin: None };
            let frame = encode_history(message.clone(), format).unwrap();
            assert_eq!(decode_history(&frame).unwrap(), message);
            for bytes in [&frame[..3], &frame[..frame.len() - 1]] {
                assert!(decode_history(bytes).is_err());
            }
            let mut trailing = frame.clone();
            trailing.push(0);
            assert!(decode_history(&trailing).is_err());
            let owner = crate::MembershipRecord {
                member: "node".into(),
                reporter: "node".into(),
                endpoint: None,
                version: crate::MembershipVersion {
                    epoch: 1,
                    incarnation: 0,
                },
                state: crate::MembershipState::Alive,
            };
            let request = ControlRequest {
                request_id: 1,
                owner: owner.clone(),
                updates: Vec::new(),
                kind: crate::ControlKind::Exchange { after: None },
            };
            assert_eq!(
                control_request(NodeId::new("node"), request.clone(), format)
                    .unwrap()
                    .1,
                request
            );
            assert!(control_request(NodeId::new("other"), request, format).is_err());
            let reply = ControlReply {
                request_id: 1,
                owner,
                ack: true,
                nack: false,
                records: Vec::new(),
                next: None,
            };
            assert_eq!(
                control_reply(NodeId::new("node"), 1, reply.clone(), format)
                    .unwrap()
                    .1,
                reply
            );
            assert!(control_reply(NodeId::new("node"), 2, reply, format).is_err());
        }
    }
}
