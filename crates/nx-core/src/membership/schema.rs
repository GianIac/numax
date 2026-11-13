use serde_json::{Value, json};

use super::state::{Status, Update};
use super::{Error, Version};

pub(super) const PREFIX: &[u8] = b"__nx/membership/";
pub(super) const SCHEMA_KEY: &[u8] = b"__nx/membership/schema";
pub(super) const HEADER: &[u8] = b"NXMB\0\x01\0\0";
pub(super) const LOCAL_KEY: &[u8] = b"__nx/membership/local";
pub(super) const FLOOR_PREFIX: &[u8] = b"__nx/membership/floor/";

pub(super) fn valid_id(member: &str) -> bool {
    !member.is_empty() && member.len() <= 256
}

pub(super) fn encode_local(member: &str, epoch: u64) -> Result<Vec<u8>, Error> {
    if !valid_id(member) || epoch == 0 {
        return Err(Error::InvalidMetadata);
    }
    serde_json::to_vec(&json!({ "member": member, "epoch": epoch }))
        .map_err(|_| Error::InvalidMetadata)
}

pub(super) fn decode_local(bytes: &[u8]) -> Result<(String, u64), Error> {
    let value: Value = serde_json::from_slice(bytes).map_err(|_| Error::InvalidMetadata)?;
    let fields = value.as_object().ok_or(Error::InvalidMetadata)?;
    let member = fields
        .get("member")
        .and_then(Value::as_str)
        .ok_or(Error::InvalidMetadata)?;
    let epoch = fields
        .get("epoch")
        .and_then(Value::as_u64)
        .ok_or(Error::InvalidMetadata)?;
    if fields.len() != 2 || !valid_id(member) || epoch == 0 {
        return Err(Error::InvalidMetadata);
    }
    Ok((member.to_owned(), epoch))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Floor {
    pub(super) member: String,
    pub(super) version: Version,
    pub(super) status: Status,
    pub(super) closed_epoch: Option<u64>,
}

impl Floor {
    pub(super) fn from_update(update: &Update) -> Result<Self, Error> {
        let floor = Self {
            member: update.member.clone(),
            version: update.version,
            status: update.status,
            closed_epoch: (update.status == Status::Left).then_some(update.version.epoch),
        };
        floor.validate()?;
        Ok(floor)
    }

    pub(super) fn validate(&self) -> Result<(), Error> {
        if !valid_id(&self.member)
            || self.version.epoch == 0
            || !matches!(self.status, Status::Dead | Status::Left)
            || self
                .closed_epoch
                .is_some_and(|epoch| epoch == 0 || epoch > self.version.epoch)
            || (self.status == Status::Left) != (self.closed_epoch == Some(self.version.epoch))
        {
            return Err(Error::InvalidMetadata);
        }
        Ok(())
    }

    pub(super) fn merge(&self, incoming: &Self) -> Result<Self, Error> {
        self.validate()?;
        incoming.validate()?;
        if self.member != incoming.member {
            return Err(Error::InvalidMetadata);
        }
        let version = self.version.max(incoming.version);
        let closed_epoch = self.closed_epoch.max(incoming.closed_epoch);
        Ok(Self {
            member: self.member.clone(),
            version,
            status: if closed_epoch == Some(version.epoch) {
                Status::Left
            } else {
                Status::Dead
            },
            closed_epoch,
        })
    }

    pub(super) fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        serde_json::to_vec(&json!({
            "epoch": self.version.epoch,
            "incarnation": self.version.incarnation,
            "status": if self.status == Status::Left { "left" } else { "dead" },
            "closed_epoch": self.closed_epoch,
        }))
        .map_err(|_| Error::InvalidMetadata)
    }

    pub(super) fn decode(member: &str, bytes: &[u8]) -> Result<Self, Error> {
        let value: Value = serde_json::from_slice(bytes).map_err(|_| Error::InvalidMetadata)?;
        let fields = value.as_object().ok_or(Error::InvalidMetadata)?;
        if fields.len() != 4 {
            return Err(Error::InvalidMetadata);
        }
        let integer = |key| {
            fields
                .get(key)
                .and_then(Value::as_u64)
                .ok_or(Error::InvalidMetadata)
        };
        let status = match fields.get("status").and_then(Value::as_str) {
            Some("dead") => Status::Dead,
            Some("left") => Status::Left,
            _ => return Err(Error::InvalidMetadata),
        };
        let closed_epoch = match fields.get("closed_epoch") {
            Some(Value::Null) => None,
            Some(value) => Some(value.as_u64().ok_or(Error::InvalidMetadata)?),
            None => return Err(Error::InvalidMetadata),
        };
        let floor = Self {
            member: member.into(),
            version: Version {
                epoch: integer("epoch")?,
                incarnation: integer("incarnation")?,
            },
            status,
            closed_epoch,
        };
        floor.validate()?;
        Ok(floor)
    }

    pub(super) fn key(&self) -> Vec<u8> {
        [FLOOR_PREFIX, self.member.as_bytes()].concat()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn floor(epoch: u64, incarnation: u64, status: Status) -> Floor {
        Floor::from_update(&Update {
            member: "bob".into(),
            version: Version { epoch, incarnation },
            status,
        })
        .unwrap()
    }

    #[test]
    fn floor_join_preserves_closed_epochs_and_is_order_independent() {
        let records = [
            floor(7, 99, Status::Dead),
            floor(7, 0, Status::Left),
            floor(8, 0, Status::Dead),
        ];
        for first in &records {
            assert_eq!(first.merge(first).unwrap(), *first);
            for second in &records {
                assert_eq!(first.merge(second).unwrap(), second.merge(first).unwrap());
                for third in &records {
                    assert_eq!(
                        first.merge(second).unwrap().merge(third).unwrap(),
                        first.merge(&second.merge(third).unwrap()).unwrap()
                    );
                }
            }
        }
        let merged = records[0]
            .merge(&records[1])
            .unwrap()
            .merge(&records[2])
            .unwrap();
        assert_eq!(merged.version.epoch, 8);
        assert_eq!(merged.closed_epoch, Some(7));
        assert_eq!(merged.status, Status::Dead);
        assert_eq!(
            Floor::decode("bob", &merged.encode().unwrap()).unwrap(),
            merged
        );
    }

    #[test]
    fn metadata_rejects_nonterminal_corrupt_and_inconsistent_records() {
        assert!(
            Floor::from_update(&Update {
                member: "bob".into(),
                version: Version {
                    epoch: 1,
                    incarnation: 0
                },
                status: Status::Alive
            })
            .is_err()
        );
        for bytes in [
            b"{}".as_slice(),
            b"null",
            br#"{"epoch":1,"incarnation":0,"status":"left","closed_epoch":null}"#,
            br#"{"epoch":1,"incarnation":0,"status":"dead","closed_epoch":1}"#,
        ] {
            assert!(Floor::decode("bob", bytes).is_err());
        }
        assert!(decode_local(br#"{"member":"bob","epoch":0}"#).is_err());
        assert_eq!(
            decode_local(&encode_local("alice", 3).unwrap()).unwrap(),
            ("alice".into(), 3)
        );
    }
}
