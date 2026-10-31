//! Version 1 uses SplitMix64; stream derivation is length-delimited BLAKE3.
//! This is scheduling randomness, never an authentication primitive.
pub(crate) struct Random(u64);
impl Random {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed)
    }
    pub(crate) fn stream(seed: u64, node: &str, purpose: &str) -> Self {
        let mut hash = blake3::Hasher::new();
        hash.update(b"numax-peer-scheduling-v1");
        hash.update(&seed.to_le_bytes());
        for part in [node, purpose] {
            hash.update(&(part.len() as u64).to_le_bytes());
            hash.update(part.as_bytes());
        }
        let mut bytes = [0; 8];
        bytes.copy_from_slice(&hash.finalize().as_bytes()[..8]);
        Self::new(u64::from_le_bytes(bytes))
    }
    pub(crate) fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        value ^ (value >> 31)
    }
    pub(crate) fn choose(&mut self, upper: usize) -> usize {
        assert!(upper > 0, "random choice requires a nonempty set");
        let upper = upper as u64;
        let threshold = upper.wrapping_neg() % upper;
        loop {
            let value = self.next();
            if value >= threshold {
                return (value % upper) as usize;
            }
        }
    }
}

pub(crate) trait IdSource: Send + Sync {
    fn next(&self) -> nx_sync::OpId;
}

pub(crate) struct UuidIds;
impl IdSource for UuidIds {
    fn next(&self) -> nx_sync::OpId {
        nx_sync::OpId::generate()
    }
}

#[cfg(test)]
pub(crate) struct SeededIds {
    prefix: [u8; 32],
    counter: std::sync::atomic::AtomicU64,
}
#[cfg(test)]
impl SeededIds {
    pub(crate) fn new(seed: u64, node: &str, generation: u64, purpose: &str) -> Self {
        let mut hash = blake3::Hasher::new();
        hash.update(b"numax-operation-ids-v1");
        hash.update(&seed.to_le_bytes());
        hash.update(&generation.to_le_bytes());
        for part in [node, purpose] {
            hash.update(&(part.len() as u64).to_le_bytes());
            hash.update(part.as_bytes());
        }
        Self {
            prefix: *hash.finalize().as_bytes(),
            counter: 0.into(),
        }
    }
}
#[cfg(test)]
impl IdSource for SeededIds {
    fn next(&self) -> nx_sync::OpId {
        let count = self
            .counter
            .fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |value| value.checked_add(1),
            )
            .expect("simulation identity counter exhausted");
        let mut hash = blake3::Hasher::new();
        hash.update(&self.prefix);
        hash.update(&count.to_le_bytes());
        let bytes = hash.finalize();
        let b = bytes.as_bytes();
        nx_sync::OpId::new(format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-4{:01x}{:02x}-8{:01x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            b[0],
            b[1],
            b[2],
            b[3],
            b[4],
            b[5],
            b[6] & 15,
            b[7],
            b[8] & 15,
            b[9],
            b[10],
            b[11],
            b[12],
            b[13],
            b[14],
            b[15],
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn version_one_vectors_and_streams() {
        let mut rng = Random::new(0);
        assert_eq!(rng.next(), 0xe220a8397b1dcdaf);
        assert_eq!(rng.next(), 0x6e789e6aa1b965f4);
        assert_eq!(rng.next(), 0x06c45d188009454f);
        let mut a = Random::stream(42, "a", "probes");
        let mut b = Random::stream(42, "a", "probes");
        let mut other = Random::stream(42, "a", "loss");
        for _ in 0..100 {
            assert_eq!(a.next(), b.next());
            other.next();
        }
        assert_ne!(a.next(), other.next());
        for upper in [1, 3, 50, 4096] {
            for _ in 0..100 {
                assert!(a.choose(upper) < upper);
            }
        }
        let ids = SeededIds::new(0, "a", 1, "local");
        let replay = SeededIds::new(0, "a", 1, "local");
        let restarted = SeededIds::new(0, "a", 2, "local");
        let first = ids.next();
        assert_eq!(first, replay.next());
        assert_ne!(first, restarted.next());
        assert_eq!(first.as_str().len(), 36);
        assert_ne!(first, ids.next());
    }
}
