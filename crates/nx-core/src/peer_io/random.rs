//! Version 1 uses SplitMix64; stream derivation is length-delimited BLAKE3.
//! This is scheduling randomness, never an authentication primitive.
pub(crate) struct Random(u64);
impl Random {
    pub(crate) fn new(seed: u64) -> Self { Self(seed) }
    pub(crate) fn stream(seed: u64, node: &str, purpose: &str) -> Self {
        let mut hash = blake3::Hasher::new();
        hash.update(b"numax-simulation-v1");
        hash.update(&seed.to_le_bytes());
        for part in [node, purpose] {
            hash.update(&(part.len() as u64).to_le_bytes());
            hash.update(part.as_bytes());
        }
        let mut bytes = [0; 8];
        bytes.copy_from_slice(&hash.finalize().as_bytes()[..8]);
        Self(u64::from_le_bytes(bytes))
    }
    pub(crate) fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        value ^ (value >> 31)
    }
    pub(crate) fn choose(&mut self, upper: usize) -> usize {
        // Callers only request a choice from a nonempty candidate set.
        if upper == 0 { return 0; }
        (self.next() % upper as u64) as usize
    }
}
