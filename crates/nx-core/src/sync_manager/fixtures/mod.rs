// Shared readers for immutable, release-generated logical-store fixtures.
pub(super) fn parse_fixture(fixture: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    fixture
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let (key, value) = line
                .split_once('\t')
                .expect("fixture record must contain a tab separator");
            (decode_hex(key), decode_hex(value))
        })
        .collect()
}

fn decode_hex(hex: &str) -> Vec<u8> {
    assert!(
        hex.len().is_multiple_of(2),
        "hex input must have even length"
    );
    hex.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let high = decode_nibble(pair[0]);
            let low = decode_nibble(pair[1]);
            (high << 4) | low
        })
        .collect()
}

fn decode_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => panic!("fixture contains non-lowercase-hex byte"),
    }
}
