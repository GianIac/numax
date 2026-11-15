use std::collections::BTreeMap;
use std::io::{Read, Write};

fn encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("\\{byte:02x}")).collect()
}

fn generate(values: &BTreeMap<String, String>) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    if values.len() > 32 {
        return Err("control budget exceeded".into());
    }
    let mut memory = Vec::new();
    let mut instructions = String::new();
    for (key, value) in values {
        if !(key.starts_with("demo:env/NUMAX_") || key == "demo:completed")
            || key.len() > 128
            || value.len() > 4096
        {
            return Err("invalid control key or value".into());
        }
        let key_offset = memory.len();
        memory.extend_from_slice(key.as_bytes());
        let value_offset = memory.len();
        memory.extend_from_slice(value.as_bytes());
        instructions.push_str(&format!(
            "(if (call $set (i32.const {key_offset}) (i32.const {}) (i32.const {value_offset}) (i32.const {})) (then unreachable))",
            key.len(), value.len()
        ));
    }
    if memory.len() > 65536 {
        return Err("control memory budget exceeded".into());
    }
    let source = format!(
        "(module (import \"nx\" \"db_set\" (func $set (param i32 i32 i32 i32) (result i32)))
        (memory (export \"memory\") 1) (data (i32.const 0) \"{}\")
        (func (export \"run\") {instructions}))",
        encode(&memory)
    );
    Ok(wat::parse_str(source)?)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut input = Vec::new();
    std::io::stdin().take(65537).read_to_end(&mut input)?;
    if input.len() > 65536 {
        return Err("input budget exceeded".into());
    }
    let values: BTreeMap<String, String> = serde_json::from_slice(&input)?;
    std::io::stdout().write_all(&generate(&values)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_demo_keys_and_excessive_input() {
        assert!(generate(&BTreeMap::from([("__nx/secret".into(), "value".into())])).is_err());
        assert!(
            generate(&BTreeMap::from([(
                "demo:env/NUMAX_BOOST".into(),
                "x".repeat(4097)
            )]))
            .is_err()
        );
    }

    #[test]
    fn encodes_unicode_and_quotes_as_wasm_data() {
        let bytes = generate(&BTreeMap::from([(
            "demo:env/NUMAX_SNAPSHOT_LABEL".into(),
            "a \"label\" é".into(),
        )]))
        .unwrap();
        assert!(bytes.starts_with(b"\0asm"));
    }
}
