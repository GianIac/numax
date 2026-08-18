use std::fs;
use std::path::{Path, PathBuf};

use nx_net::Message;

const PAYLOAD_TARGETS: &[&str] = &["wire_hello", "wire_push_ops", "wire_pull_since"];

fn json_message(path: &Path) -> Message {
    let json = fs::read(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let mut payload = Vec::with_capacity(json.len() + 1);
    payload.push(0x01);
    payload.extend_from_slice(&json);
    Message::from_bytes(&payload)
        .unwrap_or_else(|error| panic!("decode {}: {error}", path.display()))
}

fn json_files(directory: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<_> = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("read {}: {error}", directory.display()))
        .map(|entry| entry.expect("read corpus entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .collect();
    paths.sort();
    paths
}

fn main() {
    let corpus = Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus");

    for target in PAYLOAD_TARGETS {
        let directory = corpus.join(target);
        for json_path in json_files(&directory) {
            let frame = json_message(&json_path)
                .to_bytes()
                .expect("serialize Bincode frame");
            let output = json_path.with_extension("bincode");
            fs::write(&output, &frame[4..])
                .unwrap_or_else(|error| panic!("write {}: {error}", output.display()));
        }
    }

    let framing = corpus.join("wire_framing");
    for json_path in json_files(&framing) {
        let frame = json_message(&json_path)
            .to_bytes()
            .expect("serialize Bincode frame");
        let output = json_path.with_extension("bincode");
        fs::write(&output, frame)
            .unwrap_or_else(|error| panic!("write {}: {error}", output.display()));
    }
}
