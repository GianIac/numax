use std::hint::black_box;
use std::time::{Duration, Instant};

use nx_net::{Message, SerializationFormat};
use nx_sync::{NodeId, Op};

const OPS: usize = 1_000;
const ITERS: usize = 1_000;

fn main() {
    let msg = Message::push_ops(sample_ops());

    let json_bytes = msg.to_bytes_with_format(SerializationFormat::Json).unwrap();
    let bincode_bytes = msg
        .to_bytes_with_format(SerializationFormat::Bincode)
        .unwrap();

    let json_roundtrip = measure_roundtrip(&msg, SerializationFormat::Json, ITERS);
    let bincode_roundtrip = measure_roundtrip(&msg, SerializationFormat::Bincode, ITERS);

    println!("serialization benchmark");
    println!("ops_per_message: {OPS}");
    println!("iterations: {ITERS}");
    println!(
        "json_size_bytes: {} ({:.2}x bincode)",
        json_bytes.len(),
        json_bytes.len() as f64 / bincode_bytes.len() as f64
    );
    println!("bincode_size_bytes: {}", bincode_bytes.len());
    println!(
        "json_roundtrip_total: {:?} ({:?}/iter)",
        json_roundtrip,
        per_iter(json_roundtrip, ITERS)
    );
    println!(
        "bincode_roundtrip_total: {:?} ({:?}/iter, {:.2}x faster)",
        bincode_roundtrip,
        per_iter(bincode_roundtrip, ITERS),
        json_roundtrip.as_secs_f64() / bincode_roundtrip.as_secs_f64()
    );
}

fn sample_ops() -> Vec<Op> {
    let node = NodeId::new("bench-node");
    (0..OPS)
        .map(|i| Op::gcounter_increment(node.clone(), format!("counter:{i}"), i as u64 + 1))
        .collect()
}

fn measure_roundtrip(msg: &Message, format: SerializationFormat, iterations: usize) -> Duration {
    let started = Instant::now();
    for _ in 0..iterations {
        let bytes = black_box(msg.to_bytes_with_format(format).unwrap());
        let parsed = Message::from_bytes(black_box(&bytes[4..])).unwrap();
        black_box(parsed);
    }
    started.elapsed()
}

fn per_iter(total: Duration, iterations: usize) -> Duration {
    Duration::from_secs_f64(total.as_secs_f64() / iterations as f64)
}
