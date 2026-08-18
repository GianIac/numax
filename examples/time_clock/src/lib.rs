use nx_sdk::{nx_log, time};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    nx_log!("time_clock: unix_time_ms={}", time::now());

    let start = time::monotonic();
    let mut checksum = 0u64;
    for value in 0..core::hint::black_box(100_000u64) {
        checksum = checksum.wrapping_add(value);
    }
    core::hint::black_box(checksum);

    let elapsed_ms = time::monotonic().wrapping_sub(start);
    nx_log!("time_clock: elapsed_ms={}", elapsed_ms);
}
