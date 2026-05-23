use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use wasmtime::Linker;

use crate::runtime::HostState;

static MONOTONIC_START: OnceLock<Instant> = OnceLock::new();

fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn monotonic_millis() -> u64 {
    MONOTONIC_START
        .get_or_init(Instant::now)
        .elapsed()
        .as_millis() as u64
}

pub fn add_to_linker(linker: &mut Linker<HostState>) -> Result<()> {
    linker.func_wrap("nx", "time_now", || -> u64 { unix_time_millis() })?;

    linker.func_wrap("nx", "time_monotonic", || -> u64 { monotonic_millis() })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_now_returns_unix_epoch_millis() {
        assert!(unix_time_millis() > 1_577_836_800_000);
    }

    #[test]
    fn time_monotonic_is_non_decreasing() {
        let first = monotonic_millis();
        let second = monotonic_millis();

        assert!(second >= first);
    }
}
