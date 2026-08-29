//! Distributed Ant Colony Example — CRDT edition.
//!
//! Each Numax node plays one ant foraging on a shared grid between a nest
//! and one or more food sources ("hotspots"). The pheromone trail is a
//! `PNCounter` per cell (`ant:phero:{x}:{y}`): every ant's deposit and
//! evaporation is just an `inc`/`dec` on that CRDT, so the trail converges
//! to the same value on every node with no coordinator.
//!
//! Grid size, hotspot count, the deterministic layout seed and the nest
//! position all come from `NUMAX_*` environment variables (see
//! `swarm.config.toml` / `demo.sh` in this crate). Hotspot positions are
//! never transmitted between nodes: every node derives the identical list
//! from `(seed, grid_width, grid_height, hotspot_count)` with a seeded
//! PRNG, so the whole swarm agrees on the world without replicating it.
//!
//! One `nx run` invocation performs exactly one ant step: read local
//! position/carrying state from `nx_sdk::db`, pick a neighbor cell biased
//! by pheromone + distance-to-goal (classic ACO edge weight), move,
//! deposit pheromone, occasionally evaporate one random cell, and persist
//! the new position. Repeated invocations (see `demo.sh`) are the "ticks".

extern crate alloc;

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use nx_sdk::crdt::{gcounter, pncounter};
use nx_sdk::{NxError, crypto, db, net, nx_log, system};

const TRIPS_KEY: &str = "ant:trips_completed";
const POS_KEY: &str = "ant:pos";
const CARRYING_KEY: &str = "ant:carrying";

const DEFAULT_GRID_W: u32 = 12;
const DEFAULT_GRID_H: u32 = 12;
const DEFAULT_HOTSPOTS: u32 = 3;
const DEFAULT_SEED: u64 = 1337;
const DEFAULT_NEST_X: u32 = 0;
const DEFAULT_NEST_Y: u32 = 0;

const HEURISTIC_SCALE: u64 = 100_000;
const DEPOSIT_SEARCHING: u64 = 2;
const DEPOSIT_RETURNING: u64 = 8;
const EVAPORATE_PROBABILITY_PCT: u64 = 20;

/// Cap on how much a cell's pheromone can influence edge selection.
///
/// Without a cap, a cell that gets revisited early (e.g. right next to the
/// nest) accumulates pheromone without bound and its numerator eventually
/// swamps the distance heuristic no matter how far it is from the goal,
/// trapping ants in a self-reinforcing loop near the nest. Capping keeps
/// the distance-to-goal heuristic always meaningfully in the race, while
/// still letting pheromone break ties between similarly-distant cells.
const PHEROMONE_INFLUENCE_CAP: u64 = 20;

struct Config {
    grid_w: u32,
    grid_h: u32,
    hotspot_count: u32,
    seed: u64,
    nest: (u32, u32),
    render: bool,
}

impl Config {
    fn from_env() -> Self {
        Config {
            grid_w: env_u32("NUMAX_GRID_W", DEFAULT_GRID_W).max(2),
            grid_h: env_u32("NUMAX_GRID_H", DEFAULT_GRID_H).max(2),
            hotspot_count: env_u32("NUMAX_HOTSPOT_COUNT", DEFAULT_HOTSPOTS).max(1),
            seed: env_u64("NUMAX_SEED", DEFAULT_SEED),
            nest: (
                env_u32("NUMAX_NEST_X", DEFAULT_NEST_X),
                env_u32("NUMAX_NEST_Y", DEFAULT_NEST_Y),
            ),
            render: env_str("NUMAX_RENDER").as_deref() == Some("1"),
        }
    }
}

fn env_str(name: &str) -> Option<String> {
    system::env_get(name)
        .ok()
        .flatten()
        .and_then(|bytes| String::from_utf8(bytes).ok())
}

fn env_u32(name: &str, default: u32) -> u32 {
    env_str(name)
        .and_then(|s| s.trim().parse::<u32>().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env_str(name)
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

/// SplitMix64: a tiny, deterministic PRNG (not cryptographic) used only so
/// every node derives the exact same hotspot layout from the same config.
fn splitmix64_next(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn hotspots(cfg: &Config) -> Vec<(u32, u32)> {
    let mut state = cfg.seed
        ^ ((cfg.grid_w as u64) << 32)
        ^ (cfg.grid_h as u64)
        ^ ((cfg.hotspot_count as u64) << 16);
    let mut out: Vec<(u32, u32)> = Vec::new();
    let mut guard = 0u32;

    while (out.len() as u32) < cfg.hotspot_count && guard < 10_000 {
        guard += 1;
        let r = splitmix64_next(&mut state);
        let x = (r % cfg.grid_w as u64) as u32;
        let y = ((r >> 32) % cfg.grid_h as u64) as u32;
        if (x, y) == cfg.nest || out.contains(&(x, y)) {
            continue;
        }
        out.push((x, y));
    }

    out
}

fn manhattan(a: (u32, u32), b: (u32, u32)) -> u32 {
    a.0.abs_diff(b.0) + a.1.abs_diff(b.1)
}

fn neighbors(pos: (u32, u32), grid_w: u32, grid_h: u32) -> Vec<(u32, u32)> {
    let (x, y) = pos;
    let mut out = Vec::with_capacity(4);
    if x > 0 {
        out.push((x - 1, y));
    }
    if x + 1 < grid_w {
        out.push((x + 1, y));
    }
    if y > 0 {
        out.push((x, y - 1));
    }
    if y + 1 < grid_h {
        out.push((x, y + 1));
    }
    out
}

fn phero_key(x: u32, y: u32) -> String {
    format!("ant:phero:{x}:{y}")
}

fn phero_value(x: u32, y: u32) -> u64 {
    pncounter::value(&phero_key(x, y)).unwrap_or(0).max(0) as u64
}

/// Classic ACO edge weight: pheromone attraction combined with a
/// distance-to-goal heuristic, so ants converge even before any trail
/// exists and reinforce whichever trail is actually shortest over time.
fn edge_weight(pheromone: u64, dist_to_goal: u32) -> u64 {
    let capped = pheromone.min(PHEROMONE_INFLUENCE_CAP);
    let denom = (dist_to_goal as u64 + 1) * (dist_to_goal as u64 + 1);
    (((capped + 1) * HEURISTIC_SCALE) / denom).max(1)
}

fn rand_u64() -> Result<u64, NxError> {
    let bytes = crypto::random_bytes(8)?;
    let arr: [u8; 8] = bytes.try_into().map_err(|_| NxError::Internal)?;
    Ok(u64::from_le_bytes(arr))
}

fn choose_weighted(items: &[(u32, u32)], weights: &[u64]) -> Result<(u32, u32), NxError> {
    let total: u64 = weights.iter().sum();
    if total == 0 {
        return Ok(items[0]);
    }
    let r = rand_u64()? % total;
    let mut acc = 0u64;
    for (item, weight) in items.iter().zip(weights.iter()) {
        acc += *weight;
        if r < acc {
            return Ok(*item);
        }
    }
    Ok(*items.last().expect("items is non-empty"))
}

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    nx_log!("distributed_ants: start");

    let node_id = match net::node_id() {
        Ok(id) => id,
        Err(NxError::SyncDisabled) => {
            nx_log!("distributed_ants: sync is disabled on this runtime.");
            nx_log!("distributed_ants: start the runtime with --listen <addr> to enable CRDT replication.");
            return;
        }
        Err(e) => {
            nx_log!("distributed_ants: failed to read node id: {}", e);
            return;
        }
    };

    let cfg = Config::from_env();
    let hotspots = hotspots(&cfg);

    if cfg.render {
        nx_log!("NEST,{},{}", cfg.nest.0, cfg.nest.1);
        for (x, y) in &hotspots {
            nx_log!("HOTSPOT,{},{}", x, y);
        }
    }

    let mut pos = match db::get(POS_KEY) {
        Ok(Some(bytes)) if bytes.len() == 2 => (bytes[0] as u32, bytes[1] as u32),
        Ok(_) => cfg.nest,
        Err(e) => {
            nx_log!("distributed_ants: failed to read position: {}", e);
            return;
        }
    };
    let mut carrying = matches!(db::get(CARRYING_KEY), Ok(Some(bytes)) if bytes.first() == Some(&1));

    let goal = if carrying {
        cfg.nest
    } else {
        *hotspots
            .iter()
            .min_by_key(|h| manhattan(pos, **h))
            .unwrap_or(&cfg.nest)
    };

    let candidates = neighbors(pos, cfg.grid_w, cfg.grid_h);
    let weights: Vec<u64> = candidates
        .iter()
        .map(|&(x, y)| edge_weight(phero_value(x, y), manhattan((x, y), goal)))
        .collect();

    pos = match choose_weighted(&candidates, &weights) {
        Ok(next) => next,
        Err(e) => {
            nx_log!("distributed_ants: failed to draw random step: {}", e);
            return;
        }
    };

    let deposit = if carrying {
        DEPOSIT_RETURNING
    } else {
        DEPOSIT_SEARCHING
    };
    if let Err(e) = pncounter::inc(&phero_key(pos.0, pos.1), deposit) {
        nx_log!("distributed_ants: failed to deposit pheromone: {}", e);
    }

    // Decentralized evaporation: no coordinator sweeps the whole grid.
    // Each ant, on a fraction of its steps, evaporates one random cell by
    // one unit. Over many ants and many ticks this statistically decays
    // the whole field, favoring pheromone that keeps getting reinforced.
    match rand_u64() {
        Ok(roll) if roll % 100 < EVAPORATE_PROBABILITY_PCT => {
            if let Ok(draw) = rand_u64() {
                let ex = (draw % cfg.grid_w as u64) as u32;
                let ey = ((draw >> 32) % cfg.grid_h as u64) as u32;
                if phero_value(ex, ey) > 0 {
                    let _ = pncounter::dec(&phero_key(ex, ey), 1);
                }
            }
        }
        _ => {}
    }

    if !carrying && hotspots.iter().any(|h| *h == pos) {
        carrying = true;
    } else if carrying && pos == cfg.nest {
        carrying = false;
        if let Err(e) = gcounter::inc(TRIPS_KEY, 1) {
            nx_log!("distributed_ants: failed to increment trip counter: {}", e);
        }
    }

    if let Err(e) = db::set(POS_KEY, &[pos.0 as u8, pos.1 as u8]) {
        nx_log!("distributed_ants: failed to persist position: {}", e);
    }
    if let Err(e) = db::set(CARRYING_KEY, &[u8::from(carrying)]) {
        nx_log!("distributed_ants: failed to persist carrying flag: {}", e);
    }

    let trips = gcounter::value(TRIPS_KEY).unwrap_or(0);
    nx_log!(
        "distributed_ants: node={} pos=({},{}) carrying={} phero_here={} trips_completed={}",
        node_id,
        pos.0,
        pos.1,
        carrying,
        phero_value(pos.0, pos.1),
        trips
    );

    if cfg.render {
        for y in 0..cfg.grid_h {
            for x in 0..cfg.grid_w {
                nx_log!("PHERO,{},{},{}", x, y, phero_value(x, y));
            }
        }
    }

    nx_log!("distributed_ants: done");
}
