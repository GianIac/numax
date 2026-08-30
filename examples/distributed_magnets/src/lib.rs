//! Distributed Magnetic Optimization Algorithm (MOA) — CRDT edition.
//!
//! MOA (Tayarani-N. & Akbarzadeh-T., 2008) models candidate solutions as
//! magnetic particles: each has a "mass" derived from how good its position
//! is, and heavier particles pull lighter ones toward them, so the swarm
//! converges on the strongest regions of the search space. Here the search
//! space is a 2D grid and "how good a position is" is its magnetic pull
//! toward a handful of fixed, weighted attractor points ("anchors") — think
//! of them as demand points a network topology should end up close to.
//!
//! Every Numax node plays one magnetic particle. Unlike `distributed_ants`
//! (where the swarm only shares an anonymous pheromone field), each particle
//! here *knows about the others*: it publishes its current position as an
//! `LWW-Register` (`mag:pos:{particle_id}`) and reads every other particle's
//! register before moving, so peer attraction is a real cross-node signal,
//! not something reconstructed after the fact. A `PNCounter` grid
//! (`mag:field:{x}:{y}`) additionally tracks *current* occupancy (inc on
//! entering a cell, dec on leaving) purely for the heatmap — showing where
//! the swarm is clustered now, not an accumulated trail.
//!
//! Grid size, anchor count, the deterministic layout seed, the settle
//! radius, the particle count and this node's particle id all come from
//! `NUMAX_*` environment variables (see `swarm.config.toml` / `demo.sh` in
//! this crate). Anchor positions/weights are never transmitted between
//! nodes: every node derives the identical list from
//! `(seed, grid_width, grid_height, anchor_count)` with a seeded PRNG, so the
//! whole swarm agrees on the search space without replicating it.
//!
//! One `nx run` invocation performs exactly one particle step: read this
//! particle's position from local (non-replicated) `nx_sdk::db`, read every
//! other particle's published position, compute a magnetic pull toward the
//! anchors and toward any heavier peer, move to the neighbor cell with the
//! best pull (weighted-random, same as `distributed_ants`), publish the new
//! position, and update the occupancy field. Repeated invocations (see
//! `demo.sh`) are the "ticks".

extern crate alloc;

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use nx_sdk::crdt::{gcounter, lww_register, pncounter};
use nx_sdk::{NxError, crypto, db, net, nx_log, system};

const POS_KEY: &str = "mag:pos:self";
const SETTLED_KEY: &str = "mag:settled_flag";
const SETTLED_EVENTS_KEY: &str = "mag:settled_events";

const DEFAULT_GRID_W: u32 = 14;
const DEFAULT_GRID_H: u32 = 14;
const DEFAULT_ANCHORS: u32 = 3;
const DEFAULT_SEED: u64 = 4242;
const DEFAULT_SETTLE_RADIUS: u32 = 1;
const DEFAULT_PARTICLE_COUNT: u32 = 5;
const DEFAULT_PARTICLE_ID: u32 = 0;

const MASS_SCALE: u64 = 100_000;
const PEER_SCALE: u64 = 50_000;

/// Cap on how much a single peer's mass can influence a candidate cell's
/// score.
///
/// Without a cap, one particle that happens to spawn right on top of an
/// anchor accumulates enormous mass and its pull swamps every other
/// particle's anchor heuristic no matter how far away it is, collapsing the
/// whole swarm onto a single cell instead of letting each particle settle
/// near whichever anchor is actually closest to it. Capping keeps the
/// anchor heuristic always meaningfully in the race, while still letting
/// peer attraction break ties and pull genuinely nearby particles together.
const PEER_MASS_CAP: u64 = 20_000;

const ANCHOR_WEIGHT_MIN: u64 = 1;
const ANCHOR_WEIGHT_MAX: u64 = 3;

struct Config {
    grid_w: u32,
    grid_h: u32,
    anchor_count: u32,
    seed: u64,
    settle_radius: u32,
    particle_count: u32,
    particle_id: u32,
    render: bool,
}

impl Config {
    fn from_env() -> Self {
        Config {
            grid_w: env_u32("NUMAX_GRID_W", DEFAULT_GRID_W).max(2),
            grid_h: env_u32("NUMAX_GRID_H", DEFAULT_GRID_H).max(2),
            anchor_count: env_u32("NUMAX_ANCHOR_COUNT", DEFAULT_ANCHORS).max(1),
            seed: env_u64("NUMAX_SEED", DEFAULT_SEED),
            settle_radius: env_u32("NUMAX_SETTLE_RADIUS", DEFAULT_SETTLE_RADIUS),
            particle_count: env_u32("NUMAX_PARTICLE_COUNT", DEFAULT_PARTICLE_COUNT).max(1),
            particle_id: env_u32("NUMAX_PARTICLE_ID", DEFAULT_PARTICLE_ID),
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
/// every node derives the exact same anchor layout / spawn position from
/// the same config.
fn splitmix64_next(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// An attractor point on the grid with a fixed weight: heavier anchors pull
/// harder, exactly like a bigger magnet.
type Anchor = (u32, u32, u64);

fn anchors(cfg: &Config) -> Vec<Anchor> {
    let mut state = cfg.seed
        ^ ((cfg.grid_w as u64) << 32)
        ^ (cfg.grid_h as u64)
        ^ ((cfg.anchor_count as u64) << 16);
    let mut out: Vec<Anchor> = Vec::new();
    let mut guard = 0u32;

    while (out.len() as u32) < cfg.anchor_count && guard < 10_000 {
        guard += 1;
        let r = splitmix64_next(&mut state);
        let x = (r % cfg.grid_w as u64) as u32;
        let y = ((r >> 32) % cfg.grid_h as u64) as u32;
        if out.iter().any(|&(ax, ay, _)| (ax, ay) == (x, y)) {
            continue;
        }
        let w = ANCHOR_WEIGHT_MIN + (r >> 48) % (ANCHOR_WEIGHT_MAX - ANCHOR_WEIGHT_MIN + 1);
        out.push((x, y, w));
    }

    out
}

/// Every node derives this particle's starting cell from a PRNG stream
/// distinct from the anchors' one, keyed by its particle id, so particles
/// don't all spawn on the same cell.
fn spawn_pos(cfg: &Config) -> (u32, u32) {
    let mut state = cfg.seed
        ^ ((cfg.particle_id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
        ^ ((cfg.grid_w as u64) << 32)
        ^ (cfg.grid_h as u64)
        ^ 0xABCD_EF01_2345_6789;
    let r = splitmix64_next(&mut state);
    (
        (r % cfg.grid_w as u64) as u32,
        ((r >> 32) % cfg.grid_h as u64) as u32,
    )
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

fn field_key(x: u32, y: u32) -> String {
    format!("mag:field:{x}:{y}")
}

fn field_value(x: u32, y: u32) -> u64 {
    pncounter::value(&field_key(x, y)).unwrap_or(0).max(0) as u64
}

fn peer_pos_key(particle_id: u32) -> String {
    format!("mag:pos:{particle_id}")
}

/// Magnetic mass of a cell: the sum of every anchor's pull, inverse-square
/// in Manhattan distance (fixed-point integer math -- no floats needed for
/// a discrete grid).
fn mass_at(pos: (u32, u32), anchors: &[Anchor]) -> u64 {
    anchors
        .iter()
        .map(|&(ax, ay, w)| {
            let d = manhattan(pos, (ax, ay)) as u64;
            let denom = (d + 1) * (d + 1);
            (w * MASS_SCALE) / denom
        })
        .sum()
}

/// Every other particle's last-known position and mass at that position,
/// read straight from their published `LWW-Register`s.
fn peers(cfg: &Config, anchors: &[Anchor]) -> Vec<(u32, u32, u64)> {
    let mut out = Vec::new();
    for id in 0..cfg.particle_count {
        if id == cfg.particle_id {
            continue;
        }
        if let Ok(Some(bytes)) = lww_register::get(&peer_pos_key(id))
            && bytes.len() == 2
        {
            let pos = (bytes[0] as u32, bytes[1] as u32);
            out.push((pos.0, pos.1, mass_at(pos, anchors)));
        }
    }
    out
}

/// A candidate cell's total magnetic score: its own anchor pull, plus a
/// capped attraction toward any peer that currently outmasses this particle
/// -- the actual MOA mechanic, lighter particles get pulled toward heavier
/// ones, which is what drives clustering/convergence instead of a random
/// walk.
fn candidate_score(
    candidate: (u32, u32),
    anchors: &[Anchor],
    self_mass_now: u64,
    peers: &[(u32, u32, u64)],
) -> u64 {
    let base = mass_at(candidate, anchors).max(1);
    let peer_bonus: u64 = peers
        .iter()
        .filter(|&&(_, _, other_mass)| other_mass > self_mass_now)
        .map(|&(px, py, other_mass)| {
            let capped = other_mass.min(PEER_MASS_CAP);
            let d = manhattan(candidate, (px, py)) as u64;
            let denom = (d + 1) * (d + 1);
            (capped * PEER_SCALE) / denom
        })
        .sum();
    base + peer_bonus
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
    nx_log!("distributed_magnets: start");

    let node_id = match net::node_id() {
        Ok(id) => id,
        Err(NxError::SyncDisabled) => {
            nx_log!("distributed_magnets: sync is disabled on this runtime.");
            nx_log!(
                "distributed_magnets: start the runtime with --listen <addr> to enable CRDT replication."
            );
            return;
        }
        Err(e) => {
            nx_log!("distributed_magnets: failed to read node id: {}", e);
            return;
        }
    };

    let cfg = Config::from_env();
    let anchor_list = anchors(&cfg);

    if cfg.render {
        for (x, y, w) in &anchor_list {
            nx_log!("ANCHOR,{},{},{}", x, y, w);
        }
    }

    let spawn = spawn_pos(&cfg);
    let (had_previous, old_pos) = match db::get(POS_KEY) {
        Ok(Some(bytes)) if bytes.len() == 2 => (true, (bytes[0] as u32, bytes[1] as u32)),
        Ok(_) => (false, spawn),
        Err(e) => {
            nx_log!("distributed_magnets: failed to read position: {}", e);
            return;
        }
    };

    let self_mass_now = mass_at(old_pos, &anchor_list).max(1);
    let peer_list = peers(&cfg, &anchor_list);

    let candidates = neighbors(old_pos, cfg.grid_w, cfg.grid_h);
    let weights: Vec<u64> = candidates
        .iter()
        .map(|&c| candidate_score(c, &anchor_list, self_mass_now, &peer_list))
        .collect();

    let new_pos = match choose_weighted(&candidates, &weights) {
        Ok(next) => next,
        Err(e) => {
            nx_log!("distributed_magnets: failed to draw random step: {}", e);
            return;
        }
    };

    if had_previous && let Err(e) = pncounter::dec(&field_key(old_pos.0, old_pos.1), 1) {
        nx_log!("distributed_magnets: failed to vacate old cell: {}", e);
    }
    if let Err(e) = pncounter::inc(&field_key(new_pos.0, new_pos.1), 1) {
        nx_log!("distributed_magnets: failed to occupy new cell: {}", e);
    }

    if let Err(e) = db::set(POS_KEY, &[new_pos.0 as u8, new_pos.1 as u8]) {
        nx_log!("distributed_magnets: failed to persist position: {}", e);
    }
    if let Err(e) = lww_register::set(
        &peer_pos_key(cfg.particle_id),
        &[new_pos.0 as u8, new_pos.1 as u8],
    ) {
        nx_log!("distributed_magnets: failed to publish position: {}", e);
    }

    // Decentralized "settled" tracking: increment the swarm-wide counter
    // only on the transition into the settle radius, so a particle sitting
    // near an anchor for many ticks doesn't inflate the count every step.
    let nearest_anchor_dist = anchor_list
        .iter()
        .map(|&(ax, ay, _)| manhattan(new_pos, (ax, ay)))
        .min()
        .unwrap_or(u32::MAX);
    let now_settled = nearest_anchor_dist <= cfg.settle_radius;
    let was_settled = matches!(db::get(SETTLED_KEY), Ok(Some(bytes)) if bytes.first() == Some(&1));
    if now_settled
        && !was_settled
        && let Err(e) = gcounter::inc(SETTLED_EVENTS_KEY, 1)
    {
        nx_log!(
            "distributed_magnets: failed to increment settled counter: {}",
            e
        );
    }
    if let Err(e) = db::set(SETTLED_KEY, &[u8::from(now_settled)]) {
        nx_log!("distributed_magnets: failed to persist settled flag: {}", e);
    }

    let settled_events = gcounter::value(SETTLED_EVENTS_KEY).unwrap_or(0);
    nx_log!(
        "distributed_magnets: node={} particle={} pos=({},{}) mass_here={} settled={} settled_events={}",
        node_id,
        cfg.particle_id,
        new_pos.0,
        new_pos.1,
        mass_at(new_pos, &anchor_list),
        now_settled,
        settled_events
    );

    if cfg.render {
        for id in 0..cfg.particle_count {
            let pos = if id == cfg.particle_id {
                Some(new_pos)
            } else {
                lww_register::get(&peer_pos_key(id))
                    .ok()
                    .flatten()
                    .and_then(|bytes| {
                        if bytes.len() == 2 {
                            Some((bytes[0] as u32, bytes[1] as u32))
                        } else {
                            None
                        }
                    })
            };
            if let Some(p) = pos {
                nx_log!(
                    "PARTICLE,{},{},{},{}",
                    id,
                    p.0,
                    p.1,
                    mass_at(p, &anchor_list)
                );
            }
        }
        for y in 0..cfg.grid_h {
            for x in 0..cfg.grid_w {
                nx_log!("FIELD,{},{},{}", x, y, field_value(x, y));
            }
        }
    }

    nx_log!("distributed_magnets: done");
}
