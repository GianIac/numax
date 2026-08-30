#!/usr/bin/env bash
# Drives the distributed Magnetic Optimization Algorithm swarm end to end:
#   1. reads swarm.config.toml
#   2. launches `particles` background nx nodes, fully meshed as peers, each
#      with a stable NUMAX_PARTICLE_ID so they can find each other's
#      published LWW-Register position
#   3. runs `ticks_per_particle` sequential `nx run` invocations per node
#      (each invocation is one particle step; state persists in ./data-N)
#   4. on the very last tick of node 0, asks for a full topology dump
#      (NUMAX_RENDER=1) used by render_topology.py
#   5. prints the converged mag:settled_events GCounter from every node
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

CONFIG_FILE="${1:-swarm.config.toml}"
# `nx` is a workspace member built from the repo root (`cargo build --release
# -p nx-cli`), not from this example's own (separate) Cargo workspace.
NX_BIN="${NX_BIN:-$SCRIPT_DIR/../../target/release/nx}"
WASM="$SCRIPT_DIR/target/wasm32-unknown-unknown/release/distributed_magnets.wasm"
LOG_DIR="$SCRIPT_DIR/logs"
DATA_PREFIX="$SCRIPT_DIR/data"

config_get() {
    # Reads `key = value` (ints only, no quoting) out of a flat TOML file.
    local key="$1" default="$2"
    local line
    line="$(grep -E "^[[:space:]]*${key}[[:space:]]*=" "$CONFIG_FILE" | tail -n1 || true)"
    if [[ -z "$line" ]]; then
        echo "$default"
        return
    fi
    echo "$line" | sed -E 's/^[^=]*=[[:space:]]*//; s/[[:space:]]*(#.*)?$//'
}

GRID_W="$(config_get grid_width 14)"
GRID_H="$(config_get grid_height 14)"
ANCHOR_COUNT="$(config_get anchor_count 3)"
SEED="$(config_get seed 4242)"
SETTLE_RADIUS="$(config_get settle_radius 1)"
TICKS_PER_PARTICLE="$(config_get ticks_per_particle 50)"
PARTICLES="$(config_get particles 5)"
BASE_PORT="$(config_get base_port 9100)"

echo "== distributed magnetic optimization =="
echo "grid=${GRID_W}x${GRID_H} anchors=${ANCHOR_COUNT} seed=${SEED} settle_radius=${SETTLE_RADIUS}"
echo "particles=${PARTICLES} ticks_per_particle=${TICKS_PER_PARTICLE} base_port=${BASE_PORT}"

if [[ ! -x "$NX_BIN" ]]; then
    echo "error: $NX_BIN not found or not executable; build it first:" >&2
    echo "  (from the repo root) cargo build --release -p nx-cli" >&2
    exit 1
fi
if [[ ! -f "$WASM" ]]; then
    echo "error: $WASM not found; run:" >&2
    echo "  cargo build --release --target wasm32-unknown-unknown" >&2
    exit 1
fi

rm -rf "$LOG_DIR" "${DATA_PREFIX}"-*
mkdir -p "$LOG_DIR"

export NUMAX_GRID_W="$GRID_W"
export NUMAX_GRID_H="$GRID_H"
export NUMAX_ANCHOR_COUNT="$ANCHOR_COUNT"
export NUMAX_SEED="$SEED"
export NUMAX_SETTLE_RADIUS="$SETTLE_RADIUS"
export NUMAX_PARTICLE_COUNT="$PARTICLES"

pids=()

for ((i = 0; i < PARTICLES; i++)); do
    port=$((BASE_PORT + i))
    peers=()
    for ((j = 0; j < PARTICLES; j++)); do
        if [[ "$j" -ne "$i" ]]; then
            peers+=(--peer "127.0.0.1:$((BASE_PORT + j))")
        fi
    done

    (
        export NUMAX_PARTICLE_ID="$i"
        # Stagger start so listen windows are more likely to overlap.
        sleep "0.$(printf '%02d' $((i * 15)))"
        for ((t = 1; t <= TICKS_PER_PARTICLE; t++)); do
            render_flag=(--print-gcounter mag:settled_events)
            if [[ "$i" -eq 0 && "$t" -eq "$TICKS_PER_PARTICLE" ]]; then
                export NUMAX_RENDER=1
            else
                export NUMAX_RENDER=0
            fi
            "$NX_BIN" run "$WASM" \
                --listen "127.0.0.1:${port}" \
                ${peers[@]+"${peers[@]}"} \
                --datastore-path "${DATA_PREFIX}-${i}" \
                --wait-before-run 300ms \
                --settle-for 800ms \
                "${render_flag[@]}" \
                >>"$LOG_DIR/particle-${i}.log" 2>&1 || {
                    echo "error: particle ${i} failed at tick ${t}; see $LOG_DIR/particle-${i}.log" >&2
                    exit 1
                }
        done
    ) &
    pids+=($!)
done

worker_status=0
for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
        worker_status=1
    fi
done

if [[ "$worker_status" -ne 0 ]]; then
    exit "$worker_status"
fi

echo
echo "== final settled_events per node =="
for ((i = 0; i < PARTICLES; i++)); do
    val="$(grep -E 'mag:settled_events = ' "$LOG_DIR/particle-${i}.log" | tail -n1 || echo 'n/a')"
    echo "particle-${i}: ${val}"
done

echo
echo "logs: $LOG_DIR/particle-*.log"
echo "to render the converged topology:"
echo "  python3 render_topology.py $LOG_DIR/particle-0.log --grid-width $GRID_W --grid-height $GRID_H --out topology.html"
