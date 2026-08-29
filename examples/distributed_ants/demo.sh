#!/usr/bin/env bash
# Drives the distributed ant-colony swarm end to end:
#   1. reads swarm.config.toml
#   2. launches `ants` background nx nodes, fully meshed as peers
#   3. runs `ticks_per_ant` sequential `nx run` invocations per node
#      (each invocation is one ant step; state persists in ./data-N)
#   4. on the very last tick of node 0, asks for a full grid dump
#      (NUMAX_RENDER=1) used by render_paths.py
#   5. prints the converged ant:trips_completed GCounter from every node
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

CONFIG_FILE="${1:-swarm.config.toml}"
# `nx` is a workspace member built from the repo root (`cargo build --release
# -p nx-cli`), not from this example's own (separate) Cargo workspace.
NX_BIN="${NX_BIN:-$SCRIPT_DIR/../../target/release/nx}"
WASM="$SCRIPT_DIR/target/wasm32-unknown-unknown/release/distributed_ants.wasm"
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

GRID_W="$(config_get grid_width 12)"
GRID_H="$(config_get grid_height 12)"
HOTSPOT_COUNT="$(config_get hotspot_count 3)"
SEED="$(config_get seed 1337)"
NEST_X="$(config_get nest_x 0)"
NEST_Y="$(config_get nest_y 0)"
TICKS_PER_ANT="$(config_get ticks_per_ant 40)"
ANTS="$(config_get ants 4)"
BASE_PORT="$(config_get base_port 9000)"

echo "== distributed ant colony =="
echo "grid=${GRID_W}x${GRID_H} hotspots=${HOTSPOT_COUNT} seed=${SEED} nest=(${NEST_X},${NEST_Y})"
echo "ants=${ANTS} ticks_per_ant=${TICKS_PER_ANT} base_port=${BASE_PORT}"

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
export NUMAX_HOTSPOT_COUNT="$HOTSPOT_COUNT"
export NUMAX_SEED="$SEED"
export NUMAX_NEST_X="$NEST_X"
export NUMAX_NEST_Y="$NEST_Y"

pids=()

for ((i = 0; i < ANTS; i++)); do
    port=$((BASE_PORT + i))
    peers=()
    for ((j = 0; j < ANTS; j++)); do
        if [[ "$j" -ne "$i" ]]; then
            peers+=(--peer "127.0.0.1:$((BASE_PORT + j))")
        fi
    done

    (
        # Stagger start so listen windows are more likely to overlap.
        sleep "0.$(printf '%02d' $((i * 15)))"
        for ((t = 1; t <= TICKS_PER_ANT; t++)); do
            render_flag=(--print-gcounter ant:trips_completed)
            if [[ "$i" -eq 0 && "$t" -eq "$TICKS_PER_ANT" ]]; then
                render_flag+=(--print-pncounter "ant:phero:${NEST_X}:${NEST_Y}")
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
                >>"$LOG_DIR/ant-${i}.log" 2>&1 || {
                    echo "error: ant ${i} failed at tick ${t}; see $LOG_DIR/ant-${i}.log" >&2
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
echo "== final trips_completed per node =="
for ((i = 0; i < ANTS; i++)); do
    val="$(grep -E 'ant:trips_completed = ' "$LOG_DIR/ant-${i}.log" | tail -n1 || echo 'n/a')"
    echo "ant-${i}: ${val}"
done

echo
echo "logs: $LOG_DIR/ant-*.log"
echo "to render the pheromone map + shortest paths:"
echo "  python3 render_paths.py $LOG_DIR/ant-0.log --grid-width $GRID_W --grid-height $GRID_H --out swarm.html"
