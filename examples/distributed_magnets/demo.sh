#!/usr/bin/env bash
# Drives the distributed Magnetic Optimization Algorithm swarm end to end:
#   1. reads swarm.config.toml
#   2. launches `particles` background nx nodes, fully meshed as peers, each
#      with a stable NUMAX_PARTICLE_ID so they can find each other's
#      published LWW-Register position
#   3. runs `ticks_per_particle` sequential `nx run` invocations per node
#      (each invocation is one particle step; state persists in ./data-N)
#   4. node 0 acts as the scenario controller: phase 1 runs under the plain
#      seeded weights, then at each phaseN_at_tick (N = 2..phase_count) it
#      applies every boost in phaseN_boosts to the relevant anchors' live
#      weights (NUMAX_BOOST). A topology snapshot (NUMAX_RENDER=1 +
#      NUMAX_SNAPSHOT_LABEL) is captured on the tick immediately before each
#      boost lands, plus a final one at the end -- one per phase, for
#      render_topology.py to rotate through
#   5. prints the converged mag:settled_events GCounter from every node
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

CONFIG_FILE="${1:-swarm.config.toml}"
if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "error: config file not found: $CONFIG_FILE" >&2
    exit 1
fi

# `nx` is a workspace member built from the repo root (`cargo build --release
# -p nx-cli`), not from this example's own (separate) Cargo workspace.
NX_BIN="${NX_BIN:-$SCRIPT_DIR/../../target/release/nx}"
WASM="$SCRIPT_DIR/target/wasm32-unknown-unknown/release/distributed_magnets.wasm"
LOG_DIR="$SCRIPT_DIR/logs"
DATA_PREFIX="$SCRIPT_DIR/data"

config_get() {
    # Reads `key = value` out of a flat TOML file (ints unquoted, strings in
    # double quotes -- no nesting, no escaping).
    local key="$1" default="$2"
    local line
    line="$(grep -E "^[[:space:]]*${key}[[:space:]]*=" "$CONFIG_FILE" | tail -n1 || true)"
    if [[ -z "$line" ]]; then
        echo "$default"
        return
    fi
    echo "$line" | sed -E 's/^[^=]*=[[:space:]]*//; s/[[:space:]]*(#.*)?$//; s/^"(.*)"$/\1/'
}

require_uint() {
    local name="$1" value="$2"
    if ! [[ "$value" =~ ^[0-9]+$ ]]; then
        echo "error: $name must be a non-negative integer, got: '$value'" >&2
        exit 1
    fi
}

validate_boosts() {
    # "idx:delta,idx:delta,..." -- each idx must be < anchor_count and each
    # delta a plain (optionally negative) integer.
    local name="$1" value="$2" anchor_count="$3"
    [[ -z "$value" ]] && return
    local entry idx delta
    IFS=',' read -ra entries <<<"$value"
    for entry in "${entries[@]}"; do
        entry="$(echo "$entry" | xargs)"
        [[ -z "$entry" ]] && continue
        if ! [[ "$entry" =~ ^([0-9]+):(-?[0-9]+)$ ]]; then
            echo "error: $name entry '$entry' must look like 'anchor_index:delta'" >&2
            exit 1
        fi
        idx="${BASH_REMATCH[1]}"
        delta="${BASH_REMATCH[2]}"
        if (( idx >= anchor_count )); then
            echo "error: $name anchor index $idx must be < anchor_count ($anchor_count)" >&2
            exit 1
        fi
        if (( delta == 0 )); then
            echo "error: $name entry '$entry' has a zero delta, which does nothing" >&2
            exit 1
        fi
    done
}

GRID_W="$(config_get grid_width 16)"
GRID_H="$(config_get grid_height 16)"
ANCHOR_COUNT="$(config_get anchor_count 3)"
SEED="$(config_get seed 7)"
SETTLE_RADIUS="$(config_get settle_radius 1)"
TICKS_PER_PARTICLE="$(config_get ticks_per_particle 120)"
PARTICLES="$(config_get particles 14)"
BASE_PORT="$(config_get base_port 9100)"
PHASE_COUNT="$(config_get phase_count 1)"
POLARITY_ENABLED="$(config_get polarity_enabled true)"

for pair in "grid_width $GRID_W" "grid_height $GRID_H" "anchor_count $ANCHOR_COUNT" \
    "settle_radius $SETTLE_RADIUS" "ticks_per_particle $TICKS_PER_PARTICLE" \
    "particles $PARTICLES" "base_port $BASE_PORT" "phase_count $PHASE_COUNT"; do
    require_uint ${pair}
done
if ! [[ "$SEED" =~ ^-?[0-9]+$ ]]; then
    echo "error: seed must be an integer, got: '$SEED'" >&2
    exit 1
fi
if [[ "$POLARITY_ENABLED" != "true" && "$POLARITY_ENABLED" != "false" ]]; then
    echo "error: polarity_enabled must be 'true' or 'false', got: '$POLARITY_ENABLED'" >&2
    exit 1
fi
if (( PARTICLES < 1 )); then
    echo "error: particles must be >= 1, got: $PARTICLES" >&2
    exit 1
fi
if (( ANCHOR_COUNT < 1 )); then
    echo "error: anchor_count must be >= 1, got: $ANCHOR_COUNT" >&2
    exit 1
fi
if (( TICKS_PER_PARTICLE < 1 )); then
    echo "error: ticks_per_particle must be >= 1, got: $TICKS_PER_PARTICLE" >&2
    exit 1
fi
if (( PHASE_COUNT < 1 )); then
    echo "error: phase_count must be >= 1, got: $PHASE_COUNT" >&2
    exit 1
fi

# Read every phase-2..phase_count entry into parallel arrays. Bash 3.2 (the
# macOS default) has neither namerefs nor associative arrays, so plain
# indexed arrays keyed by "phase number - 2" are the portable option.
PHASE_AT_TICK=()
PHASE_BOOSTS=()
prev_tick=1
for ((p = 2; p <= PHASE_COUNT; p++)); do
    at_tick="$(config_get "phase${p}_at_tick" 0)"
    boosts="$(config_get "phase${p}_boosts" "")"
    require_uint "phase${p}_at_tick" "$at_tick"
    if (( at_tick == 0 )); then
        echo "error: phase${p}_at_tick is required when phase_count=$PHASE_COUNT" >&2
        exit 1
    fi
    if [[ -z "$boosts" ]]; then
        echo "error: phase${p}_boosts is required when phase_count=$PHASE_COUNT" >&2
        exit 1
    fi
    validate_boosts "phase${p}_boosts" "$boosts" "$ANCHOR_COUNT"
    if (( at_tick <= prev_tick || at_tick > TICKS_PER_PARTICLE )); then
        echo "error: phase${p}_at_tick ($at_tick) must be > the previous phase's tick ($prev_tick) and <= ticks_per_particle ($TICKS_PER_PARTICLE)" >&2
        exit 1
    fi
    PHASE_AT_TICK+=("$at_tick")
    PHASE_BOOSTS+=("$boosts")
    prev_tick="$at_tick"
done

echo "== distributed magnetic optimization =="
echo "grid=${GRID_W}x${GRID_H} anchors=${ANCHOR_COUNT} seed=${SEED} settle_radius=${SETTLE_RADIUS}"
echo "particles=${PARTICLES} ticks_per_particle=${TICKS_PER_PARTICLE} base_port=${BASE_PORT}"
echo "polarity_enabled=${POLARITY_ENABLED}"
echo "phases=${PHASE_COUNT}"
for ((p = 2; p <= PHASE_COUNT; p++)); do
    echo "  phase ${p} @ tick ${PHASE_AT_TICK[$((p - 2))]}: ${PHASE_BOOSTS[$((p - 2))]}"
done

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
if [[ "$POLARITY_ENABLED" == "true" ]]; then
    export NUMAX_POLARITY=1
else
    export NUMAX_POLARITY=0
fi

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
        sleep "0.$(printf '%03d' $((i * 15)))"
        for ((t = 1; t <= TICKS_PER_PARTICLE; t++)); do
            render_flag=(--print-gcounter mag:settled_events)
            unset NUMAX_RENDER NUMAX_SNAPSHOT_LABEL NUMAX_BOOST || true

            if [[ "$i" -eq 0 ]]; then
                # Snapshot the tick right before each upcoming boost lands
                # (phase p's snapshot shows convergence under phase p's own
                # weights, just before phase p+1's boost changes them), plus
                # a final snapshot on the very last tick.
                for ((p = 2; p <= PHASE_COUNT; p++)); do
                    if [[ "$t" -eq $((PHASE_AT_TICK[$((p - 2))] - 1)) ]]; then
                        if [[ "$p" -eq 2 ]]; then
                            export NUMAX_RENDER=1 NUMAX_SNAPSHOT_LABEL="phase 1: initial weights"
                        else
                            export NUMAX_RENDER=1
                            export NUMAX_SNAPSHOT_LABEL="phase $((p - 1)): after ${PHASE_BOOSTS[$((p - 3))]}"
                        fi
                    fi
                done
                if [[ "$t" -eq "$TICKS_PER_PARTICLE" ]]; then
                    export NUMAX_RENDER=1
                    if [[ "$PHASE_COUNT" -ge 2 ]]; then
                        export NUMAX_SNAPSHOT_LABEL="phase ${PHASE_COUNT}: after ${PHASE_BOOSTS[$((PHASE_COUNT - 2))]}"
                    else
                        export NUMAX_SNAPSHOT_LABEL="phase 1: initial weights"
                    fi
                fi

                for ((p = 2; p <= PHASE_COUNT; p++)); do
                    if [[ "$t" -eq "${PHASE_AT_TICK[$((p - 2))]}" ]]; then
                        export NUMAX_BOOST="${PHASE_BOOSTS[$((p - 2))]}"
                    fi
                done
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
echo "to render the adaptive topology story (one tab per phase):"
echo "  python3 render_topology.py $LOG_DIR/particle-0.log --grid-width $GRID_W --grid-height $GRID_H --settle-radius $SETTLE_RADIUS --out topology.html"
