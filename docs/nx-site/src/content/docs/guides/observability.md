---
title: Observability
description: Run Prometheus and Grafana against Numax runtime metrics.
---

Numax can expose a small HTTP observability endpoint with:

- `/health`
- `/ready`
- `/metrics`

The metrics endpoint uses Prometheus text format and is enough to run a basic
Prometheus + Grafana stack without adding anything to the runtime.

## Start Numax With Metrics

Run any module with the observability endpoint enabled:

```bash
nx run target/wasm32-unknown-unknown/release/distributed_counter.wasm \
  --observability-listen 127.0.0.1:9100 \
  --datastore-path /tmp/numax-observability-demo \
  --settle-for 5s
```

Check the endpoint directly:

```bash
curl http://127.0.0.1:9100/health
curl http://127.0.0.1:9100/ready
curl http://127.0.0.1:9100/metrics
```

You can also run the lightweight verification script:

```bash
docs/scripts/check-observability.sh http://127.0.0.1:9100
```

## Run Prometheus And Grafana

Start the ready-made stack:

```bash
docker compose -f docs/compose/observability.yml up
```

Open:

- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`

Grafana credentials:

- user: `admin`
- password: `admin`

The Numax dashboard is provisioned automatically from
`docs/dashboards/numax.json`.

## Metrics

The dashboard uses only metrics currently emitted by Numax:

| Metric                               | Type    | Meaning                                    |
| ------------------------------------ | ------- | ------------------------------------------ |
| `numax_ops_total`                    | counter | operations processed by the runtime        |
| `numax_peers_connected`              | gauge   | currently connected peers                  |
| `numax_sync_latency_ms`              | gauge   | last recorded sync latency in milliseconds |
| `numax_sync_errors_total`            | counter | sync-related errors                        |
| `numax_observability_requests_total` | counter | observability endpoint requests            |
| `numax_observability_errors_total`   | counter | observability endpoint errors              |
| `numax_peer_connects_total`          | counter | peer connection events                     |
| `numax_peer_disconnects_total`       | counter | peer disconnection events                  |
| `numax_broadcast_batches_total`      | counter | broadcast batches sent                     |
| `numax_broadcast_ops_total`          | counter | operations broadcast to peers              |
| `numax_store_keys`                   | gauge   | key count in the local store               |
| `numax_store_bytes`                  | gauge   | approximate store payload bytes            |
| `numax_wasm_invocations_total`       | counter | module invocations by outcome              |
| `numax_wasm_module_cache_lookups_total` | counter | compiled-module cache hits and misses    |
| `numax_wasm_*_duration_seconds_total` | counter | compilation, instantiation and execution time |
| `numax_wasm_linear_memory_*_bytes`   | gauge/counter | current, peak and cumulative growth bytes |

WASM metrics use the module's BLAKE3 digest as the `module` label. 
This keeps the identity stable without exposing local paths. 

Numax bounds the registry to 128 labels and aggregates additional modules under `module="overflow"`.

Execution duration is wall-clock time and can include asynchronous host-call waits; it is not raw CPU time. Linear-memory metrics describe the guest memory exported as `memory`, not individual `malloc` and `free` operations inside the guest allocator.

## Useful PromQL

Operations throughput:

```txt
rate(numax_ops_total[1m])
```

Broadcast throughput:

```txt
rate(numax_broadcast_ops_total[1m])
```

Recent sync errors:

```txt
increase(numax_sync_errors_total[5m])
```

Connected peers:

```txt
numax_peers_connected
```

Store growth:

```txt
numax_store_bytes
```

## Alert Examples

Target down:

```txt
up{job="numax"} == 0
```

No connected peers:

```txt
numax_peers_connected == 0
```

Sync errors observed:

```txt
increase(numax_sync_errors_total[5m]) > 0
```

Observability endpoint errors observed:

```txt
increase(numax_observability_errors_total[5m]) > 0
```

Store size above 1 GiB:

```txt
numax_store_bytes > 1073741824
```

## CPU Flamegraphs On Ubuntu/Linux

The `three_node_sync_load` benchmark can generate an opt-in CPU flamegraph for
the load phase. Profiling starts after the three-node cluster is connected and
stops before convergence and shutdown, keeping setup noise out of the report.

```bash
cargo bench --profile profiling -p nx-core \
  --features cpu-profiling \
  --bench three_node_sync_load -- \
  --duration-secs 10 \
  --target-ops-sec-per-node 1000 \
  --settle-secs 10 \
  --cpu-profile reports/profiling/three-node-sync-load.svg
```

The `cpu-profiling` feature and the `profiling` Cargo profile do not affect the default Numax runtime or the unprofiled regression benchmark. CI uses Ubuntu as the canonical profiling environment and uploads the SVG as `cpu-flamegraph-ubuntu`; cross-platform profiling remains a post-`v0.2.0` candidate.

## Heap Profiles On Ubuntu/Linux

The same benchmark can generate an opt-in DHAT profile for allocations made during the load phase. Run it separately from CPU profiling so that the DHAT allocator does not distort CPU samples:

```bash
cargo bench --profile profiling -p nx-core \
  --features heap-profiling \
  --bench three_node_sync_load -- \
  --duration-secs 5 \
  --target-ops-sec-per-node 250 \
  --settle-secs 5 \
  --heap-profile reports/profiling/three-node-sync-load-heap.json
```

Open the resulting JSON in the DHAT viewer to inspect allocation sites, peak
memory, and memory still live when the load phase ends. CI validates the JSON
and uploads it as `heap-profile-ubuntu`; it is a diagnostic artifact, not a
regression threshold. The feature is disabled in normal builds and does not
change the default allocator or the unprofiled regression benchmark.
