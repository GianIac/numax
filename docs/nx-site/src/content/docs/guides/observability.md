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

The runtime exposes the following metrics:

| Metric                               | Type    | Meaning                                    |
| ------------------------------------ | ------- | ------------------------------------------ |
| `numax_ops_total`                    | counter | operations processed by the runtime        |
| `numax_peers_connected`              | gauge   | currently connected peers                  |
| `numax_sync_latency_ms`              | gauge   | last local gossip batch send duration in milliseconds; not RTT or remote durability |
| `numax_gossip_rate_limited_total` | counter | fast gossip attempts deferred to durable repair |
| `numax_gossip_admitted_bytes_total` | counter | framed application bytes admitted for send attempts, including failed/partial writes |
| `numax_gossip_rate_bytes_per_second` | gauge | last per-peer pacing decision; not aggregate throughput; zero before any decision |
| `numax_gossip_control_rtt_seconds` | gauge | last smoothed direct-control round trip used by pacing, excluding connect/TLS setup; omitted when unknown |
| `numax_sync_errors_total`            | counter | sync-related errors                        |
| `numax_observability_requests_total` | counter | observability endpoint requests            |
| `numax_observability_errors_total`   | counter | observability endpoint errors              |
| `numax_peer_connects_total`          | counter | peer connection events                     |
| `numax_peer_disconnects_total`       | counter | peer disconnection events                  |
| `numax_broadcast_batches_total`      | counter | broadcast batches sent                     |
| `numax_broadcast_ops_total`          | counter | operations broadcast to peers              |
| `numax_remote_ops_received_total`    | counter | remote operations received before deduplication |
| `numax_remote_ops_applied_total`     | counter | remote operations successfully applied     |
| `numax_remote_ops_duplicate_total`   | counter | duplicate remote operations skipped        |
| `numax_remote_op_batches_total`      | counter | non-empty remote operation batches processed |
| `numax_remote_op_apply_errors_total` | counter | remote batches that failed during apply or persistence |
| `numax_remote_op_batch_apply_duration_seconds` | histogram | end-to-end remote batch apply latency |
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

## Peer management metrics

Peer metric labels use fixed enumerations. Endpoints and arbitrary node,
operation or CRDT-key identities are available through authenticated peer
views rather than metric labels.

| Metric | Type / unit | Meaning and bounded labels |
| --- | --- | --- |
| `numax_membership_probe_attempts_total{route}` | counter / RPCs | Actual dispatched probe requests, `direct` or `indirect`; an indirect dispatch goes to a helper |
| `numax_membership_probe_results_total{outcome}` | counter / completed logical probes | `success`, `failed`, `local_degradation`; local degradation is not proof of peer failure |
| `numax_membership_transitions_total{transition}` | counter / transitions | `suspect`, `refuted`, `declared_dead` (local detector after terminal persistence), `learned_dead` (other terminal publication), `left`; duplicate snapshot publication is not counted |
| `numax_queue_items{queue}` | gauge / items | Volatile queues `local`, `forward`, `candidates`, `control_ingress`, `membership_writes` |
| `numax_queue_capacity_items{queue}` | gauge / slots | Corresponding configured capacity |
| `numax_queue_bytes{queue}` | gauge / bytes | Occupied byte permits in `local` / `forward` delivery queues |
| `numax_queue_capacity_bytes{queue}` | gauge / bytes | Corresponding byte budget, not wire bytes or retained durable history |
| `numax_queue_rejections_total{queue,reason}` | counter / rejected enqueues | `local` / `forward`, reasons `slots`, `bytes`, `closed`; dropping these attempts does not delete accepted durable operations |
| `numax_peer_dial_attempts_total{outcome}` | counter / attempts | Actual data dials: `connected`, `failed`, `fatal`, `rate_limited`; already-connected and local full-slot skips are excluded |
| `numax_peer_retries_scheduled_total{reason}` | counter / retry schedules | Backoff actually scheduled for `failure` / `rate_limited`; not every loop turn or successful connection |
| `numax_gossip_fanout{path,stage}` | histogram / recipients | `local` / `forward`, `selected` / `attempted`; attempted counts non-deferred send calls, including errors, not remote receipts or ACKs |
| `numax_recovery_origins{state}` | gauge / known origins | `pending`, `baseline_unconfirmed`, `source_rejected`, `no_known_gap`; these categories may overlap |
| `numax_recovery_gap_positions` | gauge / operation positions | Sum of known high minus contiguous coverage minus sparse receipts, excluding unknown origins and baseline contents |
| `numax_recovery_events_total{event}` | counter / events | Correlated `history_unavailable`, sent `baseline_requested`, successfully imported `baseline_accepted`, current `baseline_rejected` from a source, or history-handler `error` |

Counters accumulate for the process lifetime. Queue item/byte gauges cover
entries until dequeue or drop, excluding batches in progress and empty reserved
slots. Control, candidate and persistence queues are sampled at owner turns;
the active persistence worker is outside its queue.

Recovery gauges refresh at most once per second from event/repair loops and can
lag by the repair cadence and storage service time. Zero-valued series before a
component starts do not establish an active subsystem. Recovery states can
overlap, and zero known gaps does not prove global convergence.

Fanout buckets are `0, 1, 2, 4, 8, 16, 32, +Inf`, with `_sum` and `_count`.
Pacing admission and attempted fanout describe local delivery work, not remote
receipt or durability.

| Observation | Next check |
| --- | --- |
| Dial failures grow without connections | Inspect `?view=candidates`, endpoint health, handshake and TLS errors. |
| Queue rejections or pacing deferrals grow | Compare occupied capacity and writer/RTT metrics; inspect whether anti-entropy is repairing the backlog. |
| Known gap positions remain nonzero | Inspect `?view=recovery` for missing ranges, in-flight requests and baseline requirements. |
| Recovery reports `source_rejected` | Read the source-specific failure; another source may still supply the required history or baseline. |

The [peer observation views](../management-api/#independent-observation-views)
provide detailed state. Gauges and views are independent local observations.

## Useful PromQL

Operations throughput:

```txt
rate(numax_ops_total[1m])
```

Broadcast throughput:

```txt
rate(numax_broadcast_ops_total[1m])
```

Remote apply p95 latency:

```txt
histogram_quantile(0.95, rate(numax_remote_op_batch_apply_duration_seconds_bucket[5m]))
```

Remote duplicate ratio:

```txt
rate(numax_remote_ops_duplicate_total[5m])
/
rate(numax_remote_ops_received_total[5m])
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
