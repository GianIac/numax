#!/usr/bin/env bash
set -euo pipefail

base_url="${1:-http://127.0.0.1:9100}"

fetch() {
  local path="$1"
  curl --fail --silent --show-error "${base_url}${path}"
}

health="$(fetch /health)"
if [[ "$health" != "ok" ]]; then
  echo "unexpected /health response: ${health}" >&2
  exit 1
fi

ready="$(curl --silent --show-error "${base_url}/ready" || true)"
if [[ "$ready" != "ready" && "$ready" != "not ready" ]]; then
  echo "unexpected /ready response: ${ready}" >&2
  exit 1
fi

metrics="$(fetch /metrics)"
required_metrics=(
  numax_ops_total
  numax_peers_connected
  numax_sync_latency_ms
  numax_sync_errors_total
  numax_observability_requests_total
  numax_observability_errors_total
  numax_peer_connects_total
  numax_peer_disconnects_total
  numax_broadcast_batches_total
  numax_broadcast_ops_total
  numax_store_keys
  numax_store_bytes
)

for metric in "${required_metrics[@]}"; do
  if ! grep -q "^${metric} " <<<"$metrics"; then
    echo "missing metric: ${metric}" >&2
    exit 1
  fi
done

echo "Numax observability endpoint is healthy: ${base_url}"
