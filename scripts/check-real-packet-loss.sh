#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "--groups" ]]; then
  directory="$2"
  host_namespace="$3"
  binary="${4:-}"
  codec="${5:-json}"
  loss="${6:-0}"
  profile="${7:-probe}"
  parent_namespace="$(readlink /proc/self/ns/net)"
  if [[ "$parent_namespace" == "$host_namespace" || "$(id -u)" != 0 ]] \
    || ! ip -j link show | jq -e 'length == 1 and .[0].ifname == "lo"' >/dev/null; then
    echo "refusing group setup outside a fresh private namespace" >&2
    exit 1
  fi
  holders=()
  cleanup_groups() {
    for holder in "${holders[@]}"; do
      if kill -0 "$holder" 2>/dev/null; then
        nsenter --net="/proc/$holder/ns/net" tc -j -s qdisc show \
          >"$directory/group-$holder-qdisc-at-cleanup.json" || true
        kill "$holder" || true
      fi
      wait "$holder" || true
    done
  }
  trap cleanup_groups EXIT
  for group in 0 1; do
    unshare --net tail -f /dev/null &
    holders+=("$!")
    holder="${holders[$group]}"
    deadline="$((SECONDS + 5))"
    while [[ "$(readlink "/proc/$holder/ns/net")" == "$parent_namespace" ]]; do
      kill -0 "$holder"
      if [[ "$SECONDS" -ge "$deadline" ]]; then
        echo "group namespace did not become ready" >&2
        exit 1
      fi
    done
  done
  ip link add nx-left type veth peer name nx-right
  ip link set nx-left netns "${holders[0]}"
  ip link set nx-right netns "${holders[1]}"
  for group in 0 1; do
    namespace_path="/proc/${holders[$group]}/ns/net"
    interface=nx-left
    if [[ "$group" == 1 ]]; then interface=nx-right; fi
    nsenter --net="$namespace_path" ip link set "$interface" name nx-link
    nsenter --net="$namespace_path" ip addr add "10.200.0.$((group + 1))/30" dev nx-link
    nsenter --net="$namespace_path" ip link set lo up
    nsenter --net="$namespace_path" ip link set nx-link up
    nsenter --net="$namespace_path" tc qdisc add dev nx-link root netem loss random "${loss}%"
    nsenter --net="$namespace_path" ip -j addr show >"$directory/group-$group-addresses.json"
  done
  jq -n --arg first "/proc/${holders[0]}/ns/net" --arg second "/proc/${holders[1]}/ns/net" \
    --arg codec "$codec" --arg directory "$directory" --argjson loss "$loss" \
    '{namespaces: [$first, $second], addresses: ["10.200.0.1", "10.200.0.2"], loss_percent: $loss, codec: $codec, directory: $directory}' \
    >"$directory/groups.json"
  if [[ "$profile" == probe ]]; then
    jq . "$directory/groups.json"
    exit
  fi
  test_suffix=groups_smoke
  case "$profile" in
    cluster) test_suffix=partition_50_nodes ;;
    cluster-load-smoke) test_suffix=groups_load_smoke ;;
    cluster-load) test_suffix=partition_50_nodes_with_load ;;
  esac
  test_name="sync_manager::manager::tests::detector_soak::packet_loss::$test_suffix"
  export NUMAX_PACKET_LOSS_GROUPS
  NUMAX_PACKET_LOSS_GROUPS="$(jq -c . "$directory/groups.json")"
  test_status=0
  timeout --kill-after=5s 1200s "$binary" --exact "$test_name" --ignored --nocapture \
    >"$directory/test.log" 2>&1 || test_status="$?"
  if [[ ! -f "$directory/cluster-report.json" ]]; then
    jq -n --argjson status "$test_status" '{version: 2, passed: false, test_exit_code: $status, error: "cluster did not produce a completion report"}' >"$directory/report.json"
    jq . "$directory/report.json"
    exit 1
  fi
  drops_before="$(jq -s '[.[][] | select(.kind == "netem") | .drops] | add // 0' "$directory"/group-*-before-partition-qdisc.json)"
  drops_rejoin="$(jq -s '[.[][] | select(.kind == "netem") | .drops] | add // 0' "$directory"/group-*-after-rejoin-qdisc.json)"
  drops_final="$(jq -s '[.[][] | select(.kind == "netem") | .drops] | add // 0' "$directory"/group-*-after-recovery-qdisc.json)"
  retrans_before="$(jq -s '[.[].kernel.TcpRetransSegs] | add' "$directory"/group-*-before-partition-tcp.json)"
  retrans_rejoin="$(jq -s '[.[].kernel.TcpRetransSegs] | add' "$directory"/group-*-after-rejoin-tcp.json)"
  retrans_final="$(jq -s '[.[].kernel.TcpRetransSegs] | add' "$directory"/group-*-after-recovery-tcp.json)"
  outside_drops="$((drops_before + drops_final - drops_rejoin))"
  outside_retrans="$((retrans_before + retrans_final - retrans_rejoin))"
  configuration_valid=true
  for phase in initial before-partition after-rejoin after-recovery; do
    if ! jq -se --argjson loss "$loss" \
      'length == 2 and all(.[]; any(.[]; .kind == "netem" and (.options["loss-random"].loss // 0) == ($loss / 100)))' \
      "$directory"/group-*-"$phase"-qdisc.json >/dev/null; then
      configuration_valid=false
    fi
  done
  if ! jq -se 'length == 2 and all(.[]; any(.[]; .kind == "netem" and .options["loss-random"].loss == 1))' \
    "$directory"/group-*-end-partition-qdisc.json >/dev/null; then
    configuration_valid=false
  fi
  passed=false
  if [[ "$test_status" == 0 && "$configuration_valid" == true ]] \
    && grep -q 'test result: ok\. 1 passed; 0 failed;' "$directory/test.log" \
    && { [[ "$loss" == 0 && "$outside_drops" == 0 ]] || [[ "$loss" == 10 && "$outside_drops" -gt 0 && "$outside_retrans" -gt 0 ]]; }; then
    passed=true
  fi
  jq --argjson status "$test_status" --argjson passed "$passed" \
    --argjson drops "$outside_drops" --argjson retrans "$outside_retrans" --argjson configuration "$configuration_valid" \
    '. + {test_exit_code: $status, passed: $passed, packet_drops_outside_partition: $drops,
      qdisc_configuration_verified: $configuration,
      tcp_retransmitted_segments_outside_partition: $retrans,
      loss_model: "netem random loss on both veth egress paths; cross-group TCP including ACKs; intra-group loopback unaffected",
      partition_model: "100% veth egress loss in both directions; excluded from random-loss counters"}' \
    "$directory/cluster-report.json" >"$directory/report.json"
  jq . "$directory/report.json"
  [[ "$passed" == true ]]
  exit
fi

if [[ "${1:-}" == "--namespace" ]]; then
  binary="$2"
  codec="$3"
  loss="$4"
  directory="$5"
  host_namespace="$6"
  namespace="$(readlink /proc/self/ns/net)"
  if [[ "$namespace" == "$host_namespace" || "$(id -u)" != 0 ]]; then
    echo "refusing network configuration outside the private mapped-root namespace" >&2
    exit 1
  fi
  if ! ip -j link show | jq -e 'length == 1 and .[0].ifname == "lo"' >/dev/null; then
    echo "expected a fresh namespace containing only loopback" >&2
    exit 1
  fi
  ip link set lo up
  tc qdisc add dev lo root netem loss random "${loss}%"
  cleanup() {
    tc -j -s qdisc show dev lo >"$directory/qdisc-at-cleanup.json"
    tc qdisc del dev lo root
    tc -j qdisc show dev lo >"$directory/qdisc-after-cleanup.json"
  }
  trap cleanup EXIT
  tc -j -s qdisc show dev lo >"$directory/qdisc-before.json"
  nstat -azj >"$directory/tcp-before.json"
  test_name="sync_manager::manager::tests::topology::real_control_data_partition_${codec}"
  started="$SECONDS"
  test_status=0
  timeout --kill-after=5s 120s "$binary" --exact "$test_name" --nocapture \
    >"$directory/test.log" 2>&1 || test_status="$?"
  elapsed="$((SECONDS - started))"
  tc -j -s qdisc show dev lo >"$directory/qdisc-after.json"
  nstat -azj >"$directory/tcp-after.json"
  drops="$(jq '[.[] | select(.kind == "netem") | .drops] | add // 0' "$directory/qdisc-after.json")"
  retransmissions="$(jq -n --slurpfile before "$directory/tcp-before.json" \
    --slurpfile after "$directory/tcp-after.json" \
    '$after[0].kernel.TcpRetransSegs - $before[0].kernel.TcpRetransSegs')"
  passed=false
  if [[ "$test_status" == 0 ]] \
    && grep -q 'test result: ok\. 1 passed; 0 failed;' "$directory/test.log" \
    && jq -e --argjson loss "$loss" 'any(.[]; .kind == "netem" and (.options["loss-random"].loss // 0) == ($loss / 100))' "$directory/qdisc-after.json" >/dev/null \
    && { [[ "$loss" == 0 && "$drops" == 0 ]] || [[ "$loss" == 10 && "$drops" -gt 0 && "$retransmissions" -gt 0 ]]; }; then
    passed=true
  fi
  jq -n --arg codec "$codec" --arg test "$test_name" --arg namespace "$namespace" \
    --argjson loss "$loss" --argjson status "$test_status" --argjson elapsed "$elapsed" \
    --argjson drops "$drops" --argjson retransmissions "$retransmissions" --argjson passed "$passed" \
    '{version: 1, codec: $codec, test: $test, namespace: $namespace,
      transport: "real loopback TCP in a private Linux network namespace",
      loss_percent: $loss, loss_model: "netem random loss on loopback egress, including TCP ACKs and proxy traffic",
      seed: null, randomness: "kernel netem randomness; not deterministic replay",
      test_exit_code: $status, elapsed_s: $elapsed, qdisc_dropped_packets: $drops,
      tcp_retransmitted_segments: $retransmissions, passed: $passed}' >"$directory/report.json"
  jq . "$directory/report.json"
  [[ "$passed" == true ]]
  exit
fi

profile=two-nodes
if [[ "${1:-}" == --cluster-smoke || "${1:-}" == --cluster \
  || "${1:-}" == --cluster-load-smoke || "${1:-}" == --cluster-load ]]; then
  profile="${1#--}"
  shift
fi
if [[ "$#" != 1 ]]; then
  echo "usage: bash scripts/check-real-packet-loss.sh [--cluster-smoke|--cluster|--cluster-load-smoke|--cluster-load] NEW_ARTIFACT_DIRECTORY" >&2
  exit 2
fi
for tool in cargo ip tc unshare jq nstat timeout readlink tar sha256sum; do
  command -v "$tool" >/dev/null
done
if [[ "$profile" != two-nodes ]]; then command -v nsenter >/dev/null; fi
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
script="$root/scripts/check-real-packet-loss.sh"
mkdir -- "$1"
directory="$(cd -- "$1" && pwd)"
host_namespace="$(readlink /proc/self/ns/net)"
tc -j qdisc show >"$directory/host-qdisc-before.json"
unshare --user --map-root-user --net ip -j link show >"$directory/namespace-preflight.json"
cd -- "$root"
git rev-parse HEAD >"$directory/base-commit.txt"
git diff >"$directory/working-tree.patch"
tar -cf "$directory/runner-fixture-sources.tar" scripts/check-real-packet-loss.sh \
  crates/nx-core/src/sync_manager/tests/packet_loss.rs
sha256sum "$directory/runner-fixture-sources.tar" >"$directory/source-archive.sha256"
uname -srmo >"$directory/platform.txt"
tc -V >"$directory/tc-version.txt"
cargo test --release --locked -p nx-core --lib --no-run --message-format=json \
  >"$directory/build.jsonl" 2>"$directory/build.log"
binary="$(jq -sr '[.[] | select(.reason == "compiler-artifact" and .target.name == "nx_core" and .profile.test and .executable != null) | .executable] | unique | if length == 1 then .[0] else error("expected one nx-core test binary") end' "$directory/build.jsonl")"
sha256sum "$binary" >"$directory/test-binary.sha256"
overall_status=0
for loss in 0 10; do
  for codec in json bincode; do
    case_directory="$directory/${codec}-loss-${loss}"
    mkdir -- "$case_directory"
    if [[ "$profile" == two-nodes ]]; then
      if ! unshare --user --map-root-user --net bash "$script" --namespace \
        "$binary" "$codec" "$loss" "$case_directory" "$host_namespace"; then
        overall_status=1
      fi
      if [[ ! -f "$case_directory/qdisc-after-cleanup.json" ]] \
        || ! jq -e 'all(.[]; .kind != "netem")' "$case_directory/qdisc-after-cleanup.json" >/dev/null; then
        echo "netem cleanup was not verified: $case_directory" >&2
        overall_status=1
      fi
    else
      if ! unshare --user --map-root-user --net bash "$script" --groups \
        "$case_directory" "$host_namespace" "$binary" "$codec" "$loss" "$profile"; then
        overall_status=1
      fi
      if jq -e '.namespaces[]' "$case_directory/groups.json" >/dev/null; then
        while IFS= read -r namespace_path; do
          if [[ -e "$namespace_path" ]]; then
            echo "group namespace holder remains after cleanup: $namespace_path" >&2
            overall_status=1
          fi
        done < <(jq -r '.namespaces[]' "$case_directory/groups.json")
      else
        overall_status=1
      fi
    fi
  done
done
tc -j qdisc show >"$directory/host-qdisc-after.json"
if ! cmp -s "$directory/host-qdisc-before.json" "$directory/host-qdisc-after.json"; then
  echo "host qdisc configuration changed during the campaign" >&2
  overall_status=1
fi
jq -s --argjson status "$overall_status" \
  '{version: 1, cases: ., runner_exit_code: $status, passed: ($status == 0 and length == 4 and all(.passed))}' \
  "$directory"/*/report.json >"$directory/campaign.json"
exit "$overall_status"