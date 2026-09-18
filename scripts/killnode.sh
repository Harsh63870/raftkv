#!/usr/bin/env bash
# Stop a node and prove the remaining two elect a leader and keep serving.
# With no argument it kills the current leader, which is the interesting case.
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/lib.sh

require_cluster

before=$(wait_leader 30) || { echo "no leader to start with" >&2; exit 1; }
read -r leader_id _ <<<"$before"
victim=${1:-$leader_id}

survivors=()
for node in "${NODES[@]}"; do
  [ "$node" = "$victim" ] || survivors+=("$node")
done
if [ ${#survivors[@]} -eq ${#NODES[@]} ]; then
  echo "unknown node: $victim (expected one of: ${NODES[*]})" >&2
  exit 2
fi

echo "== before =="
echo "leader:    $leader_id"
ctl "${survivors[0]}" set demo/before "written-while-$leader_id-led" >/dev/null
echo "wrote:     demo/before"

echo
echo "== stopping $victim =="
start=$(date +%s)
dc stop -t 2 "$victim" >/dev/null

after=$(wait_leader 30 "${survivors[@]}") || {
  echo "FAIL: no leader elected after killing $victim" >&2
  exit 1
}
read -r new_leader _ <<<"$after"
echo "new leader: $new_leader (after $(( $(date +%s) - start ))s)"

echo
echo "== after =="
for node in "${survivors[@]}"; do
  got=$(ctl "$node" get demo/before)
  echo "read  $node: demo/before = $got"
  [ "$got" = "written-while-$leader_id-led" ] || { echo "FAIL: stale or lost value on $node" >&2; exit 1; }
done

# Write through a follower to exercise forwarding to the new leader.
for node in "${survivors[@]}"; do
  echo "write $node: $(ctl "$node" set "demo/after-$node" ok)"
done
for node in "${survivors[@]}"; do
  for key in "${survivors[@]/#/demo/after-}"; do
    got=$(ctl "$node" get "$key")
    [ "$got" = "ok" ] || { echo "FAIL: $key missing on $node" >&2; exit 1; }
  done
done
echo "all post-failover writes replicated to both survivors"

echo
echo "PASS: cluster survived losing $victim"
echo "restore it with: docker compose start $victim"
