#!/usr/bin/env bash
# Throughput check: writes on the leader, writes through a follower (forwarded),
# and local reads. Usage: scripts/bench.sh [ops] [concurrency]
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/lib.sh

ops=${1:-2000}
conc=${2:-16}

require_cluster
current=$(wait_leader 30) || { echo "no leader" >&2; exit 1; }
read -r leader_id _ <<<"$current"

follower=""
for node in "${NODES[@]}"; do
  if [ "$node" != "$leader_id" ] && running "$node"; then
    follower=$node
    break
  fi
done

echo "leader=$leader_id follower=${follower:-none} ops=$ops concurrency=$conc"
echo

echo -n "leader writes      "
ctl "$leader_id" bench -n "$ops" -c "$conc"

if [ -n "$follower" ]; then
  echo -n "forwarded writes   "
  ctl "$follower" bench -n "$ops" -c "$conc"

  echo -n "local reads        "
  ctl "$follower" bench -n "$ops" -c "$conc" -read
fi

echo
echo -n "snapshot: "
ctl "$leader_id" snapshot
