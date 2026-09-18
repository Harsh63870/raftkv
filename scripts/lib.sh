#!/usr/bin/env bash
# Shared helpers for the demo scripts.

NODES=(node1 node2 node3)

dc() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  else
    docker-compose "$@"
  fi
}

# ctl <node> <raftkvctl args...>
ctl() {
  local node=$1
  shift
  dc exec -T "$node" raftkvctl -addr 127.0.0.1:50051 "$@"
}

running() { [ -n "$(dc ps -q --status running "$1" 2>/dev/null)" ]; }

member() {
  local needle=$1 x
  shift
  for x in "$@"; do
    [ "$x" = "$needle" ] && return 0
  done
  return 1
}

# leader [nodes to ask...] -> "<node-id> <grpc-addr>"
leader() {
  local candidates=("$@")
  [ ${#candidates[@]} -gt 0 ] || candidates=("${NODES[@]}")
  local node out
  for node in "${candidates[@]}"; do
    running "$node" || continue
    if out=$(ctl "$node" leader 2>/dev/null); then
      echo "$out"
      return 0
    fi
  done
  return 1
}

# wait_leader <timeout-seconds> [expected nodes...]
# Requires the reported leader to be one of the expected nodes. Right after a
# node dies its peers still name it as leader until the heartbeat timeout
# expires, so accepting the first answer would hand back a dead leader.
wait_leader() {
  local timeout=$1
  shift
  local candidates=("$@")
  [ ${#candidates[@]} -gt 0 ] || candidates=("${NODES[@]}")

  local deadline=$(($(date +%s) + timeout))
  local out id
  while [ "$(date +%s)" -le "$deadline" ]; do
    if out=$(leader "${candidates[@]}"); then
      read -r id _ <<<"$out"
      if member "$id" "${candidates[@]}"; then
        echo "$out"
        return 0
      fi
    fi
    sleep 0.2
  done
  return 1
}

require_cluster() {
  local node
  for node in "${NODES[@]}"; do
    running "$node" && return 0
  done
  echo "cluster is not running; start it with: docker compose up -d --build" >&2
  exit 1
}
