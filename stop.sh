#!/usr/bin/env bash
# clubrunner — stop the orchestrator loop and the dashboard.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

stop_one() {
  local name="$1" pidfile="$2"
  if [ ! -f "$pidfile" ]; then
    echo "$name: no pidfile"
    return
  fi
  local pid
  pid="$(cat "$pidfile")"
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    sleep 1
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
    echo "$name stopped (was pid $pid)"
  else
    echo "$name: not running"
  fi
  rm -f "$pidfile"
}

stop_one orchestrator "$ROOT/run/orchestrator.pid"
stop_one dashboard    "$ROOT/run/dashboard.pid"
