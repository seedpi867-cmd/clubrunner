#!/usr/bin/env bash
# clubrunner — start the orchestrator loop and the dashboard.
# Idempotent: safe to re-run; existing processes are reused.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

PY="${PYTHON:-python3}"
mkdir -p logs run

ORCH_PID="$ROOT/run/orchestrator.pid"
DASH_PID="$ROOT/run/dashboard.pid"

is_alive() {
  [ -f "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null
}

if is_alive "$ORCH_PID"; then
  echo "orchestrator already running (pid $(cat "$ORCH_PID"))"
else
  nohup "$PY" -u orchestrator.py --loop \
    > "logs/orchestrator.log" 2>&1 &
  echo $! > "$ORCH_PID"
  echo "orchestrator started (pid $(cat "$ORCH_PID")) → logs/orchestrator.log"
fi

if is_alive "$DASH_PID"; then
  echo "dashboard already running (pid $(cat "$DASH_PID"))"
else
  nohup "$PY" -u -m dashboard.server \
    > "logs/dashboard.log" 2>&1 &
  echo $! > "$DASH_PID"
  echo "dashboard started (pid $(cat "$DASH_PID")) → logs/dashboard.log"
fi

PORT=$("$PY" -c "import config; print(config.get().get('dashboard',{}).get('port',8089))")
echo
echo "  dashboard: http://127.0.0.1:$PORT/"
echo "  stop with: ./stop.sh"
