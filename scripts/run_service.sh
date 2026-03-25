#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$APP_DIR"
mkdir -p logs var

PY_BIN="${APP_DIR}/.venv/bin/python3"
if [ ! -x "$PY_BIN" ]; then
  PY_BIN="$(command -v python3 || true)"
fi

if [ -z "${PY_BIN:-}" ]; then
  echo "[service] ERROR: python3 not found" | tee -a "logs/service_$(date +%F).log"
  exit 1
fi

export SYNC_INTERVAL_SECONDS="${SYNC_INTERVAL_SECONDS:-600}"

LOG_DAY="$(date +%F)"
echo "[service] Starting main.py, interval=${SYNC_INTERVAL_SECONDS}s, python=$("$PY_BIN" -V 2>&1)" | tee -a "logs/service_${LOG_DAY}.log"
exec "$PY_BIN" main.py
