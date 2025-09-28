#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$APP_DIR"
mkdir -p logs var

# --- выбор интерпретатора Python ---
PY_BIN="${APP_DIR}/.venv/bin/python3"
if [ ! -x "$PY_BIN" ]; then
  # фолбэк на системный python3
  PY_BIN="$(command -v python3 || true)"
fi
if [ -z "${PY_BIN:-}" ]; then
  echo "[service] ERROR: python3 not found (neither ${APP_DIR}/.venv/bin/python3 nor system)!" | tee -a "logs/service_$(date +%F).log"
  exit 1
fi

LOG_DAY="$(date +%F)"

# первый полный импорт — один раз
FULL_FLAG="var/full_import_done"
if [ ! -f "$FULL_FLAG" ]; then
  echo "[service] Running initial full import..." | tee -a "logs/service_${LOG_DAY}.log"
  "$PY_BIN" cli.py --log-level INFO --log-file "logs/import_all_${LOG_DAY}.log" import-all || true
  touch "$FULL_FLAG"
fi

SYNC_INTERVAL_SECONDS="${SYNC_INTERVAL_SECONDS:-300}"
echo "[service] Loop started, interval=${SYNC_INTERVAL_SECONDS}s, python=$("$PY_BIN" -V 2>&1)" | tee -a "logs/service_${LOG_DAY}.log"

while true; do
  LOG_DAY="$(date +%F)"
  echo "[service] tick: $(date -Is)" | tee -a "logs/service_${LOG_DAY}.log"
  "$PY_BIN" cli.py --log-level INFO --log-file "logs/import_today_${LOG_DAY}.log" import-today || true
  "$PY_BIN" cli.py --log-level INFO --log-file "logs/sync_${LOG_DAY}.log"        sync-b24    || true
  sleep "$SYNC_INTERVAL_SECONDS"
done
