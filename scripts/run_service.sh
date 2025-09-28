#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$APP_DIR"
mkdir -p logs var
LOG_DAY="$(date +%F)"
if [ -x ".venv/bin/activate" ]; then
  . .venv/bin/activate
fi
FULL_FLAG="var/full_import_done"
if [ ! -f "$FULL_FLAG" ]; then
  echo "[service] Running initial full import..." | tee -a "logs/service_${LOG_DAY}.log"
  python cli.py --log-level INFO --log-file "logs/import_all_${LOG_DAY}.log" import-all || true
  touch "$FULL_FLAG"
fi
SYNC_INTERVAL_SECONDS="${SYNC_INTERVAL_SECONDS:-300}"
echo "[service] Loop started, interval=${SYNC_INTERVAL_SECONDS}s" | tee -a "logs/service_${LOG_DAY}.log"
while true; do
  LOG_DAY="$(date +%F)"
  echo "[service] tick: $(date -Is)" | tee -a "logs/service_${LOG_DAY}.log"
  python cli.py --log-level INFO --log-file "logs/import_today_${LOG_DAY}.log" import-today || true
  python cli.py --log-level INFO --log-file "logs/sync_${LOG_DAY}.log" sync-b24 || true
  sleep "$SYNC_INTERVAL_SECONDS"
done