#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# каталоги
mkdir -p data logs var

# venv
if [ ! -x .venv/bin/python ]; then
  python3 -m venv .venv
fi
. .venv/bin/activate

PY=".venv/bin/python"
LOG_PATH="logs/sync_$(date +%F).log"

# 1) инициализация схемы (идемпотентно)
$PY cli.py --log-level INFO init-db || true

FLAG="var/initialized.flag"

# 2) первичный полный прогон (один раз)
if [ ! -f "$FLAG" ]; then
  echo "[INIT] Первый запуск: выполняю полный импорт и синхронизацию..."
  $PY cli.py --log-level INFO --log-file "$LOG_PATH" import-all || true
  $PY cli.py --log-level INFO --log-file "$LOG_PATH" sync-b24 || true
  date -Iseconds > "$FLAG"
fi

# 3) постоянная обработка «сегодняшних» с интервалом
INTERVAL="${SYNC_INTERVAL_SECONDS:-300}"  # по умолчанию 5 минут
echo "[LOOP] Запуск цикла обработки 'сегодня' с интервалом ${INTERVAL}s"
while true; do
  LOG_PATH="logs/sync_$(date +%F).log"
  $PY cli.py --log-level INFO --log-file "$LOG_PATH" import-today || true
  $PY cli.py --log-level INFO --log-file "$LOG_PATH" sync-b24 || true
  sleep "$INTERVAL"
done
