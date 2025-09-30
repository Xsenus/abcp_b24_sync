#!/usr/bin/env python3
from __future__ import annotations
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
# .env лежит в корне проекта рядом с main.py
os.environ.setdefault("DOTENV_PATH", str(BASE_DIR / ".env"))
# На время локальной отладки можно перебить переменные среды значениями из .env:
os.environ.setdefault("DOTENV_OVERRIDE", "1")
# Зафиксируем рабочую директорию на корень проекта (пути к БД/логам будут предсказуемы)
os.chdir(BASE_DIR)

import sys
import time
import signal
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, date
from typing import Optional

from dotenv import find_dotenv

from config import assert_config, SQLITE_PATH
from db import init_db, get_engine, get_meta, set_meta
from sqlalchemy.orm import Session

from sync_service import import_all, import_today, sync_to_b24

# ---------- настройки ----------
ENV_SYNC_INTERVAL = "SYNC_INTERVAL_SECONDS"
DEFAULT_INTERVAL = 1  # сек

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
LOG_DIR = "logs"
LOG_FILE_BASENAME = "service.log"

_stop = False


def _setup_logging(level: str = "INFO") -> None:
    os.makedirs(LOG_DIR, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # консоль
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter(LOG_FORMAT))
    root.addHandler(sh)

    # файловый — с ротацией по полуночи и датой в имени
    fh = TimedRotatingFileHandler(
        os.path.join(LOG_DIR, LOG_FILE_BASENAME),
        when="midnight",
        backupCount=14,
        encoding="utf-8",
        utc=False,
    )
    # имя будет service.log.YYYY-MM-DD
    fh.suffix = "%Y-%m-%d"
    fh.setFormatter(logging.Formatter(LOG_FORMAT))
    root.addHandler(fh)


def _handle_sig(signum, frame):
    global _stop
    logging.info("Got signal %s — stopping loop …", signum)
    _stop = True


def _get_interval() -> int:
    try:
        return int(os.getenv(ENV_SYNC_INTERVAL, str(DEFAULT_INTERVAL)))
    except Exception:
        return DEFAULT_INTERVAL


def _full_import_done(session: Session) -> bool:
    ts = get_meta(session, "last_full_import_at")
    return bool(ts)


def _mark_full_import(session: Session) -> None:
    set_meta(session, "last_full_import_at", datetime.utcnow().isoformat())
    session.commit()


def run_daemon() -> None:
    _setup_logging(os.getenv("LOG_LEVEL", "INFO"))

    # Диагностика: покажем, какой .env определён и где нас запустили
    logging.info("CWD=%s", os.getcwd())
    logging.info("ENV file=%s (found=%s)",
                 os.environ.get("DOTENV_PATH"),
                 bool(find_dotenv(filename=".env", usecwd=True)))

    logging.info("Service start. DB=%s", SQLITE_PATH)

    # Конфиг + БД
    assert_config()
    init_db(SQLITE_PATH)

    # Сигналы для graceful shutdown
    signal.signal(signal.SIGINT, _handle_sig)
    try:
        signal.signal(signal.SIGTERM, _handle_sig) 
    except Exception:
        pass

    # Один раз — полный импорт, если ещё не делали
    with Session(get_engine(SQLITE_PATH)) as session:
        if not _full_import_done(session):
            logging.info("Initial full import: start")
            try:
                cnt = import_all()
                logging.info("Initial full import: done, users=%d", cnt)
                _mark_full_import(session)
            except Exception:
                logging.exception("Initial full import FAILED")
                # продолжаем — в цикле пойдёт инкрементальная загрузка

    interval = _get_interval()
    logging.info("Loop: every %ss", interval)

    # Основной цикл
    while not _stop:
        started = time.perf_counter()
        try:
            logging.info("Tick: import_today")
            cnt_i = import_today(today=date.today())
            logging.info("Tick: import_today done, users=%d", cnt_i)

            logging.info("Tick: sync_to_b24")
            cnt_s = sync_to_b24()
            logging.info("Tick: sync_to_b24 done, synced=%d", cnt_s)
        except Exception:
            logging.exception("Tick FAILED")
        finally:
            took = (time.perf_counter() - started) * 1000
            logging.info("Tick finished in %.1f ms", took)

        # аккуратный сон с возможностью быстрой остановки
        sleep_left = interval
        while sleep_left > 0 and not _stop:
            time.sleep(min(1, sleep_left))
            sleep_left -= 1

    logging.info("Service stopped.")


if __name__ == "__main__":
    try:
        run_daemon()
    except Exception:
        logging.exception("Fatal error at top-level")
        sys.exit(1)
