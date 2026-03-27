#!/usr/bin/env python

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from time import perf_counter

from config import ABCP_INITIAL_IMPORT_MODE, SQLITE_PATH, assert_config
from db import init_db
from sync_service import import_all, import_incremental, import_today, sync_to_b24

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def _default_log_path() -> str:
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    return str(log_dir / f"sync_{datetime.now().strftime('%Y-%m-%d')}.log")


def _setup_logging(level_str: str, log_file: str | None) -> None:
    level = getattr(logging, (level_str or "DEBUG").upper(), logging.DEBUG)
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    if log_file:
        try:
            handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
        except Exception as exc:
            logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
            logging.warning("Failed to open log file %r: %s", log_file, exc)

    logging.basicConfig(level=level, format=LOG_FORMAT, handlers=handlers)


def main() -> None:
    parser = argparse.ArgumentParser(description="ABCP -> Bitrix24 sync CLI")
    parser.add_argument("--log-level", default="DEBUG", help="DEBUG/INFO/WARNING/ERROR")
    parser.add_argument("--log-file", default=None, help="Path to log file")
    parser.add_argument("-v", "--verbose", action="store_true", help="Alias for --log-level=DEBUG")

    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init-db", help="Initialize SQLite DB")
    sub.add_parser("import-all", help="Full import from ABCP")
    sub.add_parser("import-incremental", help="Incremental import by registration/update window")
    sub.add_parser("import-today", help="Legacy alias for import-incremental")

    p_sync = sub.add_parser("sync-b24", help="Sync unsynced rows to Bitrix24")
    p_sync.add_argument("--limit", type=int, default=None, help="Limit rows per run")

    sub.add_parser("run", help="Bootstrap import then sync to Bitrix24")

    args = parser.parse_args()

    if args.log_file is None:
        args.log_file = _default_log_path()

    level = "DEBUG" if args.verbose else args.log_level
    _setup_logging(level, args.log_file)
    logging.info("CLI start: cmd=%s, db=%s, level=%s, log_file=%s", args.cmd, SQLITE_PATH, level, args.log_file)

    started = perf_counter()
    try:
        if args.cmd == "init-db":
            init_db(SQLITE_PATH)
            print("DB initialized:", SQLITE_PATH)
            return

        assert_config()

        if args.cmd == "import-all":
            count = import_all()
            print("Imported users:", count)
        elif args.cmd == "import-incremental":
            count = import_incremental()
            print("Imported incremental:", count)
        elif args.cmd == "import-today":
            count = import_today()
            print("Imported incremental:", count)
        elif args.cmd == "sync-b24":
            count = sync_to_b24(limit=args.limit)
            print("Synced to Bitrix24:", count)
        elif args.cmd == "run":
            if ABCP_INITIAL_IMPORT_MODE == "incremental":
                logging.info(
                    "run: ABCP_INITIAL_IMPORT_MODE=%r, using import-incremental instead of import-all",
                    ABCP_INITIAL_IMPORT_MODE,
                )
                imported = import_incremental()
            else:
                imported = import_all()
            synced = sync_to_b24()
            print("Imported:", imported)
            print("Synced:", synced)
        else:
            logging.error("Unknown command: %s", args.cmd)
            sys.exit(2)
    except Exception as exc:
        logging.exception("Fatal error: %s", exc)
        sys.exit(1)
    finally:
        took_ms = (perf_counter() - started) * 1000.0
        logging.info("CLI done: cmd=%s, took=%.1fms", args.cmd, took_ms)


if __name__ == "__main__":
    main()
