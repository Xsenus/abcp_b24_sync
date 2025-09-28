#!/usr/bin/env python

# Импорт стандартных библиотек
import argparse            # парсинг аргументов командной строки
import logging             # логирование (INFO/DEBUG в консоль/файл)
import sys                 # для выхода с кодом возврата
from datetime import date, datetime  # дата для import_today и текущая дата для имени лог-файла
from time import perf_counter         # измерение длительности выполнения
from pathlib import Path              # для каталога logs/

# Импорт проектных модулей/функций
from config import assert_config, SQLITE_PATH           # проверка конфигурации и путь к БД
from db import init_db                                  # инициализация SQLite (создание схемы)
from sync_service import import_all, import_today, sync_to_b24  # команды синхронизации

# ---- Константы формата логов ----
# Формат сообщения: 2025-09-28 12:00:00,123 INFO module: текст
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def _default_log_path() -> str:
    """
    Возвращает путь logs/sync_YYYY-MM-DD.log и гарантирует наличие каталога.
    """
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    return str(log_dir / f"sync_{datetime.now().strftime('%Y-%m-%d')}.log")


def _setup_logging(level_str: str, log_file: str | None) -> None:
    """
    Настраивает логирование по уровню и (опционально) в файл.
    :param level_str: строка уровня ('DEBUG', 'INFO', 'WARNING', ...)
    :param log_file: путь к лог-файлу или None (только консоль)
    """
    # Преобразуем строковый уровень в числовой (по умолчанию DEBUG)
    level = getattr(logging, (level_str or "DEBUG").upper(), logging.DEBUG)

    # Базовая конфигурация — потоковый обработчик (консоль)
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    # Если указан файл — добавляем файловый обработчик
    if log_file:
        try:
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            handlers.append(file_handler)
        except Exception as e:
            # Если не удалось открыть файл — предупредим и продолжим только с консолью
            logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
            logging.warning("Не удалось открыть лог-файл %r: %s — продолжаю без файла", log_file, e)

    # Применяем конфигурацию логгера
    logging.basicConfig(level=level, format=LOG_FORMAT, handlers=handlers)


def main() -> None:
    """
    Точка входа CLI.
    Поддерживаемые команды:
      - init-db         : создать/инициализировать SQLite-схему
      - import-all      : полный импорт пользователей из ABCP
      - import-today    : инкремент по «сегодня»
      - sync-b24 [--limit N] : синхронизация в Bitrix24 (не более N записей)
      - run             : import-all + sync-b24 (полный цикл)
    Глобальные флаги:
      --log-level, --log-file, -v/--verbose
    """
    # ---- Аргументы командной строки ----
    parser = argparse.ArgumentParser(description="ABCP -> Bitrix24 sync CLI")
    # Глобальные опции логирования
    parser.add_argument("--log-level", default="DEBUG",
                        help="Уровень логирования: DEBUG/INFO/WARNING/ERROR (по умолчанию DEBUG)")
    parser.add_argument("--log-file", default=None,
                        help="Путь к лог-файлу (по умолчанию logs/sync_YYYY-MM-DD.log + консоль)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Синоним --log-level=DEBUG")

    # Группа подкоманд
    sub = parser.add_subparsers(dest="cmd", required=True)

    # init-db: создание схемы БД
    sub.add_parser("init-db", help="Инициализировать SQLite БД и создать таблицы")

    # import-all: полный импорт ABCP
    sub.add_parser("import-all", help="Полный импорт пользователей из ABCP (постранично)")

    # import-today: инкремент за сегодня
    sub.add_parser("import-today", help="Инкрементальный импорт: зарегистрированы сегодня")

    # sync-b24: выгрузка в Bitrix24
    p_sync = sub.add_parser("sync-b24", help="Синхронизация в Bitrix24 для несинхронизированных записей")
    p_sync.add_argument("--limit", type=int, default=None, help="Лимит записей за прогон (по умолчанию без лимита)")

    # run: полный цикл (import-all + sync-b24)
    sub.add_parser("run", help="Полный цикл: import-all затем sync-b24")

    # Парсим аргументы
    args = parser.parse_args()

    # ---- Настройка логов до выполнения любой логики ----
    # Если путь к логу не указан — используем файл текущей даты
    if args.log_file is None:
        args.log_file = _default_log_path()

    # Если задан --verbose, принудительно DEBUG
    level = "DEBUG" if args.verbose else args.log_level
    _setup_logging(level, args.log_file)

    # Логируем старт CLI и входные параметры (без секретов)
    logging.info("CLI start: cmd=%s, db=%s, level=%s, log_file=%s",
                 args.cmd, SQLITE_PATH, level, args.log_file)

    # Засекаем время для общей длительности
    t0 = perf_counter()

    try:
        # Ветка команд
        if args.cmd == "init-db":
            # init-db не требует assert_config (в нём нет внешних вызовов)
            logging.info("init-db: инициализация БД %s", SQLITE_PATH)
            init_db(SQLITE_PATH)
            logging.info("init-db: БД инициализирована успешно")
            print("DB initialized:", SQLITE_PATH)  # дублируем в stdout для скриптов
            return

        # Для остальных команд сначала проверим конфигурацию (обязательные переменные)
        logging.debug("Проверка конфигурации окружения (.env)")
        assert_config()
        logging.debug("Конфигурация валидна")

        if args.cmd == "import-all":
            logging.info("import-all: старт")
            cnt = import_all()
            logging.info("import-all: завершено, записей=%d", cnt)
            print("Imported users:", cnt)
        elif args.cmd == "import-today":
            logging.info("import-today: старт (date=%s)", date.today().isoformat())
            cnt = import_today(today=date.today())
            logging.info("import-today: завершено, записей=%d", cnt)
            print("Imported today:", cnt)
        elif args.cmd == "sync-b24":
            logging.info("sync-b24: старт (limit=%s)", args.limit)
            cnt = sync_to_b24(limit=args.limit)
            logging.info("sync-b24: завершено, синхронизировано=%d", cnt)
            print("Synced to Bitrix24:", cnt)
        elif args.cmd == "run":
            # Полный цикл: import-all + sync-b24
            logging.info("run: полный цикл — import-all → sync-b24")
            cnt1 = import_all()
            logging.info("run: импорт завершён, записей=%d — запускаю sync-b24", cnt1)
            cnt2 = sync_to_b24()
            logging.info("run: синхронизация завершена, записей=%d", cnt2)
            print("Imported:", cnt1)
            print("Synced:", cnt2)
        else:
            # На случай появления неизвестной подкоманды
            logging.error("Неизвестная команда: %s", args.cmd)
            sys.exit(2)

    except Exception as e:
        # Любая непойманная ошибка — в лог со стеком, код возврата 1
        logging.exception("Fatal error: %s", e)
        sys.exit(1)
    finally:
        # Финальный лог с длительностью выполнения
        dt = (perf_counter() - t0) * 1000.0
        logging.info("CLI done: cmd=%s, took=%.1fms", args.cmd, dt)


# Точка входа при запуске как скрипт
if __name__ == "__main__":
    main()
