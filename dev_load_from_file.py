# dev_load_from_file.py

# Стандартные библиотеки
import argparse           # парсинг аргументов командной строки
import json               # загрузка JSON-файла с данными ABCP
import logging            # логирование (DEBUG/INFO и т.д.)
import os                 # работа с файловой системой (каталоги/пути)
from typing import Any, Dict, List  # аннотации типов

# SQLAlchemy ORM
from sqlalchemy.orm import Session  # управление транзакциями

# Наши проектные модули
from db import init_db, get_engine, upsert_user, User  # БД и upsert
from config import SQLITE_PATH                         # путь к SQLite из .env


# ---------- Настройка логгера ----------
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
log = logging.getLogger(__name__)


def _setup_logging(level_str: str, log_file: str | None) -> None:
    """
    Готовит логирование: уровень и (опционально) файл.
    """
    level = getattr(logging, (level_str or "INFO").upper(), logging.INFO)
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        try:
            handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
        except Exception as e:
            logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
            logging.warning("Не удалось открыть лог-файл %r: %s — продолжаю без файла", log_file, e)
    logging.basicConfig(level=level, format=LOG_FORMAT, handlers=handlers)


def load_from_file(path: str) -> Dict[str, Any]:
    """
    Безопасно загружает JSON из файла `path`.
    Возвращает словарь (корневой JSON).
    """
    log.info("Чтение JSON из файла: %s", path)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Файл не найден: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Ожидался объект JSON (dict), получено: {type(data)}")
    # Краткая сводка по содержимому
    items_preview = len(data.get("items") or []) if isinstance(data.get("items"), list) else "n/a"
    log.debug("JSON загружен: есть ключи=%s, items_count=%s", list(data.keys()), items_preview)
    return data


def ensure_data_dir() -> None:
    """
    Гарантирует наличие каталога `data` для SQLite (если путь по умолчанию).
    """
    data_dir = "data"
    if not os.path.exists(data_dir):
        log.debug("Каталог %s отсутствует — создаю", data_dir)
        os.makedirs(data_dir, exist_ok=True)


def main() -> None:
    """
    Локальная загрузка пользователей из JSON-файла в SQLite.
    Использование:
      python dev_load_from_file.py --path test.json --log-level DEBUG
    """
    # ---- Аргументы CLI ----
    parser = argparse.ArgumentParser(description="Локальная проверка парсинга JSON (без обращений к ABCP).")
    parser.add_argument("--path", default="test.json", help="Путь к JSON-файлу (по умолчанию test.json).")
    parser.add_argument("--log-level", default="INFO", help="Уровень логирования: DEBUG/INFO/...")
    parser.add_argument("--log-file", default=None, help="Логировать также в файл.")
    parser.add_argument("--commit-every", type=int, default=500, help="Коммит каждые N записей (по умолчанию 500).")
    args = parser.parse_args()

    # ---- Логи ----
    _setup_logging(args.log_level, args.log_file)
    log.info("Старт локальной загрузки: file=%s, db=%s", args.path, SQLITE_PATH)

    # ---- Подготовка БД ----
    ensure_data_dir()
    init_db(SQLITE_PATH)
    engine = get_engine(SQLITE_PATH)

    # ---- Загрузка JSON ----
    payload = load_from_file(args.path)
    items: List[Dict[str, Any]] = payload.get("items", []) if isinstance(payload.get("items"), list) else []
    log.info("Найдено items=%d — начинаю upsert в БД", len(items))

    # ---- Транзакция upsert ----
    processed = 0
    created_or_updated = 0
    with Session(engine) as session:
        for idx, it in enumerate(items, start=1):
            # Пытаемся извлечь диагностическую информацию (userId/registrationDate)
            uid = it.get("userId") or it.get("userID") or it.get("id")
            reg = it.get("registrationDate")
            log.debug("Обработка #%d: userId=%r, registrationDate=%r", idx, uid, reg)

            try:
                # upsert_user вернёт ORM-объект (новый или существующий)
                u = upsert_user(session, it)
                created_or_updated += 1
            except Exception as e:
                # В случае ошибки по конкретной записи — логируем и продолжаем
                log.exception("Ошибка upsert для записи #%d (userId=%r): %s", idx, uid, e)
                session.rollback()
                continue

            processed += 1
            # Промежуточный коммит для устойчивости на больших объёмах
            if processed % max(1, args.commit_every) == 0:
                session.commit()
                log.info("Промежуточный COMMIT: обработано=%d", processed)

        # Финальный коммит — фиксируем хвост
        session.commit()
        log.info("Финальный COMMIT: всего обработано=%d, upsert-ов=%d", processed, created_or_updated)

        # Итог: сколько строк в таблице users
        total = session.query(User).count()
        log.info("Итоговое количество пользователей в БД: %d", total)
        print("Loaded users:", total)  # оставляем вывод для совместимости со старыми скриптами


if __name__ == "__main__":
    main()
