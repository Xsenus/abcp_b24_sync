# config.py

# Подгрузка переменных окружения из файла .env (если есть)
from dotenv import load_dotenv
# Доступ к системным переменным окружения
import os
# Хелперы приведения типов с дефолтами (из нашего utils.py)
from utils import getenv_int, getenv_float
# Логирование состояния конфигурации
import logging
# Разбор URL, чтобы безопасно логировать домен вебхука Bitrix24
from urllib.parse import urlparse

# 1) Загружаем .env в процесс (os.environ пополняется значениями из файла)
load_dotenv()

# ------------------------- ABCP -------------------------

# Базовый endpoint ABCP (без query-параметров). Пример: https://abcpXXXX.public.api.abcp.ru/cp/users
ABCP_BASE_URL = os.getenv("ABCP_BASE_URL", "").strip()
# Логин для ABCP API
ABCP_USERLOGIN = os.getenv("ABCP_USERLOGIN", "").strip()
# Пароль/ключ для ABCP API (СЕКРЕТ! В логах — только маска)
ABCP_USERPSW   = os.getenv("ABCP_USERPSW", "").strip()
# Размер страницы для пагинации (int, по умолчанию 500)
ABCP_LIMIT     = getenv_int("ABCP_LIMIT", 500)
# Максимум страниц (int или None — без лимита)
ABCP_MAX_PAGES = getenv_int("ABCP_MAX_PAGES", None)

# ------------------------ Bitrix24 ----------------------

# Базовый вебхук Bitrix24: https://{domain}.bitrix24.ru/rest/{user}/{token}/
B24_WEBHOOK_URL       = os.getenv("B24_WEBHOOK_URL", "").strip()
# Префикс для названий сделок (по умолчанию "ABCP Регистрация:")
B24_DEAL_TITLE_PREFIX = os.getenv("B24_DEAL_TITLE_PREFIX", "ABCP Регистрация:").strip()

# Новые параметры для воронки «Пользователи»:
# CATEGORY_ID — целочисленный ID воронки; STAGE_ID — код стартовой стадии в этой воронке.
B24_DEAL_CATEGORY_ID_USERS = getenv_int("B24_DEAL_CATEGORY_ID_USERS", None)  # ОБЯЗАТЕЛЬНО УКАЗАТЬ
B24_DEAL_STAGE_NEW_USERS   = os.getenv("B24_DEAL_STAGE_NEW_USERS", "").strip()  # ОБЯЗАТЕЛЬНО УКАЗАТЬ (например, "C{CATEGORY_ID}:NEW")

# UF-поля сделки (кастомные поля, созданные в B24; значения — коды вида UF_CRM_*********)
UF_B24_DEAL_ABCP_USER_ID = os.getenv("UF_B24_DEAL_ABCP_USER_ID", "UF_CRM_1738181468").strip()
UF_B24_DEAL_INN          = os.getenv("UF_B24_DEAL_INN",          "UF_CRM_1713393074421").strip()
UF_B24_DEAL_SALDO        = os.getenv("UF_B24_DEAL_SALDO",        "UF_CRM_1738182431").strip()
UF_B24_DEAL_REG_DATE     = os.getenv("UF_B24_DEAL_REG_DATE",     "UF_CRM_1759089715").strip()
UF_B24_DEAL_UPDATE_TIME  = os.getenv("UF_B24_DEAL_UPDATE_TIME",  "UF_CRM_1738256915999").strip()

# ------------------------- Storage ----------------------

# Путь к локальной SQLite-БД (по умолчанию data/abcp_b24.sqlite3)
SQLITE_PATH = os.getenv("SQLITE_PATH", "data/abcp_b24.sqlite3")

# -------------------------- HTTP ------------------------

# Таймаут HTTP-запросов (сек)
REQUESTS_TIMEOUT       = getenv_int("REQUESTS_TIMEOUT", 20)
# Кол-во повторов при ошибках сети/5xx
REQUESTS_RETRIES       = getenv_int("REQUESTS_RETRIES", 3)
# Базовая задержка между повторами (сек)
REQUESTS_RETRY_BACKOFF = getenv_float("REQUESTS_RETRY_BACKOFF", 1.5)
# Пауза между успешными вызовами (сек) — бережём rate-limits
RATE_LIMIT_SLEEP       = getenv_float("RATE_LIMIT_SLEEP", 0.2)

# ---------------------- Проверка конфигурации ----------------------

def assert_config() -> None:
    """
    Проверяет наличие обязательных переменных окружения.
    При успехе пишет INFO «OK», при отсутствии — AssertionError.
    """
    logger = logging.getLogger(__name__)
    logger.debug("Проверка обязательных переменных окружения...")

    # Требуемые параметры для ABCP
    assert ABCP_BASE_URL,  "ABCP_BASE_URL required"
    assert ABCP_USERLOGIN, "ABCP_USERLOGIN required"
    assert ABCP_USERPSW,   "ABCP_USERPSW required"

    # Требуемые параметры для Bitrix24
    assert B24_WEBHOOK_URL, "B24_WEBHOOK_URL required"
    assert B24_DEAL_CATEGORY_ID_USERS is not None, "B24_DEAL_CATEGORY_ID_USERS required (id воронки «Пользователи»)"
    assert B24_DEAL_STAGE_NEW_USERS, "B24_DEAL_STAGE_NEW_USERS required (стартовая стадия воронки «Пользователи»)"

    logger.info("Конфигурация проверена: OK")

# ---------------------- Логирование конфигурации ----------------------

def _mask_secret(s: str) -> str:
    """
    Маскирует секрет (пароль/токен): первые 2 символа, затем ***, затем последние 2.
    Пример: 'abcdef' -> 'ab***ef'. Пустое значение остаётся пустым.
    """
    if not s:
        return ""
    if len(s) <= 4:
        return "*" * len(s)
    return f"{s[:2]}***{s[-2:]}"

def _describe_webhook(url: str) -> str:
    """
    Возвращает безопасное описание вебхука B24:
    - домен;
    - структура пути (не выводим user/token);
    - схема.
    """
    if not url:
        return "(empty)"
    try:
        u = urlparse(url)
        # Путь не раскрываем: показываем только кол-во сегментов
        segs = [seg for seg in (u.path or "").split("/") if seg]
        return f"{u.scheme}://{u.netloc}/rest/*/*  (segments={len(segs)})"
    except Exception:
        return "(parse-error)"

def log_config(level: int = logging.DEBUG) -> None:
    """
    Печатает в лог (DEBUG/INFO) безопасную сводку конфигурации:
    - без пароля/токена (маска или описание);
    - с ключевыми параметрами и их наличием.
    """
    logger = logging.getLogger(__name__)
    # ABCP
    logger.log(level, "ABCP_BASE_URL=%s", ABCP_BASE_URL or "(empty)")
    logger.log(level, "ABCP_USERLOGIN=%s", ABCP_USERLOGIN or "(empty)")
    logger.log(level, "ABCP_USERPSW=%s", _mask_secret(ABCP_USERPSW))
    logger.log(level, "ABCP_LIMIT=%s, ABCP_MAX_PAGES=%s", ABCP_LIMIT, ABCP_MAX_PAGES)

    # B24
    logger.log(level, "B24_WEBHOOK_URL=%s", _describe_webhook(B24_WEBHOOK_URL))
    logger.log(level, "B24_DEAL_TITLE_PREFIX=%r", B24_DEAL_TITLE_PREFIX)
    logger.log(level, "B24_DEAL_CATEGORY_ID_USERS=%s", B24_DEAL_CATEGORY_ID_USERS)
    logger.log(level, "B24_DEAL_STAGE_NEW_USERS=%r", B24_DEAL_STAGE_NEW_USERS)

    # UF-поля
    logger.log(level, "UF fields: ABCP_USER_ID=%s, INN=%s, SALDO=%s, REG_DATE=%s, UPDATE_TIME=%s",
               UF_B24_DEAL_ABCP_USER_ID, UF_B24_DEAL_INN, UF_B24_DEAL_SALDO,
               UF_B24_DEAL_REG_DATE, UF_B24_DEAL_UPDATE_TIME)

    # Storage / HTTP
    logger.log(level, "SQLITE_PATH=%s", SQLITE_PATH)
    logger.log(level, "HTTP: timeout=%s, retries=%s, backoff=%s, sleep=%s",
               REQUESTS_TIMEOUT, REQUESTS_RETRIES, REQUESTS_RETRY_BACKOFF, RATE_LIMIT_SLEEP)
