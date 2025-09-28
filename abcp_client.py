# abcp_client.py

# Импорт стандартного логгера
import logging
# Импорт функции задержки между запросами для бережного rate-limit
import time
# HTTP-клиент для вызова API ABCP
import requests
# Типы для аннотаций (итераторы, опциональные значения, словари)
from typing import Iterator, Optional, Dict, Any
# Тип даты для фильтрации «сегодня»
from datetime import date

# Импорт конфигурации из .env через наш модуль config
from config import (
    ABCP_BASE_URL,            # базовый URL эндпоинта /cp/users
    ABCP_USERLOGIN,           # логин для ABCP API
    ABCP_USERPSW,             # пароль/ключ для ABCP API (НЕ логируем)
    ABCP_LIMIT,               # размер страницы
    ABCP_MAX_PAGES,           # ограничение количества страниц (может быть None)
    REQUESTS_TIMEOUT,         # таймаут HTTP-запросов
    REQUESTS_RETRIES,         # число повторов при ошибках
    REQUESTS_RETRY_BACKOFF,   # коэффициент backoff между повторами
    RATE_LIMIT_SLEEP,         # пауза между запросами
)
# Универсальный помощник «с повторами» (экспоненциальный backoff реализуем в utils)
from utils import with_retries

# Инициализируем модульный логгер (имя = abcp_client)
log = logging.getLogger(__name__)

# -------- Локальные константы c жёсткими типами, чтобы убрать Optional для Pylance --------

# Таймаут запроса как целое число (сек)
_REQ_TIMEOUT: int = int(REQUESTS_TIMEOUT or 20)
# Количество повторов как целое число
_RETRIES: int = int(REQUESTS_RETRIES or 3)
# Базовая задержка между повторами как float
_BACKOFF: float = float(REQUESTS_RETRY_BACKOFF or 1.5)
# Пауза между удачными запросами (rate-limit) как float
_SLEEP: float = float(RATE_LIMIT_SLEEP or 0.0)
# Лимит записей на страницу как целое число
_LIMIT: int = int(ABCP_LIMIT or 500)
# Максимум страниц — может быть None (тогда без лимита), иначе приводим к int
_MAX_PAGES: Optional[int] = int(ABCP_MAX_PAGES) if ABCP_MAX_PAGES is not None else None


def _fetch_page(skip: int, limit: int) -> Dict[str, Any]:
    """
    Загружает одну страницу пользователей ABCP.
    :param skip: смещение (сколько записей пропустить)
    :param limit: размер страницы
    :return: распарсенный JSON-словарь ответа
    """
    # Формируем query-параметры запроса (пароль НЕ логируем)
    params: Dict[str, Any] = {
        "userlogin": ABCP_USERLOGIN,  # логин
        "userpsw": ABCP_USERPSW,      # пароль (секрет)
        "limit": limit,               # размер страницы
        "skip": skip,                 # смещение
        "format": "p",                # формат «p» согласно вашему примеру
    }

    # Отладочно фиксируем старт запроса (без секрета)
    log.debug("ABCP GET %s?skip=%s&limit=%s&format=p&userlogin=%s",
              ABCP_BASE_URL, skip, limit, ABCP_USERLOGIN)

    # Внутренняя функция, непосредственно выполняющая HTTP-вызов
    def do() -> Dict[str, Any]:
        # Выполняем GET на ABCP_BASE_URL с параметрами и таймаутом
        r = requests.get(ABCP_BASE_URL, params=params, timeout=_REQ_TIMEOUT)
        # Бросаем исключение при HTTP-ошибке (4xx/5xx)
        r.raise_for_status()
        # Пытаемся распарсить JSON
        data = r.json()
        # Проверяем тип, ожидаем словарь (dict)
        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected ABCP response type: {type(data)}")
        # Возвращаем распарсенный JSON
        return data

    # Вызываем do() с повторами при ошибках (retries/backoff настроены в константах выше)
    data = with_retries(do, retries=_RETRIES, backoff=_BACKOFF)

    # Логируем успешный ответ на уровне DEBUG с краткой сводкой
    items = data.get("items")
    log.debug("ABCP page fetched: skip=%s, limit=%s, items_count=%s",
              skip, limit, (len(items) if isinstance(items, list) else "n/a"))

    # Делаем небольшую паузу для соблюдения rate-limit (если задана)
    if _SLEEP > 0:
        log.debug("Rate-limit sleep: %s sec", _SLEEP)
        time.sleep(_SLEEP)

    # Возвращаем тело ответа
    return data


def iter_all_users() -> Iterator[Dict[str, Any]]:
    """
    Итерирует по всем пользователям ABCP постранично.
    :yield: словарь пользователя (как в JSON ABCP)
    """
    # Начальное смещение
    skip = 0
    # Номер страницы (для лога/ограничения)
    page = 0
    # Размер страницы, фиксируем один раз локально
    limit = _LIMIT

    # Информируем о старте общей итерации по всем пользователям
    log.info("ABCP iterate all users: start, limit=%s, max_pages=%s", limit, _MAX_PAGES)

    # Бесконечный цикл, прервёмся по пустой странице или достижении лимита страниц
    while True:
        # Если задан максимум страниц и мы его достигли — выходим
        if _MAX_PAGES is not None and page >= _MAX_PAGES:
            log.warning("ABCP_MAX_PAGES reached at page=%s, stopping.", page)
            break

        # Загружаем страницу (skip/limit)
        payload = _fetch_page(skip=skip, limit=limit)

        # Извлекаем массив пользователей из ответа
        items = payload.get("items") or []

        # Если нет элементов — это сигнал окончания данных
        if not items:
            log.info("ABCP iterate all users: no items on page=%s (skip=%s). Done.", page, skip)
            break

        # Логируем прогресс страницы и количество найденных записей
        log.info("ABCP page=%s fetched: items=%s (skip=%s, limit=%s)",
                 page, len(items), skip, limit)

        # Поочерёдно отдаём наружу каждого пользователя
        for it in items:
            # При желании можно логировать идентификаторы (если есть)
            user_id = it.get("userId") or it.get("userID") or it.get("id")
            reg_date = it.get("registrationDate")
            log.debug("ABCP yield user: userId=%r, registrationDate=%r", user_id, reg_date)
            yield it

        # Увеличиваем смещение на размер фактически полученной порции
        processed = len(items)
        skip += processed
        # Переходим к следующей странице
        page += 1

    # Финальный лог о завершении итерации
    log.info("ABCP iterate all users: finished at page=%s, last skip=%s", page, skip)


def iter_today_users(today: Optional[date] = None) -> Iterator[Dict[str, Any]]:
    """
    Итерирует по пользователям, зарегистрированным «сегодня».
    Клиентская фильтрация по полю 'registrationDate', начинающемуся с 'YYYY-MM-DD'.
    :param today: дата «сегодня» (для тестов можно подставить)
    :yield: словарь пользователя, отфильтрованный по текущей дате
    """
    # Если дата не передана — берём системную
    today = today or date.today()
    # Строка сравнения вида 'YYYY-MM-DD'
    today_str = today.strftime("%Y-%m-%d")

    # Логируем старт инкрементальной выборки
    log.info("ABCP iterate today users: start for date=%s", today_str)

    # Обходим всех пользователей и фильтруем по регистрации «сегодня»
    for it in iter_all_users():
        # Берём поле даты регистрации (может отсутствовать)
        reg = (it.get("registrationDate") or "").strip()
        # Идентификатор (для лога)
        user_id = it.get("userId") or it.get("userID") or it.get("id")
        # Логируем решение: совпало/не совпало
        if reg.startswith(today_str):
            log.debug("ABCP today match: userId=%r, registrationDate=%r", user_id, reg)
            # Если дата начинается с сегодня — отдаём итератором
            yield it
        else:
            log.debug("ABCP today skip:  userId=%r, registrationDate=%r", user_id, reg)

    # Логируем окончание инкрементальной выборки
    log.info("ABCP iterate today users: finished for date=%s", today_str)
