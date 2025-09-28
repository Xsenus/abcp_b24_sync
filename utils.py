#!/usr/bin/env python
# utils.py

# Работа с переменными окружения и временем ожидания
import os
import time
# Логирование для диагностических сообщений
import logging
# Типы для корректной аннотации и проверки Pylance
from typing import Callable, TypeVar, Any

# Тип-параметр для обобщённой функции с повторами (возвращает значение того же типа, что и исходная функция)
T = TypeVar("T")

# Модульный логгер (имя = utils)
log = logging.getLogger(__name__)


def getenv_str(key: str, default: str | None = None) -> str | None:
    """
    Безопасно получает строковую переменную окружения.
    Пустая строка трактуется как отсутствие значения → возвращается default.
    """
    val = os.getenv(key)                               # читаем значение из окружения
    return val if val not in (None, "") else default   # нормализуем: '' → default


def getenv_int(key: str, default: int | None = None) -> int | None:
    """
    Получает переменную окружения и приводит к int.
    При отсутствии/ошибке парсинга возвращает default.
    """
    val = os.getenv(key)                               # читаем как строку
    try:
        return int(val) if val not in (None, "") else default   # пробуем привести к int
    except Exception:
        # Лог в DEBUG, чтобы не засорять INFO предупреждениями при нечисловых значениях
        log.debug("getenv_int: key=%r не удалось привести значение %r к int — возвращаю default=%r",
                  key, val, default)
        return default


def getenv_float(key: str, default: float | None = None) -> float | None:
    """
    Получает переменную окружения и приводит к float.
    При отсутствии/ошибке парсинга возвращает default.
    """
    val = os.getenv(key)                               # читаем как строку
    try:
        return float(val) if val not in (None, "") else default  # пробуем привести к float
    except Exception:
        log.debug("getenv_float: key=%r не удалось привести значение %r к float — возвращаю default=%r",
                  key, val, default)
        return default


def with_retries(fn: Callable[[], T], *, retries: int, backoff: float) -> T:
    """
    Универсальная обёртка для повторного выполнения функции без аргументов.
    :param fn: вызываемая функция (без параметров), может бросать исключения
    :param retries: количество повторов ПОВЕРХ первой попытки (т.е. будет максимум retries попыток)
    :param backoff: базовая задержка между попытками (секунды), умножается на номер попытки (1..retries)
    :return: результат fn() при успешном выполнении
    :raises: последнее пойманное исключение, если все попытки исчерпаны
    Пример: with_retries(lambda: requests.get(...), retries=3, backoff=1.5)
            даст до 3 попыток с паузами 1.5s, 3.0s, 4.5s между ними.
    """
    last_exc: Exception | None = None                  # сюда сохраняем последнюю ошибку
    for attempt in range(1, retries + 1):              # нумерация попыток с 1
        try:
            log.debug("with_retries: attempt=%d/%d — START", attempt, retries)
            result = fn()                              # выполняем функцию
            log.debug("with_retries: attempt=%d/%d — SUCCESS", attempt, retries)
            return result                              # успех — возвращаем результат
        except Exception as e:
            last_exc = e                               # сохраняем исключение для последующего выброса
            # В WARNING фиксируем саму ошибку; стек трейс обычно печатается на верхнем уровне
            logging.warning("Attempt %d/%d failed: %s", attempt, retries, e)
            # Вычисляем задержку: backoff * номер попытки; не допускаем отрицательных значений
            delay = max(0.0, backoff * attempt)
            if attempt < retries:                      # если будут ещё попытки — спим
                log.debug("with_retries: sleeping %.3fs before next attempt", delay)
                time.sleep(delay)

    # Если дошли сюда — все попытки неудачны; last_exc должен быть установлен
    assert last_exc is not None
    log.error("with_retries: all %d attempts failed; raising last exception: %s", retries, last_exc)
    raise last_exc
