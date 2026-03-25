# Эксплуатация

## Рекомендуемый порядок запуска на сервере

1. Обновить код:

```bash
git pull
```

2. Проверить виртуальное окружение и зависимости:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. Проверить `.env`.

4. Если это самый первый запуск:

```bash
python cli.py init-db
python cli.py import-all
python cli.py sync-b24
```

5. Для постоянной работы:

```bash
python main.py
```

или:

```bash
./scripts/run_service.sh
```

## Что смотреть после запуска

### 1. Основной лог сервиса

```bash
tail -f logs/service.log
```

Ищем строки:

- `Initial full import: done`
- `Tick: import_incremental done`
- `Tick: sync_to_b24 done`

### 2. Логи ручных прогонов

```bash
tail -f logs/sync_$(date +%F).log
```

### 3. Есть ли зависшая очередь

```bash
python - << 'PY'
from config import SQLITE_PATH
from db import get_engine, User
from sqlalchemy.orm import Session

engine = get_engine(SQLITE_PATH)
with Session(engine) as session:
    print(session.query(User).filter(User.synced == False).count())
PY
```

Если ответ не `0`, нужно смотреть свежие ошибки в логе.

## Регулярные ручные команды

Проверить, что ABCP отдаёт свежие изменения:

```bash
python cli.py import-incremental
```

Досинхронизировать очередь в Bitrix24:

```bash
python cli.py sync-b24
```

Прогон маленькой пачкой:

```bash
python cli.py sync-b24 --limit 10
```

## Где лежат логи

### Daemon

```text
logs/service.log
logs/service.log.YYYY-MM-DD
```

### CLI

```text
logs/sync_YYYY-MM-DD.log
```

## PowerShell-команды для логов

Следить за daemon-логом:

```powershell
Get-Content .\logs\service.log -Wait
```

Последние 100 строк:

```powershell
Get-Content .\logs\service.log -Tail 100
```

Следить за CLI-логом:

```powershell
Get-Content .\logs\sync_2026-03-25.log -Tail 100 -Wait
```

## Важные особенности текущей схемы

- Инкремент работает по `dateUpdatedStart/dateUpdatedEnd`.
- На вашем ABCP endpoint фильтр `dateRegisteredStart/dateRegisteredEnd` по live-проверке от 25 марта 2026 не дал корректной серверной фильтрации и возвращал почти весь массив пользователей.
- Из-за этого в боевой логике используется только `updateTime`.
- Новые регистрации не теряются, потому что у новых пользователей `updateTime` совпадает с `registrationDate`.

## Восстановление после потери локальной БД

Если локальная SQLite потеряна:

1. Поднять пустую БД:

```bash
python cli.py init-db
```

2. Сделать полный импорт:

```bash
python cli.py import-all
```

3. Сделать синк в Bitrix24:

```bash
python cli.py sync-b24
```

Почему это безопаснее, чем раньше:

- контакт ищется по телефону/email;
- сделка дополнительно ищется по `UF_B24_DEAL_ABCP_USER_ID`.

## Типовые признаки проблемы

### Слишком много запросов в ABCP

Проверить:

- не запущен ли старый внешний цикл с частотой меньше 5 минут;
- не выставлен ли слишком маленький `SYNC_INTERVAL_SECONDS`;
- не был ли вручную включён старый способ опроса полного списка.

Нормальная схема сейчас:

```text
SYNC_INTERVAL_SECONDS=600
ABCP_INCREMENTAL_LOOKBACK_MINUTES=15
ABCP_INCREMENTAL_OVERLAP_MINUTES=5
```

### Несинхронизированные записи висят в очереди

Смотреть:

- `logs/service.log`
- `logs/sync_YYYY-MM-DD.log`

Обычно причина одна из трёх:

- ошибка Bitrix24 по полям;
- временная сетевая ошибка;
- новые данные пришли в overlap-окне и ещё не были досинхронизированы следующим тиком.
