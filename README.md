# ABCP -> Bitrix24 Sync

Сервис синхронизирует пользователей из ABCP в локальную SQLite и затем выгружает их в Bitrix24:

- создаёт или обновляет контакт;
- создаёт или обновляет сделку в воронке "Пользователи";
- хранит локальные связи `b24_contact_id` и `b24_deal_id`, чтобы не дублировать сущности.

## Как это работает сейчас

Текущая рабочая схема после live-проверки:

1. Один раз выполняется полный импорт всех пользователей ABCP.
2. Дальше сервис работает инкрементально по `updateTime`.
3. На каждом тике берётся окно изменений с overlap, чтобы не потерять записи на границе запусков.
4. В Bitrix24 синхронизируются только записи, у которых реально изменился исходный JSON.

Важно:

- По live-проверке от **25 марта 2026** параметры `dateRegisteredStart/dateRegisteredEnd` на вашем ABCP endpoint фактически не фильтровали выборку и возвращали почти весь массив пользователей.
- Поэтому регулярный инкремент в проекте переведён на `dateUpdatedStart/dateUpdatedEnd`.
- Для новых регистраций этого достаточно, потому что у новых пользователей `updateTime` совпадает с `registrationDate`.

## Основные команды

Инициализация БД:

```bash
python cli.py init-db
```

Полный импорт из ABCP:

```bash
python cli.py import-all
```

Инкрементальный импорт по окну изменений:

```bash
python cli.py import-incremental
```

Старый алиас команды тоже оставлен:

```bash
python cli.py import-today
```

Синхронизация в Bitrix24:

```bash
python cli.py sync-b24
```

Синхронизация с лимитом:

```bash
python cli.py sync-b24 --limit 100
```

Полный цикл вручную:

```bash
python cli.py run
```

## Регулярный запуск

Основной daemon-режим:

```bash
python main.py
```

Что делает `main.py`:

- проверяет конфиг;
- поднимает SQLite;
- если полного импорта ещё не было, делает его один раз;
- затем запускает бесконечный цикл:
  - `import-incremental`
  - `sync-b24`

Интервал по умолчанию:

```text
SYNC_INTERVAL_SECONDS=600
```

Это 10 минут. Если нужно, можно изменить в `.env`.

Для Linux есть обёртка:

```bash
./scripts/run_service.sh
```

Скрипт просто выставляет интервал по умолчанию и запускает `main.py`.

## Как запускать с нуля

### Windows PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
python cli.py init-db
python cli.py import-all
python cli.py sync-b24
python main.py
```

### Linux

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python cli.py init-db
python cli.py import-all
python cli.py sync-b24
python main.py
```

## Логи

### Логи daemon-режима

`main.py` пишет в:

```text
logs/service.log
```

После ротации остаются файлы вида:

```text
logs/service.log.YYYY-MM-DD
```

Смотреть логи в PowerShell:

```powershell
Get-Content .\logs\service.log -Wait
```

Смотреть последние строки:

```powershell
Get-Content .\logs\service.log -Tail 100
```

Смотреть логи в Linux:

```bash
tail -f logs/service.log
```

### Логи CLI-команд

CLI по умолчанию пишет в файл:

```text
logs/sync_YYYY-MM-DD.log
```

Пример просмотра в PowerShell:

```powershell
Get-Content .\logs\sync_2026-03-25.log -Tail 100 -Wait
```

Пример просмотра в Linux:

```bash
tail -f logs/sync_$(date +%F).log
```

Можно указать свой файл:

```bash
python cli.py --log-level INFO --log-file logs/manual_sync.log sync-b24
```

## Конфигурация

Все рабочие переменные перечислены в `.env.example`.

Ключевые настройки:

```text
ABCP_BASE_URL
ABCP_USERLOGIN
ABCP_USERPSW
ABCP_LIMIT
ABCP_INCREMENTAL_LOOKBACK_MINUTES
ABCP_INCREMENTAL_OVERLAP_MINUTES

B24_WEBHOOK_URL
B24_DEAL_CATEGORY_ID_USERS
B24_DEAL_STAGE_NEW_USERS

UF_B24_DEAL_ABCP_USER_ID
UF_B24_DEAL_INN
UF_B24_DEAL_SALDO
UF_B24_DEAL_REG_DATE
UF_B24_DEAL_UPDATE_TIME

SQLITE_PATH
SYNC_INTERVAL_SECONDS
```

Рекомендуемые значения для регулярной работы:

```text
ABCP_INCREMENTAL_LOOKBACK_MINUTES=15
ABCP_INCREMENTAL_OVERLAP_MINUTES=5
SYNC_INTERVAL_SECONDS=600
RATE_LIMIT_SLEEP=3
```

## Поведение Bitrix24

Сервис не должен плодить дубли при нормальной работе, потому что:

- контакт переиспользуется по `b24_contact_id`, а при первом создании ищется по телефону/email;
- сделка переиспользуется по `b24_deal_id`;
- если локальная БД потеряна, сделка дополнительно ищется по `UF_B24_DEAL_ABCP_USER_ID`.

## Проверка состояния

Сколько записей в локальной базе и есть ли очередь на синк:

```bash
python - << 'PY'
from config import SQLITE_PATH
from db import get_engine, User
from sqlalchemy.orm import Session

engine = get_engine(SQLITE_PATH)
with Session(engine) as session:
    total = session.query(User).count()
    unsynced = session.query(User).filter(User.synced == False).count()
    print("total =", total)
    print("unsynced =", unsynced)
PY
```

## Эксплуатационная документация

Подробные инструкции по запуску и сопровождению:

[docs/OPERATIONS.md](docs/OPERATIONS.md)

## Безопасность

- `.env` не коммитится.
- В репозиторий попадает только `.env.example`.
- SQLite-файл тоже не коммитится.
- В логах не должны появляться секреты webhook/API.
