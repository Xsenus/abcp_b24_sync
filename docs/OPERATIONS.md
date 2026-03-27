# Эксплуатация

Документ описывает практический runbook для запуска, мониторинга и восстановления сервиса `ABCP -> Bitrix24 Sync`.

## 1. Подготовка сервера

Перед запуском убедитесь, что на сервере есть:

- Python;
- доступ в сеть к ABCP и Bitrix24;
- корректный `.env`;
- права на запись в каталог проекта, SQLite и `logs/`.

Рекомендуемая последовательность:

```bash
git pull
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Для Windows:

```powershell
git pull
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 2. Проверка конфигурации

Перед первым запуском проверьте:

- `ABCP_BASE_URL`
- `ABCP_USERLOGIN`
- `ABCP_USERPSW`
- `B24_WEBHOOK_URL`
- `B24_DEAL_CATEGORY_ID_USERS`
- `B24_DEAL_STAGE_NEW_USERS`
- UF-поля сделки
- `SQLITE_PATH`
- `SYNC_INTERVAL_SECONDS`

Для регулярной эксплуатации рекомендуется:

```text
ABCP_INITIAL_IMPORT_MODE=incremental
ABCP_INITIAL_INCREMENTAL_LOOKBACK_MINUTES=1440
ABCP_INCREMENTAL_LOOKBACK_MINUTES=15
ABCP_INCREMENTAL_OVERLAP_MINUTES=5
ABCP_INCREMENTAL_MAX_WINDOW_MINUTES=1440
SYNC_INTERVAL_SECONDS=600
RATE_LIMIT_SLEEP=3
REQUEST_ANALYTICS_ENABLED=1
```

Важно:

- если `SYNC_INTERVAL_SECONDS < 300`, сервис автоматически поднимет его до `300`;
- текущая рабочая схема инкремента использует только `updateTime`;
- это связано с live-наблюдением от **25 марта 2026**, когда `dateRegisteredStart/dateRegisteredEnd` на вашем ABCP endpoint не давал надёжной фильтрации.

## 3. Сценарии первого запуска

### Вариант A. Без полного исторического импорта

Рекомендуемый вариант для нового сервера:

```bash
python cli.py init-db
python main.py
```

Что произойдёт:

- БД создастся;
- сервис выставит стартовый checkpoint назад на `ABCP_INITIAL_INCREMENTAL_LOOKBACK_MINUTES`;
- затем начнёт регулярный `import_incremental` и `sync_to_b24`.

### Вариант B. С полной историей

Используйте, если нужно поднять полный архив пользователей:

```bash
python cli.py init-db
python cli.py import-all
python cli.py sync-b24
python main.py
```

Перед этим выставьте:

```text
ABCP_INITIAL_IMPORT_MODE=full
```

## 4. Регулярный запуск

Основной запуск:

```bash
python main.py
```

Linux-обёртка:

```bash
./scripts/run_service.sh
```

Цикл `main.py`:

1. `import_incremental`
2. `sync_to_b24`
3. сон до следующего тика

## 5. Ручные команды оператора

Полный импорт:

```bash
python cli.py import-all
```

Инкрементальный импорт:

```bash
python cli.py import-incremental
```

Синхронизация в Bitrix24:

```bash
python cli.py sync-b24
```

Осторожный прогон небольшой пачки:

```bash
python cli.py sync-b24 --limit 10
```

Полный ручной цикл:

```bash
python cli.py run
```

Импорт тестового JSON в SQLite:

```bash
python dev_load_from_file.py --path data/sample.json --log-level INFO
```

## 6. Что смотреть сразу после запуска

### Основной лог сервиса

```bash
tail -f logs/service.log
```

Ищите строки:

- `Initial full import: done`
- `Initial full import skipped: incremental bootstrap checkpoint=...`
- `Tick: import_incremental done`
- `Tick: sync_to_b24 done`
- `Tick finished in ... ms`

### Логи ручных запусков

```bash
tail -f logs/sync_$(date +%F).log
```

### HTTP-аналитика

```bash
tail -f logs/http_requests/request_analytics_$(date +%F).log
```

В аналитике видно:

- когда ушёл запрос;
- куда именно он ушёл;
- с каким payload;
- какой был ответ;
- успех или ошибка;
- HTTP-статус;
- номер попытки;
- длительность.

## 7. Где лежат логи

### Daemon

```text
logs/service.log
logs/service.log.YYYY-MM-DD
```

Особенности:

- ротация по полуночи;
- хранение последних `14` файлов.

### CLI

```text
logs/sync_YYYY-MM-DD.log
```

### HTTP-аналитика

```text
logs/http_requests/request_analytics_YYYY-MM-DD.log
```

## 8. PowerShell-команды для эксплуатации

Следить за сервисом:

```powershell
Get-Content .\logs\service.log -Wait
```

Последние 100 строк сервиса:

```powershell
Get-Content .\logs\service.log -Tail 100
```

Следить за CLI-логом:

```powershell
Get-Content .\logs\sync_2026-03-27.log -Tail 100 -Wait
```

Следить за HTTP-аналитикой:

```powershell
Get-Content .\logs\http_requests\request_analytics_2026-03-27.log -Tail 100 -Wait
```

## 9. Проверка очереди и состояния БД

Сколько записей в базе и сколько ждут синка:

```bash
python - << 'PY'
from config import SQLITE_PATH
from db import User, get_engine
from sqlalchemy.orm import Session

engine = get_engine(SQLITE_PATH)
with Session(engine) as session:
    print("total =", session.query(User).count())
    print("unsynced =", session.query(User).filter(User.synced == False).count())
PY
```

Если `unsynced != 0`, смотрите:

- `logs/service.log`
- `logs/sync_YYYY-MM-DD.log`
- `logs/http_requests/request_analytics_YYYY-MM-DD.log`

## 10. Как понимать техническое состояние

SQLite хранит:

- `users` — локальный снимок пользователей ABCP;
- `meta` — технические отметки.

Ключевые `meta`-ключи:

- `last_full_import_started_at`
- `last_full_import_at`
- `last_incremental_import_at`
- `last_incremental_window_end`

Что важно оператору:

- `last_incremental_window_end` — это текущий checkpoint инкремента;
- если checkpoint сильно отстал, сервис сам режет backlog по `ABCP_INCREMENTAL_MAX_WINDOW_MINUTES`;
- overlap контролируется `ABCP_INCREMENTAL_OVERLAP_MINUTES`.

## 11. Поведение Bitrix24 и защита от дублей

Проект старается не плодить дубли за счёт нескольких уровней защиты:

- контакт ищется по телефону и email;
- сделка может быть переиспользована по сохранённому `b24_deal_id`;
- если `b24_deal_id` устарел, выполняется поиск по `UF_B24_DEAL_ABCP_USER_ID`;
- если `b24_contact_id` устарел, выполняется rebinding контакта;
- при изменении `raw_json` запись снова уходит в очередь на синк.

ABCР-специфика:

- имя контакта берётся из `organizationName`;
- фамилия и отчество для таких контактов намеренно не используются;
- название сделки — `Клиент №{userId}`.

## 12. Восстановление после потери SQLite

Если локальная БД потеряна:

1. Создайте пустую БД:

```bash
python cli.py init-db
```

2. Выберите стратегию:

Полный импорт:

```bash
python cli.py import-all
```

Или возврат сразу в incremental-режим:

```bash
python main.py
```

3. Догоните Bitrix24:

```bash
python cli.py sync-b24
```

Почему это обычно безопасно:

- контакт ищется по телефону и email;
- сделка дополнительно ищется по `UF_B24_DEAL_ABCP_USER_ID`;
- stale ID не считаются фатальной ошибкой, а rebinding-ятся.

## 13. Типовые признаки проблем

### Слишком много запросов в ABCP

Проверьте:

- не запущены ли несколько копий сервиса;
- не слишком ли мал `SYNC_INTERVAL_SECONDS`;
- не гоняется ли параллельный cron;
- не выполняется ли повторно частый ручной `import-all`.

Нормальная эксплуатационная схема:

```text
ABCP_INITIAL_IMPORT_MODE=incremental
ABCP_INITIAL_INCREMENTAL_LOOKBACK_MINUTES=1440
ABCP_INCREMENTAL_LOOKBACK_MINUTES=15
ABCP_INCREMENTAL_OVERLAP_MINUTES=5
ABCP_INCREMENTAL_MAX_WINDOW_MINUTES=1440
SYNC_INTERVAL_SECONDS=600
RATE_LIMIT_SLEEP=3
```

### Очередь `unsynced` не уменьшается

Обычные причины:

- ошибка Bitrix24 по полям;
- временный сетевой сбой;
- проблема с webhook;
- stale `contact_id` или `deal_id`;
- overlap-окно ещё не дообработано следующим тиком.

Что делать:

1. Проверить `service.log`.
2. Проверить последний `sync_YYYY-MM-DD.log`.
3. Проверить HTTP-аналитику, особенно отрицательные ответы `Bitrix24`.
4. Выполнить осторожный ручной прогон:

```bash
python cli.py sync-b24 --limit 10
```

### Сервис стартует, но не двигает checkpoint

Проверьте:

- что SQLite доступна на запись;
- что `assert_config()` не падает;
- что ABCP действительно отвечает;
- что в `service.log` нет постоянных исключений в `import_incremental`.

## 14. Онлайн-проверка интеграций без записи данных

Безопасная live-проверка обычно ограничивается чтением:

- ABCP `GET /cp/users` с `limit=1`;
- Bitrix24 `crm.deal.fields`;
- Bitrix24 `crm.contact.fields`;
- Bitrix24 `crm.dealcategory.list`;
- Bitrix24 `crm.status.list`.

Это позволяет проверить:

- валиден ли ABCP логин и пароль;
- отвечает ли webhook Bitrix24;
- существует ли нужная воронка;
- существует ли нужная стадия;
- существуют ли UF-поля сделки.

## 15. Локальная техническая проверка после изменений

Проверка тестов:

```bash
python -m unittest discover -s tests -v
```

Проверка синтаксиса:

```bash
python -m compileall .
```

## 16. Безопасность

- не храните реальные секреты в документации и тикетах;
- не коммитьте `.env`, SQLite и рабочие логи;
- при компрометации webhook или пароля ABCP перевыпускайте их;
- HTTP-аналитика маскирует секреты, но это не отменяет необходимости ограничивать доступ к логам.
