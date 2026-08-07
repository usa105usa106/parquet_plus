# SELFTEST v006

Дата сборки: 2026-08-08.

## Выполнено в текущем окружении

- `python -m compileall -q .` — успешно для всех Python-файлов.
- Проверен синтетический экспорт старой Gmail-авторизации в том же формате `tar.gz -> Base64`, который выдаёт команда из Termius.
- `SecretStore.import_gmail_runtime_bundle()` успешно расшифровывает synthetic `gmail_client.enc.json`, `gmail_oauth.enc.json` и соответствующий `fernet.key`.
- Проверено, что импортированные Client ID/Secret и OAuth token остаются только в памяти: Gmail-файлы на диск не создаются.
- Проверена имитация restart/redeploy созданием нового `SecretStore` на тех же каталогах: импортированная Gmail-сессия отсутствует.
- Проверена ротационная запись `full.log` и построение отчёта `/log_full`.
- Проверено редактирование секретов в `full.log`: refresh token, access token, Client Secret, Bearer Authorization и длинный Base64 import не сохраняются.
- Проверено редактирование секретов в отдельном `mail.log` Gmail-аудита.
- Проверен поиск по релизу: старой строки версии в коде/документации v006 нет.

## Не выполнялось в CAAS

Полный запуск Telegram polling + живые Binance/MEXC/Gmail-запросы здесь не выполнялся: в локальном runtime отсутствует `python-telegram-bot` и `pyarrow`, а внутренний package mirror не отдаёт `python-telegram-bot==22.8`. Это не выдаётся за live/integration test.

`docker compose config` также не запускался, потому что Docker Compose в текущем runtime недоступен. YAML релиза сохранён в простом Compose-формате, синтаксис Python проверен отдельно.
