# SELFTEST v005

Дата проверки: 2026-08-07 (МСК)

## Итог

Три последовательных прогона сборки market-scan архива в изолированной Python virtual environment завершились без исключений и без нарушений проверяемых инвариантов.

| Прогон | Crypto universe | Binance candles | MEXC funding matches* | Commodities | Telegram | Gmail |
|---|---:|---:|---:|---:|---|---|
| 1 | 100 | 100/100 полных наборов | 88/100 | 3/3 | `.zip` | `.zip.jpg` |
| 2 | 100 | 100/100 полных наборов | 88/100 | 3/3 | `.zip` | `.zip.jpg` |
| 3 | 100 | 100/100 полных наборов | 88/100 | 3/3 | `.zip` | `.zip.jpg` |

\* В self-test upstream-ответы эмулируются, поэтому `88/100` — тестовое покрытие сценария OK + NOT_ON_MEXC, а не утверждение о текущем реальном количестве контрактов MEXC.

Проверено дополнительно:

- Telegram получает файл с окончанием `.zip`, без `.jpg`.
- Gmail получает те же байты с именем `.zip.jpg` и MIME `image/jpeg`.
- SHA-256/байтовая идентичность Telegram ZIP и Gmail attachment сохранена.
- `PROMPT_FOR_CHATGPT.txt` включается в архив.
- версия проекта в текущих компонентах — `v005`.
- клавиатура: `Анализ | Время`, `Parquet | Почта`, `Пинг`.
- в `compose.yaml` единственная обязательная пользовательская подстановка — `TELEGRAM_BOT_TOKEN`.
- Gmail Client ID/Secret и refresh token могут храниться в persistent storage; локальный ключ шифрования переживает повторное создание SecretStore без отдельной env-переменной.
- `SERVICE_URL_GMAILAUTH_80` оставлен как Coolify magic variable для публичного callback на container port 80.

## Ограничение тестовой среды

В текущем CAAS runtime отсутствует outbound DNS и нет установленных `pyarrow`/`python-telegram-bot`, поэтому три прогона проверяли реальную логику сборщика/ZIP/Gmail-инварианты с эмулированными upstream HTTP-ответами и test-only Parquet writer. Это не выдаётся за live-запросы Binance/MEXC. Синтаксис Python, YAML/Compose и релизная структура проверяются отдельно; версии зависимостей и поведение Coolify magic variables сверены с актуальной публичной документацией.

Синтетические размеры и длительности из JSON-отчёта не являются прогнозом production-размера/скорости.
