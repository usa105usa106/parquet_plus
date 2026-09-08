# SELFTEST v023

Проверка выполнена после исправления top-150/top-250, Telegram→Gmail delivery и очистки `exports`.

## Проверки

- `python -m compileall` и AST parse всех Python-файлов: **PASS**.
- Вход `build_market_archive()` реально принимает `100 / 150 / 200 / 250 / 300` и отклоняет неподдерживаемый limit: **PASS**.
- Synthetic universe selection для 100/150/200/250/300: точное число активов, непрерывный `analysis_rank`: **PASS**.
- Stable/wrapped exclusions: `USDT`, `USDC`, `WBTC`, `PAXG` не входят в crypto universe: **PASS**.
- CoinGecko candidate depth сохранена: top-100 = 250, top-150/top-200 = 500, top-250/top-300 = 1000: **PASS**.
- MEXC commodities mapping не изменён: `XAU_USDT`, `SILVER_USDT`, `USOIL_USDT`: **PASS**.
- Telegram archive delivery: неопределённый read/write/network результат не повторяет ZIP; подтверждённый connect failure может безопасно повториться и восстановиться: **PASS**.
- Редкий сценарий «Telegram response uncertain»: Gmail запускается ровно один раз, Telegram ZIP не повторяется, локальный архив не очищается: **PASS**.
- Подтверждённые Telegram + Gmail: текущий и старые `market_scan_*.zip` удаляются; посторонние export-файлы и ZIP другого активного параллельного скана сохраняются: **PASS**.
- Gmail safe retry: `503 → 429 → 200` восстанавливается; connect-timeout → success восстанавливается: **PASS**.
- Gmail anti-duplicate: `ServerDisconnected/read ambiguity` прекращает send после первой попытки и даёт `GmailSendUncertain`; автоматического повторного POST нет: **PASS**.
- Постоянный Gmail `400` не повторяется: **PASS**.
- Persistent Gmail ledger: второй независимый `send_archive()` того же ZIP блокируется после `status=sent`; второй HTTP POST не выполняется: **PASS**.
- Детерминированный Gmail `Message-ID` от SHA-256 ZIP сохранён: **PASS**.
- `asset_filters.py`, `security.py`, `logging_utils.py`, `requirements.txt` byte-for-byte совпадают с предыдущим релизом: **PASS**.
- `analyzer.py`, `market_data.py`, `signal_stats.py`, `config.py` после нормализации только строки текущей версии byte-for-byte совпадают с предыдущим релизом: **PASS**.
- `collector.py` отличается от предыдущего релиза только разрешением top-150/top-250: **PASS**.
- Старых строк предыдущей версии в текущем релизе нет: **PASS**.

## Ограничение локальной проверки

Live Binance/CoinGecko/CoinPaprika/MEXC/Deribit/Telegram/Gmail запросы не выполнялись. В тестовом окружении отсутствуют `pyarrow` и `python-telegram-bot`, поэтому для импорта модулей использовались минимальные stubs; бизнес-ветки selection/delivery/Gmail были выполнены mocked/synthetic тестами без реальной отправки сообщений.
