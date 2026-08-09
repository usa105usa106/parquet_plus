# SELFTEST v019

Проверено локально. Live Binance/MEXC/Deribit/Forex Factory requests из release-контейнера не выполнялись.

- `python -m compileall -q .`: **PASS**.
- AST parse всех Python-файлов: **PASS**.
- Duplicate top-level function/class scan: **PASS**.
- Telegram local verdict: отправка использует `parse_mode="HTML"`: **PASS (source/AST inspection)**.
- Формат local verdict: `LONG/SHORT`, тикер, цена, торговые числовые значения и `NO TRADE` заключены в `<b>...</b>`: **PASS**.
- Текущая цена крипто берётся из Binance Spot `ticker/24hr.lastPrice`; fallback — цена universe: **PASS (source inspection + synthetic formatting)**.
- Текущая цена XAU/XAG/USOIL берётся из MEXC Futures all-contract ticker `lastPrice`; один MEXC ticker request переиспользуется для derivatives context и commodity prices: **PASS (synthetic API payload)**.
- Tiny-price formatter: scientific notation отсутствует; `0.010000 -> $0,01`, `1.050000 -> $1,05`, `0.00000012 -> $0,00000012`: **PASS**.
- Dynamic commodity threshold: default `5.0`; `/score_s` wiring сохранён: **PASS**.
- `/log_full`: strict last-24-hours filtering и hourly rotation `backupCount=24` сохранены: **PASS (source inspection)**.
- `collector.py` byte-for-byte идентичен предыдущему релизу, SHA-256 `4433e8e4f0d7a3e78f30d786321f858b91b9fa37e8739dac04d5cb6729444fd1`: **PASS**.
- Старые current-version strings предыдущей версии отсутствуют в release text files: **PASS**.
- ZIP CRC/integrity: **PASS**.

Ограничение: реальные внешние API и Telegram `Application` в локальном тестовом окружении не запускались; сетевые пути проверены по коду и synthetic payloads.
