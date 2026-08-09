# SELFTEST v017

Проверено локально без live-запросов внешних API.

- `python -m compileall -q .`: **PASS**.
- AST parse всех Python-файлов: **PASS**.
- Duplicate top-level function/class scan: **PASS**.
- Default MEXC Futures domain в `config.py`: `https://api.mexc.com`: **PASS**.
- Default MEXC Futures domain в `market_data.py`: `https://api.mexc.com`: **PASS**.
- Старый `https://contract.mexc.com` в runtime source отсутствует: **PASS**.
- `fetch_signal_histories()` сохраняет общий limiter и дополнительно ограничивает MEXC Futures до двух одновременных history-запросов: **PASS**.
- Synthetic concurrency test: при нескольких commodity history records одновременно активных MEXC history-вызовов было не больше 2: **PASS**.
- Crypto signal-history путь остаётся Binance Spot и не проходит через MEXC limiter: **PASS**.
- `collector.py` не редактировался; его SHA-256 сохранён: `4433e8e4f0d7a3e78f30d786321f858b91b9fa37e8739dac04d5cb6729444fd1`: **PASS**.
- `VERSION`, `APP_VERSION`, Docker label и пользовательские version strings: только `v017`: **PASS**.
- ZIP CRC/integrity и `Dockerfile` в корне: **PASS**.

Важно: `config.py` намеренно изменён, поэтому Parquet runtime теперь обращается к MEXC Futures через новый официальный домен. Формат Parquet, endpoints paths, collector logic и Gmail delivery не менялись.

Live Binance/MEXC/Deribit smoke в этой локальной проверке не выполнялся.
