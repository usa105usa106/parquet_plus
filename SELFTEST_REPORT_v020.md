# SELFTEST v020

Проверено локально. Live Binance/MEXC/Deribit/Forex Factory и реальный Telegram `Application` из release-контейнера не запускались; сетевой retry проверен synthetic-тестом и source/AST inspection.

- `python -m compileall -q .`: **PASS**.
- AST parse всех Python-файлов: **PASS**.
- Duplicate top-level function/class scan: **PASS**.
- Telegram connect retry: **PASS (synthetic)** — две ошибки с причиной `httpx.ConnectTimeout`, паузы строго `2.0` и `3.0`, успех на третьей попытке.
- Telegram connect retry exhaustion: **PASS (synthetic)** — ровно 3 попытки, затем `None`/пропуск доставки без исключения наружу.
- Неопределённый read timeout: **PASS (synthetic)** — повтор не выполняется, чтобы не создавать риск дубля уже принятого Telegram сообщения/файла.
- Progress delivery: **PASS (synthetic/source)** — одна попытка без retry; Telegram error не прерывает анализ/Parquet.
- Финальный Анализ: `parse_mode="HTML"` сохранён; доставка идёт через connect-retry helper с `connect_timeout=5.0`: **PASS (source/AST inspection)**.
- Parquet ZIP: файл на каждой retry-попытке открывается заново; после трёх connect failures возвращается `TELEGRAM_FAILED`, Gmail этого круга не запускается: **PASS (source inspection)**.
- Auto-loop: вышедший наружу `telegram.error.NetworkError` поглощается внутри цикла, `auto-action-*` не завершается из-за временной сетевой ошибки Telegram: **PASS (source inspection)**.
- `collector.py` byte-for-byte идентичен предыдущему релизу, SHA-256 `4433e8e4f0d7a3e78f30d786321f858b91b9fa37e8739dac04d5cb6729444fd1`: **PASS**.
- `analyzer.py`, `market_data.py`, `signal_stats.py`: после нормализации `v020 →` предыдущая версия содержимое byte-for-byte совпадает с исходным релизом: **PASS**. Логика анализа/рынка не менялась.
- Current-version strings: `VERSION`, `config.py`, Docker metadata, docs, prompt и runtime переведены на `v020`; старых current-version strings в release-файлах нет: **PASS**.
- ZIP CRC/integrity: **PASS** (после сборки release-архива).

Ограничение: реальная доставка в Telegram и live market API не выполнялись в локальном тестовом окружении. `python-telegram-bot==22.8` API timeout-параметры сверены с официальной документацией; release requirements не менялись.
