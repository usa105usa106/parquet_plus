# SELFTEST v026

Проверка выполнена после точечных исправлений Gmail anti-duplicate, ручного Binance 7d breadth и времени последнего успешного Parquet в `/status`.

## Проверки

- `python -m compileall` и AST parse всех Python-файлов: **PASS**.
- Версия `v026`: `VERSION`, `config.py`, Docker label, analyzer/signal-stats, docs и prompt обновлены; строк предыдущей версии в релизе не осталось: **PASS**.
- Gmail HTTP `408`: ровно **1 POST**, затем `GmailSendUncertain`, автоматического повтора нет: **PASS**.
- Gmail HTTP `503`: ровно **1 POST**, затем `GmailSendUncertain`, автоматического повтора нет: **PASS**.
- Gmail `429 → 429 → 200`: разрешённые безопасные retry выполняются и завершаются успехом: **PASS**.
- Gmail постоянный `429`: максимум 4 попытки по существующей схеме `2 → 5 → 12`, затем обычная ошибка без статуса «возможно доставлено»: **PASS**.
- Неоднозначные network/read/write ошибки после начала Gmail POST по-прежнему не повторяются; deterministic `Message-ID` и persistent ledger не изменены: **PASS**.
- Ручной Binance 7d: при 8 закрытых дневных close рассчитывается `change_7d_pct = latest / close_7_intervals_ago - 1`: **PASS**.
- Синтетика BTC `100 → 107` даёт `+7.0%`, ETH `200 → 180` даёт `-10.0%`; breadth получает `positive_7d_pct=50%`, median `-1.5%`, BTC 7d `+7.0%`: **PASS**.
- При истории короче 8 закрытых 1D свечей 7d не выдумывается и остаётся `None`: **PASS**.
- `/status`: `last_parquet_at` и `last_parquet_duration_seconds` находятся под guard `if parquet_built`; неудачная сборка не перезаписывает время последнего успешного ZIP: **PASS**.
- Основной локальный Анализ (`_perform_analysis`) AST-identical предыдущему релизу: **PASS**.
- Основной `build_market_archive()`, top-selection, breadth core, MEXC OHLCV/commodities AST-identical предыдущему релизу: **PASS**.
- Telegram archive delivery, `_send_archive_then_gmail()`, ручные `/binance`/`/mexc`, top-toggle и auto-loop AST-identical предыдущему релизу: **PASS**.
- Имена ручных архивов остаются `manual_scan_<exchange>_....zip`; Gmail продолжает получать те же ZIP-байты с добавлением `.jpg`: **PASS**.
- Candidate pool основного скана не изменён: top-100/150 = 500, top-200/250/300 = 1000: **PASS**.
- Очистка `market_scan_*.zip` и `manual_scan_*.zip` после dual-confirmed Telegram+Gmail delivery не изменялась: **PASS**.

## Ограничение локальной проверки

Live Binance/MEXC/Telegram/Gmail запросы не выполнялись. В текущем окружении отсутствуют `pyarrow` и `python-telegram-bot`; для unit-проверки функций `collector.py` использовался минимальный `pyarrow` stub. Runtime-зависимости установятся штатно из `requirements.txt` при Docker build.
