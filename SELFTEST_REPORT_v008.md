# SELFTEST v008

Проверено перед упаковкой:

- `python -m compileall -q .` — успешно.
- По проекту не осталось старого номера версии; `VERSION`, `config.py`, Docker label, analyzer, signal stats, prompt и документация используют `v008`.
- Stablecoin/wrapped-фильтр вынесен в единый `asset_filters.py` и подключён к `collector.py` и `market_data.py`.
- Синтетические проверки фильтра прошли для 25 случаев: `USDT`, `USDC`, `USDE`, `USDS`, `FDUSD`, `PYUSD`, `RLUSD`, `BFUSD`, `USD1`, `CRVUSD`, `EURC`, `EURI`, `AEUR`, `DAI`, `FRAX`, `GHO`, `MIM`, `DOLA` исключаются; `USUAL`, `BTC`, `ETH`, `GRAM`, `AERO` не считаются stablecoins; `PAXG` и `XAUT` исключаются как tokenized-underlying assets.
- В Parquet-сборщике top-100 добирается следующими по капитализации кандидатами после исключения stablecoin/wrapped активов.
- Ограничение `999` осталось мягким: OHLCV сохраняет `min(доступная история, 999)` закрытых 1H-свечей; при меньшем количестве ставится `PARTIAL_HISTORY`, но актив из universe не выбрасывается.
- Текущая открытая 1H-свеча по-прежнему исключается.
- Gmail import, `/log_full`, отправка ZIP в Telegram/Gmail, таймер и прочая логика не менялись относительно предыдущей версии.

Live-проверка внешних API в этой локальной среде не выполнялась; изменение v008 касается фильтра universe и документации политики неполной истории.
