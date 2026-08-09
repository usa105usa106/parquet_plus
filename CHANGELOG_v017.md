# v017

- Обновлён default-домен MEXC Futures с устаревшего `https://contract.mexc.com` на официальный `https://api.mexc.com`.
- Изменение применяется и к локальному анализу, и к Parquet-сборщику через общий `MEXC_CONTRACT_BASE_URL`; пути API и логика данных не менялись.
- В `fetch_signal_histories()` добавлен отдельный лимитер MEXC Futures (`Semaphore(2)`) поверх общего лимитера истории, чтобы несколько незавершённых XAU/XAG/USOIL-сигналов не создавали burst запросов.
- Остальная логика предыдущего релиза сохранена: локальные XAU/XAG/USOIL работают через MEXC Futures, Yahoo отсутствует, Parquet-структура/Gmail/таймер/мастер-промпт не менялись по логике.
- Номер версии обновлён в коде, Docker metadata, документации, prompt, diagnostic strings и self-test отчётах.
