# SELFTEST v007

Проверено локально перед упаковкой:

- `python -m compileall -q .` — успешно.
- По всему проекту отсутствуют остаточные строки версии `v006`; `VERSION`, `config.py`, Docker label, prompt и документация используют `v007`.
- В Telegram-обработчике кнопка **Почта** при отсутствии Gmail сразу переводит пользователя в `gmail_import_bundle` и отвечает `📥 Gmail import · v007`.
- Старый пользовательский OAuth/callback-flow удалён из интерфейса. Старые callback-id (`gmail_check`, `gmail_config`, `gmail_login`, `gmail_oauth_setup`) блокируются и направляют на обычную кнопку **Почта**.
- HTTP route `/gmail/callback` в v007 не регистрируется; port 80 остаётся только для `/healthz`.
- Синтетический архив формата `tar.gz -> Base64` с `gmail_client.enc.json`, `gmail_oauth.enc.json`, `fernet.key` успешно импортируется через `SecretStore.import_gmail_runtime_bundle()`.
- После импорта Gmail Client/OAuth остаются только в памяти; encrypted Gmail-файлы из импорта не записываются на диск. После `clear_runtime_gmail()` данные исчезают.
- Проверена фильтрация логов: refresh token, access token, Client Secret, Fernet key, Bearer token и длинный Base64 bundle не остаются в результате `redact_text()`.
- Команда `/log_full` зарегистрирована в `app.py`.

Ограничение среды проверки: пакет `python-telegram-bot` в локальной среде не установлен, поэтому полный live-запуск Telegram polling здесь не выполнялся. В Dockerfile зависимости устанавливаются из `requirements.txt` при сборке Coolify.
