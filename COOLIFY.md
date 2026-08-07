# Coolify deployment — v006

## Что вводить в Coolify

Только одну пользовательскую переменную:

```env
TELEGRAM_BOT_TOKEN=123456:...
```

Остальные значения уже заданы внутри проекта.

## Порты

- приложение слушает container port `80`;
- `/healthz` используется для проверки приложения;
- `/gmail/callback` нужен только для запасного обычного Google OAuth-flow;
- `SERVICE_URL_GMAILAUTH_80` позволяет Coolify направить публичный HTTPS URL на container port 80.

## Быстрый Gmail import

1. Deploy v006.
2. В Telegram нажать **Почта**.
3. Отправить одной строкой ранее сохранённый Base64-экспорт Gmail-авторизации старого бота.
4. Бот сразу удалит сообщение, расшифрует данные в памяти и проверит Google-сессию.
5. При успехе бот напишет `✅ Почта подключена`.

Импортированная Gmail-авторизация работает **только до следующего restart/redeploy**. Она не записывается в `/app/storage` и не попадает в журналы. После redeploy просто повторите импорт той же строки.

## Логи

`/log_full` присылает полный журнал основных операций. Файл `full.log` находится в `/app/storage/logs/` и ротируется. Gmail имеет отдельный `mail.log`.

В логи не пишутся тела секретных сообщений, OAuth access/refresh tokens, Client Secret, Fernet key/token, Bearer Authorization, Telegram bot token и длинные Base64-секреты.
