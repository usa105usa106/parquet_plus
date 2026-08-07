# Coolify deployment — v005

## Что вводить в Coolify

Только одну пользовательскую переменную:

```env
TELEGRAM_BOT_TOKEN=123456:...
```

Больше вручную в Environment Variables ничего задавать не требуется.

## Что делает compose автоматически

- контейнер слушает port `80` для `/healthz` и `/gmail/callback`;
- `SERVICE_URL_GMAILAUTH_80` — magic environment variable Coolify, поэтому Coolify генерирует публичный URL и проксирует его на container port 80;
- данные, состояние таймера, Gmail Client ID/Secret, OAuth refresh token и ledger защиты от дублей сохраняются в persistent volumes;
- ключ шифрования создаётся локально в persistent storage, если внешний ключ не задан;
- Gmail auto-send включён;
- лимит Gmail-вложения бота — 24 MB;
- часовой пояс интерфейса — Europe/Moscow.

## После Deploy

1. Открыть Telegram-бота и отправить `/start`.
2. Нажать **Почта** (то же самое, что `/gmail`).
3. Бот покажет проверку публичного callback Coolify.
4. После успешной проверки бот покажет точный Google Redirect URI.
5. В Google Cloud создать OAuth Client типа **Web application** и добавить показанный Redirect URI.
6. Client ID и Client Secret отправить боту в Telegram по его запросу; сообщения с секретами бот удаляет.
7. Войти через Google и разрешить `gmail.send`.

Для работы Gmail у Coolify должен быть настроен рабочий публичный HTTPS/wildcard domain, из которого magic `SERVICE_URL_*` может создать URL. Сам URL вручную в environment variables вводить не нужно.
