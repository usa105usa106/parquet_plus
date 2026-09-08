# Coolify deployment — v023

## Что вводить в Coolify

Только одну пользовательскую переменную:

```env
TELEGRAM_BOT_TOKEN=123456:...
```

## Порт

Приложение слушает container port `80`, но в v023 он используется только для `/healthz` и Docker/Coolify healthcheck. Google OAuth callback для подключения Gmail **не используется**.

## Gmail

1. Deploy v023.
2. Сначала нажать **Пинг** и убедиться, что бот пишет `Версия: v023`.
3. Нажать **Почта**. Бот обязан ответить строкой `📥 Gmail import · v023`.
4. Отправить одним Telegram-сообщением сохранённую Base64-строку Gmail-авторизации.
5. Бот сразу удалит это сообщение, импортирует данные только в память и проверит Gmail.
6. После restart/redeploy нажать **Почта** и отправить ту же строку снова.

Если после deploy кнопка **Почта** предлагает «проверить внешний callback», значит Coolify всё ещё запустил старый образ, а не v023.

## Логи

`/log_full` присылает только последние 24 часа основных операций. `full.log` хранится в `/app/storage/logs/`, ротируется каждый час и держит 24 backup-файла.

В лог не передаются содержимое Gmail-import, OAuth access/refresh tokens, Client Secret, Fernet key/token, Bearer Authorization, Telegram bot token и длинные Base64-секреты.


## Score сырья

По умолчанию локальный порог XAU/XAG/USOIL равен `5.0`. `/score_s` показывает текущее значение, `/score_s 4.5` меняет его, `/score_s reset` возвращает `5.0`. Настройка хранится в persistent state и переживает restart/redeploy.
