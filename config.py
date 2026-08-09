from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

APP_VERSION = "v019"


def _normalize_public_base_url(value: str) -> str:
    value = (value or "").strip().strip('"\'')
    if not value:
        return ""
    value = value.split(",", 1)[0].strip()
    if "://" not in value:
        value = f"https://{value}"
    parsed = urlsplit(value)
    host = (parsed.hostname or "").lower()
    if not host:
        return ""
    scheme = parsed.scheme.lower() or "https"
    if host not in {"localhost", "127.0.0.1", "::1"}:
        scheme = "https"
    port = parsed.port
    if port in {80, 443}:
        port = None
    netloc = host if port is None else f"{host}:{port}"
    path = parsed.path.rstrip("/")
    for suffix in ("/gmail/callback", "/healthz"):
        if path.endswith(suffix):
            path = path[: -len(suffix)].rstrip("/")
            break
    return urlunsplit((scheme, netloc, path, "", "")).rstrip("/")


def _resolve_gmail_public_base_url() -> str:
    for key in (
        "GMAIL_PUBLIC_BASE_URL",
        "GMAIL_REDIRECT_URI",
        "SERVICE_URL_GMAILAUTH_80",
        "SERVICE_URL_GMAILAUTH",
        "SERVICE_FQDN_GMAILAUTH",
        "COOLIFY_URL",
    ):
        base = _normalize_public_base_url(os.getenv(key, ""))
        if base:
            return base
    return ""


def _endpoint(base: str, path: str) -> str:
    return f"{base.rstrip('/')}/{path.lstrip('/')}" if base else ""


@dataclass(frozen=True)
class Settings:
    telegram_bot_token: str
    admin_telegram_id: int | None
    allowed_chat_ids: frozenset[int]
    data_root: Path
    state_file: Path
    exports_dir: Path
    work_dir: Path
    logs_dir: Path
    state_dir: Path
    secrets_dir: Path
    telegram_send_limit_mb: int
    market_http_timeout: float
    max_concurrent_scans: int
    scan_cooldown_seconds: int
    user_timezone: str
    binance_spot_base_url: str
    mexc_contract_base_url: str
    coinpaprika_base_url: str
    coingecko_base_url: str
    deribit_base_url: str
    candle_limit: int
    daily_candle_limit: int
    m15_candle_limit: int
    binance_depth_limit: int
    funding_history_count: int
    secret_encryption_key: str | None
    gmail_client_id: str
    gmail_client_secret: str
    gmail_public_base_url: str
    gmail_redirect_uri: str
    gmail_health_url: str
    gmail_send_to: str | None
    gmail_auto_send_archives: bool
    gmail_max_attachment_mb: int
    gmail_oauth_listen_host: str
    gmail_oauth_listen_port: int
    gmail_backup_root: Path | None
    gmail_session_only: bool
    app_version: str

    def ensure_dirs(self) -> None:
        for path in (self.data_root, self.exports_dir, self.work_dir, self.logs_dir, self.state_dir, self.secrets_dir):
            path.mkdir(parents=True, exist_ok=True)


def _ids(raw: str) -> frozenset[int]:
    result: set[int] = set()
    for part in (raw or "").split(","):
        part = part.strip()
        if part and part.lstrip("-").isdigit():
            result.add(int(part))
    return frozenset(result)


def load_settings() -> Settings:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    admin_raw = os.getenv("ADMIN_TELEGRAM_ID", "").strip()
    admin_id = int(admin_raw) if admin_raw.lstrip("-").isdigit() else None
    allowed = _ids(os.getenv("ALLOWED_CHAT_IDS", ""))
    if admin_id is not None:
        allowed = frozenset(set(allowed) | {admin_id})

    data_root = Path(os.getenv("DATA_ROOT", "/app/storage")).expanduser().resolve()
    public_base = _resolve_gmail_public_base_url()
    backup_raw = os.getenv("GMAIL_BACKUP_ROOT", "").strip()
    backup_root = Path(backup_raw).expanduser().resolve() if backup_raw else None

    settings = Settings(
        telegram_bot_token=token,
        admin_telegram_id=admin_id,
        allowed_chat_ids=allowed,
        data_root=data_root,
        state_file=Path(os.getenv("STATE_FILE", str(data_root / "state" / "bot_state.json"))).expanduser().resolve(),
        exports_dir=data_root / "exports",
        work_dir=data_root / "work",
        logs_dir=data_root / "logs",
        state_dir=data_root / "state",
        secrets_dir=data_root / "secrets",
        telegram_send_limit_mb=max(1, int(os.getenv("TELEGRAM_SEND_LIMIT_MB", "48"))),
        market_http_timeout=max(5.0, float(os.getenv("MARKET_HTTP_TIMEOUT", "25"))),
        max_concurrent_scans=max(1, int(os.getenv("MAX_CONCURRENT_SCANS", "1"))),
        scan_cooldown_seconds=max(0, int(os.getenv("SCAN_COOLDOWN_SECONDS", "20"))),
        user_timezone=os.getenv("USER_TIMEZONE", "Europe/Moscow").strip() or "Europe/Moscow",
        binance_spot_base_url=os.getenv("BINANCE_SPOT_BASE_URL", "https://api.binance.com").strip().rstrip("/"),
        mexc_contract_base_url=os.getenv("MEXC_CONTRACT_BASE_URL", "https://api.mexc.com").strip().rstrip("/"),
        coinpaprika_base_url=os.getenv("COINPAPRIKA_BASE_URL", "https://api.coinpaprika.com/v1").strip().rstrip("/"),
        coingecko_base_url=os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3").strip().rstrip("/"),
        deribit_base_url=os.getenv("DERIBIT_BASE_URL", "https://www.deribit.com/api/v2").strip().rstrip("/"),
        candle_limit=999,
        daily_candle_limit=365,
        m15_candle_limit=288,
        binance_depth_limit=max(100, min(1000, int(os.getenv("BINANCE_DEPTH_LIMIT", "500")))),
        funding_history_count=max(0, min(1000, int(os.getenv("MEXC_FUNDING_HISTORY_COUNT", "30")))),
        secret_encryption_key=(
            os.getenv("SECRET_ENCRYPTION_KEY", "").strip()
            or os.getenv("GMAIL_STABLE_ENCRYPTION_KEY", "").strip()
            or os.getenv("SERVICE_REALBASE64_32_GMAIL-STORE", "").strip()
            or None
        ),
        gmail_client_id=os.getenv("GMAIL_CLIENT_ID", "").strip(),
        gmail_client_secret=os.getenv("GMAIL_CLIENT_SECRET", "").strip(),
        gmail_public_base_url=public_base,
        gmail_redirect_uri=_endpoint(public_base, "/gmail/callback"),
        gmail_health_url=_endpoint(public_base, "/healthz"),
        gmail_send_to=os.getenv("GMAIL_SEND_TO", "").strip() or None,
        gmail_auto_send_archives=os.getenv("GMAIL_AUTO_SEND_ARCHIVES", "true").strip().lower() in {"1", "true", "yes", "on"},
        gmail_max_attachment_mb=max(1, int(os.getenv("GMAIL_MAX_ATTACHMENT_MB", "24"))),
        gmail_oauth_listen_host=os.getenv("GMAIL_OAUTH_LISTEN_HOST", "0.0.0.0").strip() or "0.0.0.0",
        gmail_oauth_listen_port=int(os.getenv("GMAIL_OAUTH_LISTEN_PORT", "80")),
        gmail_backup_root=backup_root,
        gmail_session_only=os.getenv("GMAIL_SESSION_ONLY", "true").strip().lower() in {"1", "true", "yes", "on"},
        app_version=APP_VERSION,
    )
    settings.ensure_dirs()
    return settings
