from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Iterable


# Defense-in-depth redaction. Sensitive Telegram import payloads are never passed
# to loggers in the first place; these patterns protect against accidental
# inclusion by exception text or third-party libraries.
_REDACTIONS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"(?i)(Authorization\s*[:=]\s*[\"']?Bearer\s+)[A-Za-z0-9._~+/=-]+"), r"\1<REDACTED>"),
    (re.compile(r"(?i)([\"']authorization[\"']\s*:\s*[\"']Bearer\s+)[A-Za-z0-9._~+/=-]+"), r"\1<REDACTED>"),
    (re.compile(r"(?i)(Bearer\s+)[A-Za-z0-9._~+/=-]{16,}"), r"\1<REDACTED>"),
    (re.compile(r"(?i)([\"']?(?:refresh_token|access_token|client_secret|fernet_key|secret_encryption_key|telegram_bot_token|api_secret)[\"']?\s*[:=]\s*[\"']?)([^\"'\s,}\]]+)"), r"\1<REDACTED>"),
    (re.compile(r"\b\d{6,}:[A-Za-z0-9_-]{20,}\b"), "<TELEGRAM_TOKEN_REDACTED>"),
    (re.compile(r"\bGOCSPX-[A-Za-z0-9_-]{8,}\b"), "<GOOGLE_CLIENT_SECRET_REDACTED>"),
    (re.compile(r"\bgAAAAA[A-Za-z0-9_-]{40,}\b"), "<FERNET_TOKEN_REDACTED>"),
    # The import bundle is a long Base64 blob. It should never be logged, but
    # redact any such accidental blob if one reaches a formatter.
    (re.compile(r"(?<![A-Za-z0-9+/=_-])[A-Za-z0-9+/=_-]{180,}(?![A-Za-z0-9+/=_-])"), "<LONG_SECRET_REDACTED>"),
)


def redact_text(value: str) -> str:
    text = str(value)
    for pattern, replacement in _REDACTIONS:
        text = pattern.sub(replacement, text)
    return text


class RedactingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return redact_text(super().format(record))


def _formatter() -> RedactingFormatter:
    return RedactingFormatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def configure_console_logging(level_name: str = "INFO") -> None:
    level = getattr(logging, str(level_name).upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(_formatter())
        root.addHandler(handler)
    else:
        for handler in root.handlers:
            handler.setLevel(level)
            handler.setFormatter(_formatter())


def enable_full_file_logging(logs_dir: Path, level_name: str = "INFO") -> Path:
    logs_dir.mkdir(parents=True, exist_ok=True)
    path = logs_dir / "full.log"
    level = getattr(logging, str(level_name).upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)

    resolved = str(path.resolve())
    for handler in root.handlers:
        if isinstance(handler, RotatingFileHandler) and str(Path(handler.baseFilename).resolve()) == resolved:
            return path

    handler = RotatingFileHandler(path, maxBytes=12 * 1024 * 1024, backupCount=5, encoding="utf-8")
    handler.setLevel(level)
    handler.setFormatter(_formatter())
    root.addHandler(handler)
    return path


def _rotated_files(path: Path) -> Iterable[Path]:
    for idx in range(5, 0, -1):
        candidate = path.with_name(f"{path.name}.{idx}")
        if candidate.is_file():
            yield candidate
    if path.is_file():
        yield path


def build_full_log_report(log_path: Path, exports_dir: Path, *, max_bytes: int = 20 * 1024 * 1024) -> Path:
    exports_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    report = exports_dir / f"full_log_{stamp}.txt"
    chunks: list[str] = [
        "FULL OPERATION LOG",
        "Sensitive OAuth/session data, encryption keys and access tokens are redacted or never logged.",
        "=" * 88,
    ]
    used = 0
    for source in _rotated_files(log_path):
        data = source.read_bytes()
        remaining = max_bytes - used
        if remaining <= 0:
            chunks.append("[log output truncated]")
            break
        if len(data) > remaining:
            data = data[-remaining:]
            chunks.append(f"\n--- {source.name} (tail; truncated) ---")
        else:
            chunks.append(f"\n--- {source.name} ---")
        text = data.decode("utf-8", errors="replace")
        chunks.append(redact_text(text))
        used += min(len(data), remaining)
    if used == 0:
        chunks.append("No operations have been logged yet.")
    report.write_text("\n".join(chunks).rstrip() + "\n", encoding="utf-8")
    return report
