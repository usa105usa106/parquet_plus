from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken


class SecretStore:
    def __init__(
        self,
        secrets_dir: Path,
        state_dir: Path,
        env_key: str | None = None,
        backup_root: Path | None = None,
    ):
        self.secrets_dir = secrets_dir
        self.state_dir = state_dir
        self.backup_root = Path(backup_root) if backup_root else None
        self.backup_secrets_dir = self.backup_root / "secrets" if self.backup_root else None
        self.backup_state_dir = self.backup_root / "state" if self.backup_root else None
        self.backup_bundle_file = self.backup_root / "gmail_bundle_backup.json" if self.backup_root else None
        self.secrets_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        if self.backup_secrets_dir:
            self.backup_secrets_dir.mkdir(parents=True, exist_ok=True)
        if self.backup_state_dir:
            self.backup_state_dir.mkdir(parents=True, exist_ok=True)

        self.key_file = self.state_dir / "fernet.key"
        self.api_file = self.secrets_dir / "mexc_api.enc.json"
        self.gmail_file = self.secrets_dir / "gmail_oauth.enc.json"
        self.gmail_client_file = self.secrets_dir / "gmail_client.enc.json"
        self.gmail_sent_file = self.state_dir / "gmail_sent_archives.json"
        self.storage_identity_file = self.state_dir / "storage_identity.json"

        # If a deployment created an empty primary bind directory, recover the
        # complete Gmail encryption bundle from the redundant named volume
        # before a new key can be generated.
        self.recovered_from_backup = self._restore_gmail_bundle_from_backup_if_needed()
        self.fernet = Fernet(self._load_or_create_key(env_key))
        self._touch_storage_identity()
        self._mirror_gmail_bundle_to_backup()

    def _read_json_file(self, path: Path) -> Any | None:
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def _write_backup_bundle(self) -> None:
        """Atomically mirror one self-contained Gmail recovery bundle.

        The encryption key and all matching encrypted files are committed in a
        single JSON rename. This avoids a crash leaving the backup key from one
        generation and ciphertext from another generation.
        """
        if not self.backup_bundle_file or not self.key_file.exists():
            return
        try:
            payload = {
                "version": 1,
                "fernet_key": self.key_file.read_text(encoding="utf-8").strip(),
                "gmail_client": self._read_json_file(self.gmail_client_file),
                "gmail_oauth": self._read_json_file(self.gmail_file),
                "gmail_sent_archives": self._read_json_file(self.gmail_sent_file),
                "storage_identity": self._read_json_file(self.storage_identity_file),
            }
            self.backup_bundle_file.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.backup_bundle_file.with_name(f".{self.backup_bundle_file.name}.tmp")
            tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
            tmp.replace(self.backup_bundle_file)
            try:
                self.backup_bundle_file.chmod(0o600)
            except Exception:
                pass
        except Exception:
            # Persistence redundancy must never break the running bot.
            pass

    def _mirror_file(self, path: Path) -> None:
        del path
        self._write_backup_bundle()

    def _delete_backup_file(self, path: Path) -> None:
        del path
        self._write_backup_bundle()

    def _restore_encrypted_record(
        self,
        record: Any,
        target: Path,
        source_fernet: Fernet,
        target_fernet: Fernet,
    ) -> None:
        if not isinstance(record, dict):
            return
        token = str(record.get("encrypted") or "").encode("utf-8")
        if not token:
            return
        plaintext = source_fernet.decrypt(token)
        restored = dict(record)
        restored["encrypted"] = target_fernet.encrypt(plaintext).decode("utf-8")
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(f".{target.name}.restore.tmp")
        tmp.write_text(json.dumps(restored, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(target)
        try:
            target.chmod(0o600)
        except Exception:
            pass

    def _restore_gmail_bundle_from_backup_if_needed(self) -> bool:
        if not self.backup_root or self.gmail_client_file.exists():
            return False
        try:
            bundle = self._read_json_file(self.backup_bundle_file) if self.backup_bundle_file else None
            if isinstance(bundle, dict) and bundle.get("gmail_client"):
                backup_key = self._coerce_fernet_key(str(bundle.get("fernet_key") or ""))
                client_record = bundle.get("gmail_client")
                oauth_record = bundle.get("gmail_oauth")
                ledger_record = bundle.get("gmail_sent_archives")
                identity_record = bundle.get("storage_identity")
            else:
                # Legacy v62 layout: separate state/secrets files in the named volume.
                if not self.backup_secrets_dir or not self.backup_state_dir:
                    return False
                backup_key_file = self.backup_state_dir / "fernet.key"
                backup_client_file = self.backup_secrets_dir / "gmail_client.enc.json"
                if not (backup_key_file.exists() and backup_client_file.exists()):
                    return False
                backup_key = self._coerce_fernet_key(backup_key_file.read_bytes())
                client_record = self._read_json_file(backup_client_file)
                oauth_record = self._read_json_file(self.backup_secrets_dir / "gmail_oauth.enc.json")
                ledger_record = self._read_json_file(self.backup_state_dir / "gmail_sent_archives.json")
                identity_record = self._read_json_file(self.backup_state_dir / "storage_identity.json")

            backup_fernet = Fernet(backup_key)
            if self.key_file.exists():
                target_key = self._coerce_fernet_key(self.key_file.read_bytes())
            else:
                target_key = backup_key
                self.key_file.write_bytes(target_key)
                try:
                    self.key_file.chmod(0o600)
                except Exception:
                    pass
            target_fernet = Fernet(target_key)

            self._restore_encrypted_record(client_record, self.gmail_client_file, backup_fernet, target_fernet)
            self._restore_encrypted_record(oauth_record, self.gmail_file, backup_fernet, target_fernet)
            if isinstance(ledger_record, dict):
                self.gmail_sent_file.parent.mkdir(parents=True, exist_ok=True)
                tmp = self.gmail_sent_file.with_name(f".{self.gmail_sent_file.name}.restore.tmp")
                tmp.write_text(json.dumps(ledger_record, indent=2, ensure_ascii=False), encoding="utf-8")
                tmp.replace(self.gmail_sent_file)
            if isinstance(identity_record, dict) and not self.storage_identity_file.exists():
                self.storage_identity_file.parent.mkdir(parents=True, exist_ok=True)
                tmp = self.storage_identity_file.with_name(f".{self.storage_identity_file.name}.restore.tmp")
                tmp.write_text(json.dumps(identity_record, indent=2, ensure_ascii=False), encoding="utf-8")
                tmp.replace(self.storage_identity_file)
            return self.gmail_client_file.exists()
        except Exception:
            return False

    def _mirror_gmail_bundle_to_backup(self) -> None:
        self._write_backup_bundle()

    def _touch_storage_identity(self) -> None:
        import secrets
        import time

        payload: dict[str, Any]
        if self.storage_identity_file.exists():
            try:
                loaded = json.loads(self.storage_identity_file.read_text(encoding="utf-8"))
                payload = loaded if isinstance(loaded, dict) else {}
            except Exception:
                payload = {}
        else:
            payload = {}
        payload.setdefault("storage_id", secrets.token_hex(8))
        payload["boot_count"] = int(payload.get("boot_count") or 0) + 1
        payload["last_boot_unix"] = int(time.time())
        payload["recovered_from_backup"] = bool(self.recovered_from_backup)
        self._atomic_write_json(self.storage_identity_file, payload, mode=0o600)

    def storage_status(self) -> dict[str, Any]:
        try:
            payload = json.loads(self.storage_identity_file.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                payload = {}
        except Exception:
            payload = {}
        payload["backup_bundle_ok"] = bool(self.backup_bundle_file and self.backup_bundle_file.is_file())
        return payload

    @staticmethod
    def _coerce_fernet_key(value: str | bytes) -> bytes:
        """Return a valid stable Fernet key from any non-empty secret.

        Coolify's SERVICE_REALBASE64 value is normally already suitable. If a
        Coolify version returns another Base64/random representation, deriving a
        SHA-256 key keeps the result deterministic across redeploys.
        """
        raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        raw = raw.strip()
        if not raw:
            raise ValueError("empty encryption key")
        try:
            Fernet(raw)
            return raw
        except Exception:
            return base64.urlsafe_b64encode(hashlib.sha256(raw).digest())

    def _load_or_create_key(self, env_key: str | None) -> bytes:
        old_key: bytes | None = None
        if self.key_file.exists():
            try:
                old_key = self._coerce_fernet_key(self.key_file.read_bytes())
            except Exception:
                old_key = None

        if env_key:
            stable_key = self._coerce_fernet_key(env_key)
            if old_key and old_key != stable_key:
                self._migrate_encrypted_files(old_key, stable_key)
            self.key_file.write_bytes(stable_key)
            try:
                self.key_file.chmod(0o600)
            except Exception:
                pass
            self._mirror_file(self.key_file)
            return stable_key

        if old_key:
            return old_key

        key = Fernet.generate_key()
        self.key_file.write_bytes(key)
        try:
            self.key_file.chmod(0o600)
        except Exception:
            pass
        self._mirror_file(self.key_file)
        return key

    def _migrate_encrypted_files(self, old_key: bytes, new_key: bytes) -> None:
        """Re-encrypt v61 secrets with the v62 stable Coolify key atomically."""
        old_fernet = Fernet(old_key)
        new_fernet = Fernet(new_key)
        for path in (self.api_file, self.gmail_file, self.gmail_client_file):
            if not path.exists():
                continue
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                token = str(data.get("encrypted") or "").encode("utf-8")
                if not token:
                    continue
                # It may already have been migrated during an interrupted deploy.
                try:
                    new_fernet.decrypt(token)
                    continue
                except InvalidToken:
                    pass
                plaintext = old_fernet.decrypt(token)
                data["encrypted"] = new_fernet.encrypt(plaintext).decode("utf-8")
                self._atomic_write_json(path, data, mode=0o600)
            except Exception:
                # Never destroy an unreadable secret. Keep a copy for manual
                # recovery and let the bot ask for credentials again if needed.
                try:
                    backup = path.with_suffix(path.suffix + ".unreadable-v62.bak")
                    if not backup.exists():
                        backup.write_bytes(path.read_bytes())
                        backup.chmod(0o600)
                except Exception:
                    pass

    @staticmethod
    def mask(value: str) -> str:
        if not value:
            return ""
        if len(value) <= 8:
            return value[:2] + "****"
        return value[:4] + "****" + value[-4:]

    def _atomic_write_json(self, path: Path, payload: Any, *, mode: int | None = None) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f".{path.name}.tmp")
        tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(path)
        if mode is not None:
            try:
                path.chmod(mode)
            except Exception:
                pass
        self._mirror_file(path)

    def _save_encrypted(self, path: Path, payload: dict[str, Any], mask: dict[str, Any]) -> dict[str, Any]:
        token = self.fernet.encrypt(json.dumps(payload, ensure_ascii=False).encode("utf-8")).decode("utf-8")
        saved = {"encrypted": token, "mask": mask}
        self._atomic_write_json(path, saved, mode=0o600)
        return mask

    def _load_encrypted(self, path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            decrypted = self.fernet.decrypt(data["encrypted"].encode("utf-8"))
            payload = json.loads(decrypted.decode("utf-8"))
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    def save_mexc_api(self, api_key: str, api_secret: str) -> dict:
        payload = {
            "api_key": api_key.strip(),
            "api_secret": api_secret.strip(),
        }
        return self._save_encrypted(
            self.api_file,
            payload,
            {
                "api_key": self.mask(payload["api_key"]),
                "api_secret": self.mask(payload["api_secret"]),
            },
        )

    def load_mexc_api_mask(self) -> dict | None:
        if not self.api_file.exists():
            return None
        data = json.loads(self.api_file.read_text(encoding="utf-8"))
        return data.get("mask")

    def load_mexc_api(self) -> dict | None:
        return self._load_encrypted(self.api_file)

    def save_gmail_client(self, client_id: str, client_secret: str) -> dict[str, str]:
        payload = {
            "client_id": client_id.strip(),
            "client_secret": client_secret.strip(),
        }
        return self._save_encrypted(
            self.gmail_client_file,
            payload,
            {
                "client_id": self.mask(payload["client_id"]),
                "client_secret": self.mask(payload["client_secret"]),
            },
        )

    def load_gmail_client(self) -> dict[str, Any] | None:
        return self._load_encrypted(self.gmail_client_file)

    def load_gmail_client_mask(self) -> dict[str, str] | None:
        if not self.gmail_client_file.exists():
            return None
        try:
            data = json.loads(self.gmail_client_file.read_text(encoding="utf-8"))
        except Exception:
            return None
        mask = data.get("mask")
        return mask if isinstance(mask, dict) else None

    def clear_gmail_client(self) -> None:
        if self.gmail_client_file.exists():
            self.gmail_client_file.unlink()
        self._delete_backup_file(self.gmail_client_file)

    def save_gmail_oauth(self, payload: dict) -> None:
        self._save_encrypted(
            self.gmail_file,
            payload,
            {"email": payload.get("email", "")},
        )

    def load_gmail_oauth(self) -> dict | None:
        return self._load_encrypted(self.gmail_file)

    def clear_gmail_oauth(self) -> None:
        if self.gmail_file.exists():
            self.gmail_file.unlink()
        self._delete_backup_file(self.gmail_file)

    def load_gmail_send_ledger(self) -> dict[str, Any]:
        if not self.gmail_sent_file.exists():
            return {"version": 1, "archives": {}}
        try:
            payload = json.loads(self.gmail_sent_file.read_text(encoding="utf-8"))
        except Exception:
            return {"version": 1, "archives": {}}
        if not isinstance(payload, dict):
            return {"version": 1, "archives": {}}
        archives = payload.get("archives")
        if not isinstance(archives, dict):
            payload["archives"] = {}
        payload.setdefault("version", 1)
        return payload

    def save_gmail_send_ledger(self, payload: dict[str, Any]) -> None:
        self._atomic_write_json(self.gmail_sent_file, payload, mode=0o600)

    def clear(self) -> None:
        # /reset historically clears only optional MEXC API metadata. Gmail OAuth,
        # Google client credentials and the duplicate ledger intentionally survive
        # ordinary bot resets and Coolify redeploys.
        if self.api_file.exists():
            self.api_file.unlink()
