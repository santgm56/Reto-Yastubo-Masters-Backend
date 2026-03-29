from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import logging
import secrets
import smtplib
from email.message import EmailMessage
from urllib.parse import quote

import jwt
from passlib.exc import UnknownHashError
from passlib.context import CryptContext
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import get_settings


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
_REVOKED_REFRESH_TOKENS: dict[str, int] = {}
_logger = logging.getLogger(__name__)


class AuthService:
    def __init__(self, db: Session):
        self.db = db
        self.settings = get_settings()

    def login(self, email: str, password: str) -> dict:
        user = self._find_user_by_email(email)
        if not user:
            raise ValueError("Credenciales invalidas.")

        if not self._is_user_active(user.get("status")):
            raise PermissionError("La cuenta esta inactiva.")

        password_hash = str(user.get("password") or "")
        if not password_hash or not self._verify_password(password, password_hash):
            raise ValueError("Credenciales invalidas.")

        return self.issue_tokens_for_user_id(int(user["id"]))

    def refresh(self, refresh_token: str) -> dict:
        self._cleanup_revoked_refresh_tokens()
        if self._is_refresh_token_revoked(refresh_token):
            raise ValueError("Refresh token revocado.")

        claims = self._decode_token(refresh_token, expected_type="refresh")
        user_id = int(claims.get("sub") or 0)

        user = self._find_user_by_id(user_id)
        if not user:
            raise ValueError("Token invalido.")

        if not self._is_user_active(user.get("status")):
            raise PermissionError("La cuenta esta inactiva.")

        role = str(user.get("realm") or "GUEST").upper()

        new_access_token = self._build_token(
            user_id=user_id,
            role=role,
            token_type="access",
            expires_in_minutes=self.settings.jwt_access_token_exp_minutes,
        )

        return {
            "access_token": new_access_token,
            "token_type": "bearer",
            "expires_in": self.settings.jwt_access_token_exp_minutes * 60,
        }

    def logout(self, refresh_token: str | None) -> dict:
        self._cleanup_revoked_refresh_tokens()

        token = str(refresh_token or "").strip()
        if not token:
            return {"revoked": False, "reason": "no_refresh_token"}

        try:
            claims = self._decode_token(token, expected_type="refresh")
        except ValueError:
            return {"revoked": False, "reason": "invalid_refresh_token"}

        exp = int(claims.get("exp") or 0)
        if exp <= int(datetime.now(tz=timezone.utc).timestamp()):
            return {"revoked": False, "reason": "refresh_token_expired"}

        token_hash = self._hash_token(token)
        _REVOKED_REFRESH_TOKENS[token_hash] = exp

        return {"revoked": True}

    def issue_tokens_for_user_id(self, user_id: int) -> dict:
        user = self._find_user_by_id(user_id)
        if not user:
            raise ValueError("Token invalido.")

        if not self._is_user_active(user.get("status")):
            raise PermissionError("La cuenta esta inactiva.")

        role = str(user.get("realm") or "GUEST").upper()
        permissions = self._load_permissions_for_user(user_id)

        access_token = self._build_token(
            user_id=user_id,
            role=role,
            token_type="access",
            expires_in_minutes=self.settings.jwt_access_token_exp_minutes,
        )
        refresh_token = self._build_token(
            user_id=user_id,
            role=role,
            token_type="refresh",
            expires_in_minutes=self.settings.jwt_refresh_token_exp_minutes,
        )

        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
            "expires_in": self.settings.jwt_access_token_exp_minutes * 60,
            "user": {
                "id": user_id,
                "name": str(user.get("name") or user.get("email") or ""),
                "email": str(user.get("email") or ""),
                "role": role,
                "permissions": permissions,
            },
        }

    def send_admin_reset_link(self, user_id: int, frontend_origin: str) -> dict:
        user = self._find_user_by_id(user_id)
        if not user or str(user.get("realm") or "").strip().lower() != "admin":
            raise ValueError("Usuario admin no encontrado.")

        email = str(user.get("email") or "").strip().lower()
        if not email:
            raise ValueError("El usuario no tiene email valido.")

        existing = self.db.execute(
            text(
                """
                SELECT created_at
                FROM password_reset_tokens_admin
                WHERE email = :email
                LIMIT 1
                """
            ),
            {"email": email},
        ).mappings().first()

        created_at = self._coerce_datetime((existing or {}).get("created_at"))
        now = datetime.now(tz=timezone.utc)
        throttle_seconds = int(self.settings.password_reset_admin_throttle_seconds or 60)
        if created_at and (now - created_at).total_seconds() < throttle_seconds:
            raise ValueError("Debes esperar antes de solicitar otro correo de reset.")

        token = secrets.token_urlsafe(48)
        token_hash = pwd_context.hash(token)

        self.db.execute(
            text(
                """
                INSERT INTO password_reset_tokens_admin (email, token, created_at)
                VALUES (:email, :token, :created_at)
                ON DUPLICATE KEY UPDATE token = VALUES(token), created_at = VALUES(created_at)
                """
            ),
            {
                "email": email,
                "token": token_hash,
                "created_at": now.replace(tzinfo=None),
            },
        )
        self.db.commit()

        origin = str(frontend_origin or "").strip().rstrip("/")
        reset_url = f"{origin}/admin/reset-password/{quote(token)}?email={quote(email)}"
        self._send_admin_reset_email(
            recipient=email,
            display_name=str(user.get("name") or email),
            reset_url=reset_url,
        )

        return {
            "email": email,
            "queued": True,
        }

    def me(self, access_token: str) -> dict:
        claims = self._decode_token(access_token, expected_type="access")
        user_id = int(claims.get("sub") or 0)

        user = self._find_user_by_id(user_id)
        if not user:
            raise ValueError("Token invalido.")

        role = str(user.get("realm") or "GUEST").upper()
        permissions = self._load_permissions_for_user(user_id)

        return {
            "id": user_id,
            "name": str(user.get("name") or user.get("email") or ""),
            "email": str(user.get("email") or ""),
            "role": role,
            "permissions": permissions,
            "status": str(user.get("status") or ""),
        }

    def _build_token(self, user_id: int, role: str, token_type: str, expires_in_minutes: int) -> str:
        now = datetime.now(tz=timezone.utc)
        payload = {
            "sub": str(user_id),
            "role": role,
            "type": token_type,
            "iat": int(now.timestamp()),
            "exp": int((now + timedelta(minutes=max(1, expires_in_minutes))).timestamp()),
        }
        return jwt.encode(payload, self.settings.jwt_secret, algorithm=self.settings.jwt_algorithm)

    def _decode_token(self, token: str, expected_type: str) -> dict:
        try:
            claims = jwt.decode(token, self.settings.jwt_secret, algorithms=[self.settings.jwt_algorithm])
        except jwt.PyJWTError as exc:
            raise ValueError("Token invalido o expirado.") from exc

        token_type = str(claims.get("type") or "").strip().lower()
        if token_type != expected_type:
            raise ValueError("Tipo de token invalido.")

        return claims

    def _verify_password(self, plain_password: str, password_hash: str) -> bool:
        # Compatibilidad con hashes bcrypt emitidos por Laravel ($2y$).
        normalized_hash = password_hash.replace("$2y$", "$2b$")
        verified = False
        try:
            verified = bool(pwd_context.verify(plain_password, normalized_hash))
        except (ValueError, UnknownHashError):
            verified = False

        if verified:
            return True

        # Fallback para entornos donde passlib+bcrypt falla por incompatibilidad
        # del backend (detectado en algunos setups con Python 3.14) o donde
        # passlib devuelve falso negativo pese a tratarse de un hash bcrypt valido.
        try:
            import bcrypt

            return bool(
                bcrypt.checkpw(
                    plain_password.encode("utf-8"),
                    normalized_hash.encode("utf-8"),
                )
            )
        except Exception:
            return False

    def _is_user_active(self, status: object) -> bool:
        normalized = str(status or "ACTIVE").strip().upper()
        return normalized in {"ACTIVE", "ACTIVO", "1", "TRUE"}

    def _find_user_by_email(self, email: str) -> dict | None:
        row = self.db.execute(
            text(
                """
                SELECT
                    id,
                    COALESCE(NULLIF(display_name, ''), NULLIF(CONCAT_WS(' ', first_name, last_name), ''), email) AS name,
                    email,
                    password,
                    status,
                    realm
                FROM users
                WHERE email = :email
                  AND deleted_at IS NULL
                LIMIT 1
                """
            ),
            {"email": email.strip().lower()},
        ).mappings().first()

        return dict(row) if row else None

    def _find_user_by_id(self, user_id: int) -> dict | None:
        row = self.db.execute(
            text(
                """
                SELECT
                    id,
                    COALESCE(NULLIF(display_name, ''), NULLIF(CONCAT_WS(' ', first_name, last_name), ''), email) AS name,
                    email,
                    password,
                    status,
                    realm
                FROM users
                WHERE id = :user_id
                  AND deleted_at IS NULL
                LIMIT 1
                """
            ),
            {"user_id": user_id},
        ).mappings().first()

        return dict(row) if row else None

    def _load_permissions_for_user(self, user_id: int) -> list[str]:
        query = text(
            """
            SELECT DISTINCT p.name
            FROM permissions p
            INNER JOIN model_has_permissions mhp
                ON mhp.permission_id = p.id
               AND mhp.model_type LIKE :model_type_suffix
               AND mhp.model_id = :user_id

            UNION

            SELECT DISTINCT p2.name
            FROM permissions p2
            INNER JOIN role_has_permissions rhp
                ON rhp.permission_id = p2.id
            INNER JOIN model_has_roles mhr
                ON mhr.role_id = rhp.role_id
               AND mhr.model_type LIKE :model_type_suffix
               AND mhr.model_id = :user_id

            ORDER BY name ASC
            """
        )

        rows = self.db.execute(
            query,
            {
                "model_type_suffix": "%User",
                "user_id": user_id,
            },
        ).mappings().all()

        return [str(item["name"]) for item in rows if item.get("name")]

    def _hash_token(self, token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    def _send_admin_reset_email(self, recipient: str, display_name: str, reset_url: str) -> None:
        subject = "Restablecer contraseña (Admin)"
        minutes = int(self.settings.password_reset_admin_expire_minutes or 30)
        text_body = (
            f"Hola {display_name},\n\n"
            f"Usa el siguiente enlace para restablecer tu contraseña de admin:\n{reset_url}\n\n"
            f"Este enlace expirará en {minutes} minutos."
        )
        html_body = (
            "<p>Hola {name},</p>"
            "<p>Usa el siguiente enlace para restablecer tu contraseña de admin:</p>"
            "<p><a href=\"{url}\">Restablecer contraseña</a></p>"
            "<p>Este enlace expirará en {minutes} minutos.</p>"
        ).format(name=display_name, url=reset_url, minutes=minutes)

        mailer = str(self.settings.mail_mailer or "log").strip().lower()
        if mailer == "log":
            _logger.info(
                "Admin password reset email queued",
                extra={"recipient": recipient, "reset_url": reset_url},
            )
            return

        message = EmailMessage()
        message["Subject"] = subject
        message["From"] = f"{self.settings.mail_from_name} <{self.settings.mail_from_address}>"
        message["To"] = recipient
        message.set_content(text_body)
        message.add_alternative(html_body, subtype="html")

        scheme = str(self.settings.mail_scheme or "").strip().lower()
        host = str(self.settings.mail_host or "").strip()
        port = int(self.settings.mail_port or 0)
        username = str(self.settings.mail_username or "").strip()
        password = str(self.settings.mail_password or "")

        if scheme in {"ssl", "smtps"}:
            with smtplib.SMTP_SSL(host, port or 465, timeout=10) as server:
                if username:
                    server.login(username, password)
                server.send_message(message)
            return

        with smtplib.SMTP(host, port or 25, timeout=10) as server:
            server.ehlo()
            if scheme in {"tls", "starttls"}:
                server.starttls()
                server.ehlo()
            if username:
                server.login(username, password)
            server.send_message(message)

    def _coerce_datetime(self, value) -> datetime | None:
        if value is None:
            return None

        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

        raw = str(value).strip()
        if not raw:
            return None

        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            try:
                parsed = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None

        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

    def _is_refresh_token_revoked(self, token: str) -> bool:
        token_hash = self._hash_token(token)
        return token_hash in _REVOKED_REFRESH_TOKENS

    def _cleanup_revoked_refresh_tokens(self) -> None:
        now_ts = int(datetime.now(tz=timezone.utc).timestamp())
        expired_hashes = [token_hash for token_hash, exp in _REVOKED_REFRESH_TOKENS.items() if exp <= now_ts]
        for token_hash in expired_hashes:
            _REVOKED_REFRESH_TOKENS.pop(token_hash, None)
