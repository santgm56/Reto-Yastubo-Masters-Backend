import base64
import json

from fastapi import Response

from app.core.config import get_settings


settings = get_settings()

REFRESH_COOKIE_NAME = "yastubo_refresh_token"
REFRESH_COOKIE_MAX_AGE_SECONDS = 60 * 60 * 24 * 7
ACCESS_COOKIE_NAME = "yastubo_access_token"
ACCESS_COOKIE_MAX_AGE_SECONDS = 60 * 60 * 8
IMPERSONATOR_REFRESH_COOKIE_NAME = "yastubo_impersonator_refresh_token"
IMPERSONATOR_ACCESS_COOKIE_NAME = "yastubo_impersonator_access_token"
IMPERSONATION_META_COOKIE_NAME = "yastubo_impersonation_meta"
IMPERSONATION_COOKIE_MAX_AGE_SECONDS = 60 * 60 * 8


def set_auth_cookie(response: Response, key: str, value: str, max_age: int) -> None:
    domain = str(settings.auth_cookie_domain or "").strip() or None
    response.set_cookie(
        key=key,
        value=value,
        httponly=True,
        samesite=str(settings.auth_cookie_samesite or "lax"),
        secure=bool(settings.auth_cookie_secure),
        max_age=max_age,
        path=str(settings.auth_cookie_path or "/"),
        domain=domain,
    )


def delete_auth_cookie(response: Response, key: str) -> None:
    domain = str(settings.auth_cookie_domain or "").strip() or None
    response.delete_cookie(
        key=key,
        path=str(settings.auth_cookie_path or "/"),
        domain=domain,
    )


def encode_impersonation_meta(meta: dict) -> str:
    raw = json.dumps(meta, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_impersonation_meta(raw: str | None) -> dict | None:
    token = str(raw or "").strip()
    if not token:
        return None

    padding = "=" * (-len(token) % 4)
    try:
        decoded = base64.urlsafe_b64decode(f"{token}{padding}".encode("ascii"))
        payload = json.loads(decoded.decode("utf-8"))
    except Exception:
        return None

    return payload if isinstance(payload, dict) else None