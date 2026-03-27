from uuid import uuid4
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.database import get_db
from app.schemas.auth import AuthLoginRequest, AuthLogoutRequest, AuthRefreshRequest
from app.schemas.common import ApiResponse
from app.services.auth_service import AuthService

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])
settings = get_settings()
_LOGIN_ATTEMPTS: dict[str, list[datetime]] = {}
_LOGIN_MAX_ATTEMPTS = 5
_LOGIN_WINDOW_SECONDS = 900
_REFRESH_COOKIE_NAME = "yastubo_refresh_token"
_REFRESH_COOKIE_MAX_AGE_SECONDS = 60 * 60 * 24 * 7
_ACCESS_COOKIE_NAME = "yastubo_access_token"
_ACCESS_COOKIE_MAX_AGE_SECONDS = 60 * 60 * 8


def _request_id() -> str:
    return f"req_{uuid4().hex}"


def _extract_bearer_token(authorization: str | None) -> str:
    if not authorization:
        return ""

    raw = authorization.strip()
    if not raw.lower().startswith("bearer "):
        return ""

    return raw[7:].strip()


def _normalize_email(email: str) -> str:
    return email.strip().lower()


def _rate_limit_key(email: str, request: Request) -> str:
    client_ip = (request.client.host if request.client and request.client.host else "unknown").strip()
    return f"{_normalize_email(email)}|{client_ip}"


def _cleanup_login_attempts(now: datetime) -> None:
    boundary = now - timedelta(seconds=_LOGIN_WINDOW_SECONDS)
    expired_keys: list[str] = []

    for key, attempts in _LOGIN_ATTEMPTS.items():
        _LOGIN_ATTEMPTS[key] = [attempt for attempt in attempts if attempt >= boundary]
        if not _LOGIN_ATTEMPTS[key]:
            expired_keys.append(key)

    for key in expired_keys:
        _LOGIN_ATTEMPTS.pop(key, None)


def _is_rate_limited(key: str, now: datetime) -> bool:
    _cleanup_login_attempts(now)
    return len(_LOGIN_ATTEMPTS.get(key, [])) >= _LOGIN_MAX_ATTEMPTS


def _register_failed_attempt(key: str, now: datetime) -> None:
    attempts = _LOGIN_ATTEMPTS.get(key, [])
    attempts.append(now)
    _LOGIN_ATTEMPTS[key] = attempts


def _clear_attempts(key: str) -> None:
    _LOGIN_ATTEMPTS.pop(key, None)


def _set_auth_cookie(response: Response, key: str, value: str, max_age: int) -> None:
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


def _delete_auth_cookie(response: Response, key: str) -> None:
    domain = str(settings.auth_cookie_domain or "").strip() or None
    response.delete_cookie(
        key=key,
        path=str(settings.auth_cookie_path or "/"),
        domain=domain,
    )


@router.post("/login", response_model=ApiResponse)
def login(
    payload: AuthLoginRequest,
    request: Request,
    response: Response,
    db: Session = Depends(get_db),
) -> ApiResponse:
    service = AuthService(db)
    now = datetime.now(tz=timezone.utc)
    key = _rate_limit_key(payload.email, request)

    if _is_rate_limited(key, now):
        raise HTTPException(
            status_code=429,
            detail={
                "code": "API_TOO_MANY_REQUESTS",
                "message": "Demasiados intentos de login. Intenta nuevamente en unos minutos.",
                "details": {"origin": "auth.login", "window_seconds": _LOGIN_WINDOW_SECONDS},
                "request_id": _request_id(),
            },
        )

    try:
        data = service.login(payload.email, payload.password)
    except ValueError as exc:
        _register_failed_attempt(key, now)
        raise HTTPException(
            status_code=401,
            detail={
                "code": "API_UNAUTHORIZED",
                "message": str(exc),
                "details": {"origin": "auth.login"},
                "request_id": _request_id(),
            },
        ) from exc
    except PermissionError as exc:
        _register_failed_attempt(key, now)
        raise HTTPException(
            status_code=403,
            detail={
                "code": "API_FORBIDDEN",
                "message": str(exc),
                "details": {"origin": "auth.login"},
                "request_id": _request_id(),
            },
        ) from exc

    _clear_attempts(key)

    refresh_token = str(data.get("refresh_token") or "").strip()
    if refresh_token:
        _set_auth_cookie(response, _REFRESH_COOKIE_NAME, refresh_token, _REFRESH_COOKIE_MAX_AGE_SECONDS)

    access_token = str(data.get("access_token") or "").strip()
    if access_token:
        _set_auth_cookie(response, _ACCESS_COOKIE_NAME, access_token, _ACCESS_COOKIE_MAX_AGE_SECONDS)

    public_data = dict(data)
    public_data.pop("refresh_token", None)

    return ApiResponse(ok=True, message="Sesion iniciada", data=public_data, request_id=_request_id())


@router.post("/refresh", response_model=ApiResponse)
def refresh(
    request: Request,
    response: Response,
    payload: AuthRefreshRequest | None = None,
    db: Session = Depends(get_db),
) -> ApiResponse:
    service = AuthService(db)

    token_from_body = str((payload.refresh_token if payload else "") or "").strip()
    token_from_cookie = str((request.cookies.get(_REFRESH_COOKIE_NAME) or "")).strip()
    refresh_token = token_from_body or token_from_cookie

    if not refresh_token:
        raise HTTPException(
            status_code=401,
            detail={
                "code": "API_UNAUTHORIZED",
                "message": "Refresh token requerido.",
                "details": {"origin": "auth.refresh"},
                "request_id": _request_id(),
            },
        )

    try:
        data = service.refresh(refresh_token)
    except ValueError as exc:
        raise HTTPException(
            status_code=401,
            detail={
                "code": "API_UNAUTHORIZED",
                "message": str(exc),
                "details": {"origin": "auth.refresh"},
                "request_id": _request_id(),
            },
        ) from exc
    except PermissionError as exc:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "API_FORBIDDEN",
                "message": str(exc),
                "details": {"origin": "auth.refresh"},
                "request_id": _request_id(),
            },
        ) from exc

    access_token = str(data.get("access_token") or "").strip()
    if access_token:
        _set_auth_cookie(response, _ACCESS_COOKIE_NAME, access_token, _ACCESS_COOKIE_MAX_AGE_SECONDS)

    return ApiResponse(ok=True, message="Token renovado", data=data, request_id=_request_id())


@router.get("/me", response_model=ApiResponse)
def me(
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> ApiResponse:
    token = _extract_bearer_token(authorization)
    if not token:
        token = str(request.cookies.get(_ACCESS_COOKIE_NAME) or "").strip()

    if not token:
        raise HTTPException(
            status_code=401,
            detail={
                "code": "API_UNAUTHORIZED",
                "message": "Token Bearer requerido.",
                "details": {"origin": "auth.me"},
                "request_id": _request_id(),
            },
        )

    service = AuthService(db)

    try:
        data = service.me(token)
    except ValueError as exc:
        raise HTTPException(
            status_code=401,
            detail={
                "code": "API_UNAUTHORIZED",
                "message": str(exc),
                "details": {"origin": "auth.me"},
                "request_id": _request_id(),
            },
        ) from exc

    return ApiResponse(ok=True, message="Usuario autenticado", data=data, request_id=_request_id())


@router.post("/logout", response_model=ApiResponse)
def logout(
    request: Request,
    response: Response,
    payload: AuthLogoutRequest | None = None,
    db: Session = Depends(get_db),
) -> ApiResponse:
    service = AuthService(db)

    token_from_body = str((payload.refresh_token if payload else "") or "").strip()
    token_from_cookie = str((request.cookies.get(_REFRESH_COOKIE_NAME) or "")).strip()
    refresh_token = token_from_body or token_from_cookie

    data = service.logout(refresh_token)

    _delete_auth_cookie(response, _REFRESH_COOKIE_NAME)
    _delete_auth_cookie(response, _ACCESS_COOKIE_NAME)

    return ApiResponse(ok=True, message="Sesion finalizada", data=data, request_id=_request_id())
