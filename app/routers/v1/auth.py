from uuid import uuid4
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.database import get_db
from app.routers.v1.auth_cookies import (
    ACCESS_COOKIE_MAX_AGE_SECONDS,
    ACCESS_COOKIE_NAME,
    IMPERSONATION_META_COOKIE_NAME,
    IMPERSONATOR_ACCESS_COOKIE_NAME,
    IMPERSONATOR_REFRESH_COOKIE_NAME,
    REFRESH_COOKIE_MAX_AGE_SECONDS,
    REFRESH_COOKIE_NAME,
    decode_impersonation_meta,
    delete_auth_cookie,
    set_auth_cookie,
)
from app.schemas.auth import AuthImpersonationStopRequest, AuthLoginRequest, AuthLogoutRequest, AuthPasswordCheckRequest, AuthRefreshRequest
from app.schemas.common import ApiResponse
from app.services.auth_service import AuthService

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])
settings = get_settings()
_LOGIN_ATTEMPTS: dict[str, list[datetime]] = {}
_LOGIN_MAX_ATTEMPTS = 5
_LOGIN_WINDOW_SECONDS = 900
_PASSWORD_BANNED = ["password", "123456", "qwerty", "letmein", "admin"]


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


def _wants_json_response(request: Request) -> bool:
    accept = str(request.headers.get("accept") or "").lower()
    requested_with = str(request.headers.get("x-requested-with") or "").lower()
    return "application/json" in accept or requested_with == "xmlhttprequest"


def _resolve_frontend_origin(request: Request) -> str:
    origin = str(request.headers.get("origin") or "").strip().rstrip("/")
    if origin:
        return origin

    referer = str(request.headers.get("referer") or "").strip()
    if referer:
        parts = urlsplit(referer)
        if parts.scheme and parts.netloc:
            return f"{parts.scheme}://{parts.netloc}"

    fallback = str(settings.frontend_admin_legacy_base_url or "").strip().rstrip("/")
    if fallback:
        return fallback

    return ""


def _password_policy_payload() -> dict:
    return {
        "min": int(settings.password_min),
        "max": int(settings.password_max),
        "require": {
            "uppercase": bool(settings.password_require_uppercase),
            "lowercase": bool(settings.password_require_lowercase),
            "numbers": bool(settings.password_require_numbers),
            "symbols": bool(settings.password_require_symbols),
            "mixed_case": bool(settings.password_require_mixed_case),
            "letters": bool(
                settings.password_require_uppercase
                or settings.password_require_lowercase
                or settings.password_require_mixed_case
            ),
        },
        "messages": {
            "min": f"Debe tener al menos {int(settings.password_min)} caracteres.",
            "uppercase": "Debe incluir al menos una mayuscula.",
            "lowercase": "Debe incluir al menos una minuscula.",
            "numbers": "Debe incluir al menos un numero.",
            "symbols": "Debe incluir al menos un simbolo.",
            "max": f"No debe exceder {int(settings.password_max)} caracteres.",
            "noPersonal": "No debe incluir tu nombre ni tu email.",
        },
    }


def _check_password_errors(payload: AuthPasswordCheckRequest, policy: dict) -> list[str]:
    password = str(payload.password or "")
    errors: list[str] = []

    min_len = int(policy.get("min") or 0)
    max_len = int(policy.get("max") or 0)
    require = policy.get("require") or {}
    messages = policy.get("messages") or {}

    if min_len > 0 and len(password) < min_len:
        errors.append(str(messages.get("min") or "Debe cumplir longitud minima."))

    if max_len > 0 and len(password) > max_len:
        errors.append(str(messages.get("max") or "No debe exceder longitud maxima."))

    if bool(require.get("uppercase")) and not any(ch.isupper() for ch in password):
        errors.append(str(messages.get("uppercase") or "Debe incluir mayuscula."))

    if bool(require.get("lowercase")) and not any(ch.islower() for ch in password):
        errors.append(str(messages.get("lowercase") or "Debe incluir minuscula."))

    if bool(require.get("numbers")) and not any(ch.isdigit() for ch in password):
        errors.append(str(messages.get("numbers") or "Debe incluir numero."))

    if bool(require.get("symbols")) and all(ch.isalnum() for ch in password):
        errors.append(str(messages.get("symbols") or "Debe incluir simbolo."))

    if bool(require.get("mixed_case")):
        has_upper = any(ch.isupper() for ch in password)
        has_lower = any(ch.islower() for ch in password)
        if not (has_upper and has_lower):
            errors.append(str(messages.get("uppercase") or "Debe incluir mayuscula."))
            errors.append(str(messages.get("lowercase") or "Debe incluir minuscula."))

    lowered = password.lower()
    for banned in _PASSWORD_BANNED:
        if banned and banned in lowered:
            errors.append("La contraseña contiene patrones inseguros.")
            break

    email_local = ""
    if payload.email:
        email_local = str(payload.email).split("@", maxsplit=1)[0].lower().strip()

    personal_parts = [
        str(payload.first_name or "").lower().strip(),
        str(payload.last_name or "").lower().strip(),
        str(payload.display_name or "").lower().strip(),
        email_local,
    ]
    if any(part and part in lowered for part in personal_parts):
        errors.append(str(messages.get("noPersonal") or "No debe incluir datos personales."))

    return errors


@router.get("/password-policy")
def password_policy() -> dict:
    return _password_policy_payload()


@router.post("/password-check")
def password_check(payload: AuthPasswordCheckRequest) -> dict:
    policy = _password_policy_payload()
    errors = _check_password_errors(payload, policy)

    return {
        "valid": len(errors) == 0,
        "errors": errors,
    }


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
        set_auth_cookie(response, REFRESH_COOKIE_NAME, refresh_token, REFRESH_COOKIE_MAX_AGE_SECONDS)

    access_token = str(data.get("access_token") or "").strip()
    if access_token:
        set_auth_cookie(response, ACCESS_COOKIE_NAME, access_token, ACCESS_COOKIE_MAX_AGE_SECONDS)

    delete_auth_cookie(response, IMPERSONATOR_REFRESH_COOKIE_NAME)
    delete_auth_cookie(response, IMPERSONATOR_ACCESS_COOKIE_NAME)
    delete_auth_cookie(response, IMPERSONATION_META_COOKIE_NAME)

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
    token_from_cookie = str((request.cookies.get(REFRESH_COOKIE_NAME) or "")).strip()
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
        set_auth_cookie(response, ACCESS_COOKIE_NAME, access_token, ACCESS_COOKIE_MAX_AGE_SECONDS)

    return ApiResponse(ok=True, message="Token renovado", data=data, request_id=_request_id())


@router.get("/me", response_model=ApiResponse)
def me(
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> ApiResponse:
    token = _extract_bearer_token(authorization)
    if not token:
        token = str(request.cookies.get(ACCESS_COOKIE_NAME) or "").strip()

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
    token_from_cookie = str((request.cookies.get(REFRESH_COOKIE_NAME) or "")).strip()
    refresh_token = token_from_body or token_from_cookie

    data = service.logout(refresh_token)

    delete_auth_cookie(response, REFRESH_COOKIE_NAME)
    delete_auth_cookie(response, ACCESS_COOKIE_NAME)
    delete_auth_cookie(response, IMPERSONATOR_REFRESH_COOKIE_NAME)
    delete_auth_cookie(response, IMPERSONATOR_ACCESS_COOKIE_NAME)
    delete_auth_cookie(response, IMPERSONATION_META_COOKIE_NAME)

    return ApiResponse(ok=True, message="Sesion finalizada", data=data, request_id=_request_id())


@router.post("/impersonation/stop")
def stop_impersonation(
    request: Request,
    response: Response,
    payload: AuthImpersonationStopRequest | None = None,
    db: Session = Depends(get_db),
):
    original_refresh = str((request.cookies.get(IMPERSONATOR_REFRESH_COOKIE_NAME) or "")).strip()
    original_access = str((request.cookies.get(IMPERSONATOR_ACCESS_COOKIE_NAME) or "")).strip()
    meta = decode_impersonation_meta(request.cookies.get(IMPERSONATION_META_COOKIE_NAME)) or {}

    fallback_redirect = str((payload.redirect_to if payload else "") or "").strip()
    frontend_origin = _resolve_frontend_origin(request)
    redirect_to = fallback_redirect or (f"{frontend_origin}/admin" if frontend_origin else "/admin")

    restored = False
    service = AuthService(db)

    if original_refresh:
        refreshed = service.refresh(original_refresh)
        access_token = str(refreshed.get("access_token") or "").strip()
        if access_token:
            set_auth_cookie(response, ACCESS_COOKIE_NAME, access_token, ACCESS_COOKIE_MAX_AGE_SECONDS)
            set_auth_cookie(response, REFRESH_COOKIE_NAME, original_refresh, REFRESH_COOKIE_MAX_AGE_SECONDS)
            restored = True
    elif original_access:
        service.me(original_access)
        set_auth_cookie(response, ACCESS_COOKIE_NAME, original_access, ACCESS_COOKIE_MAX_AGE_SECONDS)
        restored = True

    delete_auth_cookie(response, IMPERSONATOR_REFRESH_COOKIE_NAME)
    delete_auth_cookie(response, IMPERSONATOR_ACCESS_COOKIE_NAME)
    delete_auth_cookie(response, IMPERSONATION_META_COOKIE_NAME)

    message = "Impersonación finalizada." if restored else "No había una impersonación activa."
    data = {
        "restored": restored,
        "redirect_to": redirect_to,
        "impersonation": meta,
    }

    if _wants_json_response(request):
        return ApiResponse(ok=True, message=message, data=data, request_id=_request_id())

    return RedirectResponse(url=redirect_to, status_code=303)
