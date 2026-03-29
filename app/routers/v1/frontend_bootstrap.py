from uuid import uuid4

from fastapi import APIRouter, Depends, Header, Request
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.database import get_db
from app.schemas.common import ApiResponse
from app.services.auth_service import AuthService

router = APIRouter(prefix="/api/v1/frontend", tags=["frontend-bootstrap"])


def _request_id() -> str:
    return f"req_{uuid4().hex}"


def _safe_header(request: Request, key: str, fallback: str = "") -> str:
    value = request.headers.get(key, "")
    normalized = str(value).strip()
    return normalized if normalized else fallback


def _extract_bearer_token(authorization: str | None) -> str:
    if not authorization:
        return ""

    raw = authorization.strip()
    if not raw.lower().startswith("bearer "):
        return ""

    return raw[7:].strip()


def _to_abilities_map(permissions: list[str]) -> dict[str, bool]:
    abilities: dict[str, bool] = {}
    for permission in permissions:
        key = str(permission or "").strip()
        if key:
            abilities[key] = True
    return abilities


def _map_role_to_channel(role: str) -> str:
    normalized = str(role or "").strip().upper()
    if normalized == "ADMIN":
        return "admin"
    if normalized == "SELLER":
        return "seller"
    if normalized == "CUSTOMER":
        return "customer"
    return "web"


def _resolve_public_api_base_url(request: Request) -> str:
    configured = str(get_settings().public_api_base_url or "").strip().rstrip("/")
    if configured:
        return configured

    return str(request.base_url).strip().rstrip("/")


@router.get("/bootstrap", response_model=ApiResponse)
def bootstrap(
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> ApiResponse:
    settings = get_settings()

    channel = _safe_header(request, "x-frontend-channel", "web")
    role = _safe_header(request, "x-frontend-role", "GUEST")
    user_id = _safe_header(request, "x-frontend-user-id", "")
    permissions: list[str] = []

    token = _extract_bearer_token(authorization)
    if token:
        try:
            me_payload = AuthService(db).me(token)
            role = str(me_payload.get("role") or role).strip().upper()
            user_id = str(me_payload.get("id") or user_id).strip()
            permissions = [str(item) for item in (me_payload.get("permissions") or []) if str(item).strip()]
            channel = _map_role_to_channel(role)
        except ValueError:
            # Token invalido/expirado: mantener fallback anonimo sin bloquear bootstrap.
            pass

    runtime_config = {
        "autosaveDelayMs": 800,
        "perPageShort": 5,
        "perPageMedium": 10,
        "perPageLarge": 15,
        "apiBaseUrl": _resolve_public_api_base_url(request),
        "apiCutoverEnabled": True,
        "abilities": _to_abilities_map(permissions),
    }

    app_config = {
        "locale": "es",
        "numberLocale": "es-ES",
        "dateFormat": "d/m/Y",
        "timeFormat": "H:i",
        "dateTimeFormat": "d/m/Y H:i",
        "jsDateFormat": "dd/MM/yyyy",
    }

    frontend_context = {
        "channel": channel,
        "role": role,
        "userId": user_id,
    }

    return ApiResponse(
        ok=True,
        message="Bootstrap frontend disponible",
        data={
            "runtimeConfig": runtime_config,
            "appConfig": app_config,
            "frontendContext": frontend_context,
            "service": settings.app_name,
        },
        request_id=_request_id(),
    )
