import json

from fastapi import APIRouter
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from ...core.config import get_settings


router = APIRouter(tags=["web-backoffice-shell"])
settings = get_settings()


def _normalize_base_url(url: str) -> str:
    return str(url or "").strip().rstrip("/")


def _resolve_bootstrap_endpoint() -> str:
    api_base = _normalize_base_url(str(settings.public_api_base_url or ""))
    if api_base:
        return f"{api_base}/api/v1/frontend/bootstrap"

    return "/api/v1/frontend/bootstrap"


def _resolve_shell_entry_url() -> str:
    entry = str(settings.frontend_shell_entry_url or "").strip()
    return entry or "http://127.0.0.1:5173/resources/js/app.js"


def _build_shell_html(pathname: str, channel: str, role: str, pilot: str, legacy_retire_at: str) -> str:
    runtime_config_json = json.dumps(
        {
            "apiBaseUrl": _normalize_base_url(str(settings.public_api_base_url or "")),
            "apiCutoverEnabled": True,
        },
        separators=(",", ":"),
    )

    context_json = json.dumps(
        {
            "channel": channel,
            "role": role,
            "userId": "",
        },
        separators=(",", ":"),
    )

    f3_context_json = json.dumps(
        {
            "pilot": pilot,
            "legacyRetireAt": legacy_retire_at,
            "requestedPath": pathname,
        },
        separators=(",", ":"),
    )

    bootstrap_endpoint = _resolve_bootstrap_endpoint()
    shell_entry_url = _resolve_shell_entry_url()

    return f"""<!doctype html>
<html lang=\"es\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Yastubo Backoffice</title>
</head>
<body data-f3-backoffice-shell=\"1\" data-f3-channel=\"{channel}\">
  <div id=\"app\"></div>
  <script>
    window.__BOOTSTRAP_ENDPOINT__ = {json.dumps(bootstrap_endpoint)};
    window.__RUNTIME_CONFIG__ = {runtime_config_json};
    window.__FRONTEND_CONTEXT__ = {context_json};
    window.__F3_BACKOFFICE_CONTEXT__ = {f3_context_json};
  </script>
  <script type=\"module\" src=\"{shell_entry_url}\"></script>
</body>
</html>
"""


def _resolve_legacy_redirect_target(pathname: str, legacy_base: str) -> str:
    if not bool(settings.frontend_legacy_redirects_enabled):
        return ""

    normalized = _normalize_base_url(legacy_base)
    if not normalized:
        return ""

    return f"{normalized}{pathname}"


def _render_or_redirect(
    pathname: str,
    shell_enabled: bool,
    channel: str,
    role: str,
    pilot: str,
    legacy_retire_at: str,
    legacy_base_url: str,
    legacy_redirect_enabled: bool,
) -> Response:
    if shell_enabled:
        return HTMLResponse(_build_shell_html(pathname, channel, role, pilot, legacy_retire_at))

    if not legacy_redirect_enabled:
        return Response(status_code=503, content=f"{pilot} shell no disponible")

    legacy_target = _resolve_legacy_redirect_target(pathname, legacy_base_url)
    if legacy_target:
        return RedirectResponse(url=legacy_target, status_code=307)

    return Response(status_code=503, content=f"{pilot} shell no disponible")


@router.get("/admin", response_class=HTMLResponse)
def admin_root_shell() -> Response:
    return _render_or_redirect(
        pathname="/admin",
        shell_enabled=bool(settings.frontend_admin_shell_enabled),
        channel="admin",
        role="GUEST",
        pilot="admin",
        legacy_retire_at=str(settings.frontend_admin_legacy_retire_at or ""),
        legacy_base_url=str(settings.frontend_admin_legacy_base_url or ""),
        legacy_redirect_enabled=bool(settings.frontend_admin_legacy_redirect_enabled),
    )


@router.get("/admin/{path:path}", response_class=HTMLResponse)
def admin_path_shell(path: str) -> Response:
    normalized_path = str(path or "").lstrip("/")
    pathname = f"/admin/{normalized_path}" if normalized_path else "/admin"

    return _render_or_redirect(
        pathname=pathname,
        shell_enabled=bool(settings.frontend_admin_shell_enabled),
        channel="admin",
        role="GUEST",
        pilot="admin",
        legacy_retire_at=str(settings.frontend_admin_legacy_retire_at or ""),
        legacy_base_url=str(settings.frontend_admin_legacy_base_url or ""),
        legacy_redirect_enabled=bool(settings.frontend_admin_legacy_redirect_enabled),
    )


@router.get("/seller", response_class=HTMLResponse)
def seller_root_shell() -> Response:
    return _render_or_redirect(
        pathname="/seller",
        shell_enabled=bool(settings.frontend_seller_shell_enabled),
        channel="seller",
        role="GUEST",
        pilot="seller",
        legacy_retire_at=str(settings.frontend_seller_legacy_retire_at or ""),
        legacy_base_url=str(settings.frontend_seller_legacy_base_url or ""),
        legacy_redirect_enabled=bool(settings.frontend_seller_legacy_redirect_enabled),
    )


@router.get("/seller/{path:path}", response_class=HTMLResponse)
def seller_path_shell(path: str) -> Response:
    normalized_path = str(path or "").lstrip("/")
    pathname = f"/seller/{normalized_path}" if normalized_path else "/seller"

    return _render_or_redirect(
        pathname=pathname,
        shell_enabled=bool(settings.frontend_seller_shell_enabled),
        channel="seller",
        role="GUEST",
        pilot="seller",
        legacy_retire_at=str(settings.frontend_seller_legacy_retire_at or ""),
        legacy_base_url=str(settings.frontend_seller_legacy_base_url or ""),
        legacy_redirect_enabled=bool(settings.frontend_seller_legacy_redirect_enabled),
    )
