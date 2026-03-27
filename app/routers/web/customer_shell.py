import json

from fastapi import APIRouter
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from ...core.config import get_settings


router = APIRouter(tags=["web-customer-shell"])
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


def _build_customer_shell_html(pathname: str) -> str:
    runtime_config_json = json.dumps(
        {
            "apiBaseUrl": _normalize_base_url(str(settings.public_api_base_url or "")),
            "apiCutoverEnabled": True,
        },
        separators=(",", ":"),
    )

    context_json = json.dumps(
        {
            "channel": "customer",
            "role": "GUEST",
            "userId": "",
        },
        separators=(",", ":"),
    )

    f3_context_json = json.dumps(
        {
            "pilot": "customer",
            "legacyRetireAt": str(settings.frontend_customer_legacy_retire_at or ""),
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
  <title>Yastubo Customer</title>
</head>
<body data-f3-customer-shell=\"1\">
  <div id=\"app\"></div>
  <script>
    window.__BOOTSTRAP_ENDPOINT__ = {json.dumps(bootstrap_endpoint)};
    window.__RUNTIME_CONFIG__ = {runtime_config_json};
    window.__FRONTEND_CONTEXT__ = {context_json};
    window.__F3_CUSTOMER_CONTEXT__ = {f3_context_json};
  </script>
  <script type=\"module\" src=\"{shell_entry_url}\"></script>
</body>
</html>
"""


def _resolve_legacy_redirect_target(pathname: str) -> str:
    if not bool(settings.frontend_legacy_redirects_enabled):
        return ""

    if not bool(settings.frontend_customer_legacy_redirect_enabled):
        return ""

    legacy_base = _normalize_base_url(str(settings.frontend_customer_legacy_base_url or ""))
    if not legacy_base:
        return ""

    return f"{legacy_base}{pathname}"


@router.get("/customer", response_class=HTMLResponse)
def customer_root_shell() -> Response:
    if bool(settings.frontend_customer_shell_enabled):
        return HTMLResponse(_build_customer_shell_html("/customer"))

    legacy_target = _resolve_legacy_redirect_target("/customer")
    if legacy_target:
        return RedirectResponse(url=legacy_target, status_code=307)

    return Response(status_code=503, content="Customer shell no disponible")


@router.get("/customer/{path:path}", response_class=HTMLResponse)
def customer_path_shell(path: str) -> Response:
    normalized_path = str(path or "").lstrip("/")
    pathname = f"/customer/{normalized_path}" if normalized_path else "/customer"

    if bool(settings.frontend_customer_shell_enabled):
        return HTMLResponse(_build_customer_shell_html(pathname))

    legacy_target = _resolve_legacy_redirect_target(pathname)
    if legacy_target:
        return RedirectResponse(url=legacy_target, status_code=307)

    return Response(status_code=503, content="Customer shell no disponible")
