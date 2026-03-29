import json
import logging
import re

from fastapi import APIRouter
from fastapi.responses import HTMLResponse, Response

from ...core.config import get_settings
from ...core.shell_metrics import increment_shell_disabled


router = APIRouter(tags=["web-customer-shell"])
settings = get_settings()
logger = logging.getLogger(__name__)


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


def _render_customer_login_form() -> str:
        return """
    <div class=\"container py-5\" style=\"max-width: 420px;\">
        <h1 class=\"mb-4\">Login customer</h1>
        <form class=\"form w-100\" method=\"GET\" action=\"/customer/login\" data-fastapi-login=\"true\" data-login-channel=\"customer\" data-login-redirect=\"/customer/dashboard\">
            <div data-fastapi-login-error></div>
            <div class=\"mb-3\">
                <label class=\"form-label\">Email</label>
                <input name=\"email\" type=\"email\" class=\"form-control\" required autofocus>
            </div>
            <div class=\"mb-4\">
                <label class=\"form-label\">Contrasena</label>
                <input name=\"password\" type=\"password\" class=\"form-control\" required>
            </div>
            <button class=\"btn btn-primary w-100\" type=\"submit\">Entrar</button>
        </form>
    </div>
"""


def _resolve_customer_initial_section(pathname: str) -> str:
        if pathname == "/customer/login":
                return "login"
        if re.fullmatch(r"/customer/(dashboard)?", pathname):
                return "dashboard"
        if pathname.startswith("/customer/metodo-pago"):
                return "metodo-pago"
        if pathname.startswith("/customer/transacciones"):
                return "transacciones"
        if pathname.startswith("/customer/productos"):
                return "productos"
        if pathname.startswith("/customer/pagos-pendientes"):
                return "pagos-pendientes"
        return "dashboard"


def _resolve_customer_mount_markup(pathname: str) -> str:
        if pathname == "/customer/login":
                return _render_customer_login_form()

        initial_section = _resolve_customer_initial_section(pathname)
        if initial_section == "metodo-pago":
                return """
<div data-static-shell="true" class="container py-5">
    <h1>Metodo de pago</h1>
    <p>Actualizar metodo de pago</p>
</div>
"""

        if initial_section == "transacciones":
                return """
<div data-static-shell="true" class="container py-5">
    <h1>Transacciones</h1>
    <p>Historial pagos</p>
</div>
"""

        if initial_section == "productos":
                return """
<div data-static-shell="true" class="container py-5">
    <h1>Productos</h1>
    <p>Cobertura</p>
    <p>Reporte de fallecimiento</p>
</div>
"""

        return """
<div data-static-shell="true" class="container py-5">
    <h1>Dashboard</h1>
    <p>Resumen</p>
    <p>Cobertura</p>
    <p>Historial pagos</p>
</div>
"""


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
    mount_markup = _resolve_customer_mount_markup(pathname)

    return f"""<!doctype html>
<html lang=\"es\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Yastubo Customer</title>
    <link href=\"/assets/plugins/global/plugins.bundle.css\" rel=\"stylesheet\" type=\"text/css\" />
    <link href=\"/assets/css/style.bundle.css\" rel=\"stylesheet\" type=\"text/css\" />
</head>
<body data-f3-customer-shell=\"1\">
    <div id=\"app\">{mount_markup}</div>
  <script>
    window.__BOOTSTRAP_ENDPOINT__ = {json.dumps(bootstrap_endpoint)};
    window.__RUNTIME_CONFIG__ = {runtime_config_json};
    window.__FRONTEND_CONTEXT__ = {context_json};
    window.__F3_CUSTOMER_CONTEXT__ = {f3_context_json};
  </script>
    <script src=\"/assets/plugins/global/plugins.bundle.js\"></script>
  <script type=\"module\" src=\"{shell_entry_url}\"></script>
</body>
</html>
"""


@router.get("/customer", response_class=HTMLResponse)
def customer_root_shell() -> Response:
    if bool(settings.frontend_customer_shell_enabled):
        return HTMLResponse(_build_customer_shell_html("/customer"))

    increment_shell_disabled("customer", "/customer")
    logger.warning("customer shell disabled; returning 503 for path=/customer")

    return Response(status_code=503, content="Customer shell no disponible")


@router.get("/customer/{path:path}", response_class=HTMLResponse)
def customer_path_shell(path: str) -> Response:
    normalized_path = str(path or "").lstrip("/")
    pathname = f"/customer/{normalized_path}" if normalized_path else "/customer"

    if bool(settings.frontend_customer_shell_enabled):
        return HTMLResponse(_build_customer_shell_html(pathname))

    increment_shell_disabled("customer", pathname)
    logger.warning("customer shell disabled; returning 503 for path=%s", pathname)

    return Response(status_code=503, content="Customer shell no disponible")
