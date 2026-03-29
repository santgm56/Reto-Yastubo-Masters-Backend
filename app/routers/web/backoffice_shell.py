import json
import logging
import re

from fastapi import APIRouter
from fastapi.responses import HTMLResponse, Response

from ...core.config import get_settings
from ...core.shell_metrics import increment_shell_disabled


router = APIRouter(tags=["web-backoffice-shell"])
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


def _render_login_form(channel: str, redirect_path: str) -> str:
    return f"""
  <div class=\"container py-5\" style=\"max-width: 420px;\">
    <h1 class=\"mb-4\">Login {channel}</h1>
    <form class=\"form w-100\" method=\"GET\" action=\"/{channel}/login\" data-fastapi-login=\"true\" data-login-channel=\"{channel}\" data-login-redirect=\"{redirect_path}\">
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


def _resolve_admin_mount_markup(pathname: str) -> str:
    if pathname == "/admin/login":
        return _render_login_form("admin", "/admin")

    company_edit_match = re.fullmatch(r"/admin/companies/(\d+)/edit", pathname)
    if company_edit_match:
        company_id = int(company_edit_match.group(1))
        return (
            "<admin-companies-edit :company-id=\""
            f"{company_id}"
            "\"></admin-companies-edit>"
        )

    if pathname == "/admin/companies":
        return "<admin-companies-index></admin-companies-index>"

    if pathname == "/admin/config":
        return "<admin-config-index></admin-config-index>"

    if pathname == "/admin/countries":
        return "<admin-countries-index></admin-countries-index>"

    if pathname == "/admin/zones":
        return "<admin-zones-index></admin-zones-index>"

    if pathname == "/admin/coverages":
        return "<admin-coverages-index></admin-coverages-index>"

    if pathname == "/admin/products":
        return "<admin-products-index></admin-products-index>"

    if re.fullmatch(r"/admin/products/\d+/plans", pathname):
        return "<admin-plans-index></admin-plans-index>"

    if re.fullmatch(r"/admin/products/\d+/plans/\d+/edit", pathname):
        return "<admin-plans-edit></admin-plans-edit>"

    if pathname == "/admin/regalias":
        return "<admin-regalias-index></admin-regalias-index>"

    return "<div class=\"container py-5\"><h1>Admin shell activo</h1></div>"


def _resolve_seller_mount_markup(pathname: str) -> str:
    if pathname == "/seller/login":
        return _render_login_form("seller", "/seller/dashboard")

    if pathname.startswith("/seller/customers"):
        return """
<div data-static-shell="true" class="container py-5">
  <h1>Seller Workspace</h1>
  <h2>Clientes del canal seller</h2>
</div>
"""

    if pathname.startswith("/seller/sales"):
        return """
<div data-static-shell="true" class="container py-5">
  <h1>Seller Workspace</h1>
  <h2>Ventas y cobros</h2>
</div>
"""

    return """
<div data-static-shell="true" class="container py-5">
  <h1>Seller Workspace</h1>
  <p>Clientes registrados</p>
  <p>Planes activos</p>
  <h2>Clientes recientes</h2>
</div>
"""


def _resolve_mount_markup(pathname: str, channel: str) -> str:
    if channel == "seller":
        return _resolve_seller_mount_markup(pathname)

    return _resolve_admin_mount_markup(pathname)


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
    mount_markup = _resolve_mount_markup(pathname, channel)

    return f"""<!doctype html>
<html lang=\"es\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Yastubo Backoffice</title>
    <link href=\"/assets/plugins/global/plugins.bundle.css\" rel=\"stylesheet\" type=\"text/css\" />
    <link href=\"/assets/css/style.bundle.css\" rel=\"stylesheet\" type=\"text/css\" />
</head>
<body data-f3-backoffice-shell=\"1\" data-f3-channel=\"{channel}\">
    <div id=\"app\">{mount_markup}</div>
  <script>
    window.__BOOTSTRAP_ENDPOINT__ = {json.dumps(bootstrap_endpoint)};
    window.__RUNTIME_CONFIG__ = {runtime_config_json};
    window.__FRONTEND_CONTEXT__ = {context_json};
    window.__F3_BACKOFFICE_CONTEXT__ = {f3_context_json};
  </script>
    <script src=\"/assets/plugins/global/plugins.bundle.js\"></script>
  <script type=\"module\" src=\"{shell_entry_url}\"></script>
</body>
</html>
"""


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

    increment_shell_disabled(pilot, pathname)
    logger.warning("%s shell disabled; returning 503 for path=%s", pilot, pathname)

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
