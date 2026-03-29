from app.core import shell_metrics
from app.routers.web import backoffice_shell, customer_shell


def test_shell_metrics_count_disabled_shell_requests(client) -> None:
    original_admin_enabled = backoffice_shell.settings.frontend_admin_shell_enabled
    original_seller_enabled = backoffice_shell.settings.frontend_seller_shell_enabled
    original_customer_enabled = customer_shell.settings.frontend_customer_shell_enabled

    shell_metrics.reset_for_tests()

    backoffice_shell.settings.frontend_admin_shell_enabled = False
    backoffice_shell.settings.frontend_seller_shell_enabled = False
    customer_shell.settings.frontend_customer_shell_enabled = False

    try:
        client.get("/admin/dashboard", follow_redirects=False)
        client.get("/seller/ventas", follow_redirects=False)
        client.get("/customer/transacciones", follow_redirects=False)

        response = client.get("/health/shells")
    finally:
        backoffice_shell.settings.frontend_admin_shell_enabled = original_admin_enabled
        backoffice_shell.settings.frontend_seller_shell_enabled = original_seller_enabled
        customer_shell.settings.frontend_customer_shell_enabled = original_customer_enabled
        shell_metrics.reset_for_tests()

    assert response.status_code == 200

    payload = response.json()
    assert payload.get("ok") == "true"

    shells = payload.get("shells") or {}
    totals = shells.get("totals") or {}
    paths = shells.get("paths") or {}

    assert int(totals.get("admin") or 0) == 1
    assert int(totals.get("seller") or 0) == 1
    assert int(totals.get("customer") or 0) == 1

    assert int((paths.get("admin") or {}).get("/admin/dashboard") or 0) == 1
    assert int((paths.get("seller") or {}).get("/seller/ventas") or 0) == 1
    assert int((paths.get("customer") or {}).get("/customer/transacciones") or 0) == 1
