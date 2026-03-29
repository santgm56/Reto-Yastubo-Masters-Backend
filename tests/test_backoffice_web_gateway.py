from app.routers.web import backoffice_shell


def test_admin_and_seller_shell_served_by_fastapi_when_enabled(client) -> None:
    original_admin_enabled = backoffice_shell.settings.frontend_admin_shell_enabled
    original_seller_enabled = backoffice_shell.settings.frontend_seller_shell_enabled
    original_entry = backoffice_shell.settings.frontend_shell_entry_url
    original_base = backoffice_shell.settings.public_api_base_url
    original_admin_legacy = backoffice_shell.settings.frontend_admin_legacy_base_url
    original_seller_legacy = backoffice_shell.settings.frontend_seller_legacy_base_url

    backoffice_shell.settings.frontend_admin_shell_enabled = True
    backoffice_shell.settings.frontend_seller_shell_enabled = True
    backoffice_shell.settings.frontend_shell_entry_url = "http://127.0.0.1:5173/resources/js/app.js"
    backoffice_shell.settings.public_api_base_url = "http://127.0.0.1:8001"
    backoffice_shell.settings.frontend_admin_legacy_base_url = ""
    backoffice_shell.settings.frontend_seller_legacy_base_url = ""

    try:
        admin_response = client.get("/admin/dashboard")
        seller_response = client.get("/seller/ventas")
    finally:
        backoffice_shell.settings.frontend_admin_shell_enabled = original_admin_enabled
        backoffice_shell.settings.frontend_seller_shell_enabled = original_seller_enabled
        backoffice_shell.settings.frontend_shell_entry_url = original_entry
        backoffice_shell.settings.public_api_base_url = original_base
        backoffice_shell.settings.frontend_admin_legacy_base_url = original_admin_legacy
        backoffice_shell.settings.frontend_seller_legacy_base_url = original_seller_legacy

    assert admin_response.status_code == 200
    admin_body = admin_response.text
    assert "data-f3-backoffice-shell=\"1\"" in admin_body
    assert "data-f3-channel=\"admin\"" in admin_body
    assert "window.__BOOTSTRAP_ENDPOINT__" in admin_body
    assert "http://127.0.0.1:8001/api/v1/frontend/bootstrap" in admin_body
    assert "window.__F3_BACKOFFICE_CONTEXT__" in admin_body

    assert seller_response.status_code == 200
    seller_body = seller_response.text
    assert "data-f3-backoffice-shell=\"1\"" in seller_body
    assert "data-f3-channel=\"seller\"" in seller_body
    assert "window.__BOOTSTRAP_ENDPOINT__" in seller_body
    assert "http://127.0.0.1:8001/api/v1/frontend/bootstrap" in seller_body
    assert "window.__F3_BACKOFFICE_CONTEXT__" in seller_body


def test_admin_and_seller_shell_return_503_when_disabled_even_if_legacy_redirect_is_enabled(client) -> None:
    original_admin_enabled = backoffice_shell.settings.frontend_admin_shell_enabled
    original_seller_enabled = backoffice_shell.settings.frontend_seller_shell_enabled
    original_admin_legacy = backoffice_shell.settings.frontend_admin_legacy_base_url
    original_seller_legacy = backoffice_shell.settings.frontend_seller_legacy_base_url
    original_global_legacy_redirects = backoffice_shell.settings.frontend_legacy_redirects_enabled
    original_admin_legacy_redirect_enabled = backoffice_shell.settings.frontend_admin_legacy_redirect_enabled
    original_seller_legacy_redirect_enabled = backoffice_shell.settings.frontend_seller_legacy_redirect_enabled

    backoffice_shell.settings.frontend_admin_shell_enabled = False
    backoffice_shell.settings.frontend_seller_shell_enabled = False
    backoffice_shell.settings.frontend_admin_legacy_base_url = "http://127.0.0.1:8000"
    backoffice_shell.settings.frontend_seller_legacy_base_url = "http://127.0.0.1:8000"
    backoffice_shell.settings.frontend_legacy_redirects_enabled = True
    backoffice_shell.settings.frontend_admin_legacy_redirect_enabled = True
    backoffice_shell.settings.frontend_seller_legacy_redirect_enabled = True

    try:
        admin_response = client.get("/admin/reportes", follow_redirects=False)
        seller_response = client.get("/seller/ordenes", follow_redirects=False)
    finally:
        backoffice_shell.settings.frontend_admin_shell_enabled = original_admin_enabled
        backoffice_shell.settings.frontend_seller_shell_enabled = original_seller_enabled
        backoffice_shell.settings.frontend_admin_legacy_base_url = original_admin_legacy
        backoffice_shell.settings.frontend_seller_legacy_base_url = original_seller_legacy
        backoffice_shell.settings.frontend_legacy_redirects_enabled = original_global_legacy_redirects
        backoffice_shell.settings.frontend_admin_legacy_redirect_enabled = original_admin_legacy_redirect_enabled
        backoffice_shell.settings.frontend_seller_legacy_redirect_enabled = original_seller_legacy_redirect_enabled

    assert admin_response.status_code == 503
    assert "shell no disponible" in admin_response.text

    assert seller_response.status_code == 503
    assert "shell no disponible" in seller_response.text


def test_backoffice_shell_returns_503_when_legacy_redirects_are_globally_disabled(client) -> None:
    original_admin_enabled = backoffice_shell.settings.frontend_admin_shell_enabled
    original_seller_enabled = backoffice_shell.settings.frontend_seller_shell_enabled
    original_admin_legacy = backoffice_shell.settings.frontend_admin_legacy_base_url
    original_seller_legacy = backoffice_shell.settings.frontend_seller_legacy_base_url
    original_global_legacy_redirects = backoffice_shell.settings.frontend_legacy_redirects_enabled

    backoffice_shell.settings.frontend_admin_shell_enabled = False
    backoffice_shell.settings.frontend_seller_shell_enabled = False
    backoffice_shell.settings.frontend_admin_legacy_base_url = "http://127.0.0.1:8000"
    backoffice_shell.settings.frontend_seller_legacy_base_url = "http://127.0.0.1:8000"
    backoffice_shell.settings.frontend_legacy_redirects_enabled = False

    try:
        admin_response = client.get("/admin/reportes", follow_redirects=False)
        seller_response = client.get("/seller/ordenes", follow_redirects=False)
    finally:
        backoffice_shell.settings.frontend_admin_shell_enabled = original_admin_enabled
        backoffice_shell.settings.frontend_seller_shell_enabled = original_seller_enabled
        backoffice_shell.settings.frontend_admin_legacy_base_url = original_admin_legacy
        backoffice_shell.settings.frontend_seller_legacy_base_url = original_seller_legacy
        backoffice_shell.settings.frontend_legacy_redirects_enabled = original_global_legacy_redirects

    assert admin_response.status_code == 503
    assert "shell no disponible" in admin_response.text
    assert seller_response.status_code == 503
    assert "shell no disponible" in seller_response.text


def test_admin_shell_returns_503_when_admin_legacy_redirect_is_disabled(client) -> None:
    original_admin_enabled = backoffice_shell.settings.frontend_admin_shell_enabled
    original_seller_enabled = backoffice_shell.settings.frontend_seller_shell_enabled
    original_admin_legacy = backoffice_shell.settings.frontend_admin_legacy_base_url
    original_seller_legacy = backoffice_shell.settings.frontend_seller_legacy_base_url
    original_global_legacy_redirects = backoffice_shell.settings.frontend_legacy_redirects_enabled
    original_admin_legacy_redirect_enabled = backoffice_shell.settings.frontend_admin_legacy_redirect_enabled
    original_seller_legacy_redirect_enabled = backoffice_shell.settings.frontend_seller_legacy_redirect_enabled

    backoffice_shell.settings.frontend_admin_shell_enabled = False
    backoffice_shell.settings.frontend_seller_shell_enabled = False
    backoffice_shell.settings.frontend_admin_legacy_base_url = "http://127.0.0.1:8000"
    backoffice_shell.settings.frontend_seller_legacy_base_url = "http://127.0.0.1:8000"
    backoffice_shell.settings.frontend_legacy_redirects_enabled = True
    backoffice_shell.settings.frontend_admin_legacy_redirect_enabled = False
    backoffice_shell.settings.frontend_seller_legacy_redirect_enabled = True

    try:
        admin_response = client.get("/admin/reportes", follow_redirects=False)
        seller_response = client.get("/seller/ordenes", follow_redirects=False)
    finally:
        backoffice_shell.settings.frontend_admin_shell_enabled = original_admin_enabled
        backoffice_shell.settings.frontend_seller_shell_enabled = original_seller_enabled
        backoffice_shell.settings.frontend_admin_legacy_base_url = original_admin_legacy
        backoffice_shell.settings.frontend_seller_legacy_base_url = original_seller_legacy
        backoffice_shell.settings.frontend_legacy_redirects_enabled = original_global_legacy_redirects
        backoffice_shell.settings.frontend_admin_legacy_redirect_enabled = original_admin_legacy_redirect_enabled
        backoffice_shell.settings.frontend_seller_legacy_redirect_enabled = original_seller_legacy_redirect_enabled

    assert admin_response.status_code == 503
    assert "shell no disponible" in admin_response.text
    assert seller_response.status_code == 503
    assert "shell no disponible" in seller_response.text


def test_seller_shell_returns_503_when_seller_legacy_redirect_is_disabled(client) -> None:
    original_admin_enabled = backoffice_shell.settings.frontend_admin_shell_enabled
    original_seller_enabled = backoffice_shell.settings.frontend_seller_shell_enabled
    original_admin_legacy = backoffice_shell.settings.frontend_admin_legacy_base_url
    original_seller_legacy = backoffice_shell.settings.frontend_seller_legacy_base_url
    original_global_legacy_redirects = backoffice_shell.settings.frontend_legacy_redirects_enabled
    original_admin_legacy_redirect_enabled = backoffice_shell.settings.frontend_admin_legacy_redirect_enabled
    original_seller_legacy_redirect_enabled = backoffice_shell.settings.frontend_seller_legacy_redirect_enabled

    backoffice_shell.settings.frontend_admin_shell_enabled = False
    backoffice_shell.settings.frontend_seller_shell_enabled = False
    backoffice_shell.settings.frontend_admin_legacy_base_url = "http://127.0.0.1:8000"
    backoffice_shell.settings.frontend_seller_legacy_base_url = "http://127.0.0.1:8000"
    backoffice_shell.settings.frontend_legacy_redirects_enabled = True
    backoffice_shell.settings.frontend_admin_legacy_redirect_enabled = True
    backoffice_shell.settings.frontend_seller_legacy_redirect_enabled = False

    try:
        admin_response = client.get("/admin/reportes", follow_redirects=False)
        seller_response = client.get("/seller/ordenes", follow_redirects=False)
    finally:
        backoffice_shell.settings.frontend_admin_shell_enabled = original_admin_enabled
        backoffice_shell.settings.frontend_seller_shell_enabled = original_seller_enabled
        backoffice_shell.settings.frontend_admin_legacy_base_url = original_admin_legacy
        backoffice_shell.settings.frontend_seller_legacy_base_url = original_seller_legacy
        backoffice_shell.settings.frontend_legacy_redirects_enabled = original_global_legacy_redirects
        backoffice_shell.settings.frontend_admin_legacy_redirect_enabled = original_admin_legacy_redirect_enabled
        backoffice_shell.settings.frontend_seller_legacy_redirect_enabled = original_seller_legacy_redirect_enabled

    assert admin_response.status_code == 503
    assert "shell no disponible" in admin_response.text
    assert seller_response.status_code == 503
    assert "shell no disponible" in seller_response.text
