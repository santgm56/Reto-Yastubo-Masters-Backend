from app.routers.web import customer_shell


def test_customer_shell_served_by_fastapi_when_enabled(client) -> None:
    original_enabled = customer_shell.settings.frontend_customer_shell_enabled
    original_entry = customer_shell.settings.frontend_shell_entry_url
    original_base = customer_shell.settings.public_api_base_url
    original_legacy = customer_shell.settings.frontend_customer_legacy_base_url

    customer_shell.settings.frontend_customer_shell_enabled = True
    customer_shell.settings.frontend_shell_entry_url = "http://127.0.0.1:5173/resources/js/app.js"
    customer_shell.settings.public_api_base_url = "http://127.0.0.1:8001"
    customer_shell.settings.frontend_customer_legacy_base_url = ""

    try:
        response = client.get("/customer/dashboard")
    finally:
        customer_shell.settings.frontend_customer_shell_enabled = original_enabled
        customer_shell.settings.frontend_shell_entry_url = original_entry
        customer_shell.settings.public_api_base_url = original_base
        customer_shell.settings.frontend_customer_legacy_base_url = original_legacy

    assert response.status_code == 200
    body = response.text

    assert "data-f3-customer-shell=\"1\"" in body
    assert "window.__BOOTSTRAP_ENDPOINT__" in body
    assert "http://127.0.0.1:8001/api/v1/frontend/bootstrap" in body
    assert "window.__F3_CUSTOMER_CONTEXT__" in body
    assert "legacyRetireAt" in body


def test_customer_shell_redirects_to_legacy_when_disabled(client) -> None:
    original_enabled = customer_shell.settings.frontend_customer_shell_enabled
    original_legacy = customer_shell.settings.frontend_customer_legacy_base_url
    original_customer_legacy_redirect = customer_shell.settings.frontend_customer_legacy_redirect_enabled

    customer_shell.settings.frontend_customer_shell_enabled = False
    customer_shell.settings.frontend_customer_legacy_base_url = "http://127.0.0.1:8000"
    customer_shell.settings.frontend_customer_legacy_redirect_enabled = True

    try:
        response = client.get("/customer/transacciones", follow_redirects=False)
    finally:
        customer_shell.settings.frontend_customer_shell_enabled = original_enabled
        customer_shell.settings.frontend_customer_legacy_base_url = original_legacy
        customer_shell.settings.frontend_customer_legacy_redirect_enabled = original_customer_legacy_redirect

    assert response.status_code == 307
    assert response.headers.get("location") == "http://127.0.0.1:8000/customer/transacciones"


def test_customer_shell_returns_503_when_legacy_redirects_are_globally_disabled(client) -> None:
    original_enabled = customer_shell.settings.frontend_customer_shell_enabled
    original_legacy = customer_shell.settings.frontend_customer_legacy_base_url
    original_global_legacy_redirects = customer_shell.settings.frontend_legacy_redirects_enabled

    customer_shell.settings.frontend_customer_shell_enabled = False
    customer_shell.settings.frontend_customer_legacy_base_url = "http://127.0.0.1:8000"
    customer_shell.settings.frontend_legacy_redirects_enabled = False

    try:
        response = client.get("/customer/transacciones", follow_redirects=False)
    finally:
        customer_shell.settings.frontend_customer_shell_enabled = original_enabled
        customer_shell.settings.frontend_customer_legacy_base_url = original_legacy
        customer_shell.settings.frontend_legacy_redirects_enabled = original_global_legacy_redirects

    assert response.status_code == 503
    assert "shell no disponible" in response.text


def test_customer_shell_returns_503_when_customer_legacy_redirect_is_disabled(client) -> None:
    original_enabled = customer_shell.settings.frontend_customer_shell_enabled
    original_legacy = customer_shell.settings.frontend_customer_legacy_base_url
    original_global_legacy_redirects = customer_shell.settings.frontend_legacy_redirects_enabled
    original_customer_legacy_redirect = customer_shell.settings.frontend_customer_legacy_redirect_enabled

    customer_shell.settings.frontend_customer_shell_enabled = False
    customer_shell.settings.frontend_customer_legacy_base_url = "http://127.0.0.1:8000"
    customer_shell.settings.frontend_legacy_redirects_enabled = True
    customer_shell.settings.frontend_customer_legacy_redirect_enabled = False

    try:
        response = client.get("/customer/transacciones", follow_redirects=False)
    finally:
        customer_shell.settings.frontend_customer_shell_enabled = original_enabled
        customer_shell.settings.frontend_customer_legacy_base_url = original_legacy
        customer_shell.settings.frontend_legacy_redirects_enabled = original_global_legacy_redirects
        customer_shell.settings.frontend_customer_legacy_redirect_enabled = original_customer_legacy_redirect

    assert response.status_code == 503
    assert "shell no disponible" in response.text
