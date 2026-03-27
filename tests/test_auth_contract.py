from app.routers.v1 import auth as auth_router
from app.services.auth_service import AuthService


def test_auth_login_sets_http_only_cookies_and_hides_refresh_token(client, monkeypatch) -> None:
    def fake_login(_self, _email: str, _password: str) -> dict:
        return {
            "access_token": "access-token-001",
            "refresh_token": "refresh-token-001",
            "token_type": "bearer",
            "expires_in": 3600,
            "user": {
                "id": 10,
                "name": "Admin Test",
                "role": "ADMIN",
                "permissions": ["payments.read.all"],
            },
        }

    monkeypatch.setattr(AuthService, "login", fake_login)

    response = client.post(
        "/api/v1/auth/login",
        json={
            "email": "admin@test.com",
            "password": "secret",
        },
    )

    assert response.status_code == 200
    payload = response.json()

    assert payload.get("ok") is True
    data = payload.get("data") or {}
    assert data.get("access_token") == "access-token-001"
    assert "refresh_token" not in data

    set_cookie_header = response.headers.get("set-cookie", "")
    assert "yastubo_refresh_token=" in set_cookie_header
    assert "yastubo_access_token=" in set_cookie_header
    assert "HttpOnly" in set_cookie_header


def test_auth_refresh_accepts_cookie_token_without_body(client, monkeypatch) -> None:
    def fake_refresh(_self, refresh_token: str) -> dict:
        assert refresh_token == "cookie-refresh-token"
        return {
            "access_token": "access-token-new",
            "token_type": "bearer",
            "expires_in": 3600,
        }

    monkeypatch.setattr(AuthService, "refresh", fake_refresh)

    response = client.post(
        "/api/v1/auth/refresh",
        cookies={"yastubo_refresh_token": "cookie-refresh-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    data = payload.get("data") or {}

    assert data.get("access_token") == "access-token-new"


def test_auth_refresh_uses_body_fallback_when_cookie_missing(client, monkeypatch) -> None:
    def fake_refresh(_self, refresh_token: str) -> dict:
        assert refresh_token == "body-refresh-token"
        return {
            "access_token": "access-token-body",
            "token_type": "bearer",
            "expires_in": 3600,
        }

    monkeypatch.setattr(AuthService, "refresh", fake_refresh)

    response = client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": "body-refresh-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload.get("ok") is True


def test_auth_me_accepts_access_cookie_without_authorization_header(client, monkeypatch) -> None:
    def fake_me(_self, access_token: str) -> dict:
        assert access_token == "cookie-access-token"
        return {
            "id": 99,
            "name": "Cookie User",
            "email": "cookie@test.com",
            "role": "CUSTOMER",
            "permissions": ["customer.products.read"],
            "status": "ACTIVE",
        }

    monkeypatch.setattr(AuthService, "me", fake_me)

    response = client.get(
        "/api/v1/auth/me",
        cookies={"yastubo_access_token": "cookie-access-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    data = payload.get("data") or {}

    assert data.get("role") == "CUSTOMER"
    assert isinstance(data.get("permissions"), list)


def test_auth_logout_revokes_using_cookie_and_clears_auth_cookies(client, monkeypatch) -> None:
    def fake_logout(_self, refresh_token: str | None) -> dict:
        assert refresh_token == "cookie-refresh-token"
        return {"revoked": True}

    monkeypatch.setattr(AuthService, "logout", fake_logout)

    response = client.post(
        "/api/v1/auth/logout",
        cookies={"yastubo_refresh_token": "cookie-refresh-token"},
        json={},
    )

    assert response.status_code == 200
    payload = response.json()

    assert payload.get("ok") is True
    assert (payload.get("data") or {}).get("revoked") is True

    set_cookie_header = response.headers.get("set-cookie", "")
    assert "yastubo_refresh_token=" in set_cookie_header
    assert "yastubo_access_token=" in set_cookie_header


def test_auth_login_rate_limit_returns_429_after_too_many_failures(client, monkeypatch) -> None:
    auth_router.__dict__["_LOGIN_ATTEMPTS"].clear()

    def fake_login_fail(_self, _email: str, _password: str) -> dict:
        raise ValueError("Credenciales invalidas.")

    monkeypatch.setattr(AuthService, "login", fake_login_fail)

    for _ in range(5):
        response = client.post(
            "/api/v1/auth/login",
            json={"email": "ratelimit@test.com", "password": "bad-pass"},
        )
        assert response.status_code == 401

    blocked = client.post(
        "/api/v1/auth/login",
        json={"email": "ratelimit@test.com", "password": "bad-pass"},
    )

    assert blocked.status_code == 429
    payload = blocked.json()
    assert payload.get("code") == "API_TOO_MANY_REQUESTS"

    auth_router.__dict__["_LOGIN_ATTEMPTS"].clear()
