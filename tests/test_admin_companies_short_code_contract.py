from app.db.database import get_db
from app.main import app
from app.services.auth_service import AuthService


class _FakeResult:
    def __init__(self, *, first_row=None):
        self._first_row = first_row

    def mappings(self):
        return self

    def first(self):
        return self._first_row


class _FakeDb:
    def __init__(self, exists: bool):
        self.exists = exists
        self.calls = []

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        self.calls.append({"sql": sql, "params": params})

        if "FROM companies" in sql and "UPPER(short_code) = :short_code" in sql:
            return _FakeResult(first_row={"one": 1} if self.exists else None)

        return _FakeResult(first_row=None)


def test_admin_companies_short_code_available_contract(client, monkeypatch):
    fake_db = _FakeDb(exists=False)

    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(
        AuthService,
        "me",
        lambda _self, _token: {"permissions": [], "role": "ADMIN"},
    )

    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.get(
            "/api/v1/admin/companies/check-short-code",
            params={"short_code": "abc", "company_id": 9},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload == {
        "short_code": "ABC",
        "is_available": True,
        "reason": None,
    }

    assert fake_db.calls[0]["params"]["short_code"] == "ABC"
    assert fake_db.calls[0]["params"]["company_id"] == 9


def test_admin_companies_short_code_taken_contract(client, monkeypatch):
    fake_db = _FakeDb(exists=True)

    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(
        AuthService,
        "me",
        lambda _self, _token: {"permissions": [], "role": "ADMIN"},
    )

    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.get(
            "/api/v1/admin/companies/check-short-code",
            params={"short_code": "abc"},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload == {
        "short_code": "ABC",
        "is_available": False,
        "reason": "taken",
    }


def test_admin_companies_short_code_empty_contract(client, monkeypatch):
    fake_db = _FakeDb(exists=False)

    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(
        AuthService,
        "me",
        lambda _self, _token: {"permissions": [], "role": "ADMIN"},
    )

    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.get(
            "/api/v1/admin/companies/check-short-code",
            params={"short_code": "  "},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload == {
        "short_code": "",
        "is_available": False,
        "reason": "empty",
    }


def test_admin_companies_short_code_forbidden_non_admin(client, monkeypatch):
    fake_db = _FakeDb(exists=False)

    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(
        AuthService,
        "me",
        lambda _self, _token: {"permissions": [], "role": "CUSTOMER"},
    )

    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.get(
            "/api/v1/admin/companies/check-short-code",
            params={"short_code": "abc"},
            cookies={"yastubo_access_token": "token-other"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 403
