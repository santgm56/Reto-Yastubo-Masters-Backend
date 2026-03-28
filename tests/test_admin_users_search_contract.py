from app.db.database import get_db
from app.main import app
from app.services.auth_service import AuthService


class _FakeResult:
    def __init__(self, *, first_row=None, all_rows=None):
        self._first_row = first_row
        self._all_rows = all_rows or []

    def mappings(self):
        return self

    def first(self):
        return self._first_row

    def all(self):
        return self._all_rows


class _FakeDb:
    def __init__(self):
        self.calls = []

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        self.calls.append({"sql": sql, "params": params})

        if "SELECT COUNT(*) AS c FROM users" in sql:
            return _FakeResult(first_row={"c": 2})

        if "FROM users" in sql and "ORDER BY display_name ASC, id ASC" in sql:
            return _FakeResult(
                all_rows=[
                    {
                        "id": 11,
                        "display_name": "Admin Uno",
                        "email": "uno@test.com",
                        "status": "active",
                    },
                    {
                        "id": 12,
                        "display_name": "Admin Dos",
                        "email": "dos@test.com",
                        "status": "suspended",
                    },
                ]
            )

        return _FakeResult()


def test_admin_users_search_contract_shape(client, monkeypatch):
    fake_db = _FakeDb()

    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(
        AuthService,
        "me",
        lambda _self, _token: {"permissions": ["users.viewAny"], "role": "ADMIN"},
    )

    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.get(
            "/api/v1/admin/users/search",
            params={"page": 1, "per_page": 20, "q": "adm", "status": "active"},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()

    assert isinstance(payload.get("data"), list)
    assert len(payload.get("data") or []) == 2
    assert payload["data"][0]["id"] == 11
    assert payload["data"][0]["display_name"] == "Admin Uno"
    assert payload["data"][0]["email"] == "uno@test.com"
    assert payload["data"][0]["status"] == "active"

    meta = (payload.get("meta") or {}).get("pagination") or {}
    assert meta == {
        "current_page": 1,
        "last_page": 1,
        "per_page": 20,
        "total": 2,
        "from": 1,
        "to": 2,
    }

    first_call = fake_db.calls[0]
    assert first_call["params"]["status"] == "active"
    assert first_call["params"]["q"] == "%adm%"


def test_admin_users_search_forbidden_without_permission(client, monkeypatch):
    fake_db = _FakeDb()

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
            "/api/v1/admin/users/search",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 403
