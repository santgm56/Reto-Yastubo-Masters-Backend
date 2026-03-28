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
        self.company = {
            "id": 22,
            "name": "Compania Test",
            "short_code": "TEST",
            "phone": "555000",
            "email": "company@test.com",
            "description": "Desc",
            "status": "active",
            "commission_beneficiary_user_id": 8,
            "branding_logo_file_id": 100,
            "pdf_template_id": 7,
            "branding_text_dark": None,
            "branding_bg_light": None,
            "branding_text_light": None,
            "branding_bg_dark": None,
        }
        self.users = {8, 9, 10}
        self.attachments = {(22, 8), (22, 9)}

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        self.calls.append({"sql": sql, "params": params})

        if "FROM companies" in sql and "WHERE id = :company_id" in sql:
            if int(params.get("company_id") or 0) == 22:
                return _FakeResult(first_row=dict(self.company))
            return _FakeResult(first_row=None)

        if "FROM users" in sql and "WHERE id = :user_id" in sql:
            user_id = int(params.get("user_id") or 0)
            if user_id in self.users:
                return _FakeResult(first_row={"id": user_id})
            return _FakeResult(first_row=None)

        if "FROM company_user" in sql and "WHERE company_id = :company_id AND user_id = :user_id" in sql and "SELECT 1" in sql:
            key = (int(params.get("company_id") or 0), int(params.get("user_id") or 0))
            return _FakeResult(first_row={"one": 1} if key in self.attachments else None)

        if "INSERT INTO company_user" in sql:
            key = (int(params.get("company_id") or 0), int(params.get("user_id") or 0))
            self.attachments.add(key)
            return _FakeResult(first_row=None)

        if "DELETE FROM company_user" in sql:
            key = (int(params.get("company_id") or 0), int(params.get("user_id") or 0))
            self.attachments.discard(key)
            return _FakeResult(first_row=None)

        if "UPDATE companies" in sql and "commission_beneficiary_user_id = NULL" in sql:
            self.company["commission_beneficiary_user_id"] = None
            return _FakeResult(first_row=None)

        if "SELECT user_id" in sql and "FROM company_user" in sql:
            company_id = int(params.get("company_id") or 0)
            rows = [{"user_id": user_id} for (cid, user_id) in sorted(self.attachments) if cid == company_id]
            return _FakeResult(all_rows=rows)

        if "FROM files" in sql and "WHERE id = :file_id" in sql:
            if int(params.get("file_id") or 0) == 100:
                return _FakeResult(
                    first_row={
                        "id": 100,
                        "uuid": "logo-uuid",
                        "original_name": "logo.png",
                    }
                )
            return _FakeResult(first_row=None)

        return _FakeResult(first_row=None)

    def commit(self):
        return None


def test_admin_companies_users_attach_contract(client, monkeypatch):
    fake_db = _FakeDb()

    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(AuthService, "me", lambda _self, _token: {"permissions": [], "role": "ADMIN"})
    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.post(
            "/api/v1/admin/companies/22/users/10",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["id"] == 22
    assert payload["data"]["users_ids"] == [8, 9, 10]
    assert payload["data"]["branding"]["logo"] == {
        "id": 100,
        "url": "/api/v1/files/logo-uuid",
        "original_name": "logo.png",
        "is_custom": True,
    }


def test_admin_companies_users_detach_contract(client, monkeypatch):
    fake_db = _FakeDb()

    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(AuthService, "me", lambda _self, _token: {"permissions": [], "role": "ADMIN"})
    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.delete(
            "/api/v1/admin/companies/22/users/8",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["id"] == 22
    assert payload["data"]["users_ids"] == [9]
    assert payload["data"]["commission_beneficiary_user_id"] is None


def test_admin_companies_users_attach_returns_404_when_company_missing(client, monkeypatch):
    fake_db = _FakeDb()

    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(AuthService, "me", lambda _self, _token: {"permissions": [], "role": "ADMIN"})
    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.post(
            "/api/v1/admin/companies/999/users/10",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 404


def test_admin_companies_users_detach_returns_404_when_user_missing(client, monkeypatch):
    fake_db = _FakeDb()

    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(AuthService, "me", lambda _self, _token: {"permissions": [], "role": "ADMIN"})
    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.delete(
            "/api/v1/admin/companies/22/users/999",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 404


def test_admin_companies_users_attach_forbidden_when_not_admin(client, monkeypatch):
    fake_db = _FakeDb()

    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(AuthService, "me", lambda _self, _token: {"permissions": [], "role": "CUSTOMER"})
    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.post(
            "/api/v1/admin/companies/22/users/10",
            cookies={"yastubo_access_token": "token-user"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 403
