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
    def __init__(self, initial_status: str = "active"):
        self.calls = []
        self.company = {
            "id": 22,
            "name": "Compania Test",
            "short_code": "TEST",
            "phone": "555000",
            "email": "company@test.com",
            "description": "Desc",
            "status": initial_status,
            "commission_beneficiary_user_id": 8,
            "branding_logo_file_id": None,
            "pdf_template_id": 7,
            "branding_text_dark": None,
            "branding_bg_light": None,
            "branding_text_light": None,
            "branding_bg_dark": None,
        }
        self.attachments = {(22, 8), (22, 9)}

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        self.calls.append({"sql": sql, "params": params})

        if "FROM companies" in sql and "WHERE id = :company_id" in sql:
            if int(params.get("company_id") or 0) == 22:
                return _FakeResult(first_row=dict(self.company))
            return _FakeResult(first_row=None)

        if "UPDATE companies" in sql and "SET status = :status" in sql:
            if int(params.get("company_id") or 0) == 22:
                self.company["status"] = str(params.get("status") or self.company["status"])
            return _FakeResult(first_row=None)

        if "SELECT user_id" in sql and "FROM company_user" in sql:
            company_id = int(params.get("company_id") or 0)
            rows = [{"user_id": user_id} for (cid, user_id) in sorted(self.attachments) if cid == company_id]
            return _FakeResult(all_rows=rows)

        if "FROM files" in sql and "WHERE id = :file_id" in sql:
            return _FakeResult(first_row=None)

        return _FakeResult(first_row=None)

    def commit(self):
        return None


def _setup_admin(monkeypatch, fake_db):
    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(AuthService, "me", lambda _self, _token: {"permissions": [], "role": "ADMIN"})
    app.dependency_overrides[get_db] = fake_get_db


def _teardown_override():
    app.dependency_overrides.pop(get_db, None)


def test_admin_companies_suspend_contract(client, monkeypatch):
    fake_db = _FakeDb(initial_status="active")
    _setup_admin(monkeypatch, fake_db)

    try:
        response = client.put(
            "/api/v1/admin/companies/22/suspend",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["id"] == 22
    assert payload["data"]["status"] == "inactive"
    assert payload["toast"] == {
        "type": "success",
        "message": "Empresa suspendida correctamente.",
    }


def test_admin_companies_archive_contract(client, monkeypatch):
    fake_db = _FakeDb(initial_status="inactive")
    _setup_admin(monkeypatch, fake_db)

    try:
        response = client.put(
            "/api/v1/admin/companies/22/archive",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["status"] == "archived"
    assert payload["toast"] == {
        "type": "success",
        "message": "Empresa archivada correctamente.",
    }


def test_admin_companies_activate_contract(client, monkeypatch):
    fake_db = _FakeDb(initial_status="archived")
    _setup_admin(monkeypatch, fake_db)

    try:
        response = client.put(
            "/api/v1/admin/companies/22/activate",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["status"] == "active"
    assert payload["toast"] == {
        "type": "success",
        "message": "Empresa activada correctamente.",
    }


def test_admin_companies_suspend_returns_404_when_company_missing(client, monkeypatch):
    fake_db = _FakeDb(initial_status="active")
    _setup_admin(monkeypatch, fake_db)

    try:
        response = client.put(
            "/api/v1/admin/companies/999/suspend",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 404


def test_admin_companies_archive_returns_404_when_company_missing(client, monkeypatch):
    fake_db = _FakeDb(initial_status="active")
    _setup_admin(monkeypatch, fake_db)

    try:
        response = client.put(
            "/api/v1/admin/companies/999/archive",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 404


def test_admin_companies_activate_returns_404_when_company_missing(client, monkeypatch):
    fake_db = _FakeDb(initial_status="active")
    _setup_admin(monkeypatch, fake_db)

    try:
        response = client.put(
            "/api/v1/admin/companies/999/activate",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 404


def test_admin_companies_suspend_forbidden_when_not_admin(client, monkeypatch):
    fake_db = _FakeDb(initial_status="active")

    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(AuthService, "me", lambda _self, _token: {"permissions": [], "role": "CUSTOMER"})
    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.put(
            "/api/v1/admin/companies/22/suspend",
            cookies={"yastubo_access_token": "token-user"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 403


def test_admin_companies_archive_forbidden_when_not_admin(client, monkeypatch):
    fake_db = _FakeDb(initial_status="active")

    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(AuthService, "me", lambda _self, _token: {"permissions": [], "role": "CUSTOMER"})
    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.put(
            "/api/v1/admin/companies/22/archive",
            cookies={"yastubo_access_token": "token-user"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 403


def test_admin_companies_activate_forbidden_when_not_admin(client, monkeypatch):
    fake_db = _FakeDb(initial_status="active")

    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(AuthService, "me", lambda _self, _token: {"permissions": [], "role": "CUSTOMER"})
    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.put(
            "/api/v1/admin/companies/22/activate",
            cookies={"yastubo_access_token": "token-user"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 403
