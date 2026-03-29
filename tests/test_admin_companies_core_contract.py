from app.db.database import get_db
from app.main import app
from app.services.auth_service import AuthService


class _FakeResult:
    def __init__(self, *, first_row=None, all_rows=None, lastrowid=None):
        self._first_row = first_row
        self._all_rows = all_rows or []
        self.lastrowid = lastrowid

    def mappings(self):
        return self

    def first(self):
        return self._first_row

    def all(self):
        return self._all_rows


class _FakeDb:
    def __init__(self):
        self.calls = []
        self.next_company_id = 23
        self.index_rows = [
            {
                "id": 22,
                "name": "Compania Test",
                "short_code": "TEST",
                "phone": "555000",
                "email": "company@test.com",
                "description": "Desc",
                "status": "active",
                "commission_beneficiary_user_id": 8,
                "branding_logo_file_id": None,
                "pdf_template_id": 7,
                "branding_text_dark": None,
                "branding_bg_light": None,
                "branding_text_light": None,
                "branding_bg_dark": None,
            },
            {
                "id": 24,
                "name": "Beta Salud",
                "short_code": "BETA",
                "phone": "555111",
                "email": "beta@test.com",
                "description": "Desc",
                "status": "inactive",
                "commission_beneficiary_user_id": None,
                "branding_logo_file_id": None,
                "pdf_template_id": None,
                "branding_text_dark": None,
                "branding_bg_light": None,
                "branding_text_light": None,
                "branding_bg_dark": None,
            },
            {
                "id": 25,
                "name": "Gamma Life",
                "short_code": "GAMM",
                "phone": "555222",
                "email": "gamma@test.com",
                "description": "Desc",
                "status": "archived",
                "commission_beneficiary_user_id": None,
                "branding_logo_file_id": None,
                "pdf_template_id": None,
                "branding_text_dark": None,
                "branding_bg_light": None,
                "branding_text_light": None,
                "branding_bg_dark": None,
            },
        ]
        self.company = {
            "id": 22,
            "name": "Compania Test",
            "short_code": "TEST",
            "phone": "555000",
            "email": "company@test.com",
            "description": "Desc",
            "status": "active",
            "commission_beneficiary_user_id": 8,
            "branding_logo_file_id": None,
            "pdf_template_id": 7,
            "branding_text_dark": None,
            "branding_bg_light": None,
            "branding_text_light": None,
            "branding_bg_dark": None,
        }
        self.attachments = {(22, 8), (22, 9)}
        self.users = {
            8: {"id": 8, "email": "ana@test.com", "first_name": "Ana", "last_name": "Lopez"},
            9: {"id": 9, "email": "luis@test.com", "first_name": "Luis", "last_name": "Perez"},
        }
        self.templates = [{"id": 7, "name": "Plantilla A"}]

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        self.calls.append({"sql": sql, "params": params})

        # Keep list fixture aligned with the mutable primary company row.
        self.index_rows = [
            dict(self.company) if int(row.get("id") or 0) == int(self.company["id"]) else row
            for row in self.index_rows
        ]

        if "FROM companies" in sql and "UPPER(short_code)" in sql:
            short_code = str(params.get("short_code") or "")
            if short_code == "DUPL":
                return _FakeResult(first_row={"id": 99})
            return _FakeResult(first_row=None)

        if "INSERT INTO companies" in sql:
            self.company = {
                **self.company,
                "id": self.next_company_id,
                "name": params["name"],
                "short_code": params["short_code"],
                "status": "active",
            }
            return _FakeResult(lastrowid=self.next_company_id)

        if "SELECT LAST_INSERT_ID() AS id" in sql:
            return _FakeResult(first_row={"id": self.next_company_id})

        if "FROM companies" in sql and "ORDER BY name ASC" in sql:
            rows = list(self.index_rows)
            status_filter = str(params.get("status") or "").strip()
            search_filter = str(params.get("search") or "").strip().strip("%")

            if status_filter:
                rows = [row for row in rows if str(row.get("status") or "") == status_filter]

            if search_filter:
                query = search_filter.lower()
                rows = [
                    row
                    for row in rows
                    if query in str(row.get("name") or "").lower()
                    or query in str(row.get("short_code") or "").lower()
                    or query in str(row.get("phone") or "").lower()
                    or query in str(row.get("email") or "").lower()
                ]

            return _FakeResult(all_rows=rows)

        if "FROM companies" in sql and "WHERE id = :company_id" in sql:
            if int(params.get("company_id") or 0) == int(self.company["id"]):
                return _FakeResult(first_row=dict(self.company))
            return _FakeResult(first_row=None)

        if "SELECT user_id" in sql and "FROM company_user" in sql:
            company_id = int(params.get("company_id") or 0)
            rows = [{"user_id": user_id} for (cid, user_id) in sorted(self.attachments) if cid == company_id]
            return _FakeResult(all_rows=rows)

        if "FROM users u" in sql and "INNER JOIN company_user" in sql:
            company_id = int(params.get("company_id") or 0)
            rows = []
            for (cid, user_id) in sorted(self.attachments):
                if cid != company_id:
                    continue
                user = self.users[user_id]
                rows.append(dict(user))
            return _FakeResult(all_rows=rows)

        if "FROM users" in sql and "ORDER BY first_name" in sql and "INNER JOIN" not in sql:
            return _FakeResult(all_rows=[dict(v) for v in self.users.values()])

        if "FROM users WHERE id = :user_id" in sql:
            user_id = int(params.get("user_id") or 0)
            return _FakeResult(first_row={"id": user_id} if user_id in self.users else None)

        if "FROM templates" in sql and "UPPER(type) = 'PDF'" in sql:
            return _FakeResult(all_rows=list(self.templates))

        if "UPDATE companies" in sql and "WHERE id = :company_id" in sql:
            if int(params.get("company_id") or 0) != int(self.company["id"]):
                return _FakeResult(first_row=None)
            if "branding_logo_file_id = NULL" in sql:
                self.company["branding_logo_file_id"] = None
            for key, value in params.items():
                if key == "company_id":
                    continue
                self.company[key] = value
            return _FakeResult(first_row=None)

        if "FROM files" in sql and "WHERE id = :file_id" in sql:
            return _FakeResult(first_row=None)

        return _FakeResult(first_row=None)

    def commit(self):
        return None



def _setup_admin(monkeypatch, fake_db):
    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(AuthService, "me", lambda _self, _token: {"permissions": [], "role": "ADMIN", "id": 501})
    app.dependency_overrides[get_db] = fake_get_db



def _teardown_override():
    app.dependency_overrides.pop(get_db, None)



def test_admin_companies_show_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup_admin(monkeypatch, fake_db)

    try:
        response = client.get(
            "/api/v1/admin/companies/22",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["id"] == 22
    assert payload["data"]["short_code"] == "TEST"
    assert isinstance(payload["assigned_users"], list)
    assert isinstance(payload["pdf_templates"], list)


def test_admin_companies_index_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup_admin(monkeypatch, fake_db)

    try:
        response = client.get(
            "/api/v1/admin/companies?status=inactive&search=beta",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 200
    payload = response.json()
    assert payload["filters"]["status"] == "inactive"
    assert payload["filters"]["search"] == "beta"
    assert len(payload["companies"]) == 1
    assert payload["companies"][0]["short_code"] == "BETA"


def test_admin_companies_index_invalid_status_defaults_to_active(client, monkeypatch):
    fake_db = _FakeDb()
    _setup_admin(monkeypatch, fake_db)

    try:
        response = client.get(
            "/api/v1/admin/companies?status=invalid-status",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 200
    payload = response.json()
    assert payload["filters"]["status"] == "active"
    assert len(payload["companies"]) == 1
    assert payload["companies"][0]["id"] == 22



def test_admin_companies_store_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup_admin(monkeypatch, fake_db)

    try:
        response = client.post(
            "/api/v1/admin/companies",
            json={"name": "Nueva", "short_code": "NUEV"},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["id"] == 23
    assert payload["data"]["name"] == "Nueva"
    assert payload["data"]["short_code"] == "NUEV"



def test_admin_companies_update_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup_admin(monkeypatch, fake_db)

    try:
        response = client.put(
            "/api/v1/admin/companies/22",
            json={"name": "Renombrada", "short_code": "ABCD", "status": "inactive"},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["name"] == "Renombrada"
    assert payload["data"]["short_code"] == "ABCD"
    assert payload["data"]["status"] == "inactive"



def test_admin_companies_update_logo_remove_contract(client, monkeypatch):
    fake_db = _FakeDb()
    fake_db.company["branding_logo_file_id"] = 45
    _setup_admin(monkeypatch, fake_db)

    try:
        response = client.put(
            "/api/v1/admin/companies/22",
            data={"branding_logo_remove": "1"},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["branding_logo_file_id"] is None



def test_admin_companies_store_validation_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup_admin(monkeypatch, fake_db)

    try:
        response = client.post(
            "/api/v1/admin/companies",
            json={"name": "", "short_code": "12"},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 422



def test_admin_companies_store_duplicate_short_code_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup_admin(monkeypatch, fake_db)

    try:
        response = client.post(
            "/api/v1/admin/companies",
            json={"name": "Otra", "short_code": "DUPL"},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 422



def test_admin_companies_show_returns_404_when_company_missing(client, monkeypatch):
    fake_db = _FakeDb()
    _setup_admin(monkeypatch, fake_db)

    try:
        response = client.get(
            "/api/v1/admin/companies/999",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 404



def test_admin_companies_core_forbidden_when_not_admin(client, monkeypatch):
    fake_db = _FakeDb()

    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(AuthService, "me", lambda _self, _token: {"permissions": [], "role": "CUSTOMER", "id": 20})
    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.get(
            "/api/v1/admin/companies/22",
            cookies={"yastubo_access_token": "token-user"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 403
