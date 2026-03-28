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
        self.deleted_ids = []
        self.inserted_rows = []
        self.updated_rows = []
        self.last_inserted_id = None
        self.users = {
            8: {"email": "a@test.com", "display_name": "Alpha User"},
            9: {"email": "b@test.com", "display_name": "Beta User"},
            10: {"email": "c@test.com", "display_name": "Gamma User"},
        }
        self.commission_rows = {
            55: {"id": 55, "company_id": 22, "user_id": 8, "commission": "12.5"},
            56: {"id": 56, "company_id": 22, "user_id": 9, "commission": "0"},
        }

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        self.calls.append({"sql": sql, "params": params})

        if "FROM company_commission_users ccu" in sql and "WHERE ccu.id = :commission_user_id" in sql:
            commission_user_id = int(params.get("commission_user_id") or 0)
            if commission_user_id == 999:
                return _FakeResult(
                    first_row={
                        "id": 999,
                        "company_id": 999,
                        "user_id": 8,
                        "commission": "1.00",
                        "user_ref_id": 8,
                        "email": "a@test.com",
                        "user_display_name": "Alpha User",
                    }
                )
            if commission_user_id == 404:
                return _FakeResult(first_row=None)
            row = self.commission_rows.get(commission_user_id)
            if row:
                user_id = int(row.get("user_id") or 0)
                user = self.users.get(user_id, {})
                return _FakeResult(
                    first_row={
                        "id": int(row["id"]),
                        "company_id": int(row["company_id"]),
                        "user_id": user_id,
                        "commission": row.get("commission") or "0",
                        "user_ref_id": user_id,
                        "email": user.get("email"),
                        "user_display_name": user.get("display_name"),
                    }
                )
            return _FakeResult(first_row=None)

        if "FROM company_commission_users" in sql and "WHERE id = :commission_user_id" in sql and "SELECT" in sql:
            commission_user_id = int(params.get("commission_user_id") or 0)
            if commission_user_id == 999:
                return _FakeResult(first_row={"id": 999, "company_id": 999})
            if commission_user_id == 404:
                return _FakeResult(first_row=None)
            row = self.commission_rows.get(commission_user_id)
            if row:
                return _FakeResult(first_row={"id": commission_user_id, "company_id": int(row["company_id"])})
            return _FakeResult(first_row=None)

        if "FROM users" in sql and "WHERE id = :user_id" in sql:
            user_id = int(params.get("user_id") or 0)
            if user_id in self.users:
                return _FakeResult(first_row={"id": user_id})
            return _FakeResult(first_row=None)

        if "FROM company_commission_users" in sql and "WHERE company_id = :company_id AND user_id = :user_id" in sql:
            company_id = int(params.get("company_id") or 0)
            user_id = int(params.get("user_id") or 0)
            for row in self.commission_rows.values():
                if int(row.get("company_id") or 0) == company_id and int(row.get("user_id") or 0) == user_id:
                    return _FakeResult(first_row={"id": int(row["id"])})
            return _FakeResult(first_row=None)

        if "INSERT INTO company_commission_users" in sql:
            company_id = int(params.get("company_id") or 0)
            user_id = int(params.get("user_id") or 0)
            self.last_inserted_id = 777
            self.inserted_rows.append(
                {
                    "company_id": company_id,
                    "user_id": user_id,
                    "commission": float(params.get("commission") or 0),
                }
            )
            self.commission_rows[int(self.last_inserted_id)] = {
                "id": int(self.last_inserted_id),
                "company_id": company_id,
                "user_id": user_id,
                "commission": str(float(params.get("commission") or 0)),
            }
            return _FakeResult(first_row=None)

        if "SELECT LAST_INSERT_ID() AS id" in sql:
            return _FakeResult(first_row={"id": int(self.last_inserted_id or 0)})

        if "UPDATE company_commission_users" in sql:
            commission_user_id = int(params.get("commission_user_id") or 0)
            if commission_user_id in self.commission_rows:
                self.commission_rows[commission_user_id]["commission"] = str(float(params.get("commission") or 0))
            self.updated_rows.append(
                {
                    "commission_user_id": commission_user_id,
                    "commission": float(params.get("commission") or 0),
                }
            )
            return _FakeResult(first_row=None)

        if "DELETE FROM company_commission_users" in sql:
            commission_user_id = int(params.get("commission_user_id") or 0)
            self.deleted_ids.append(commission_user_id)
            self.commission_rows.pop(commission_user_id, None)
            return _FakeResult(first_row=None)

        if "FROM company_commission_users ccu" in sql and "LEFT JOIN users u" in sql:
            rows = []
            for row in sorted(self.commission_rows.values(), key=lambda item: int(item["id"])):
                user_id = int(row.get("user_id") or 0)
                user = self.users.get(user_id, {})
                rows.append(
                    {
                        "id": int(row["id"]),
                        "user_id": user_id,
                        "commission": row.get("commission") or "0",
                        "user_ref_id": user_id,
                        "email": user.get("email"),
                        "user_display_name": user.get("display_name"),
                    }
                )
            return _FakeResult(all_rows=rows)

        if "SELECT COUNT(*) AS c" in sql and "FROM users u" in sql:
            return _FakeResult(first_row={"c": 2})

        if "FROM users u" in sql and "LEFT JOIN company_commission_users ccu" in sql:
            return _FakeResult(
                all_rows=[
                    {
                        "id": 8,
                        "email": "a@test.com",
                        "display_name": "Alpha User",
                        "commission_user_id": 55,
                    },
                    {
                        "id": 9,
                        "email": "b@test.com",
                        "display_name": "Beta User",
                        "commission_user_id": None,
                    },
                ]
            )

        return _FakeResult()

    def commit(self):
        return None


def test_admin_companies_commission_users_available_contract(client, monkeypatch):
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
            "/api/v1/admin/companies/22/commission-users/available",
            params={"page": 1, "per_page": 20, "q": "a"},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()

    assert isinstance(payload.get("data"), list)
    assert payload["data"][0]["id"] == 8
    assert payload["data"][0]["attached"] is True
    assert payload["data"][0]["commission_user_id"] == 55
    assert payload["data"][1]["id"] == 9
    assert payload["data"][1]["attached"] is False
    assert payload["data"][1]["commission_user_id"] is None

    assert payload.get("meta") == {
        "current_page": 1,
        "last_page": 1,
        "per_page": 20,
        "total": 2,
    }

    first_call = fake_db.calls[0]
    assert first_call["params"]["company_id"] == 22
    assert first_call["params"]["q"] == "%a%"


def test_admin_companies_commission_users_index_contract(client, monkeypatch):
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
            "/api/v1/admin/companies/22/commission-users",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload.get("data"), list)
    assert payload["data"][0] == {
        "id": 55,
        "user_id": 8,
        "commission": "12.50",
        "user": {
            "id": 8,
            "email": "a@test.com",
            "display_name": "Alpha User",
        },
    }
    assert payload["data"][1]["commission"] == "0.00"


def test_admin_companies_commission_users_available_forbidden(client, monkeypatch):
    fake_db = _FakeDb()

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
            "/api/v1/admin/companies/22/commission-users/available",
            cookies={"yastubo_access_token": "token-user"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 403


def test_admin_companies_commission_users_destroy_contract(client, monkeypatch):
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
        response = client.delete(
            "/api/v1/admin/companies/22/commission-users/55",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload == {
        "toast": {
            "type": "success",
            "message": "Usuario eliminado de la lista de comisiones.",
        }
    }
    assert 55 in fake_db.deleted_ids


def test_admin_companies_commission_users_destroy_returns_404_when_not_found(client, monkeypatch):
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
        response = client.delete(
            "/api/v1/admin/companies/22/commission-users/404",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 404


def test_admin_companies_commission_users_destroy_returns_404_when_company_mismatch(client, monkeypatch):
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
        response = client.delete(
            "/api/v1/admin/companies/22/commission-users/999",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 404


def test_admin_companies_commission_users_store_contract(client, monkeypatch):
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
        response = client.post(
            "/api/v1/admin/companies/22/commission-users",
            json={"user_id": 10},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()

    assert payload["data"] == {
        "id": 777,
        "user_id": 10,
        "commission": "0.00",
        "user": {
            "id": 10,
            "email": "c@test.com",
            "display_name": "Gamma User",
        },
    }
    assert payload["toast"] == {
        "type": "success",
        "message": "Usuario anadido a la lista de comisiones.",
    }
    assert fake_db.inserted_rows == [
        {"company_id": 22, "user_id": 10, "commission": 0.0}
    ]


def test_admin_companies_commission_users_store_returns_422_when_user_does_not_exist(client, monkeypatch):
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
        response = client.post(
            "/api/v1/admin/companies/22/commission-users",
            json={"user_id": 321},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 422
    payload = response.json()
    assert payload["code"] == "API_HTTP_ERROR"
    assert payload["message"] == "Validation Error"


def test_admin_companies_commission_users_store_returns_422_when_already_attached(client, monkeypatch):
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
        response = client.post(
            "/api/v1/admin/companies/22/commission-users",
            json={"user_id": 8},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 422
    payload = response.json()
    assert payload["code"] == "API_HTTP_ERROR"
    assert payload["message"] == "El usuario ya esta asociado como beneficiario de comisiones en esta empresa."


def test_admin_companies_commission_users_update_contract(client, monkeypatch):
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
        response = client.patch(
            "/api/v1/admin/companies/22/commission-users/55",
            json={"commission": 24.5},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"] == {
        "id": 55,
        "user_id": 8,
        "commission": "24.50",
        "user": {
            "id": 8,
            "email": "a@test.com",
            "display_name": "Alpha User",
        },
    }
    assert payload["toast"] == {
        "type": "success",
        "message": "Comision actualizada correctamente.",
    }
    assert fake_db.updated_rows == [{"commission_user_id": 55, "commission": 24.5}]


def test_admin_companies_commission_users_update_returns_404_when_company_mismatch(client, monkeypatch):
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
        response = client.patch(
            "/api/v1/admin/companies/22/commission-users/999",
            json={"commission": 5},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 404
