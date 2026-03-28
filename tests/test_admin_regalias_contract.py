from app.db.database import get_db
from app.main import app
from app.core.config import get_settings
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
        self.regalias = [
            {
                "id": 501,
                "source_type": "user",
                "source_id": 11,
                "beneficiary_user_id": 31,
                "commission": 12.0,
            }
        ]
        self._next_regalia_id = 700

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        self.calls.append({"sql": sql, "params": params})

        if "SELECT COUNT(*) AS c FROM users u" in sql:
            return _FakeResult(first_row={"c": 1})

        if "FROM users u" in sql and "EXISTS (SELECT 1 FROM regalias r0" in sql:
            return _FakeResult(
                all_rows=[
                    {
                        "id": 31,
                        "display_name": "Beneficiario Uno",
                        "email": "beneficiario@test.com",
                        "status": "active",
                    }
                ]
            )

        if "FROM regalias" in sql and "beneficiary_user_id IN" in sql:
            return _FakeResult(all_rows=list(self.regalias))

        if "FROM users" in sql and "WHERE id IN" in sql:
            return _FakeResult(
                all_rows=[
                    {
                        "id": 11,
                        "display_name": "Origen Usuario",
                        "email": "origen@test.com",
                        "status": "active",
                    }
                ]
            )

        if "SELECT id FROM users WHERE id = :id AND realm = 'admin'" in sql:
            target_id = int(params.get("id") or 0)
            if target_id in {31, 11, 12}:
                return _FakeResult(first_row={"id": target_id})
            return _FakeResult(first_row=None)

        if "SELECT id FROM users WHERE id = :id AND realm = 'admin' LIMIT 1" in sql:
            target_id = int(params.get("id") or 0)
            if target_id in {31, 11, 12}:
                return _FakeResult(first_row={"id": target_id})
            return _FakeResult(first_row=None)

        if "SELECT beneficiary_user_id, source_id" in sql and "source_type = 'user'" in sql:
            return _FakeResult(
                all_rows=[
                    {
                        "beneficiary_user_id": row["beneficiary_user_id"],
                        "source_id": row["source_id"],
                    }
                    for row in self.regalias
                    if row["source_type"] == "user"
                ]
            )

        if "INSERT INTO regalias" in sql:
            self._next_regalia_id += 1
            self.regalias.append(
                {
                    "id": self._next_regalia_id,
                    "source_type": str(params.get("source_type") or ""),
                    "source_id": int(params.get("source_id") or 0),
                    "beneficiary_user_id": int(params.get("beneficiary_user_id") or 0),
                    "commission": float(params.get("commission") or 0),
                }
            )
            return _FakeResult()

        if "SELECT id, beneficiary_user_id, source_type, source_id, commission" in sql and "ORDER BY id DESC" in sql:
            if not self.regalias:
                return _FakeResult(first_row=None)
            row = self.regalias[-1]
            return _FakeResult(first_row=row)

        if "SELECT id" in sql and "FROM regalias" in sql and "LIMIT 1" in sql and "source_type = :source_type" in sql:
            for row in self.regalias:
                if (
                    int(row["beneficiary_user_id"]) == int(params.get("beneficiary_user_id") or -1)
                    and str(row["source_type"]) == str(params.get("source_type") or "")
                    and int(row["source_id"]) == int(params.get("source_id") or -1)
                ):
                    return _FakeResult(first_row={"id": row["id"]})
            return _FakeResult(first_row=None)

        if "SELECT id, beneficiary_user_id, source_type, source_id, commission" in sql and "WHERE id = :id" in sql:
            regalia_id = int(params.get("id") or 0)
            for row in self.regalias:
                if int(row["id"]) == regalia_id:
                    return _FakeResult(first_row=dict(row))
            return _FakeResult(first_row=None)

        if "UPDATE regalias SET commission" in sql:
            regalia_id = int(params.get("id") or 0)
            for row in self.regalias:
                if int(row["id"]) == regalia_id:
                    row["commission"] = float(params.get("commission") or 0)
            return _FakeResult()

        if "SELECT id, beneficiary_user_id, source_type, source_id" in sql and "WHERE id = :id" in sql:
            regalia_id = int(params.get("id") or 0)
            for row in self.regalias:
                if int(row["id"]) == regalia_id:
                    return _FakeResult(
                        first_row={
                            "id": row["id"],
                            "beneficiary_user_id": row["beneficiary_user_id"],
                            "source_type": row["source_type"],
                            "source_id": row["source_id"],
                        }
                    )
            return _FakeResult(first_row=None)

        if "DELETE FROM regalias WHERE id = :id" in sql:
            regalia_id = int(params.get("id") or 0)
            self.regalias = [row for row in self.regalias if int(row["id"]) != regalia_id]
            return _FakeResult()

        return _FakeResult()

    def commit(self):
        return None

    def rollback(self):
        return None



def test_admin_regalias_beneficiaries_contract_shape(client, monkeypatch):
    fake_db = _FakeDb()

    def fake_get_db():
        yield fake_db

    monkeypatch.setenv("APP_REGALIAS", "user,unit")
    get_settings.cache_clear()
    monkeypatch.setattr(
        AuthService,
        "me",
        lambda _self, _token: {"permissions": ["regalia.users.read"], "role": "ADMIN"},
    )

    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.get(
            "/api/v1/admin/regalias/beneficiaries",
            params={"page": 1, "per_page": 20, "q": "benef"},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()

    assert isinstance(payload.get("data"), list)
    assert len(payload["data"]) == 1
    assert payload["data"][0]["beneficiary"]["id"] == 31
    assert payload["data"][0]["beneficiary"]["display_name"] == "Beneficiario Uno"

    regalias = payload["data"][0].get("regalias") or []
    assert len(regalias) == 1
    assert regalias[0]["id"] == 501
    assert regalias[0]["source_type"] == "user"
    assert regalias[0]["origin_user"]["id"] == 11

    meta = (payload.get("meta") or {}).get("pagination") or {}
    assert meta == {
        "current_page": 1,
        "last_page": 1,
        "per_page": 20,
        "total": 1,
        "from": 1,
        "to": 1,
    }
    assert (payload.get("meta") or {}).get("regalias_sources") == ["user", "unit"]



def test_admin_regalias_write_endpoints_contract(client, monkeypatch):
    fake_db = _FakeDb()

    def fake_get_db():
        yield fake_db

    monkeypatch.setenv("APP_REGALIAS", "user,unit")
    get_settings.cache_clear()
    monkeypatch.setattr(
        AuthService,
        "me",
        lambda _self, _token: {"permissions": ["regalia.users.edit"], "role": "ADMIN"},
    )

    app.dependency_overrides[get_db] = fake_get_db

    try:
        created = client.post(
            "/api/v1/admin/regalias/regalias",
            json={"beneficiary_user_id": 31, "source_type": "user", "source_id": 12},
            cookies={"yastubo_access_token": "token-admin"},
        )
        assert created.status_code == 201
        created_payload = created.json()
        assert created_payload["message"] == "Regalia creada."
        created_id = int(created_payload["data"]["id"])

        updated = client.patch(
            f"/api/v1/admin/regalias/regalias/{created_id}",
            json={"commission": 18.5},
            cookies={"yastubo_access_token": "token-admin"},
        )
        assert updated.status_code == 200
        assert updated.json()["data"]["commission"] == 18.5

        deleted = client.delete(
            f"/api/v1/admin/regalias/regalias/{created_id}",
            cookies={"yastubo_access_token": "token-admin"},
        )
        assert deleted.status_code == 200
        assert deleted.json()["data"]["id"] == created_id
    finally:
        app.dependency_overrides.pop(get_db, None)



def test_admin_regalias_forbidden_without_permission(client, monkeypatch):
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
            "/api/v1/admin/regalias/beneficiaries",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 403
