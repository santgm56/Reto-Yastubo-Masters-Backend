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
        self.last_insert_id = 0
        self.role_rows = [
            {"id": 10, "name": "system.admin", "guard_name": "admin", "scope": "system", "label": {"es": "Administrador"}, "level": 1},
        ]
        self.permission_rows = [
            {"id": 200, "name": "system.roles", "guard_name": "admin", "description": "Gestion ACL"},
        ]
        self.pivots = {(10, 200)}

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}

        if "FROM roles" in sql and "WHERE guard_name = :guard_name" in sql and "ORDER BY name, id" in sql:
            guard_name = params.get("guard_name")
            rows = [row for row in self.role_rows if row.get("guard_name") == guard_name]
            return _FakeResult(all_rows=rows)

        if "FROM permissions" in sql and "WHERE guard_name = :guard_name" in sql and "ORDER BY name, id" in sql:
            guard_name = params.get("guard_name")
            rows = [row for row in self.permission_rows if row.get("guard_name") == guard_name]
            return _FakeResult(all_rows=rows)

        if "FROM role_has_permissions" in sql and "SELECT role_id, permission_id" in sql:
            rows = [{"role_id": role_id, "permission_id": permission_id} for role_id, permission_id in sorted(self.pivots)]
            return _FakeResult(all_rows=rows)

        if "SELECT id" in sql and "FROM roles" in sql and "name = :name" in sql and "LIMIT 1" in sql and "id <>" not in sql:
            for row in self.role_rows:
                if row.get("guard_name") == params.get("guard_name") and row.get("name") == params.get("name"):
                    return _FakeResult(first_row={"id": row.get("id")})
            return _FakeResult(first_row=None)

        if "INSERT INTO roles" in sql:
            self.last_insert_id = 11
            self.role_rows.append(
                {
                    "id": 11,
                    "name": params.get("name"),
                    "guard_name": params.get("guard_name"),
                    "scope": params.get("scope"),
                    "label": params.get("label"),
                    "level": None,
                }
            )
            return _FakeResult(first_row=None)

        if "SELECT LAST_INSERT_ID() AS id" in sql:
            return _FakeResult(first_row={"id": self.last_insert_id})

        if "FROM roles" in sql and "WHERE id = :role_id" in sql:
            role_id = int(params.get("role_id") or 0)
            row = next((row for row in self.role_rows if int(row.get("id") or 0) == role_id), None)
            return _FakeResult(first_row=row)

        if "SELECT id, guard_name FROM roles WHERE id = :id" in sql:
            role_id = int(params.get("id") or 0)
            row = next((row for row in self.role_rows if int(row.get("id") or 0) == role_id), None)
            if not row:
                return _FakeResult(first_row=None)
            return _FakeResult(first_row={"id": row.get("id"), "guard_name": row.get("guard_name")})

        if "SELECT id, guard_name FROM permissions WHERE id = :id" in sql:
            permission_id = int(params.get("id") or 0)
            row = next((row for row in self.permission_rows if int(row.get("id") or 0) == permission_id), None)
            if not row:
                return _FakeResult(first_row=None)
            return _FakeResult(first_row={"id": row.get("id"), "guard_name": row.get("guard_name")})

        if "INSERT IGNORE INTO role_has_permissions" in sql:
            self.pivots.add((int(params.get("role_id") or 0), int(params.get("permission_id") or 0)))
            return _FakeResult(first_row=None)

        if "DELETE FROM role_has_permissions" in sql:
            self.pivots.discard((int(params.get("role_id") or 0), int(params.get("permission_id") or 0)))
            return _FakeResult(first_row=None)

        return _FakeResult(first_row=None)

    def commit(self):
        return None

    def rollback(self):
        return None


def _setup(monkeypatch, fake_db, permissions):
    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(
        AuthService,
        "me",
        lambda _self, _token: {
            "id": 1,
            "role": "ADMIN",
            "permissions": permissions,
        },
    )
    app.dependency_overrides[get_db] = fake_get_db


def _teardown():
    app.dependency_overrides.pop(get_db, None)


def test_admin_acl_matrix_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["system.roles"])

    try:
        response = client.get(
            "/api/v1/admin/acl/roles/admin/matrix",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["roles"]) == 1
    assert len(payload["permissions"]) == 1
    assert payload["matrix"]["10"] == [200]


def test_admin_acl_store_role_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["system.roles"])

    try:
        response = client.post(
            "/api/v1/admin/acl/roles/admin/roles",
            json={
                "name": "system.viewer",
                "scope": "system",
                "label": {"es": "Visualizador", "en": "Viewer"},
            },
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["role"]["id"] == 11
    assert payload["role"]["name"] == "system.viewer"


def test_admin_acl_toggle_forbidden_without_permission(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=[])

    try:
        response = client.post(
            "/api/v1/admin/acl/roles/admin/toggle",
            json={
                "role_id": 10,
                "permission_id": 200,
                "value": True,
            },
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 403
