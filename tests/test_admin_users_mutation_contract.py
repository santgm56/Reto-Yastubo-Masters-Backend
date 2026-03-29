from app.db.database import get_db
from app.main import app
from app.services.auth_service import AuthService


class _FakeResult:
    def __init__(self, *, first_row=None, all_rows=None, rowcount=0):
        self._first_row = first_row
        self._all_rows = all_rows or []
        self.rowcount = rowcount

    def mappings(self):
        return self

    def first(self):
        return self._first_row

    def all(self):
        return self._all_rows


class _FakeDb:
    def __init__(self):
        self.calls = []
        self.commit_calls = 0
        self.last_insert_id = 0
        self.roles = {
            "admin": {"id": 1, "name": "admin", "label": "Administrador"},
            "vendedor_regular": {"id": 2, "name": "vendedor_regular", "label": "Vendedor regular"},
            "vendedor_capitados": {"id": 3, "name": "vendedor_capitados", "label": "Vendedor capitados"},
            "superadmin": {"id": 4, "name": "superadmin", "label": "Superadmin"},
        }
        self.users = {
            11: {
                "id": 11,
                "realm": "admin",
                "first_name": "Admin",
                "last_name": "Uno",
                "display_name": "Admin Uno",
                "email": "uno@test.com",
                "status": "active",
                "force_password_change": True,
                "password": "hash-uno",
                "last_login_at": "2026-03-29 10:00:00",
                "deleted_at": None,
            },
            12: {
                "id": 12,
                "realm": "admin",
                "first_name": "Admin",
                "last_name": "Dos",
                "display_name": "Admin Dos",
                "email": "dos@test.com",
                "status": "suspended",
                "force_password_change": True,
                "password": "hash-dos",
                "last_login_at": None,
                "deleted_at": "2026-03-20 09:00:00",
            },
        }
        self.staff_profiles = {
            11: {
                "user_id": 11,
                "work_phone": "+56 9 1111 1111",
                "notes_admin": "nota uno",
                "commission_regular_first_year_pct": 12.5,
                "commission_regular_renewal_pct": 5.0,
                "commission_capitados_pct": 3.0,
            },
            12: {
                "user_id": 12,
                "work_phone": "+56 9 2222 2222",
                "notes_admin": "nota dos",
                "commission_regular_first_year_pct": None,
                "commission_regular_renewal_pct": None,
                "commission_capitados_pct": None,
            },
        }
        self.role_assignments = {11: ["admin"], 12: ["vendedor_regular"]}
        self.sessions_by_user = {11: 3, 12: 2}
        self.permission_by_role = {
            "admin": [],
            "vendedor_regular": ["sales.regular.use"],
            "vendedor_capitados": ["sales.capitados.use"],
            "superadmin": ["rbac.superadmin.identity"],
        }

    def commit(self):
        self.commit_calls += 1

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        self.calls.append({"sql": sql, "params": params})

        if "SELECT id\n            FROM users\n            WHERE realm = 'admin'" in sql:
            email = str(params.get("email") or "").lower()
            exclude_user_id = params.get("exclude_user_id")
            for user in self.users.values():
                if user["deleted_at"] is not None:
                    continue
                if exclude_user_id is not None and int(user["id"]) == int(exclude_user_id):
                    continue
                if str(user["email"]).lower() == email:
                    return _FakeResult(first_row={"id": user["id"]})
            return _FakeResult()

        if "SELECT\n                id,\n                name,\n                COALESCE(NULLIF(label, ''), name) AS label\n            FROM roles" in sql:
            return _FakeResult(all_rows=list(self.roles.values()))

        if "SELECT id\n                FROM roles\n                WHERE guard_name = 'admin'" in sql:
            role = self.roles.get(str(params.get("role_name") or ""))
            return _FakeResult(first_row={"id": role["id"]} if role else None)

        if "DELETE FROM model_has_roles" in sql:
            self.role_assignments[int(params["user_id"])] = []
            return _FakeResult(rowcount=1)

        if "INSERT INTO model_has_roles" in sql:
            user_id = int(params["user_id"])
            role_id = int(params["role_id"])
            role = next((item for item in self.roles.values() if int(item["id"]) == role_id), None)
            if role:
                self.role_assignments.setdefault(user_id, []).append(role["name"])
            return _FakeResult(rowcount=1)

        if "SELECT user_id FROM staff_profiles" in sql:
            user_id = int(params["user_id"])
            profile = self.staff_profiles.get(user_id)
            return _FakeResult(first_row={"user_id": user_id} if profile else None)

        if "INSERT INTO staff_profiles" in sql:
            user_id = int(params["user_id"])
            self.staff_profiles[user_id] = {
                "user_id": user_id,
                "work_phone": params.get("work_phone"),
                "notes_admin": params.get("notes_admin"),
                "commission_regular_first_year_pct": params.get("regular_first"),
                "commission_regular_renewal_pct": params.get("regular_renewal"),
                "commission_capitados_pct": params.get("capitados"),
            }
            return _FakeResult(rowcount=1)

        if "UPDATE staff_profiles" in sql:
            user_id = int(params["user_id"])
            profile = self.staff_profiles.setdefault(user_id, {"user_id": user_id})
            profile["work_phone"] = params.get("work_phone")
            profile["notes_admin"] = params.get("notes_admin")
            if "regular_first" in params:
                profile["commission_regular_first_year_pct"] = params.get("regular_first")
                profile["commission_regular_renewal_pct"] = params.get("regular_renewal")
                profile["commission_capitados_pct"] = params.get("capitados")
            return _FakeResult(rowcount=1)

        if "INSERT INTO users" in sql:
            new_id = max(self.users) + 1
            self.last_insert_id = new_id
            self.users[new_id] = {
                "id": new_id,
                "realm": "admin",
                "first_name": params.get("first_name"),
                "last_name": params.get("last_name"),
                "display_name": params.get("display_name"),
                "email": params.get("email"),
                "status": params.get("status"),
                "force_password_change": True,
                "password": params.get("password"),
                "last_login_at": None,
                "deleted_at": None,
            }
            self.sessions_by_user[new_id] = 0
            return _FakeResult(rowcount=1)

        if "SELECT LAST_INSERT_ID() AS id" in sql:
            return _FakeResult(first_row={"id": self.last_insert_id})

        if "UPDATE users\n            SET first_name = :first_name" in sql:
            user = self.users[int(params["user_id"])]
            user["first_name"] = params.get("first_name")
            user["last_name"] = params.get("last_name")
            user["display_name"] = params.get("display_name")
            user["email"] = params.get("email")
            user["status"] = params.get("status")
            return _FakeResult(rowcount=1)

        if "UPDATE users\n            SET deleted_at = NOW()" in sql:
            self.users[int(params["user_id"])] ["deleted_at"] = "2026-03-29 12:00:00"
            return _FakeResult(rowcount=1)

        if "UPDATE users\n            SET deleted_at = NULL" in sql:
            self.users[int(params["user_id"])] ["deleted_at"] = None
            return _FakeResult(rowcount=1)

        if "DELETE FROM sessions WHERE user_id = :user_id" in sql:
            user_id = int(params["user_id"])
            revoked = int(self.sessions_by_user.get(user_id, 0))
            self.sessions_by_user[user_id] = 0
            return _FakeResult(rowcount=revoked)

        if "FROM users u\n            LEFT JOIN staff_profiles sp" in sql:
            user_id = int(params["user_id"])
            user = self.users.get(user_id)
            if not user:
                return _FakeResult()
            if "u.deleted_at IS NULL" in sql and user["deleted_at"] is not None:
                return _FakeResult()
            profile = self.staff_profiles.get(user_id, {})
            return _FakeResult(
                first_row={
                    **user,
                    "work_phone": profile.get("work_phone"),
                    "notes_admin": profile.get("notes_admin"),
                    "commission_regular_first_year_pct": profile.get("commission_regular_first_year_pct"),
                    "commission_regular_renewal_pct": profile.get("commission_regular_renewal_pct"),
                    "commission_capitados_pct": profile.get("commission_capitados_pct"),
                }
            )

        if "FROM roles r\n            INNER JOIN model_has_roles mhr" in sql:
            user_id = int(params["user_id"])
            rows = []
            for role_name in self.role_assignments.get(user_id, []):
                role = self.roles[role_name]
                rows.append({"id": role["id"], "name": role["name"], "label": role["label"]})
            return _FakeResult(all_rows=rows)

        if "SELECT DISTINCT p.name" in sql and "UNION" in sql:
            user_id = int(params["user_id"])
            rows = []
            for role_name in self.role_assignments.get(user_id, []):
                for permission in self.permission_by_role.get(role_name, []):
                    rows.append({"name": permission})
            unique_rows = []
            seen = set()
            for row in rows:
                if row["name"] in seen:
                    continue
                seen.add(row["name"])
                unique_rows.append(row)
            return _FakeResult(all_rows=unique_rows)

        return _FakeResult()


def _override_auth(monkeypatch, *, permissions, actor_id=99):
    monkeypatch.setattr(
        AuthService,
        "me",
        lambda _self, _token: {"permissions": permissions, "role": "ADMIN", "id": actor_id},
    )


def test_admin_user_create_contract_shape(client, monkeypatch):
    fake_db = _FakeDb()

    def fake_get_db():
        yield fake_db

    _override_auth(
        monkeypatch,
        permissions=["users.create", "users.roles.assign", "users.commissions.edit"],
    )
    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.post(
            "/api/v1/admin/users",
            json={
                "first_name": "Nuevo",
                "last_name": "Usuario",
                "display_name": "Nuevo Usuario",
                "email": "nuevo@test.com",
                "status": "active",
                "roles": ["vendedor_regular"],
                "work_phone": "+56 9 3333 3333",
                "notes_admin": "nota nueva",
                "commission_regular_first_year_pct": 10,
                "commission_regular_renewal_pct": 4,
            },
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["message"] == "Usuario creado correctamente."
    assert payload["data"]["user"]["email"] == "nuevo@test.com"
    assert payload["data"]["assigned_roles"] == [{"id": 2, "name": "vendedor_regular", "label": "Vendedor regular"}]
    assert payload["data"]["target_capabilities"] == {"can_regular_sales": True, "can_capitados_sales": False}
    assert isinstance(payload["data"]["temporary_password"], str)
    assert len(payload["data"]["temporary_password"]) >= 8


def test_admin_user_update_contract_shape(client, monkeypatch):
    fake_db = _FakeDb()

    def fake_get_db():
        yield fake_db

    _override_auth(
        monkeypatch,
        permissions=[
            "users.update",
            "users.roles.assign",
            "users.email.update",
            "users.status.update",
            "users.commissions.edit",
            "users.sessions.revoke",
        ],
    )
    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.put(
            "/api/v1/admin/users/11",
            json={
                "first_name": "Admin Editado",
                "last_name": "Uno Editado",
                "display_name": "Admin Editado",
                "email": "editado@test.com",
                "status": "locked",
                "roles": ["vendedor_capitados"],
                "work_phone": "+56 9 9999 9999",
                "notes_admin": "nota editada",
                "commission_capitados_pct": 7,
                "revoke_sessions": True,
            },
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["message"] == "Usuario actualizado correctamente."
    assert payload["data"]["user"]["email"] == "editado@test.com"
    assert payload["data"]["user"]["status"] == "locked"
    assert payload["data"]["assigned_roles"] == [{"id": 3, "name": "vendedor_capitados", "label": "Vendedor capitados"}]
    assert payload["data"]["target_capabilities"] == {"can_regular_sales": False, "can_capitados_sales": True}
    assert payload["data"]["revoked_sessions"] == 3


def test_admin_user_delete_contract_shape(client, monkeypatch):
    fake_db = _FakeDb()

    def fake_get_db():
        yield fake_db

    _override_auth(monkeypatch, permissions=["users.delete"], actor_id=99)
    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.delete(
            "/api/v1/admin/users/11",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    assert response.json() == {"message": "Usuario eliminado.", "data": {"id": 11, "deleted": True}}
    assert fake_db.users[11]["deleted_at"] is not None


def test_admin_user_restore_contract_shape(client, monkeypatch):
    fake_db = _FakeDb()

    def fake_get_db():
        yield fake_db

    _override_auth(monkeypatch, permissions=["users.restore"], actor_id=99)
    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.post(
            "/api/v1/admin/users/12/restore",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["message"] == "Usuario restaurado."
    assert payload["data"]["user"]["id"] == 12
    assert fake_db.users[12]["deleted_at"] is None


def test_admin_user_revoke_sessions_contract_shape(client, monkeypatch):
    fake_db = _FakeDb()

    def fake_get_db():
        yield fake_db

    _override_auth(monkeypatch, permissions=["users.sessions.revoke", "users.update"], actor_id=99)
    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.post(
            "/api/v1/admin/users/11/sessions/revoke",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    assert response.json() == {
        "ok": True,
        "message": "Se revocaron 3 sesiones del usuario.",
        "revoked": 3,
    }
    assert fake_db.sessions_by_user[11] == 0