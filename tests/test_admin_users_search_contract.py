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

        if "SELECT COUNT(DISTINCT u.id) AS c FROM users u" in sql:
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

        if "FROM users u" in sql and "ORDER BY resolved_display_name ASC, u.id ASC" in sql:
            return _FakeResult(
                all_rows=[
                    {
                        "id": 11,
                        "first_name": "Admin",
                        "last_name": "Uno",
                        "display_name": "",
                        "resolved_display_name": "Admin Uno",
                        "email": "uno@test.com",
                        "status": "active",
                        "last_login_at": "2026-03-29 10:00:00",
                    },
                    {
                        "id": 12,
                        "first_name": "Admin",
                        "last_name": "Dos",
                        "display_name": "Admin Dos",
                        "resolved_display_name": "Admin Dos",
                        "email": "dos@test.com",
                        "status": "suspended",
                        "last_login_at": None,
                    },
                ]
            )

        if "FROM users u" in sql and "LEFT JOIN staff_profiles sp" in sql:
            return _FakeResult(
                first_row={
                    "id": 11,
                    "realm": "admin",
                    "first_name": "Admin",
                    "last_name": "Uno",
                    "display_name": "Admin Uno",
                    "email": "uno@test.com",
                    "status": "active",
                    "last_login_at": "2026-03-29 10:00:00",
                    "work_phone": "+56 9 1111 1111",
                    "notes_admin": "nota interna",
                    "commission_regular_first_year_pct": 12.5,
                    "commission_regular_renewal_pct": 5.0,
                    "commission_capitados_pct": 3.0,
                }
            )

        if "FROM roles\n            WHERE guard_name = 'admin'" in sql or "FROM roles\r\n            WHERE guard_name = 'admin'" in sql:
            return _FakeResult(
                all_rows=[
                    {"id": 1, "name": "admin", "label": "Administrador"},
                    {"id": 2, "name": "vendedor_regular", "label": "Vendedor regular"},
                ]
            )

        if "FROM roles r" in sql and "INNER JOIN model_has_roles mhr" in sql:
            user_id = int(params.get("user_id") or 0)
            rows_by_user = {
                11: [{"id": 1, "name": "admin", "label": "Administrador"}],
                12: [{"id": 2, "name": "vendedor_regular", "label": "Vendedor regular"}],
            }
            return _FakeResult(all_rows=rows_by_user.get(user_id, []))

        if "SELECT DISTINCT p.name" in sql and "UNION" in sql:
            user_id = int(params.get("user_id") or 0)
            rows_by_user = {
                11: [
                    {"name": "sales.regular.use"},
                    {"name": "sales.capitados.use"},
                ]
            }
            return _FakeResult(all_rows=rows_by_user.get(user_id, []))

        return _FakeResult()


def _override_auth(monkeypatch, *, permissions):
    monkeypatch.setattr(
        AuthService,
        "me",
        lambda _self, _token: {"permissions": permissions, "role": "ADMIN", "id": 99},
    )


def test_admin_users_search_contract_shape(client, monkeypatch):
    fake_db = _FakeDb()

    def fake_get_db():
        yield fake_db

    _override_auth(monkeypatch, permissions=["users.viewAny"])

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

    _override_auth(monkeypatch, permissions=[])

    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.get(
            "/api/v1/admin/users/search",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 403


def test_admin_users_bootstrap_contract_shape(client, monkeypatch):
    fake_db = _FakeDb()

    def fake_get_db():
        yield fake_db

    _override_auth(
        monkeypatch,
        permissions=[
            "users.viewAny",
            "users.email.update",
            "users.roles.assign",
        ],
    )

    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.get(
            "/api/v1/admin/users/bootstrap",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()["data"]

    assert payload["roles"] == [
        {"id": 1, "name": "admin", "label": "Administrador"},
        {"id": 2, "name": "vendedor_regular", "label": "Vendedor regular"},
    ]
    assert payload["statuses"] == [
        {"value": "active", "label": "Activo"},
        {"value": "suspended", "label": "Suspendido"},
        {"value": "locked", "label": "Bloqueado"},
    ]
    assert payload["actor_capabilities"] == {
        "can_view_any": True,
        "can_update_email": True,
        "can_update_status": False,
        "can_assign_roles": True,
        "can_edit_commissions": False,
        "can_revoke_sessions": False,
        "can_impersonate": False,
    }


def test_admin_users_index_contract_shape(client, monkeypatch):
    fake_db = _FakeDb()

    def fake_get_db():
        yield fake_db

    _override_auth(
        monkeypatch,
        permissions=["users.viewAny", "users.status.update", "users.sessions.revoke"],
    )

    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.get(
            "/api/v1/admin/users",
            params={"page": 1, "per_page": 15, "q": "admin", "status": "active", "role": "admin"},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()

    assert len(payload["data"]) == 2
    assert payload["data"][0] == {
        "id": 11,
        "first_name": "Admin",
        "last_name": "Uno",
        "display_name": "Admin Uno",
        "email": "uno@test.com",
        "status": "active",
        "last_login_at": "2026-03-29 10:00:00",
        "roles": [{"id": 1, "name": "admin", "label": "Administrador"}],
    }
    assert payload["meta"]["filters"] == {"q": "admin", "status": "active", "role": "admin"}
    assert payload["actor_capabilities"] == {
        "can_view_any": True,
        "can_update_email": False,
        "can_update_status": True,
        "can_assign_roles": False,
        "can_edit_commissions": False,
        "can_revoke_sessions": True,
        "can_impersonate": False,
    }


def test_admin_user_show_contract_shape(client, monkeypatch):
    fake_db = _FakeDb()

    def fake_get_db():
        yield fake_db

    _override_auth(
        monkeypatch,
        permissions=["users.viewAny", "users.email.update", "users.commissions.edit", "users.impersonate"],
    )

    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.get(
            "/api/v1/admin/users/11",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    payload = response.json()["data"]

    assert payload["user"] == {
        "id": 11,
        "realm": "admin",
        "first_name": "Admin",
        "last_name": "Uno",
        "display_name": "Admin Uno",
        "email": "uno@test.com",
        "status": "active",
        "last_login_at": "2026-03-29 10:00:00",
    }
    assert payload["staff_profile"] == {
        "work_phone": "+56 9 1111 1111",
        "notes_admin": "nota interna",
        "commission_regular_first_year_pct": 12.5,
        "commission_regular_renewal_pct": 5.0,
        "commission_capitados_pct": 3.0,
    }
    assert payload["assigned_roles"] == [{"id": 1, "name": "admin", "label": "Administrador"}]
    assert payload["target_capabilities"] == {
        "can_regular_sales": True,
        "can_capitados_sales": True,
    }
    assert payload["actor_capabilities"] == {
        "can_view_any": True,
        "can_update_email": True,
        "can_update_status": False,
        "can_assign_roles": False,
        "can_edit_commissions": True,
        "can_revoke_sessions": False,
        "can_impersonate": True,
    }
