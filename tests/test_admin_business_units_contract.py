from app.db.database import get_db
from app.main import app
from app.services.auth_service import AuthService
from app.routers.v1 import admin_business_units as bu_router


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
        self.units = {
            10: {
                "id": 10,
                "type": "office",
                "name": "Oficina Norte",
                "status": "active",
                "parent_id": None,
                "branding_text_dark": None,
                "branding_bg_light": None,
                "branding_text_light": None,
                "branding_bg_dark": None,
                "branding_logo_file_id": None,
            }
        }

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}

        if "SELECT COUNT(*) AS c FROM business_units u" in sql:
            return _FakeResult(first_row={"c": 1})

        if "FROM business_units u" in sql and "owner_user_id" in sql:
            return _FakeResult(
                all_rows=[
                    {
                        "id": 10,
                        "type": "office",
                        "name": "Oficina Norte",
                        "status": "active",
                        "parent_id": None,
                        "parent_ref_id": None,
                        "parent_name": None,
                        "parent_type": None,
                        "parent_status": None,
                        "children_count": 0,
                        "members_count": 0,
                        "owner_user_id": None,
                        "owner_email": None,
                        "owner_first_name": None,
                        "owner_last_name": None,
                        "owner_display_name": None,
                        "owner_status": None,
                    }
                ]
            )

        if "FROM business_units" in sql and "WHERE id = :unit_id" in sql:
            unit_id = int(params.get("unit_id") or 0)
            return _FakeResult(first_row=self.units.get(unit_id))

        if "FROM memberships_business_unit m" in sql and "INNER JOIN roles r" in sql:
            return _FakeResult(all_rows=[])

        if "SELECT" in sql and "children_count" in sql and "memberships_count" in sql:
            return _FakeResult(first_row={"children_count": 0, "memberships_count": 0})

        return _FakeResult(first_row=None)

    def commit(self):
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



def test_admin_business_units_list_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["unit.structure.view"])

    try:
        response = client.get(
            "/api/v1/admin/business-units/units",
            params={"type": "office", "status": "active", "root": "true", "page": 1, "per_page": 25},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"]["permissions"]["can_structure_view"] is True
    assert len(payload["data"]) == 1
    assert payload["data"][0]["id"] == 10



def test_admin_business_units_show_forbidden_without_access(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=[])

    try:
        response = client.get(
            "/api/v1/admin/business-units/units/10",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 403


class _FakeDbMutations:
    def __init__(self):
        self.did_insert_membership = False
        self.did_insert_regalia = False

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}

        if "FROM business_units" in sql and "WHERE id = :unit_id" in sql:
            return _FakeResult(
                first_row={
                    "id": int(params.get("unit_id") or 0),
                    "type": "office",
                    "name": "Oficina Norte",
                    "status": "active",
                    "parent_id": None,
                    "branding_text_dark": None,
                    "branding_bg_light": None,
                    "branding_text_light": None,
                    "branding_bg_dark": None,
                    "branding_logo_file_id": None,
                }
            )

        if "FROM roles" in sql and "WHERE id = :role_id" in sql:
            return _FakeResult(first_row={"id": int(params.get("role_id") or 0)})

        if "FROM users WHERE email = :email" in sql:
            return _FakeResult(first_row={"id": 55, "status": "active"})

        if "FROM memberships_business_unit" in sql and "WHERE business_unit_id = :unit_id" in sql and "user_id = :user_id" in sql:
            return _FakeResult(first_row=None)

        if "INSERT INTO memberships_business_unit" in sql:
            self.did_insert_membership = True
            return _FakeResult(first_row=None)

        if "FROM users WHERE id = :id AND deleted_at IS NULL" in sql:
            return _FakeResult(
                first_row={
                    "id": int(params.get("id") or 0),
                    "status": "active",
                    "email": "target@example.com",
                    "first_name": "Target",
                    "last_name": "User",
                    "display_name": "Target User",
                }
            )

        if "FROM regalias" in sql and "source_type = 'unit'" in sql and "source_id = :unit_id" in sql:
            return _FakeResult(first_row=None)

        if "INSERT INTO regalias" in sql:
            self.did_insert_regalia = True
            return _FakeResult(first_row=None)

        if "SELECT LAST_INSERT_ID() AS id" in sql:
            return _FakeResult(first_row={"id": 999})

        return _FakeResult(first_row=None)

    def commit(self):
        return None

    def rollback(self):
        return None


def test_admin_business_units_member_link_rejects_role_out_of_scope(client, monkeypatch):
    fake_db = _FakeDbMutations()

    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(
        AuthService,
        "me",
        lambda _self, _token: {
            "id": 1,
            "role": "ADMIN",
            "permissions": ["unit.members.invite"],
        },
    )
    monkeypatch.setattr(
        bu_router,
        "_permission_levels_for_unit",
        lambda *_args, **_kwargs: {
            "unit.structure.view": 1,
            "unit.members.invite": 3,
            "unit.members.manage_roles": 3,
        },
    )
    monkeypatch.setattr(
        bu_router,
        "_roles_manageable_for_unit",
        lambda *_args, **_kwargs: [{"id": 2, "name": "unit.agent", "level": 2}],
    )
    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.post(
            "/api/v1/admin/business-units/units/10/members",
            json={
                "mode": "email",
                "email": "target@example.com",
                "role_id": 1,
            },
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 422
    assert "fuera de alcance" in response.json()["message"].lower()
    assert fake_db.did_insert_membership is False


def test_admin_business_units_gsa_store_blocks_hierarchy_redundancy(client, monkeypatch):
    fake_db = _FakeDbMutations()

    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(
        AuthService,
        "me",
        lambda _self, _token: {
            "id": 1,
            "role": "ADMIN",
            "permissions": ["unit.gsa.commission"],
        },
    )
    monkeypatch.setattr(
        bu_router,
        "_permission_levels_for_unit",
        lambda *_args, **_kwargs: {
            "unit.structure.view": 1,
            "unit.gsa.commission": 3,
        },
    )
    monkeypatch.setattr(
        bu_router,
        "_would_create_unit_redundancy_cycle_for_unit_regalia",
        lambda *_args, **_kwargs: True,
    )
    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.post(
            "/api/v1/admin/business-units/units/10/gsa-commissions",
            json={"user_id": 55},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 422
    assert "redundancia" in response.json()["message"].lower()
    assert fake_db.did_insert_regalia is False
