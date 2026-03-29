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
        self.last_insert_id = 0
        self.items = {
            10: {
                "id": 10,
                "category": "planes",
                "token": "max_age.entry",
                "name": "Edad maxima entrada",
                "type": "integer",
                "config": "{}",
                "value_int": 60,
                "value_decimal": None,
                "value_text": None,
                "value_trans": None,
                "value_file_plain_id": None,
                "value_file_es_id": None,
                "value_file_en_id": None,
                "value_date": None,
            }
        }
        self.files = {}

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        self.calls.append({"sql": sql, "params": params})

        if "FROM config_items" in sql and "WHERE id = :item_id" in sql:
            item_id = int(params.get("item_id") or 0)
            return _FakeResult(first_row=self.items.get(item_id))

        if "FROM config_items" in sql and "ORDER BY category, name" in sql:
            return _FakeResult(all_rows=list(self.items.values()))

        if "FROM config_items" in sql and "WHERE category = :category AND token = :token" in sql:
            for row in self.items.values():
                if row.get("category") == params.get("category") and row.get("token") == params.get("token"):
                    return _FakeResult(first_row={"id": row["id"]})
            return _FakeResult(first_row=None)

        if "INSERT INTO config_items" in sql:
            self.last_insert_id = 11
            self.items[11] = {
                "id": 11,
                "category": params.get("category"),
                "token": params.get("token"),
                "name": params.get("name"),
                "type": params.get("type"),
                "config": params.get("config") or "{}",
                "value_int": None,
                "value_decimal": None,
                "value_text": None,
                "value_trans": None,
                "value_file_plain_id": None,
                "value_file_es_id": None,
                "value_file_en_id": None,
                "value_date": None,
            }
            return _FakeResult(first_row=None)

        if "UPDATE config_items" in sql and "SET value_int = :value" in sql:
            item_id = int(params.get("item_id") or 0)
            if item_id in self.items:
                self.items[item_id]["value_int"] = params.get("value")
            return _FakeResult(first_row=None)

        if "DELETE FROM config_items" in sql:
            item_id = int(params.get("item_id") or 0)
            self.items.pop(item_id, None)
            return _FakeResult(first_row=None)

        if "SELECT id, uuid, original_name" in sql and "FROM files" in sql:
            file_id = int(params.get("file_id") or 0)
            return _FakeResult(first_row=self.files.get(file_id))

        if "SELECT LAST_INSERT_ID() AS id" in sql:
            return _FakeResult(first_row={"id": self.last_insert_id})

        return _FakeResult(first_row=None)

    def commit(self):
        return None


def _setup(monkeypatch, fake_db, permissions):
    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(
        AuthService,
        "me",
        lambda _self, _token: {"permissions": permissions, "role": "ADMIN", "id": 1},
    )
    app.dependency_overrides[get_db] = fake_get_db


def _teardown():
    app.dependency_overrides.pop(get_db, None)


def test_admin_config_index_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.config.read"])

    try:
        response = client.get(
            "/api/v1/admin/config",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["permissions"]["read"] is True
    assert len(payload["items"]) == 1


def test_admin_config_store_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.config.create"])

    try:
        response = client.post(
            "/api/v1/admin/config/items",
            json={
                "category": "planes",
                "name": "Nuevo item",
                "token": "nuevo.token",
                "type": "integer",
                "config": {},
            },
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["item"]["id"] == 11


def test_admin_config_update_value_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.config.fill"])

    try:
        response = client.put(
            "/api/v1/admin/config/10/value",
            json={"value": 65},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["item"]["value_int"] == 65


def test_admin_config_forbidden_without_permission(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=[])

    try:
        response = client.get(
            "/api/v1/admin/config",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 403
