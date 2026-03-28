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
        self.products = {
            101: {
                "id": 101,
                "company_id": None,
                "status": "active",
                "product_type": "plan_regular",
                "show_in_widget": 1,
                "name": {"es": "Plan Oro", "en": "Gold Plan"},
                "description": {"es": "Desc", "en": "Desc"},
            }
        }

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        self.calls.append({"sql": sql, "params": params})

        if "SELECT id FROM companies" in sql:
            company_id = int(params.get("company_id") or 0)
            if company_id == 22:
                return _FakeResult(first_row={"id": 22})
            return _FakeResult(first_row=None)

        if "FROM products" in sql and "WHERE product_type = 'plan_regular'" in sql:
            return _FakeResult(all_rows=[self.products[101]])

        if "FROM products" in sql and "WHERE id = :product_id" in sql:
            product_id = int(params.get("product_id") or 0)
            return _FakeResult(first_row=self.products.get(product_id))

        if "INSERT INTO products" in sql:
            self.last_insert_id = 102
            self.products[self.last_insert_id] = {
                "id": 102,
                "company_id": params.get("company_id"),
                "status": params.get("status") or "inactive",
                "product_type": params.get("product_type"),
                "show_in_widget": params.get("show_in_widget"),
                "name": {"es": "Nuevo", "en": "New"},
                "description": {"es": "Desc", "en": "Desc"},
            }
            return _FakeResult(first_row=None)

        if "SELECT LAST_INSERT_ID() AS id" in sql:
            return _FakeResult(first_row={"id": self.last_insert_id})

        if "UPDATE products" in sql:
            product_id = int(params.get("product_id") or 0)
            if product_id in self.products:
                self.products[product_id]["status"] = params.get("status")
                self.products[product_id]["show_in_widget"] = params.get("show_in_widget")
            return _FakeResult(first_row=None)

        return _FakeResult(first_row=None)

    def commit(self):
        return None



def _setup(monkeypatch, fake_db, permissions):
    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(
        AuthService,
        "me",
        lambda _self, _token: {"permissions": permissions, "role": "ADMIN"},
    )
    app.dependency_overrides[get_db] = fake_get_db



def _teardown():
    app.dependency_overrides.pop(get_db, None)



def test_admin_products_index_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.products.manage"])

    try:
        response = client.get(
            "/api/v1/admin/products",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"]["total"] == 1
    assert payload["data"][0]["id"] == 101



def test_admin_products_show_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.products.manage"])

    try:
        response = client.get(
            "/api/v1/admin/products/101",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["product_type"] == "plan_regular"



def test_admin_products_store_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.products.manage"])

    try:
        response = client.post(
            "/api/v1/admin/products",
            json={
                "name": {"es": "Nuevo", "en": "New"},
                "description": {"es": "Desc", "en": "Desc"},
                "product_type": "plan_regular",
                "show_in_widget": False,
            },
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["id"] == 102



def test_admin_products_update_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.products.manage"])

    try:
        response = client.put(
            "/api/v1/admin/products/101",
            json={
                "name": {"es": "Plan Oro", "en": "Gold Plan"},
                "description": {"es": "Desc", "en": "Desc"},
                "status": "inactive",
                "show_in_widget": False,
            },
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["status"] == "inactive"



def test_admin_products_forbidden_without_permission(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=[])

    try:
        response = client.get(
            "/api/v1/admin/products",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 403
