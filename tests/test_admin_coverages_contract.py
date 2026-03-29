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
        self.last_insert_id = 30
        self.categories = {
            10: {
                "id": 10,
                "name": {"es": "Medicas", "en": "Medical"},
                "description": {"es": "Desc", "en": "Desc"},
                "status": "active",
                "sort_order": 1,
            },
            11: {
                "id": 11,
                "name": {"es": "Archivada", "en": "Archived"},
                "description": {"es": "Desc", "en": "Desc"},
                "status": "archived",
                "sort_order": 2,
            },
        }
        self.units = {
            1: {
                "id": 1,
                "name": {"es": "Monto", "en": "Amount"},
                "description": {"es": "Desc", "en": "Desc"},
                "measure_type": "decimal",
                "status": "active",
            }
        }
        self.coverages = {
            20: {
                "id": 20,
                "category_id": 10,
                "unit_id": 1,
                "name": {"es": "Cobertura A", "en": "Coverage A"},
                "description": {"es": "Desc", "en": "Desc"},
                "status": "active",
                "sort_order": 1,
            }
        }
        self.usages_by_coverage = {
            20: [
                {
                    "product_version_id": 501,
                    "version_id": 501,
                    "product_id": 101,
                    "product_name": {"es": "Plan Oro", "en": "Gold Plan"},
                }
            ]
        }

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        self.calls.append({"sql": sql, "params": params})

        if "FROM units_of_measure" in sql and "WHERE status = 'active'" in sql:
            return _FakeResult(all_rows=list(self.units.values()))

        if "SELECT id FROM units_of_measure WHERE id = :unit_id" in sql:
            unit_id = int(params.get("unit_id") or 0)
            return _FakeResult(first_row={"id": unit_id} if unit_id in self.units else None)

        if "FROM coverage_categories" in sql and "WHERE status = 'active'" in sql:
            rows = [r for r in self.categories.values() if r.get("status") == "active"]
            rows.sort(key=lambda r: (int(r.get("sort_order") or 0), int(r["id"])))
            return _FakeResult(all_rows=rows)

        if "FROM coverage_categories" in sql and "WHERE status = 'archived'" in sql:
            rows = [r for r in self.categories.values() if r.get("status") == "archived"]
            rows.sort(key=lambda r: (int(r.get("sort_order") or 0), int(r["id"])))
            return _FakeResult(all_rows=rows)

        if "FROM coverage_categories" in sql and "WHERE id = :category_id" in sql:
            category_id = int(params.get("category_id") or 0)
            return _FakeResult(first_row=self.categories.get(category_id))

        if "SELECT COALESCE(MAX(sort_order), 0) AS max_order FROM coverage_categories" in sql:
            max_order = 0
            if self.categories:
                max_order = max(int(v.get("sort_order") or 0) for v in self.categories.values())
            return _FakeResult(first_row={"max_order": max_order})

        if "INSERT INTO coverage_categories" in sql:
            self.last_insert_id = 31
            self.categories[self.last_insert_id] = {
                "id": self.last_insert_id,
                "name": {"es": "Nueva", "en": "New"},
                "description": {"es": "Desc", "en": "Desc"},
                "status": "active",
                "sort_order": int(params.get("sort_order") or 1),
            }
            return _FakeResult(first_row=None)

        if "UPDATE coverage_categories" in sql and "SET status = 'archived'" in sql:
            category_id = int(params.get("category_id") or 0)
            if category_id in self.categories:
                self.categories[category_id]["status"] = "archived"
            return _FakeResult(first_row=None)

        if "UPDATE coverage_categories" in sql and "SET status = 'active'" in sql:
            category_id = int(params.get("category_id") or 0)
            if category_id in self.categories:
                self.categories[category_id]["status"] = "active"
            return _FakeResult(first_row=None)

        if "FROM coverages" in sql and "WHERE category_id = :category_id" in sql:
            category_id = int(params.get("category_id") or 0)
            rows = [r for r in self.coverages.values() if int(r.get("category_id") or 0) == category_id]
            rows.sort(key=lambda r: (int(r.get("sort_order") or 0), int(r["id"])))
            return _FakeResult(all_rows=rows)

        if "SELECT COALESCE(MAX(sort_order), 0) AS max_order FROM coverages WHERE category_id = :category_id" in sql:
            category_id = int(params.get("category_id") or 0)
            values = [int(r.get("sort_order") or 0) for r in self.coverages.values() if int(r.get("category_id") or 0) == category_id]
            return _FakeResult(first_row={"max_order": max(values) if values else 0})

        if "FROM coverages" in sql and "WHERE id = :coverage_id" in sql:
            coverage_id = int(params.get("coverage_id") or 0)
            return _FakeResult(first_row=self.coverages.get(coverage_id))

        if "INSERT INTO coverages" in sql:
            self.last_insert_id = 32
            self.coverages[self.last_insert_id] = {
                "id": self.last_insert_id,
                "category_id": int(params.get("category_id") or 0),
                "unit_id": int(params.get("unit_id") or 0),
                "name": {"es": "Nueva Cob", "en": "New Cov"},
                "description": {"es": "Desc", "en": "Desc"},
                "status": "active",
                "sort_order": int(params.get("sort_order") or 1),
            }
            return _FakeResult(first_row=None)

        if "UPDATE coverages" in sql and "SET status = 'archived'" in sql:
            coverage_id = int(params.get("coverage_id") or 0)
            if coverage_id in self.coverages:
                self.coverages[coverage_id]["status"] = "archived"
            return _FakeResult(first_row=None)

        if "UPDATE coverages" in sql and "SET status = 'active'" in sql:
            coverage_id = int(params.get("coverage_id") or 0)
            if coverage_id in self.coverages:
                self.coverages[coverage_id]["status"] = "active"
            return _FakeResult(first_row=None)

        if "SELECT 1" in sql and "FROM plan_version_coverages" in sql:
            coverage_id = int(params.get("coverage_id") or 0)
            rows = self.usages_by_coverage.get(coverage_id, [])
            return _FakeResult(first_row={"ok": 1} if rows else None)

        if "DELETE FROM coverages WHERE id = :coverage_id" in sql:
            coverage_id = int(params.get("coverage_id") or 0)
            self.coverages.pop(coverage_id, None)
            return _FakeResult(first_row=None)

        if "FROM plan_version_coverages pvc" in sql and "INNER JOIN plan_versions" in sql:
            coverage_id = int(params.get("coverage_id") or 0)
            rows = self.usages_by_coverage.get(coverage_id, [])
            return _FakeResult(all_rows=rows)

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
        lambda _self, _token: {"permissions": permissions, "role": "ADMIN"},
    )
    app.dependency_overrides[get_db] = fake_get_db



def _teardown():
    app.dependency_overrides.pop(get_db, None)



def test_admin_coverages_bootstrap_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.coverages.manage"])

    try:
        response = client.get(
            "/api/v1/admin/coverages/bootstrap",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["data"]["categories"]) == 1
    assert payload["data"]["categories"][0]["id"] == 10
    assert len(payload["data"]["units"]) == 1



def test_admin_coverages_store_category_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.coverages.manage"])

    try:
        response = client.post(
            "/api/v1/admin/coverages/categories",
            json={
                "name": {"es": "Nueva", "en": "New"},
                "description": {"es": "Desc", "en": "Desc"},
            },
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["id"] == 31



def test_admin_coverages_store_item_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.coverages.manage"])

    try:
        response = client.post(
            "/api/v1/admin/coverages/items",
            json={
                "category_id": 10,
                "unit_id": 1,
                "name": {"es": "Nueva Cob", "en": "New Cov"},
                "description": {"es": "Desc", "en": "Desc"},
            },
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["id"] == 32



def test_admin_coverages_usages_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.coverages.manage"])

    try:
        response = client.get(
            "/api/v1/admin/coverages/items/20/usages",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"][0]["version_id"] == 501
    assert payload["data"][0]["product_id"] == 101



def test_admin_coverages_destroy_conflict_when_in_use(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.coverages.manage"])

    try:
        response = client.delete(
            "/api/v1/admin/coverages/items/20",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 409



def test_admin_coverages_forbidden_without_permission(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=[])

    try:
        response = client.get(
            "/api/v1/admin/coverages/bootstrap",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 403
