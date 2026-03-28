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
        self.last_insert_id = 601
        self.products = {101: {"id": 101}}
        self.countries = {
            1: {
                "id": 1,
                "name": {"es": "Argentina", "en": "Argentina"},
                "iso2": "AR",
                "iso3": "ARG",
                "continent_code": "SA",
                "phone_code": "+54",
                "is_active": 1,
            },
            2: {
                "id": 2,
                "name": {"es": "Chile", "en": "Chile"},
                "iso2": "CL",
                "iso3": "CHL",
                "continent_code": "SA",
                "phone_code": "+56",
                "is_active": 1,
            },
            3: {
                "id": 3,
                "name": {"es": "Peru", "en": "Peru"},
                "iso2": "PE",
                "iso3": "PER",
                "continent_code": "SA",
                "phone_code": "+51",
                "is_active": 1,
            },
        }
        self.zones = {
            9: {
                "id": 9,
                "name": "Sudamerica",
                "is_active": 1,
            }
        }
        self.zone_countries = {
            9: [1, 2],
        }
        self.plan_version_countries = {
            501: {1: {"price": 100.0}},
        }
        self.plan_version_repatriation_countries = {
            501: {2},
        }
        self.plan_versions = {
            501: {
                "id": 501,
                "product_id": 101,
                "name": "Base v1",
                "status": "active",
                "terms_html": {"es": "<p>Hola</p>", "en": "<p>Hello</p>"},
                "created_at": "2026-03-20 10:00:00",
                "updated_at": "2026-03-20 10:00:00",
            }
        }

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        self.calls.append({"sql": sql, "params": params})

        if "FROM products" in sql and "WHERE id = :product_id" in sql:
            product_id = int(params.get("product_id") or 0)
            return _FakeResult(first_row=self.products.get(product_id))

        if "FROM zones" in sql and "COUNT(cz.country_id)" in sql:
            rows = []
            for zone in self.zones.values():
                if not zone.get("is_active"):
                    continue
                zone_id = int(zone["id"])
                rows.append(
                    {
                        "id": zone_id,
                        "name": zone.get("name"),
                        "countries_count": len(self.zone_countries.get(zone_id, [])),
                    }
                )
            return _FakeResult(all_rows=rows)

        if "SELECT country_id" in sql and "FROM country_zone" in sql:
            zone_id = int(params.get("zone_id") or 0)
            rows = [{"country_id": cid} for cid in self.zone_countries.get(zone_id, [])]
            return _FakeResult(all_rows=rows)

        if "SELECT id FROM zones" in sql:
            zone_id = int(params.get("zone_id") or 0)
            zone = self.zones.get(zone_id)
            if zone and ("is_active = 1" not in sql or zone.get("is_active")):
                return _FakeResult(first_row={"id": zone_id})
            return _FakeResult(first_row=None)

        if "FROM plan_versions" in sql and "WHERE product_id = :product_id" in sql and "ORDER BY id DESC" in sql:
            product_id = int(params.get("product_id") or 0)
            rows = [row for row in self.plan_versions.values() if int(row.get("product_id") or 0) == product_id]
            rows.sort(key=lambda row: int(row["id"]), reverse=True)
            return _FakeResult(all_rows=rows)

        if "FROM plan_versions" in sql and "WHERE id = :plan_version_id" in sql:
            plan_version_id = int(params.get("plan_version_id") or 0)
            product_id = int(params.get("product_id") or 0)
            row = self.plan_versions.get(plan_version_id)
            if row and int(row.get("product_id") or 0) == product_id:
                return _FakeResult(first_row=row)
            return _FakeResult(first_row=None)

        if "SELECT terms_html" in sql and "FROM plan_versions" in sql:
            plan_version_id = int(params.get("plan_version_id") or 0)
            product_id = int(params.get("product_id") or 0)
            row = self.plan_versions.get(plan_version_id)
            if row and int(row.get("product_id") or 0) == product_id:
                return _FakeResult(first_row={"terms_html": row.get("terms_html")})
            return _FakeResult(first_row=None)

        if "INSERT INTO plan_versions (product_id, name, status, created_at, updated_at)" in sql:
            self.last_insert_id = 602
            self.plan_versions[self.last_insert_id] = {
                "id": self.last_insert_id,
                "product_id": int(params.get("product_id") or 0),
                "name": params.get("name") or "",
                "status": "inactive",
                "created_at": "2026-03-27 11:00:00",
                "updated_at": "2026-03-27 11:00:00",
            }
            return _FakeResult(first_row=None)

        if "INSERT INTO plan_versions" in sql and "SELECT" in sql and "FROM plan_versions" in sql:
            source_id = int(params.get("source_id") or 0)
            source = self.plan_versions.get(source_id)
            self.last_insert_id = 603
            if source:
                self.plan_versions[self.last_insert_id] = {
                    "id": self.last_insert_id,
                    "product_id": int(source.get("product_id") or 0),
                    "name": params.get("name") or "",
                    "status": "inactive",
                    "created_at": "2026-03-27 12:00:00",
                    "updated_at": "2026-03-27 12:00:00",
                }
            return _FakeResult(first_row=None)

        if "SELECT LAST_INSERT_ID() AS id" in sql:
            return _FakeResult(first_row={"id": self.last_insert_id})

        if "INSERT INTO plan_version_coverages" in sql:
            return _FakeResult(first_row=None)

        if "FROM plan_version_countries" in sql and "INNER JOIN countries" in sql:
            plan_version_id = int(params.get("plan_version_id") or 0)
            pivot = self.plan_version_countries.get(plan_version_id, {})
            rows = []
            for country_id, info in pivot.items():
                country = self.countries.get(country_id)
                if country:
                    rows.append({**country, "price": info.get("price")})
            return _FakeResult(all_rows=rows)

        if "FROM countries c" in sql and "LEFT JOIN plan_version_countries" in sql:
            plan_version_id = int(params.get("plan_version_id") or 0)
            pivot = self.plan_version_countries.get(plan_version_id, {})
            rows = []
            for country in self.countries.values():
                cid = int(country["id"])
                info = pivot.get(cid)
                rows.append({**country, "attached": 1 if info else 0, "price": info.get("price") if info else None})
            return _FakeResult(all_rows=rows)

        if "SELECT country_id" in sql and "FROM plan_version_countries" in sql:
            plan_version_id = int(params.get("plan_version_id") or 0)
            pivot = self.plan_version_countries.get(plan_version_id, {})
            if "country_id = :country_id" in sql:
                country_id = int(params.get("country_id") or 0)
                return _FakeResult(first_row={"country_id": country_id} if country_id in pivot else None)
            return _FakeResult(all_rows=[{"country_id": cid} for cid in pivot.keys()])

        if "INSERT INTO plan_version_countries" in sql:
            plan_version_id = int(params.get("plan_version_id") or 0)
            country_id = int(params.get("country_id") or 0)
            self.plan_version_countries.setdefault(plan_version_id, {})[country_id] = {"price": None}
            return _FakeResult(first_row=None)

        if "UPDATE plan_version_countries" in sql:
            plan_version_id = int(params.get("plan_version_id") or 0)
            country_id = int(params.get("country_id") or 0)
            price = params.get("price")
            self.plan_version_countries.setdefault(plan_version_id, {}).setdefault(country_id, {"price": None})
            self.plan_version_countries[plan_version_id][country_id]["price"] = price
            return _FakeResult(first_row=None)

        if "DELETE FROM plan_version_countries" in sql:
            plan_version_id = int(params.get("plan_version_id") or 0)
            pivot = self.plan_version_countries.setdefault(plan_version_id, {})
            if "country_id = :country_id" in sql:
                country_id = int(params.get("country_id") or 0)
                if country_id in pivot:
                    del pivot[country_id]
            else:
                ids = [v for k, v in params.items() if str(k).startswith("did_")]
                for country_id in ids:
                    pivot.pop(int(country_id), None)
            return _FakeResult(first_row=None)

        if "FROM plan_version_repatriation_countries" in sql and "INNER JOIN countries" in sql:
            plan_version_id = int(params.get("plan_version_id") or 0)
            attached = self.plan_version_repatriation_countries.get(plan_version_id, set())
            rows = []
            for country_id in attached:
                country = self.countries.get(country_id)
                if country:
                    rows.append({**country})
            return _FakeResult(all_rows=rows)

        if "FROM countries c" in sql and "LEFT JOIN plan_version_repatriation_countries" in sql:
            plan_version_id = int(params.get("plan_version_id") or 0)
            attached = self.plan_version_repatriation_countries.get(plan_version_id, set())
            rows = []
            for country in self.countries.values():
                cid = int(country["id"])
                rows.append({**country, "attached": 1 if cid in attached else 0})
            return _FakeResult(all_rows=rows)

        if "SELECT country_id" in sql and "FROM plan_version_repatriation_countries" in sql:
            plan_version_id = int(params.get("plan_version_id") or 0)
            attached = self.plan_version_repatriation_countries.get(plan_version_id, set())
            if "country_id = :country_id" in sql:
                country_id = int(params.get("country_id") or 0)
                return _FakeResult(first_row={"country_id": country_id} if country_id in attached else None)
            return _FakeResult(all_rows=[{"country_id": cid} for cid in attached])

        if "INSERT INTO plan_version_repatriation_countries" in sql:
            plan_version_id = int(params.get("plan_version_id") or 0)
            country_id = int(params.get("country_id") or 0)
            self.plan_version_repatriation_countries.setdefault(plan_version_id, set()).add(country_id)
            return _FakeResult(first_row=None)

        if "DELETE FROM plan_version_repatriation_countries" in sql:
            plan_version_id = int(params.get("plan_version_id") or 0)
            attached = self.plan_version_repatriation_countries.setdefault(plan_version_id, set())
            if "country_id = :country_id" in sql:
                country_id = int(params.get("country_id") or 0)
                attached.discard(country_id)
            else:
                ids = [v for k, v in params.items() if str(k).startswith("did_")]
                for country_id in ids:
                    attached.discard(int(country_id))
            return _FakeResult(first_row=None)

        if "FROM countries" in sql and "WHERE id IN" in sql:
            ids = []
            for key, value in params.items():
                if str(key).startswith("id_"):
                    ids.append(int(value))
            rows = [self.countries[country_id] for country_id in ids if country_id in self.countries]
            return _FakeResult(all_rows=rows)

        if "DELETE FROM plan_versions" in sql:
            plan_version_id = int(params.get("plan_version_id") or 0)
            if plan_version_id in self.plan_versions:
                del self.plan_versions[plan_version_id]
            return _FakeResult(first_row=None)

        if "UPDATE plan_versions" in sql and "SET terms_html" in sql:
            plan_version_id = int(params.get("plan_version_id") or 0)
            locale_payload = params.get("terms_html") or "{}"
            row = self.plan_versions.get(plan_version_id)
            if row:
                import json

                row["terms_html"] = json.loads(locale_payload)
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


def test_admin_plans_index_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.products.manage"])

    try:
        response = client.get(
            "/api/v1/admin/products/101/plans",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"]["total"] == 1
    assert payload["data"][0]["id"] == 501


def test_admin_plans_store_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.products.manage"])

    try:
        response = client.post(
            "/api/v1/admin/products/101/plans",
            json={"name": "Nueva version"},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["id"] == 602
    assert payload["redirect_url"] == "/admin/products/101/plans/602/edit"


def test_admin_plans_clone_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.products.manage"])

    try:
        response = client.post(
            "/api/v1/admin/products/101/plans/501/clone",
            json={"name": "Copia version"},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["id"] == 603
    assert payload["redirect_url"] == "/admin/products/101/plans/603/edit"


def test_admin_plans_destroy_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.products.manage"])

    try:
        response = client.delete(
            "/api/v1/admin/products/101/plans/501",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["message"] == "Version eliminada correctamente."


def test_admin_plans_terms_html_show_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.products.manage"])

    try:
        response = client.get(
            "/api/v1/admin/products/101/plans/501/terms-html",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["terms_html"]["es"] == "<p>Hola</p>"
    assert payload["data"]["terms_html"]["en"] == "<p>Hello</p>"


def test_admin_plans_terms_html_update_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.products.manage"])

    try:
        response = client.patch(
            "/api/v1/admin/products/101/plans/501/terms-html",
            json={
                "locale": "es",
                "html": "<p>Nuevo ES</p>",
            },
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["terms_html"]["es"] == "<p>Nuevo ES</p>"
    assert payload["data"]["terms_html"]["en"] == "<p>Hello</p>"


def test_admin_plans_countries_index_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.products.manage"])

    try:
        response = client.get(
            "/api/v1/admin/products/101/plans/501/countries",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["data"]["countries"]) >= 2
    assert len(payload["data"]["plan_countries"]) == 1


def test_admin_plans_countries_update_price_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.products.manage"])

    try:
        response = client.patch(
            "/api/v1/admin/products/101/plans/501/countries/1",
            json={"price": 200.5},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["price"] == 200.5


def test_admin_plans_repatriation_store_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.products.manage"])

    try:
        response = client.post(
            "/api/v1/admin/products/101/plans/501/repatriation-countries",
            json={"country_ids": [1]},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["countries"][0]["id"] == 1


def test_admin_plans_repatriation_destroy_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.products.manage"])

    try:
        response = client.delete(
            "/api/v1/admin/products/101/plans/501/repatriation-countries/2",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["countries"][0]["id"] == 2


def test_admin_plans_forbidden_without_permission(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=[])

    try:
        response = client.get(
            "/api/v1/admin/products/101/plans",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 403