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
        self.countries = {
            1: {
                "id": 1,
                "name": {"es": "Argentina", "en": "Argentina"},
                "iso2": "AR",
                "iso3": "ARG",
                "continent_code": "SA",
                "phone_code": "54",
                "is_active": 1,
            },
            2: {
                "id": 2,
                "name": {"es": "Chile", "en": "Chile"},
                "iso2": "CL",
                "iso3": "CHL",
                "continent_code": "SA",
                "phone_code": "56",
                "is_active": 1,
            },
        }
        self.zones = {
            10: {
                "id": 10,
                "name": "Sudamérica",
                "description": None,
                "is_active": 1,
            }
        }
        self.country_zone = {10: {1}}

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        self.calls.append({"sql": sql, "params": params})

        if "FROM countries" in sql and "WHERE id = :country_id" in sql:
            country_id = int(params.get("country_id") or 0)
            return _FakeResult(first_row=self.countries.get(country_id))

        if "FROM countries" in sql and "ORDER BY name" in sql and "WHERE" in sql:
            rows = list(self.countries.values())
            if "is_active = 1" in sql:
                rows = [r for r in rows if int(r.get("is_active") or 0) == 1]
            if "is_active = 0" in sql:
                rows = [r for r in rows if int(r.get("is_active") or 0) == 0]
            if params.get("continent"):
                rows = [r for r in rows if str(r.get("continent_code") or "") == str(params.get("continent"))]
            return _FakeResult(all_rows=rows)

        if "SELECT id" in sql and "FROM countries" in sql and "WHERE iso2 = :iso2 OR iso3 = :iso3" in sql:
            for row in self.countries.values():
                if row.get("iso2") == params.get("iso2") or row.get("iso3") == params.get("iso3"):
                    return _FakeResult(first_row={"id": row["id"]})
            return _FakeResult(first_row=None)

        if "SELECT id" in sql and "FROM countries" in sql and "id != :country_id" in sql:
            for row in self.countries.values():
                if int(row["id"]) == int(params.get("country_id") or 0):
                    continue
                if row.get("iso2") == params.get("iso2") or row.get("iso3") == params.get("iso3"):
                    return _FakeResult(first_row={"id": row["id"]})
            return _FakeResult(first_row=None)

        if "INSERT INTO countries" in sql:
            self.last_insert_id = 3
            self.countries[3] = {
                "id": 3,
                "name": {"es": "Perú", "en": "Peru"},
                "iso2": params.get("iso2"),
                "iso3": params.get("iso3"),
                "continent_code": params.get("continent_code"),
                "phone_code": params.get("phone_code"),
                "is_active": 1,
            }
            return _FakeResult(first_row=None)

        if "UPDATE countries" in sql and "SET" in sql and "is_active = :is_active" in sql:
            country_id = int(params.get("country_id") or 0)
            if country_id in self.countries:
                self.countries[country_id]["is_active"] = int(params.get("is_active") or 0)
            return _FakeResult(first_row=None)

        if "UPDATE countries" in sql and "SET" in sql and "is_active = :is_active" not in sql:
            country_id = int(params.get("country_id") or 0)
            if country_id in self.countries:
                self.countries[country_id]["iso2"] = params.get("iso2")
                self.countries[country_id]["iso3"] = params.get("iso3")
                self.countries[country_id]["continent_code"] = params.get("continent_code")
                self.countries[country_id]["phone_code"] = params.get("phone_code")
            return _FakeResult(first_row=None)

        if "FROM zones" in sql and "WHERE id = :zone_id" in sql:
            zone_id = int(params.get("zone_id") or 0)
            return _FakeResult(first_row=self.zones.get(zone_id))

        if "FROM zones" in sql and "ORDER BY name" in sql:
            rows = list(self.zones.values())
            if "is_active = 1" in sql:
                rows = [r for r in rows if int(r.get("is_active") or 0) == 1]
            if "is_active = 0" in sql:
                rows = [r for r in rows if int(r.get("is_active") or 0) == 0]
            return _FakeResult(all_rows=rows)

        if "INSERT INTO zones" in sql:
            self.last_insert_id = 11
            self.zones[11] = {
                "id": 11,
                "name": params.get("name"),
                "description": params.get("description"),
                "is_active": 1,
            }
            return _FakeResult(first_row=None)

        if "UPDATE zones" in sql and "is_active = :is_active" in sql:
            zone_id = int(params.get("zone_id") or 0)
            if zone_id in self.zones:
                self.zones[zone_id]["is_active"] = int(params.get("is_active") or 0)
            return _FakeResult(first_row=None)

        if "UPDATE zones" in sql and "is_active = :is_active" not in sql:
            zone_id = int(params.get("zone_id") or 0)
            if zone_id in self.zones:
                self.zones[zone_id]["name"] = params.get("name")
                self.zones[zone_id]["description"] = params.get("description")
            return _FakeResult(first_row=None)

        if "FROM countries c" in sql and "INNER JOIN country_zone" in sql:
            zone_id = int(params.get("zone_id") or 0)
            country_ids = self.country_zone.get(zone_id, set())
            rows = [self.countries[cid] for cid in country_ids if cid in self.countries]
            return _FakeResult(all_rows=rows)

        if "SELECT country_id" in sql and "FROM country_zone" in sql:
            zone_id = int(params.get("zone_id") or 0)
            ids = self.country_zone.get(zone_id, set())
            return _FakeResult(all_rows=[{"country_id": cid} for cid in ids])

        if "INSERT INTO country_zone" in sql:
            zone_id = int(params.get("zone_id") or 0)
            country_id = int(params.get("country_id") or 0)
            self.country_zone.setdefault(zone_id, set()).add(country_id)
            return _FakeResult(first_row=None)

        if "DELETE FROM country_zone" in sql:
            zone_id = int(params.get("zone_id") or 0)
            country_id = int(params.get("country_id") or 0)
            self.country_zone.setdefault(zone_id, set()).discard(country_id)
            return _FakeResult(first_row=None)

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



def test_admin_countries_index_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.countries.manage"])

    try:
        response = client.get(
            "/api/v1/admin/countries",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["countries"]) >= 1
    assert payload["continents"]["SA"] == "Sudamérica"



def test_admin_countries_toggle_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.countries.manage"])

    try:
        response = client.put(
            "/api/v1/admin/countries/1/toggle-active",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["id"] == 1



def test_admin_zones_index_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.countries.manage"])

    try:
        response = client.get(
            "/api/v1/admin/zones",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["zones"]) == 1
    assert payload["zones"][0]["countries_count"] >= 0



def test_admin_zones_available_countries_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.countries.manage"])

    try:
        response = client.get(
            "/api/v1/admin/zones/10/countries/available",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload["countries"], list)
    assert "attached" in payload["countries"][0]



def test_admin_zones_attach_detach_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.countries.manage"])

    try:
        attach_response = client.post(
            "/api/v1/admin/zones/10/countries/2",
            cookies={"yastubo_access_token": "token-admin"},
        )
        detach_response = client.delete(
            "/api/v1/admin/zones/10/countries/1",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert attach_response.status_code == 200
    assert detach_response.status_code == 200



def test_admin_countries_forbidden_without_permission(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=[])

    try:
        response = client.get(
            "/api/v1/admin/countries",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 403
