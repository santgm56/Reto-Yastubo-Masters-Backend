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

        if "FROM companies" in sql and "WHERE id = :company_id" in sql:
            if int(params.get("company_id") or 0) == 22:
                return _FakeResult(first_row={"id": 22})
            return _FakeResult(first_row=None)

        if "SELECT COUNT(1) AS total" in sql and "FROM capitados_contracts c" in sql:
            return _FakeResult(first_row={"total": 2})

        if "FROM capitados_contracts c" in sql and "ORDER BY c.id DESC" in sql:
            return _FakeResult(
                all_rows=[
                    {
                        "id": 101,
                        "uuid": "uuid-101",
                        "company_id": 22,
                        "product_id": 5,
                        "person_id": 7001,
                        "status": "active",
                        "entry_date": "2026-01-01",
                        "valid_until": "2026-12-31",
                        "entry_age": 34,
                        "created_at": "2026-01-01 00:00:00",
                        "updated_at": "2026-01-02 00:00:00",
                        "person_full_name": "Juan Perez",
                        "person_document_number": "ABC123",
                        "person_sex": "M",
                        "product_name": "Plan Gold",
                    },
                    {
                        "id": 100,
                        "uuid": "uuid-100",
                        "company_id": 22,
                        "product_id": 5,
                        "person_id": 7000,
                        "status": "active",
                        "entry_date": "2026-01-01",
                        "valid_until": "2026-12-31",
                        "entry_age": 32,
                        "created_at": "2026-01-01 00:00:00",
                        "updated_at": "2026-01-02 00:00:00",
                        "person_full_name": "Ana Lopez",
                        "person_document_number": "XYZ999",
                        "person_sex": "F",
                        "product_name": "Plan Gold",
                    },
                ]
            )

        if "WHERE c.id = :contract_id" in sql and "AND c.company_id = :company_id" in sql:
            contract_id = int(params.get("contract_id") or 0)
            if contract_id == 101:
                return _FakeResult(
                    first_row={
                        "id": 101,
                        "uuid": "uuid-101",
                        "company_id": 22,
                        "product_id": 5,
                        "person_id": 7001,
                        "status": "active",
                        "entry_date": "2026-01-01",
                        "valid_until": "2026-12-31",
                        "entry_age": 34,
                        "created_at": "2026-01-01 00:00:00",
                        "updated_at": "2026-01-02 00:00:00",
                        "person_full_name": "Juan Perez",
                        "person_document_number": "ABC123",
                        "person_sex": "M",
                        "product_name": "Plan Gold",
                    }
                )
            return _FakeResult(first_row=None)

        if "FROM capitados_monthly_records mr" in sql and "ORDER BY mr.coverage_month DESC" in sql:
            return _FakeResult(
                first_row={
                    "id": 990,
                    "company_id": 22,
                    "product_id": 5,
                    "person_id": 7001,
                    "contract_id": 101,
                    "coverage_month": "2026-03-01",
                    "plan_version_id": 77,
                    "load_batch_id": 12,
                    "full_name": "Juan Perez",
                    "sex": "M",
                    "age_reported": 35,
                    "price_base": 100,
                    "price_source": "global",
                    "age_surcharge_rule_id": 9,
                    "age_surcharge_percent": 5,
                    "age_surcharge_amount": 5,
                    "price_final": 105,
                    "status": "active",
                    "created_at": "2026-03-01 00:00:00",
                    "updated_at": "2026-03-01 00:00:00",
                    "residence_country_id": 170,
                    "residence_country_iso2": "CO",
                    "residence_country_iso3": "COL",
                    "residence_country_name": "Colombia",
                    "repatriation_country_id": 724,
                    "repatriation_country_iso2": "ES",
                    "repatriation_country_iso3": "ESP",
                    "repatriation_country_name": "Espana",
                }
            )

        return _FakeResult(first_row=None)



def _setup(monkeypatch, fake_db, role="ADMIN"):
    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(
        AuthService,
        "me",
        lambda _self, _token: {"permissions": [], "role": role},
    )
    app.dependency_overrides[get_db] = fake_get_db



def _teardown_override():
    app.dependency_overrides.pop(get_db, None)



def test_admin_companies_capitated_contracts_index_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db)

    try:
        response = client.get(
            "/api/v1/admin/companies/22/capitated/contracts",
            params={"product_id": 5, "page": 1, "per_page": 15, "status": "active", "q": "juan"},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 200
    payload = response.json()

    assert payload["meta"] == {"current_page": 1, "last_page": 1, "per_page": 15, "total": 2}
    assert len(payload["data"]) == 2
    assert payload["data"][0]["id"] == 101
    assert payload["data"][0]["person"]["full_name"] == "Juan Perez"



def test_admin_companies_capitated_contracts_show_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db)

    try:
        response = client.get(
            "/api/v1/admin/companies/22/capitated/contracts/101",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 200
    payload = response.json()

    assert payload["contract"]["id"] == 101
    assert payload["contract"]["person"]["document_number"] == "ABC123"
    assert payload["last_monthly_record"]["id"] == 990
    assert payload["last_monthly_record"]["residence_country"]["name"] == "Colombia"



def test_admin_companies_capitated_contracts_returns_404_when_company_missing(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db)

    try:
        response = client.get(
            "/api/v1/admin/companies/999/capitated/contracts",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 404



def test_admin_companies_capitated_contracts_returns_404_when_contract_missing(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db)

    try:
        response = client.get(
            "/api/v1/admin/companies/22/capitated/contracts/999",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 404



def test_admin_companies_capitated_contracts_forbidden_for_non_admin(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, role="SELLER")

    try:
        response = client.get(
            "/api/v1/admin/companies/22/capitated/contracts",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 403
