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

        if "FROM capitated_monthly_records" in sql and "GROUP BY" in sql:
            return _FakeResult(
                all_rows=[
                    {
                        "month": "2026-03-01",
                        "active_count": 2,
                        "active_total": 150.5,
                    },
                    {
                        "month": "2026-02-01",
                        "active_count": 1,
                        "active_total": 70,
                    },
                ]
            )

        if "FROM capitated_monthly_records cmr" in sql and "LEFT JOIN countries" in sql:
            return _FakeResult(
                all_rows=[
                    {
                        "id": 501,
                        "contract_id": 9001,
                        "coverage_month": "2026-03-01",
                        "person_id": 150,
                        "full_name": "Juan Perez",
                        "sex": "M",
                        "age_reported": 34,
                        "residence_iso3": "COL",
                        "residence_iso2": "CO",
                        "residence_name": "Colombia",
                        "repatriation_iso3": "ESP",
                        "repatriation_iso2": "ES",
                        "repatriation_name": "Espana",
                        "price_source": "table",
                        "price_base": 100,
                        "age_surcharge_percent": 10,
                        "price_final": 110,
                    }
                ]
            )

        return _FakeResult(first_row=None)

    def commit(self):
        return None



def _setup_with_permissions(monkeypatch, fake_db, permissions):
    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(
        AuthService,
        "me",
        lambda _self, _token: {"permissions": permissions, "role": "ADMIN"},
    )
    app.dependency_overrides[get_db] = fake_get_db



def _teardown_override():
    app.dependency_overrides.pop(get_db, None)



def test_admin_companies_capitated_monthly_months_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup_with_permissions(monkeypatch, fake_db, ["capitados.reporte.mensual"])

    try:
        response = client.get(
            "/api/v1/admin/companies/22/capitated/reports/monthly/months",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 200
    payload = response.json()
    assert payload == {
        "months": [
            {"month": "2026-03-01", "active_count": 2, "active_total": 150.5},
            {"month": "2026-02-01", "active_count": 1, "active_total": 70.0},
        ]
    }



def test_admin_companies_capitated_monthly_download_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup_with_permissions(monkeypatch, fake_db, ["capitados.reporte.mensual"])

    try:
        response = client.get(
            "/api/v1/admin/companies/22/capitated/reports/monthly/2026-03-01/download",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 200
    assert response.headers.get("content-type", "").startswith(
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    assert "attachment; filename=\"capitados_reporte_2026_03.xlsx\"" in response.headers.get(
        "content-disposition", ""
    )



def test_admin_companies_capitated_monthly_months_returns_404_when_company_missing(client, monkeypatch):
    fake_db = _FakeDb()
    _setup_with_permissions(monkeypatch, fake_db, ["capitados.reporte.mensual"])

    try:
        response = client.get(
            "/api/v1/admin/companies/999/capitated/reports/monthly/months",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 404



def test_admin_companies_capitated_monthly_forbidden_without_permission(client, monkeypatch):
    fake_db = _FakeDb()
    _setup_with_permissions(monkeypatch, fake_db, [])

    try:
        response = client.get(
            "/api/v1/admin/companies/22/capitated/reports/monthly/months",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 403
