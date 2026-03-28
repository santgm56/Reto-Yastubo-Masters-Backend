from app.db.database import get_db
from app.main import app


class _FakeResult:
    def __init__(self, *, first_row=None):
        self._first_row = first_row

    def mappings(self):
        return self

    def first(self):
        return self._first_row


class _FakeDb:
    def __init__(self, *, has_contract=True, has_monthly=True):
        self.has_contract = has_contract
        self.has_monthly = has_monthly

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}

        if "FROM capitados_contracts c" in sql and "WHERE c.uuid = :contract_uuid" in sql:
            if not self.has_contract:
                return _FakeResult(first_row=None)
            return _FakeResult(
                first_row={
                    "id": 901,
                    "uuid": str(params.get("contract_uuid") or "uuid-x"),
                    "status": "active",
                    "entry_date": "2026-01-01",
                    "valid_until": "2026-12-31",
                    "company_id": 22,
                    "product_id": 11,
                    "person_full_name": "Juan Perez",
                    "person_document_number": "DOC1",
                    "product_name": "Plan Capitado",
                    "company_short_code": "ACME",
                    "company_name": "Acme Co",
                }
            )

        if "FROM capitados_monthly_records mr" in sql and "WHERE mr.contract_id = :contract_id" in sql:
            if not self.has_monthly:
                return _FakeResult(first_row=None)
            return _FakeResult(
                first_row={
                    "id": 301,
                    "coverage_month": "2026-03-01",
                    "price_final": 105,
                    "residence_country_name": "Colombia",
                    "repatriation_country_name": "Espana",
                }
            )

        return _FakeResult(first_row=None)



def test_public_capitated_contract_pdf_contract_ok(client):
    fake_db = _FakeDb(has_contract=True, has_monthly=True)

    def fake_get_db():
        yield fake_db

    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.get("/api/v1/public/capitated/contracts/uuid-901/pdf")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    assert response.headers.get("content-type", "").startswith("application/pdf")
    assert "inline" in response.headers.get("content-disposition", "")
    assert response.content.startswith(b"%PDF")



def test_public_capitated_contract_pdf_contract_404_when_contract_missing(client):
    fake_db = _FakeDb(has_contract=False, has_monthly=True)

    def fake_get_db():
        yield fake_db

    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.get("/api/v1/public/capitated/contracts/missing/pdf")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 404



def test_public_capitated_contract_pdf_contract_404_when_monthly_missing(client):
    fake_db = _FakeDb(has_contract=True, has_monthly=False)

    def fake_get_db():
        yield fake_db

    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.get("/api/v1/public/capitated/contracts/uuid-901/pdf")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 404
