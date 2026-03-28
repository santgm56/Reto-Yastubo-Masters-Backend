from io import BytesIO

from openpyxl import Workbook

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
        self.last_file_id = 0
        self.last_batch_id = 0
        self.batch_status = "processed"
        self.monthly_record_status = {301: "active", 302: "rolled_back"}

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        self.calls.append({"sql": sql, "params": params})

        if "FROM companies" in sql and "WHERE id = :company_id" in sql:
            if int(params.get("company_id") or 0) == 22:
                return _FakeResult(first_row={"id": 22})
            return _FakeResult(first_row=None)

        if "SELECT COUNT(1) AS total" in sql and "FROM capitados_batch_logs b" in sql:
            return _FakeResult(first_row={"total": 2})

        if "SELECT id, status, rolled_back_at" in sql and "FROM capitados_batch_logs" in sql:
            return _FakeResult(first_row={"id": 901, "status": self.batch_status, "rolled_back_at": None if self.batch_status != "rolled_back" else "2026-03-11 00:00:00"})

        if "SELECT id" in sql and "FROM capitados_batch_logs" in sql and "WHERE id = :batch_id" in sql and "company_id = :company_id" in sql:
            if int(params.get("batch_id") or 0) in {901, int(self.last_batch_id or 901)}:
                return _FakeResult(first_row={"id": 901})
            return _FakeResult(first_row=None)

        if "FROM capitados_batch_logs b" in sql and "LEFT JOIN files f" in sql and "WHERE b.id = :batch_id" in sql:
            return _FakeResult(
                first_row={
                    "id": int(self.last_batch_id or 901),
                    "company_id": 22,
                    "coverage_month": "2026-03-01",
                    "source": "excel",
                    "source_file_id": int(self.last_file_id or 501),
                    "original_filename": "batch.xlsx",
                    "file_hash": "abc",
                    "created_by_user_id": 99,
                    "status": self.batch_status,
                    "processed_at": "2026-03-10 00:00:00",
                    "rolled_back_at": None,
                    "rolled_back_by_user_id": None,
                    "total_rows": 3,
                    "total_applied": 0,
                    "total_rejected": 3,
                    "total_duplicated": 0,
                    "total_incongruences": 0,
                    "total_plan_errors": 0,
                    "total_rolled_back": 0,
                    "is_any_month_allowed": 1,
                    "cutoff_day": 15,
                    "error_summary": None,
                    "summary_json": None,
                    "created_at": "2026-03-10 00:00:00",
                    "updated_at": "2026-03-10 00:00:00",
                    "source_file_uuid": "uuid-uploaded",
                    "created_by_id": 99,
                    "created_by_display_name": "Admin Batch",
                    "created_by_first_name": "Admin",
                    "created_by_last_name": "Batch",
                    "created_by_email": "admin@test.com",
                }
            )

        if "FROM capitados_batch_logs b" in sql and "LEFT JOIN files f" in sql and "ORDER BY b.id DESC" in sql:
            return _FakeResult(
                all_rows=[
                    {
                        "id": 901,
                        "company_id": 22,
                        "coverage_month": "2026-03-01",
                        "source": "excel",
                        "source_file_id": 501,
                        "original_filename": "batch_a.xlsx",
                        "file_hash": "hash-a",
                        "created_by_user_id": 99,
                        "status": "processed",
                        "processed_at": "2026-03-10 00:00:00",
                        "rolled_back_at": None,
                        "rolled_back_by_user_id": None,
                        "total_rows": 10,
                        "total_applied": 8,
                        "total_rejected": 2,
                        "total_duplicated": 0,
                        "total_incongruences": 0,
                        "total_plan_errors": 0,
                        "total_rolled_back": 0,
                        "is_any_month_allowed": 1,
                        "cutoff_day": 15,
                        "error_summary": None,
                        "summary_json": None,
                        "created_at": "2026-03-10 00:00:00",
                        "updated_at": "2026-03-10 00:00:00",
                        "source_file_uuid": "uuid-501",
                    },
                    {
                        "id": 900,
                        "company_id": 22,
                        "coverage_month": "2026-02-01",
                        "source": "excel",
                        "source_file_id": None,
                        "original_filename": "batch_b.xlsx",
                        "file_hash": "hash-b",
                        "created_by_user_id": 99,
                        "status": "failed",
                        "processed_at": "2026-02-10 00:00:00",
                        "rolled_back_at": None,
                        "rolled_back_by_user_id": None,
                        "total_rows": 2,
                        "total_applied": 0,
                        "total_rejected": 2,
                        "total_duplicated": 0,
                        "total_incongruences": 0,
                        "total_plan_errors": 0,
                        "total_rolled_back": 0,
                        "is_any_month_allowed": 0,
                        "cutoff_day": 15,
                        "error_summary": None,
                        "summary_json": None,
                        "created_at": "2026-02-10 00:00:00",
                        "updated_at": "2026-02-10 00:00:00",
                        "source_file_uuid": None,
                    },
                ]
            )

        if "FROM products p" in sql and "EXISTS" in sql:
            return _FakeResult(
                all_rows=[
                    {"id": 11, "name": '{"es":"Colectivos JDP","en":"Colectivos JDP"}'},
                    {"id": 12, "name": "Memorias Individual"},
                ]
            )

        if "SELECT DISTINCT sheet_name" in sql and "FROM capitados_batch_item_logs" in sql:
            return _FakeResult(all_rows=[{"sheet_name": "(11) Colectivos JDP"}])

        if "SELECT COUNT(1) AS total" in sql and "FROM capitados_batch_item_logs i" in sql:
            return _FakeResult(first_row={"total": 2})

        if "FROM capitados_batch_item_logs i" in sql and "LEFT JOIN countries rc" in sql:
            return _FakeResult(
                all_rows=[
                    {
                        "id": 801,
                        "batch_id": 901,
                        "sheet_name": "(11) Colectivos JDP",
                        "row_number": 2,
                        "product_id": 11,
                        "plan_version_id": 17,
                        "residence_raw": "USA",
                        "residence_code_extracted": "USA",
                        "repatriation_raw": "COL",
                        "repatriation_code_extracted": "COL",
                        "residence_country_id": 181,
                        "repatriation_country_id": 161,
                        "document_number": "DOC1",
                        "full_name": "Juan Perez",
                        "sex": "M",
                        "age_reported": 35,
                        "result": "applied",
                        "rejection_code": None,
                        "rejection_detail": None,
                        "person_id": 7001,
                        "contract_id": 101,
                        "monthly_record_id": 301,
                        "duplicated_record_id": None,
                        "created_at": "2026-03-10 00:00:00",
                        "updated_at": "2026-03-10 00:00:00",
                        "residence_country_ref_id": 181,
                        "residence_country_name": "Estados Unidos",
                        "residence_country_iso2": "US",
                        "residence_country_iso3": "USA",
                        "repatriation_country_ref_id": 161,
                        "repatriation_country_name": "Colombia",
                        "repatriation_country_iso2": "CO",
                        "repatriation_country_iso3": "COL",
                    },
                    {
                        "id": 802,
                        "batch_id": 901,
                        "sheet_name": "(11) Colectivos JDP",
                        "row_number": 3,
                        "product_id": 11,
                        "plan_version_id": 17,
                        "residence_raw": "ESP",
                        "residence_code_extracted": "ESP",
                        "repatriation_raw": "COL",
                        "repatriation_code_extracted": "COL",
                        "residence_country_id": 180,
                        "repatriation_country_id": 161,
                        "document_number": "DOC2",
                        "full_name": "Ana Lopez",
                        "sex": "F",
                        "age_reported": 33,
                        "result": "rejected",
                        "rejection_code": "PERSON_AGE_INVALID",
                        "rejection_detail": "Edad invalida",
                        "person_id": 7002,
                        "contract_id": None,
                        "monthly_record_id": None,
                        "duplicated_record_id": None,
                        "created_at": "2026-03-10 00:00:00",
                        "updated_at": "2026-03-10 00:00:00",
                        "residence_country_ref_id": 180,
                        "residence_country_name": "Espana",
                        "residence_country_iso2": "ES",
                        "residence_country_iso3": "ESP",
                        "repatriation_country_ref_id": 161,
                        "repatriation_country_name": "Colombia",
                        "repatriation_country_iso2": "CO",
                        "repatriation_country_iso3": "COL",
                    },
                ]
            )

        if "SELECT COUNT(1) AS total" in sql and "FROM capitados_monthly_records mr" in sql:
            return _FakeResult(first_row={"total": 2})

        if "FROM capitados_monthly_records mr" in sql and "LEFT JOIN capitados_product_insureds p" in sql:
            return _FakeResult(
                all_rows=[
                    {
                        "id": 301,
                        "company_id": 22,
                        "product_id": 11,
                        "person_id": 7001,
                        "contract_id": 101,
                        "coverage_month": "2026-03-01",
                        "plan_version_id": 17,
                        "load_batch_id": 901,
                        "full_name": "Juan Perez",
                        "sex": "M",
                        "age_reported": 35,
                        "price_base": 100,
                        "price_source": "global",
                        "age_surcharge_rule_id": 9,
                        "age_surcharge_percent": 5,
                        "age_surcharge_amount": 5,
                        "price_final": 105,
                        "status": self.monthly_record_status[301],
                        "created_at": "2026-03-10 00:00:00",
                        "updated_at": "2026-03-10 00:00:00",
                        "person_ref_id": 7001,
                        "person_document_number": "DOC1",
                        "person_full_name": "Juan Perez",
                        "person_status": "active",
                        "residence_country_ref_id": 181,
                        "residence_country_name": "Estados Unidos",
                        "residence_country_iso2": "US",
                        "residence_country_iso3": "USA",
                        "repatriation_country_ref_id": 161,
                        "repatriation_country_name": "Colombia",
                        "repatriation_country_iso2": "CO",
                        "repatriation_country_iso3": "COL",
                    },
                    {
                        "id": 302,
                        "company_id": 22,
                        "product_id": 12,
                        "person_id": 7002,
                        "contract_id": 102,
                        "coverage_month": "2026-03-01",
                        "plan_version_id": 18,
                        "load_batch_id": 901,
                        "full_name": "Ana Lopez",
                        "sex": "F",
                        "age_reported": 33,
                        "price_base": 90,
                        "price_source": "country",
                        "age_surcharge_rule_id": None,
                        "age_surcharge_percent": 0,
                        "age_surcharge_amount": 0,
                        "price_final": 90,
                        "status": self.monthly_record_status[302],
                        "created_at": "2026-03-10 00:00:00",
                        "updated_at": "2026-03-10 00:00:00",
                        "person_ref_id": 7002,
                        "person_document_number": "DOC2",
                        "person_full_name": "Ana Lopez",
                        "person_status": "rolled_back",
                        "residence_country_ref_id": 180,
                        "residence_country_name": "Espana",
                        "residence_country_iso2": "ES",
                        "residence_country_iso3": "ESP",
                        "repatriation_country_ref_id": 161,
                        "repatriation_country_name": "Colombia",
                        "repatriation_country_iso2": "CO",
                        "repatriation_country_iso3": "COL",
                    },
                ]
            )

        if "SELECT DISTINCT p.id, p.name" in sql and "FROM capitados_monthly_records mr" in sql:
            return _FakeResult(
                all_rows=[
                    {"id": 11, "name": '{"es":"Colectivos JDP","en":"Colectivos JDP"}'},
                    {"id": 12, "name": "Memorias Individual"},
                ]
            )

        if "SELECT id, status" in sql and "FROM capitados_monthly_records" in sql and "load_batch_id = :batch_id" in sql:
            record_id = int(params.get("record_id") or 0)
            if record_id in self.monthly_record_status:
                return _FakeResult(first_row={"id": record_id, "status": self.monthly_record_status[record_id]})
            return _FakeResult(first_row=None)

        if "UPDATE capitados_monthly_records" in sql and "SET status = 'rolled_back'" in sql and "WHERE id = :record_id" in sql:
            record_id = int(params.get("record_id") or 0)
            if record_id in self.monthly_record_status:
                self.monthly_record_status[record_id] = "rolled_back"
            return _FakeResult()

        if "UPDATE capitados_monthly_records" in sql and "SET status = 'rolled_back'" in sql and "load_batch_id = :batch_id" in sql:
            for key in list(self.monthly_record_status.keys()):
                if self.monthly_record_status[key] == "active":
                    self.monthly_record_status[key] = "rolled_back"
            return _FakeResult()

        if "UPDATE capitados_batch_logs" in sql and "SET status = 'rolled_back'" in sql:
            self.batch_status = "rolled_back"
            return _FakeResult()

        if "INSERT INTO files" in sql:
            self.last_file_id = 501
            return _FakeResult()

        if "INSERT INTO capitados_batch_logs" in sql:
            self.last_batch_id = 901
            return _FakeResult()

        if "SELECT LAST_INSERT_ID() AS id" in sql:
            if self.last_file_id and not self.last_batch_id:
                return _FakeResult(first_row={"id": self.last_file_id})
            return _FakeResult(first_row={"id": self.last_batch_id})

        return _FakeResult()

    def commit(self):
        return None


def _setup(monkeypatch, fake_db, permissions, role="ADMIN"):
    def fake_get_db():
        yield fake_db

    monkeypatch.setattr(
        AuthService,
        "me",
        lambda _self, _token: {"permissions": permissions, "role": role, "id": 99},
    )
    app.dependency_overrides[get_db] = fake_get_db


def _teardown_override():
    app.dependency_overrides.pop(get_db, None)


def _build_excel_bytes() -> bytes:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Carga"
    sheet.append(["ID", "Nombre", "Residencia", "Nacionalidad", "Sexo", "Edad"])
    sheet.append([1, "Ana", "USA", "COL", "F", 30])
    sheet.append([2, "Luis", "ESP", "COL", "M", 40])

    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    return output.getvalue()


def test_admin_companies_capitated_batches_index_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["capitados.batch.view"])

    try:
        response = client.get(
            "/api/v1/admin/companies/22/capitated/batches",
            params={"page": 1, "per_page": 15},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 200
    payload = response.json()

    assert payload["meta"] == {"current_page": 1, "last_page": 1, "per_page": 15, "total": 2}
    assert len(payload["data"]) == 2
    assert payload["data"][0]["id"] == 901
    assert payload["data"][0]["file_temporary_url"] == "/api/v1/files/uuid-501"
    assert payload["data"][1]["file_temporary_url"] is None


def test_admin_companies_capitated_batches_template_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["capitados.batch.view"])

    try:
        response = client.get(
            "/api/v1/admin/companies/22/capitated/batches/template",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 200
    assert response.headers.get("content-type", "").startswith(
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    assert "attachment; filename=\"capitados_estructura_company_22_" in response.headers.get(
        "content-disposition", ""
    )


def test_admin_companies_capitated_batches_upload_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["capitados.batch.create_any_month"])

    excel_bytes = _build_excel_bytes()

    try:
        response = client.post(
            "/api/v1/admin/companies/22/capitated/batches/upload",
            files={"file": ("batch.xlsx", excel_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            data={"coverage_month": "2026-03-01"},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 200
    payload = response.json()
    assert payload["batch"]["id"] == 901
    assert payload["batch"]["status"] == "processed"
    assert payload["batch"]["file_temporary_url"] == "/api/v1/files/uuid-uploaded"


def test_admin_companies_capitated_batches_upload_forbidden_without_permission(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=[])

    excel_bytes = _build_excel_bytes()

    try:
        response = client.post(
            "/api/v1/admin/companies/22/capitated/batches/upload",
            files={"file": ("batch.xlsx", excel_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            data={"coverage_month": "2026-03-01"},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 403


def test_admin_companies_capitated_batches_show_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["capitados.batch.view"])

    try:
        response = client.get(
            "/api/v1/admin/companies/22/capitated/batches/901",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 200
    payload = response.json()
    assert payload["batch"]["id"] == 901
    assert payload["batch"]["can_rollback"] is True
    assert payload["batch"]["created_by"]["display_name"] == "Admin Batch"


def test_admin_companies_capitated_batches_items_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["capitados.batch.view"])

    try:
        response = client.get(
            "/api/v1/admin/companies/22/capitated/batches/901/items",
            params={"page": 1, "per_page": 25, "result": "applied"},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"] == {"current_page": 1, "last_page": 1, "per_page": 25, "total": 2}
    assert payload["filters"]["sheet"] == "(11) Colectivos JDP"
    assert payload["sheets"] == ["(11) Colectivos JDP"]
    assert payload["data"][0]["residence_country"]["name"] == "Estados Unidos"


def test_admin_companies_capitated_batches_monthly_records_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["capitados.batch.view"])

    try:
        response = client.get(
            "/api/v1/admin/companies/22/capitated/batches/901/monthly-records",
            params={"page": 1, "per_page": 25},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"] == {"current_page": 1, "last_page": 1, "per_page": 25, "total": 2}
    assert payload["data"][0]["person"]["document_number"] == "DOC1"
    assert payload["data"][0]["can_rollback"] is True
    assert payload["products"][0]["id"] == 11


def test_admin_companies_capitated_batches_rollback_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["capitados.batch.view"])

    try:
        response = client.post(
            "/api/v1/admin/companies/22/capitated/batches/901/rollback",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 200
    payload = response.json()
    assert payload["batch"]["status"] == "rolled_back"
    assert payload["batch"]["can_rollback"] is False


def test_admin_companies_capitated_batch_monthly_record_rollback_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["capitados.batch.view"])

    try:
        response = client.post(
            "/api/v1/admin/companies/22/capitated/batches/901/monthly-records/301/rollback",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 200
    payload = response.json()
    assert payload["message"] == "Registro mensual revertido correctamente."


def test_admin_companies_capitated_batches_forbidden_for_non_admin(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["capitados.batch.view"], role="CUSTOMER")

    try:
        response = client.get(
            "/api/v1/admin/companies/22/capitated/batches",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown_override()

    assert response.status_code == 403
