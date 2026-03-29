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
        self.last_insert_id = 0
        self.templates = {
            7: {
                "id": 7,
                "name": "Template A",
                "slug": "template-a",
                "type": "PDF",
                "test_data_json": '{"foo": "bar"}',
                "active_template_version_id": 70,
                "deleted_at": None,
            }
        }
        self.versions = {
            70: {
                "id": 70,
                "template_id": 7,
                "name": "Version #70",
                "content": "<h1>Hola</h1>",
                "test_data_json": None,
            }
        }

    def _template_row(self, template_id: int):
        row = self.templates.get(template_id)
        if not row:
            return None
        active_id = row.get("active_template_version_id")
        active = self.versions.get(int(active_id or 0)) if active_id else None
        return {
            "id": row.get("id"),
            "name": row.get("name"),
            "slug": row.get("slug"),
            "type": row.get("type"),
            "test_data_json": row.get("test_data_json"),
            "active_template_version_id": row.get("active_template_version_id"),
            "active_version_id": active.get("id") if active else None,
            "active_version_name": active.get("name") if active else None,
        }

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}

        if "FROM templates t" in sql and "WHERE t.deleted_at IS NULL" in sql and "ORDER BY t.id DESC" in sql:
            rows = []
            for template_id in sorted(self.templates.keys(), reverse=True):
                row = self.templates[template_id]
                if row.get("deleted_at") is None:
                    rows.append(self._template_row(template_id))
            return _FakeResult(all_rows=rows)

        if "FROM templates t" in sql and "WHERE t.id = :template_id" in sql:
            template_id = int(params.get("template_id") or 0)
            row = self.templates.get(template_id)
            if not row or row.get("deleted_at") is not None:
                return _FakeResult(first_row=None)
            return _FakeResult(first_row=self._template_row(template_id))

        if "FROM template_versions" in sql and "WHERE template_id = :template_id" in sql and "ORDER BY id ASC" in sql:
            template_id = int(params.get("template_id") or 0)
            rows = [v for v in self.versions.values() if int(v.get("template_id") or 0) == template_id]
            rows.sort(key=lambda item: int(item.get("id") or 0))
            return _FakeResult(all_rows=rows)

        if "SELECT id" in sql and "FROM templates" in sql and "slug = :slug" in sql:
            slug = str(params.get("slug") or "")
            for row in self.templates.values():
                if str(row.get("slug") or "") == slug:
                    return _FakeResult(first_row={"id": row.get("id")})
            return _FakeResult(first_row=None)

        if "INSERT INTO templates" in sql:
            self.last_insert_id = 8
            self.templates[8] = {
                "id": 8,
                "name": params.get("name"),
                "slug": params.get("slug"),
                "type": params.get("type"),
                "test_data_json": params.get("test_data_json"),
                "active_template_version_id": None,
                "deleted_at": None,
            }
            return _FakeResult(first_row=None)

        if "SELECT LAST_INSERT_ID() AS id" in sql:
            return _FakeResult(first_row={"id": self.last_insert_id})

        if "SELECT id, name, test_data_json" in sql and "active_template_version_id" not in sql and "FROM templates" in sql and "WHERE id = :template_id" in sql:
            template_id = int(params.get("template_id") or 0)
            row = self.templates.get(template_id)
            if not row or row.get("deleted_at") is not None:
                return _FakeResult(first_row=None)
            return _FakeResult(
                first_row={
                    "id": row.get("id"),
                    "name": row.get("name"),
                    "test_data_json": row.get("test_data_json"),
                }
            )

        if "SELECT id, name, test_data_json, active_template_version_id" in sql and "FROM templates" in sql and "WHERE id = :template_id" in sql:
            template_id = int(params.get("template_id") or 0)
            row = self.templates.get(template_id)
            if not row or row.get("deleted_at") is not None:
                return _FakeResult(first_row=None)
            return _FakeResult(
                first_row={
                    "id": row.get("id"),
                    "name": row.get("name"),
                    "test_data_json": row.get("test_data_json"),
                    "active_template_version_id": row.get("active_template_version_id"),
                }
            )

        if "SELECT id FROM templates WHERE id = :template_id" in sql and "deleted_at IS NULL" in sql:
            template_id = int(params.get("template_id") or 0)
            row = self.templates.get(template_id)
            if row and row.get("deleted_at") is None:
                return _FakeResult(first_row={"id": template_id})
            return _FakeResult(first_row=None)

        if "INSERT INTO template_versions" in sql and "VALUES (:template_id, '', '', NULL" in sql:
            self.last_insert_id = 71
            self.versions[71] = {
                "id": 71,
                "template_id": int(params.get("template_id") or 0),
                "name": "",
                "content": "",
                "test_data_json": None,
            }
            return _FakeResult(first_row=None)

        if "UPDATE template_versions" in sql and "SET name = :name" in sql and "WHERE id = :version_id" in sql:
            version_id = int(params.get("version_id") or 0)
            version = self.versions.get(version_id)
            if version:
                if "content" in params:
                    version["content"] = params.get("content")
                version["name"] = params.get("name")
            return _FakeResult(first_row=None)

        if "SELECT id, template_id, name, content, test_data_json" in sql and "WHERE id = :version_id" in sql and "template_id = :template_id" in sql:
            version_id = int(params.get("version_id") or 0)
            template_id = int(params.get("template_id") or 0)
            version = self.versions.get(version_id)
            if version and int(version.get("template_id") or 0) == template_id:
                return _FakeResult(first_row=version)
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
        lambda _self, _token: {
            "id": 1,
            "role": "ADMIN",
            "permissions": permissions,
        },
    )
    app.dependency_overrides[get_db] = fake_get_db



def _teardown():
    app.dependency_overrides.pop(get_db, None)



def test_admin_templates_index_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.templates.edit"])

    try:
        response = client.get(
            "/api/v1/admin/templates",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["data"]) == 1
    assert payload["data"][0]["id"] == 7
    assert payload["data"][0]["active_version"]["id"] == 70



def test_admin_templates_store_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.templates.edit"])

    try:
        response = client.post(
            "/api/v1/admin/templates",
            json={
                "name": "Template B",
                "slug": "template-b",
                "type": "HTML",
            },
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["template"]["id"] == 8
    assert payload["toast"]["message"] == "Plantilla creada."



def test_admin_templates_versions_store_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.templates.edit"])

    try:
        response = client.post(
            "/api/v1/admin/templates/7/versions",
            json={},
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["version"]["id"] == 71
    assert payload["data"]["version"]["name"] == "Version #71"



def test_admin_templates_preview_raw_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.templates.edit"])

    try:
        response = client.get(
            "/api/v1/admin/templates/7/versions/70/preview/raw",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    assert "Hola" in response.text


def test_admin_templates_preview_version_pdf_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.templates.edit"])

    try:
        response = client.get(
            "/api/v1/admin/templates/7/versions/70/preview/pdf",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    assert response.headers.get("content-type", "").startswith("application/pdf")
    assert response.content.startswith(b"%PDF")


def test_admin_templates_preview_active_pdf_contract(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=["admin.templates.edit"])

    try:
        response = client.get(
            "/api/v1/admin/templates/7/active/preview/pdf",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 200
    assert response.headers.get("content-type", "").startswith("application/pdf")
    assert response.content.startswith(b"%PDF")



def test_admin_templates_forbidden_without_permission(client, monkeypatch):
    fake_db = _FakeDb()
    _setup(monkeypatch, fake_db, permissions=[])

    try:
        response = client.get(
            "/api/v1/admin/templates",
            cookies={"yastubo_access_token": "token-admin"},
        )
    finally:
        _teardown()

    assert response.status_code == 403
