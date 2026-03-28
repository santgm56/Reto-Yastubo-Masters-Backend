from pathlib import Path
import hashlib
import hmac
import time

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
    def __init__(self, row):
        self.row = row

    def execute(self, statement, params=None):
        sql = str(statement)
        if "FROM files" in sql and "WHERE uuid = :uuid" in sql:
            return _FakeResult(first_row=self.row)
        if "FROM files" in sql and "WHERE id = :file_id" in sql:
            return _FakeResult(first_row=self.row)
        return _FakeResult(first_row=None)



def test_public_file_uuid_contract_inline_image(client, monkeypatch, tmp_path):
    storage_root = tmp_path / "storage" / "app"
    public_root = storage_root / "public"
    public_root.mkdir(parents=True, exist_ok=True)

    file_path = public_root / "logos" / "logo.png"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(b"PNGDATA")

    fake_db = _FakeDb(
        {
            "uuid": "uuid-logo",
            "disk": "public",
            "path": "logos/logo.png",
            "original_name": "logo.png",
            "mime_type": "image/png",
        }
    )

    def fake_get_db():
        yield fake_db

    monkeypatch.setenv("FRONTEND_STORAGE_ROOT", str(storage_root))
    from app.core.config import get_settings

    get_settings.cache_clear()
    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.get("/api/v1/files/uuid-logo")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    assert response.headers.get("content-type", "").startswith("image/png")
    assert "inline" in response.headers.get("content-disposition", "")



def test_public_file_uuid_not_found_contract(client):
    fake_db = _FakeDb(None)

    def fake_get_db():
        yield fake_db

    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.get("/api/v1/files/missing")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 404


def test_public_file_temp_signed_contract_ok(client, monkeypatch, tmp_path):
    storage_root = tmp_path / "storage" / "app"
    public_root = storage_root / "public"
    public_root.mkdir(parents=True, exist_ok=True)

    file_path = public_root / "docs" / "ok.pdf"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(b"%PDF-1.4")

    fake_db = _FakeDb(
        {
            "id": 7,
            "uuid": "uuid-pdf",
            "disk": "public",
            "path": "docs/ok.pdf",
            "original_name": "ok.pdf",
            "mime_type": "application/pdf",
        }
    )

    def fake_get_db():
        yield fake_db

    secret = "test-temp-secret"
    expires = int(time.time()) + 120
    payload = f"7|{expires}".encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()

    monkeypatch.setenv("FRONTEND_STORAGE_ROOT", str(storage_root))
    monkeypatch.setenv("FRONTEND_TEMP_FILE_SECRET", secret)
    from app.core.config import get_settings

    get_settings.cache_clear()
    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.get(f"/api/v1/files/temp/7?expires={expires}&signature={signature}")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    assert response.headers.get("content-type", "").startswith("application/pdf")
    assert "inline" in response.headers.get("content-disposition", "")


def test_public_file_temp_signed_contract_forbidden_when_invalid_signature(client, monkeypatch):
    fake_db = _FakeDb(
        {
            "id": 7,
            "uuid": "uuid-pdf",
            "disk": "public",
            "path": "docs/ok.pdf",
            "original_name": "ok.pdf",
            "mime_type": "application/pdf",
        }
    )

    def fake_get_db():
        yield fake_db

    monkeypatch.setenv("FRONTEND_TEMP_FILE_SECRET", "test-temp-secret")
    from app.core.config import get_settings

    get_settings.cache_clear()
    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.get("/api/v1/files/temp/7?expires=9999999999&signature=bad")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 403


def test_public_file_temp_signed_contract_forbidden_when_expired(client, monkeypatch):
    fake_db = _FakeDb(
        {
            "id": 7,
            "uuid": "uuid-pdf",
            "disk": "public",
            "path": "docs/ok.pdf",
            "original_name": "ok.pdf",
            "mime_type": "application/pdf",
        }
    )

    def fake_get_db():
        yield fake_db

    secret = "test-temp-secret"
    expires = int(time.time()) - 1
    payload = f"7|{expires}".encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()

    monkeypatch.setenv("FRONTEND_TEMP_FILE_SECRET", secret)
    from app.core.config import get_settings

    get_settings.cache_clear()
    app.dependency_overrides[get_db] = fake_get_db

    try:
        response = client.get(f"/api/v1/files/temp/7?expires={expires}&signature={signature}")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 403
