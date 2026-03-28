from __future__ import annotations

from pathlib import Path
import mimetypes
import hashlib
import hmac
import time

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.database import get_db


router = APIRouter(prefix="/api/v1/files", tags=["public-files"])


def _storage_root() -> Path:
    settings = get_settings()
    configured = Path(str(settings.frontend_storage_root or "").strip())
    if configured.is_absolute():
        return configured

    backend_root = Path(__file__).resolve().parents[3]
    return (backend_root / configured).resolve()


def _resolve_disk_root(disk: str) -> Path:
    normalized = str(disk or "").strip().lower()
    root = _storage_root()
    if normalized == "public":
        return root / "public"
    if normalized in {"local", "private"}:
        return root / "private"

    # Mantener contrato de "not found" para disks no soportados en esta fase.
    raise HTTPException(status_code=404, detail="Archivo no encontrado.")


def _is_inline_mime(mime: str | None) -> bool:
    if not mime:
        return False
    value = mime.lower().strip()
    return value.startswith("image/") or value == "application/pdf"


def _temp_file_secret() -> str:
    settings = get_settings()
    # Se mantiene simple para interoperar con firma generada desde Laravel.
    return str(getattr(settings, "frontend_temp_file_secret", "change-me-fastapi-file-temp-secret") or "change-me-fastapi-file-temp-secret")


def _sign_temp_file(file_id: int, expires: int) -> str:
    payload = f"{int(file_id)}|{int(expires)}".encode("utf-8")
    return hmac.new(_temp_file_secret().encode("utf-8"), payload, hashlib.sha256).hexdigest()


def _safe_join(root: Path, relative_path: str) -> Path:
    candidate = (root / str(relative_path or "").lstrip("/\\")).resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Archivo no encontrado.") from exc
    return candidate


def _find_file_row_by_uuid(db: Session, uuid: str):
    return db.execute(
        text(
            """
            SELECT id, uuid, disk, path, original_name, mime_type
            FROM files
            WHERE uuid = :uuid
            LIMIT 1
            """
        ),
        {"uuid": str(uuid or "").strip()},
    ).mappings().first()


def _find_file_row_by_id(db: Session, file_id: int):
    return db.execute(
        text(
            """
            SELECT id, uuid, disk, path, original_name, mime_type
            FROM files
            WHERE id = :file_id
            LIMIT 1
            """
        ),
        {"file_id": int(file_id)},
    ).mappings().first()


def _build_file_response(row) -> FileResponse:
    disk_root = _resolve_disk_root(str(row.get("disk") or ""))
    file_path = _safe_join(disk_root, str(row.get("path") or ""))
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="Archivo no encontrado.")

    filename = str(row.get("original_name") or "").strip() or file_path.name
    mime = str(row.get("mime_type") or "").strip() or (mimetypes.guess_type(file_path.name)[0] or "application/octet-stream")
    disposition = "inline" if _is_inline_mime(mime) else "attachment"

    return FileResponse(
        path=file_path,
        media_type=mime,
        filename=filename,
        content_disposition_type=disposition,
    )


@router.get("/{uuid}")
def show_file_by_uuid(uuid: str, db: Session = Depends(get_db)) -> FileResponse:
    row = _find_file_row_by_uuid(db, uuid)

    if not row:
        raise HTTPException(status_code=404, detail="Archivo no encontrado.")

    return _build_file_response(row)


@router.get("/temp/{file_id}")
def show_temporary_file(file_id: int, expires: int, signature: str, db: Session = Depends(get_db)) -> FileResponse:
    now_ts = int(time.time())
    if int(expires) < now_ts:
        raise HTTPException(status_code=403, detail="Link expirado o invalido.")

    expected = _sign_temp_file(file_id=file_id, expires=int(expires))
    if not hmac.compare_digest(expected, str(signature or "")):
        raise HTTPException(status_code=403, detail="Link expirado o invalido.")

    row = _find_file_row_by_id(db, file_id)
    if not row:
        raise HTTPException(status_code=404, detail="Archivo no encontrado.")

    return _build_file_response(row)
