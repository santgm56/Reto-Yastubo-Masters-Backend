from __future__ import annotations

import json
import re
from html import unescape
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.services.auth_service import AuthService


router = APIRouter(prefix="/api/v1/admin/templates", tags=["admin-templates"])

_ALLOWED_TYPES = {"HTML", "PDF"}


def _escape_pdf_text(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _build_simple_pdf(lines: list[str]) -> bytes:
    sanitized = [str(line or "").strip() for line in lines if str(line or "").strip()]
    if not sanitized:
        sanitized = ["Template preview"]

    content_parts = ["BT", "/F1 12 Tf", "50 780 Td", "14 TL"]
    first = _escape_pdf_text(sanitized[0])
    content_parts.append(f"({first}) Tj")
    for line in sanitized[1:]:
        content_parts.append("T*")
        content_parts.append(f"({_escape_pdf_text(line)}) Tj")
    content_parts.append("ET")

    stream = "\n".join(content_parts).encode("latin-1", errors="replace")

    objects = [
        b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
        b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>\nendobj\n",
        b"4 0 obj\n<< /Length " + str(len(stream)).encode("ascii") + b" >>\nstream\n" + stream + b"\nendstream\nendobj\n",
        b"5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n",
    ]

    pdf = b"%PDF-1.4\n"
    offsets = [0]
    for obj in objects:
        offsets.append(len(pdf))
        pdf += obj

    xref_start = len(pdf)
    xref = [f"xref\n0 {len(objects) + 1}\n", "0000000000 65535 f \n"]
    for offset in offsets[1:]:
        xref.append(f"{offset:010d} 00000 n \n")
    pdf += "".join(xref).encode("ascii")
    pdf += (
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_start}\n%%EOF\n"
    ).encode("ascii")
    return pdf


def _parse_json_object(raw: object) -> dict:
    if raw is None:
        return {}
    text_raw = str(raw).strip()
    if not text_raw:
        return {}
    try:
        parsed = json.loads(text_raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _deep_merge(base: dict, override: dict) -> dict:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(merged.get(key), dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _resolve_path(data: dict, dotted_path: str) -> object:
    current: object = data
    for part in dotted_path.split("."):
        if not isinstance(current, dict):
            return ""
        if part not in current:
            return ""
        current = current[part]
    return current


def _render_template_string(content: str, context: dict) -> str:
    source = str(content or "")

    def replace(match: re.Match[str]) -> str:
        key = (match.group(1) or "").strip()
        if not key:
            return ""
        value = _resolve_path(context, key)
        if value is None:
            return ""
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return str(value)

    return re.sub(r"\{\{\s*([^{}]+?)\s*\}\}", replace, source)


def _html_to_lines(html_content: str) -> list[str]:
    text_content = re.sub(r"<\s*br\s*/?\s*>", "\n", str(html_content or ""), flags=re.IGNORECASE)
    text_content = re.sub(r"<\s*/\s*(p|div|h1|h2|h3|h4|h5|h6|li|tr)\s*>", "\n", text_content, flags=re.IGNORECASE)
    text_content = re.sub(r"<[^>]+>", "", text_content)
    text_content = unescape(text_content)
    lines = [line.strip() for line in text_content.splitlines()]
    return [line for line in lines if line]


def _build_preview_pdf(version_row, template_row) -> bytes:
    template_test_data = _parse_json_object((template_row or {}).get("test_data_json"))
    version_test_data = _parse_json_object((version_row or {}).get("test_data_json"))
    merged_data = _deep_merge(template_test_data, version_test_data)

    rendered = _render_template_string(str((version_row or {}).get("content") or ""), merged_data)
    lines = _html_to_lines(rendered)

    metadata = [
        f"Template: {str((template_row or {}).get('name') or '-')}",
        f"Version: {str((version_row or {}).get('name') or '-')}",
        "",
    ]
    return _build_simple_pdf(metadata + lines)


def _extract_bearer_token(authorization: str | None) -> str:
    if not authorization:
        return ""
    raw = authorization.strip()
    if not raw.lower().startswith("bearer "):
        return ""
    return raw[7:].strip()


def _auth_payload(request: Request, authorization: str | None, db: Session) -> dict:
    token = _extract_bearer_token(authorization)
    if not token:
        token = str(request.cookies.get("yastubo_access_token") or "").strip()

    if not token:
        raise HTTPException(status_code=403, detail="Forbidden")

    return AuthService(db).me(token)


def _require_permission(auth_payload: dict, permission: str) -> None:
    permissions = {str(item) for item in (auth_payload.get("permissions") or [])}
    if permission not in permissions:
        raise HTTPException(status_code=403, detail="Forbidden")


def _json_422(message: str, errors: dict[str, list[str]] | None = None) -> JSONResponse:
    payload = {"message": message}
    if errors:
        payload["errors"] = errors
    return JSONResponse(status_code=422, content=payload)


def _success_toast(message: str) -> dict:
    return {"toast": {"type": "success", "message": message}}


def _serialize_template(row) -> dict:
    return {
        "id": int(row.get("id") or 0),
        "name": row.get("name"),
        "slug": row.get("slug"),
        "type": row.get("type"),
        "test_data_json": row.get("test_data_json"),
        "active_template_version_id": row.get("active_template_version_id"),
        "active_version": {
            "id": row.get("active_version_id"),
            "name": row.get("active_version_name"),
        }
        if row.get("active_version_id") is not None
        else None,
    }


def _serialize_version(row) -> dict:
    return {
        "id": int(row.get("id") or 0),
        "template_id": int(row.get("template_id") or 0),
        "name": row.get("name"),
        "content": row.get("content") or "",
        "test_data_json": row.get("test_data_json"),
    }


def _validate_json_text_field(field_name: str, value: object) -> JSONResponse | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        json.loads(raw)
    except json.JSONDecodeError:
        return _json_422("JSON invalido.", {field_name: ["El JSON no es valido."]})
    return None


def _template_exists_with_slug(db: Session, slug: str, exclude_id: int | None = None) -> bool:
    row = db.execute(
        text(
            """
            SELECT id
            FROM templates
            WHERE slug = :slug
            LIMIT 1
            """
        ),
        {"slug": slug},
    ).mappings().first()
    if not row:
        return False
    if exclude_id is None:
        return True
    return int(row.get("id") or 0) != int(exclude_id)


def _make_unique_clone_slug(db: Session, base_slug: str) -> str:
    base = str(base_slug or "").strip() or "template"
    candidate = f"{base}-copia"
    if not _template_exists_with_slug(db, candidate):
        return candidate

    suffix = 2
    while suffix <= 9999:
        candidate = f"{base}-copia-{suffix}"
        if not _template_exists_with_slug(db, candidate):
            return candidate
        suffix += 1

    raise HTTPException(status_code=422, detail={"message": "No se pudo generar slug unico."})


@router.get("")
def index_templates(
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_permission(auth_payload, "admin.templates.edit")

    rows = db.execute(
        text(
            """
            SELECT t.id, t.name, t.slug, t.type, t.test_data_json, t.active_template_version_id,
                   av.id AS active_version_id,
                   av.name AS active_version_name
            FROM templates t
            LEFT JOIN template_versions av ON av.id = t.active_template_version_id
            WHERE t.deleted_at IS NULL
            ORDER BY t.id DESC
            """
        )
    ).mappings().all()

    return {"data": [_serialize_template(row) for row in rows]}


@router.get("/{template_id:int}")
def show_template(
    template_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_permission(auth_payload, "admin.templates.edit")

    template_row = db.execute(
        text(
            """
            SELECT t.id, t.name, t.slug, t.type, t.test_data_json, t.active_template_version_id,
                   av.id AS active_version_id,
                   av.name AS active_version_name
            FROM templates t
            LEFT JOIN template_versions av ON av.id = t.active_template_version_id
            WHERE t.id = :template_id
              AND t.deleted_at IS NULL
            LIMIT 1
            """
        ),
        {"template_id": int(template_id)},
    ).mappings().first()

    if not template_row:
        raise HTTPException(status_code=404, detail="Not Found")

    versions_rows = db.execute(
        text(
            """
            SELECT id, template_id, name, content, test_data_json
            FROM template_versions
            WHERE template_id = :template_id
            ORDER BY id ASC
            """
        ),
        {"template_id": int(template_id)},
    ).mappings().all()

    return {
        "data": {
            "template": _serialize_template(template_row),
            "versions": [_serialize_version(row) for row in versions_rows],
        }
    }


@router.post("")
async def store_template(
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_permission(auth_payload, "admin.templates.edit")

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.")

    name = str(payload.get("name") or "").strip()
    slug = str(payload.get("slug") or "").strip()
    template_type = str(payload.get("type") or "").strip().upper()

    errors: dict[str, list[str]] = {}
    if not name:
        errors.setdefault("name", []).append("El nombre es obligatorio.")
    if not slug:
        errors.setdefault("slug", []).append("El slug es obligatorio.")
    if not template_type or template_type not in _ALLOWED_TYPES:
        errors.setdefault("type", []).append("El tipo debe ser HTML o PDF.")
    if slug and _template_exists_with_slug(db, slug):
        errors.setdefault("slug", []).append("El slug ya esta en uso.")

    if errors:
        return _json_422("The given data was invalid.", errors)

    db.execute(
        text(
            """
            INSERT INTO templates (
                name,
                slug,
                type,
                test_data_json,
                active_template_version_id,
                created_at,
                updated_at
            ) VALUES (
                :name,
                :slug,
                :type,
                NULL,
                NULL,
                NOW(),
                NOW()
            )
            """
        ),
        {
            "name": name,
            "slug": slug,
            "type": template_type,
        },
    )
    row = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    db.commit()

    template_id = int((row or {}).get("id") or 0)
    stored = db.execute(
        text(
            """
            SELECT id, name, slug, type, test_data_json, active_template_version_id,
                   NULL AS active_version_id, NULL AS active_version_name
            FROM templates
            WHERE id = :template_id
            LIMIT 1
            """
        ),
        {"template_id": template_id},
    ).mappings().first()

    return {
        "data": {
            "template": _serialize_template(
                stored or {"id": template_id, "name": name, "slug": slug, "type": template_type}
            )
        },
        **_success_toast("Plantilla creada."),
    }


@router.patch("/{template_id:int}/basic")
async def update_template_basic(
    template_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_permission(auth_payload, "admin.templates.edit")

    exists = db.execute(
        text("SELECT id FROM templates WHERE id = :template_id AND deleted_at IS NULL LIMIT 1"),
        {"template_id": int(template_id)},
    ).mappings().first()
    if not exists:
        raise HTTPException(status_code=404, detail="Not Found")

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.")

    name = str(payload.get("name") or "").strip()
    slug = str(payload.get("slug") or "").strip()

    errors: dict[str, list[str]] = {}
    if not name:
        errors.setdefault("name", []).append("El nombre es obligatorio.")
    if not slug:
        errors.setdefault("slug", []).append("El slug es obligatorio.")
    if slug and _template_exists_with_slug(db, slug, exclude_id=int(template_id)):
        errors.setdefault("slug", []).append("El slug ya esta en uso.")

    if errors:
        return _json_422("The given data was invalid.", errors)

    db.execute(
        text(
            """
            UPDATE templates
            SET name = :name,
                slug = :slug,
                updated_at = NOW()
            WHERE id = :template_id
            """
        ),
        {
            "template_id": int(template_id),
            "name": name,
            "slug": slug,
        },
    )
    db.commit()

    return _success_toast("Plantilla actualizada.")


@router.patch("/{template_id:int}/test-data")
async def update_template_test_data(
    template_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_permission(auth_payload, "admin.templates.edit")

    exists = db.execute(
        text("SELECT id FROM templates WHERE id = :template_id AND deleted_at IS NULL LIMIT 1"),
        {"template_id": int(template_id)},
    ).mappings().first()
    if not exists:
        raise HTTPException(status_code=404, detail="Not Found")

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.")

    test_data_raw = str(payload.get("test_data_json") or "").strip()
    json_error = _validate_json_text_field("test_data_json", test_data_raw)
    if json_error is not None:
        return json_error

    db.execute(
        text(
            """
            UPDATE templates
            SET test_data_json = :test_data_json,
                updated_at = NOW()
            WHERE id = :template_id
            """
        ),
        {
            "template_id": int(template_id),
            "test_data_json": test_data_raw or None,
        },
    )
    db.commit()

    return _success_toast("JSON guardado.")


@router.delete("/{template_id:int}")
def destroy_template(
    template_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_permission(auth_payload, "admin.templates.edit")

    exists = db.execute(
        text("SELECT id FROM templates WHERE id = :template_id AND deleted_at IS NULL LIMIT 1"),
        {"template_id": int(template_id)},
    ).mappings().first()
    if not exists:
        raise HTTPException(status_code=404, detail="Not Found")

    db.execute(
        text(
            """
            UPDATE templates
            SET deleted_at = NOW(),
                updated_at = NOW()
            WHERE id = :template_id
            """
        ),
        {"template_id": int(template_id)},
    )
    db.commit()

    return _success_toast("Plantilla eliminada.")


@router.post("/{template_id:int}/clone")
def clone_template(
    template_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_permission(auth_payload, "admin.templates.edit")

    source = db.execute(
        text(
            """
            SELECT id, name, slug, type, test_data_json, active_template_version_id
            FROM templates
            WHERE id = :template_id
              AND deleted_at IS NULL
            LIMIT 1
            """
        ),
        {"template_id": int(template_id)},
    ).mappings().first()
    if not source:
        raise HTTPException(status_code=404, detail="Not Found")

    source_id = int(source.get("id") or 0)
    source_slug = str(source.get("slug") or "")
    source_name = str(source.get("name") or "Plantilla")

    clone_slug = _make_unique_clone_slug(db, source_slug)

    db.execute(
        text(
            """
            INSERT INTO templates (
                name,
                slug,
                type,
                test_data_json,
                active_template_version_id,
                created_at,
                updated_at
            ) VALUES (
                :name,
                :slug,
                :type,
                :test_data_json,
                NULL,
                NOW(),
                NOW()
            )
            """
        ),
        {
            "name": source_name,
            "slug": clone_slug,
            "type": source.get("type"),
            "test_data_json": source.get("test_data_json"),
        },
    )
    clone_id_row = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    clone_template_id = int((clone_id_row or {}).get("id") or 0)

    db.execute(
        text(
            """
            UPDATE templates
            SET name = :name,
                updated_at = NOW()
            WHERE id = :template_id
            """
        ),
        {
            "template_id": clone_template_id,
            "name": f"{source_name} (Copia) #{clone_template_id}",
        },
    )

    active_version_id = source.get("active_template_version_id")
    if active_version_id:
        active = db.execute(
            text(
                """
                SELECT id, name, content, test_data_json
                FROM template_versions
                WHERE id = :version_id
                  AND template_id = :template_id
                LIMIT 1
                """
            ),
            {
                "version_id": int(active_version_id),
                "template_id": source_id,
            },
        ).mappings().first()

        if active:
            db.execute(
                text(
                    """
                    INSERT INTO template_versions (
                        template_id,
                        name,
                        content,
                        test_data_json,
                        created_at,
                        updated_at
                    ) VALUES (
                        :template_id,
                        :name,
                        :content,
                        :test_data_json,
                        NOW(),
                        NOW()
                    )
                    """
                ),
                {
                    "template_id": clone_template_id,
                    "name": str(active.get("name") or ""),
                    "content": str(active.get("content") or ""),
                    "test_data_json": active.get("test_data_json"),
                },
            )
            clone_version_id_row = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
            clone_version_id = int((clone_version_id_row or {}).get("id") or 0)

            db.execute(
                text(
                    """
                    UPDATE template_versions
                    SET name = :name,
                        updated_at = NOW()
                    WHERE id = :version_id
                    """
                ),
                {
                    "version_id": clone_version_id,
                    "name": f"Version #{clone_version_id}",
                },
            )

            db.execute(
                text(
                    """
                    UPDATE templates
                    SET active_template_version_id = :active_template_version_id,
                        updated_at = NOW()
                    WHERE id = :template_id
                    """
                ),
                {
                    "template_id": clone_template_id,
                    "active_template_version_id": clone_version_id,
                },
            )

    db.commit()

    stored = db.execute(
        text(
            """
            SELECT t.id, t.name, t.slug, t.type, t.test_data_json, t.active_template_version_id,
                   av.id AS active_version_id,
                   av.name AS active_version_name
            FROM templates t
            LEFT JOIN template_versions av ON av.id = t.active_template_version_id
            WHERE t.id = :template_id
            LIMIT 1
            """
        ),
        {"template_id": clone_template_id},
    ).mappings().first()

    return {
        "data": {"template": _serialize_template(stored or {"id": clone_template_id})},
        **_success_toast("Plantilla clonada."),
    }


@router.post("/{template_id:int}/versions")
def store_version(
    template_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_permission(auth_payload, "admin.templates.edit")

    template_row = db.execute(
        text("SELECT id FROM templates WHERE id = :template_id AND deleted_at IS NULL LIMIT 1"),
        {"template_id": int(template_id)},
    ).mappings().first()
    if not template_row:
        raise HTTPException(status_code=404, detail="Not Found")

    db.execute(
        text(
            """
            INSERT INTO template_versions (template_id, name, content, test_data_json, created_at, updated_at)
            VALUES (:template_id, '', '', NULL, NOW(), NOW())
            """
        ),
        {"template_id": int(template_id)},
    )
    row = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    version_id = int((row or {}).get("id") or 0)

    db.execute(
        text(
            """
            UPDATE template_versions
            SET name = :name,
                updated_at = NOW()
            WHERE id = :version_id
            """
        ),
        {
            "version_id": version_id,
            "name": f"Version #{version_id}",
        },
    )
    db.commit()

    version = db.execute(
        text(
            """
            SELECT id, template_id, name, content, test_data_json
            FROM template_versions
            WHERE id = :version_id
            LIMIT 1
            """
        ),
        {"version_id": version_id},
    ).mappings().first()

    return {
        "data": {
            "version": _serialize_version(
                version
                or {
                    "id": version_id,
                    "template_id": int(template_id),
                    "name": f"Version #{version_id}",
                    "content": "",
                    "test_data_json": None,
                }
            )
        },
        **_success_toast("Version creada."),
    }


def _fetch_version(db: Session, template_id: int, version_id: int):
    return db.execute(
        text(
            """
            SELECT id, template_id, name, content, test_data_json
            FROM template_versions
            WHERE id = :version_id
              AND template_id = :template_id
            LIMIT 1
            """
        ),
        {
            "template_id": int(template_id),
            "version_id": int(version_id),
        },
    ).mappings().first()


@router.get("/{template_id:int}/versions/{version_id:int}")
def show_version(
    template_id: int,
    version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_permission(auth_payload, "admin.templates.edit")

    version = _fetch_version(db=db, template_id=template_id, version_id=version_id)
    if not version:
        raise HTTPException(status_code=404, detail="Not Found")

    return {"data": {"version": _serialize_version(version)}}


@router.patch("/{template_id:int}/versions/{version_id:int}/basic")
async def update_version_basic(
    template_id: int,
    version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_permission(auth_payload, "admin.templates.edit")

    version = _fetch_version(db=db, template_id=template_id, version_id=version_id)
    if not version:
        raise HTTPException(status_code=404, detail="Not Found")

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.")

    name = str(payload.get("name") or "").strip()
    content = str(payload.get("content") or "")

    errors: dict[str, list[str]] = {}
    if not name:
        errors.setdefault("name", []).append("El nombre es obligatorio.")
    if content == "":
        errors.setdefault("content", []).append("El contenido es obligatorio.")

    if errors:
        return _json_422("The given data was invalid.", errors)

    db.execute(
        text(
            """
            UPDATE template_versions
            SET name = :name,
                content = :content,
                updated_at = NOW()
            WHERE id = :version_id
            """
        ),
        {
            "version_id": int(version_id),
            "name": name,
            "content": content,
        },
    )
    db.commit()

    return _success_toast("Version actualizada.")


@router.patch("/{template_id:int}/versions/{version_id:int}/test-data")
async def update_version_test_data(
    template_id: int,
    version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_permission(auth_payload, "admin.templates.edit")

    version = _fetch_version(db=db, template_id=template_id, version_id=version_id)
    if not version:
        raise HTTPException(status_code=404, detail="Not Found")

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.")

    test_data_raw = str(payload.get("test_data_json") or "").strip()
    json_error = _validate_json_text_field("test_data_json", test_data_raw)
    if json_error is not None:
        return json_error

    db.execute(
        text(
            """
            UPDATE template_versions
            SET test_data_json = :test_data_json,
                updated_at = NOW()
            WHERE id = :version_id
            """
        ),
        {
            "version_id": int(version_id),
            "test_data_json": test_data_raw or None,
        },
    )
    db.commit()

    return _success_toast("JSON guardado.")


@router.post("/{template_id:int}/versions/{version_id:int}/activate")
def activate_version(
    template_id: int,
    version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_permission(auth_payload, "admin.templates.edit")

    version = _fetch_version(db=db, template_id=template_id, version_id=version_id)
    if not version:
        raise HTTPException(status_code=404, detail="Not Found")

    db.execute(
        text(
            """
            UPDATE templates
            SET active_template_version_id = :version_id,
                updated_at = NOW()
            WHERE id = :template_id
              AND deleted_at IS NULL
            """
        ),
        {
            "template_id": int(template_id),
            "version_id": int(version_id),
        },
    )
    db.commit()

    return _success_toast("Version activada.")


@router.post("/{template_id:int}/versions/{version_id:int}/deactivate")
def deactivate_version(
    template_id: int,
    version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_permission(auth_payload, "admin.templates.edit")

    template_row = db.execute(
        text(
            """
            SELECT id, active_template_version_id
            FROM templates
            WHERE id = :template_id
              AND deleted_at IS NULL
            LIMIT 1
            """
        ),
        {"template_id": int(template_id)},
    ).mappings().first()
    if not template_row:
        raise HTTPException(status_code=404, detail="Not Found")

    active_id = int(template_row.get("active_template_version_id") or 0)
    if active_id != int(version_id):
        return _json_422("La version no es la activa.")

    db.execute(
        text(
            """
            UPDATE templates
            SET active_template_version_id = NULL,
                updated_at = NOW()
            WHERE id = :template_id
            """
        ),
        {"template_id": int(template_id)},
    )
    db.commit()

    return _success_toast("Version desactivada.")


@router.post("/{template_id:int}/versions/{version_id:int}/clone")
def clone_version(
    template_id: int,
    version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_permission(auth_payload, "admin.templates.edit")

    source = _fetch_version(db=db, template_id=template_id, version_id=version_id)
    if not source:
        raise HTTPException(status_code=404, detail="Not Found")

    db.execute(
        text(
            """
            INSERT INTO template_versions (template_id, name, content, test_data_json, created_at, updated_at)
            VALUES (:template_id, '', :content, :test_data_json, NOW(), NOW())
            """
        ),
        {
            "template_id": int(template_id),
            "content": str(source.get("content") or ""),
            "test_data_json": source.get("test_data_json"),
        },
    )
    row = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    clone_id = int((row or {}).get("id") or 0)

    db.execute(
        text(
            """
            UPDATE template_versions
            SET name = :name,
                updated_at = NOW()
            WHERE id = :version_id
            """
        ),
        {
            "version_id": clone_id,
            "name": f"Version #{clone_id}",
        },
    )
    db.commit()

    clone = _fetch_version(db=db, template_id=template_id, version_id=clone_id)

    return {
        "data": {
            "version": _serialize_version(
                clone
                or {
                    "id": clone_id,
                    "template_id": int(template_id),
                    "name": f"Version #{clone_id}",
                    "content": str(source.get("content") or ""),
                    "test_data_json": source.get("test_data_json"),
                }
            )
        },
        **_success_toast("Version clonada."),
    }


@router.delete("/{template_id:int}/versions/{version_id:int}")
def destroy_version(
    template_id: int,
    version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_permission(auth_payload, "admin.templates.edit")

    version = _fetch_version(db=db, template_id=template_id, version_id=version_id)
    if not version:
        raise HTTPException(status_code=404, detail="Not Found")

    template_row = db.execute(
        text(
            """
            SELECT active_template_version_id
            FROM templates
            WHERE id = :template_id
            LIMIT 1
            """
        ),
        {"template_id": int(template_id)},
    ).mappings().first()

    if int((template_row or {}).get("active_template_version_id") or 0) == int(version_id):
        return _json_422("No se puede eliminar una version activa.")

    db.execute(
        text("DELETE FROM template_versions WHERE id = :version_id"),
        {"version_id": int(version_id)},
    )
    db.commit()

    return _success_toast("Version eliminada.")


@router.get("/{template_id:int}/versions/{version_id:int}/preview/raw")
def preview_version_raw(
    template_id: int,
    version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_permission(auth_payload, "admin.templates.edit")

    version = _fetch_version(db=db, template_id=template_id, version_id=version_id)
    if not version:
        raise HTTPException(status_code=404, detail="Not Found")

    # El editor preview consume texto plano/HTML, no JSON.
    return Response(content=str(version.get("content") or ""), media_type="text/html")


@router.get("/{template_id:int}/versions/{version_id:int}/preview/pdf")
def preview_version_pdf(
    template_id: int,
    version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_permission(auth_payload, "admin.templates.edit")

    template_row = db.execute(
        text(
            """
            SELECT id, name, test_data_json
            FROM templates
            WHERE id = :template_id
              AND deleted_at IS NULL
            LIMIT 1
            """
        ),
        {"template_id": int(template_id)},
    ).mappings().first()
    if not template_row:
        raise HTTPException(status_code=404, detail="Not Found")

    version_row = _fetch_version(db=db, template_id=template_id, version_id=version_id)
    if not version_row:
        raise HTTPException(status_code=404, detail="Not Found")

    pdf_bytes = _build_preview_pdf(version_row=version_row, template_row=template_row)
    filename = f"template_{int(template_id)}_version_{int(version_id)}_preview.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{filename}"'},
    )


@router.get("/{template_id:int}/active/preview/pdf")
def preview_active_pdf(
    template_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_permission(auth_payload, "admin.templates.edit")

    template_row = db.execute(
        text(
            """
            SELECT id, name, test_data_json, active_template_version_id
            FROM templates
            WHERE id = :template_id
              AND deleted_at IS NULL
            LIMIT 1
            """
        ),
        {"template_id": int(template_id)},
    ).mappings().first()
    if not template_row:
        raise HTTPException(status_code=404, detail="Not Found")

    active_version_id = int((template_row or {}).get("active_template_version_id") or 0)
    if active_version_id <= 0:
        raise HTTPException(status_code=404, detail="Not Found")

    version_row = _fetch_version(db=db, template_id=template_id, version_id=active_version_id)
    if not version_row:
        raise HTTPException(status_code=404, detail="Not Found")

    pdf_bytes = _build_preview_pdf(version_row=version_row, template_row=template_row)
    filename = f"template_{int(template_id)}_active_preview.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{filename}"'},
    )
