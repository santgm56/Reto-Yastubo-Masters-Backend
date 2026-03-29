from __future__ import annotations

from pathlib import Path
from uuid import uuid4
import json
import mimetypes

from fastapi import APIRouter, Depends, File, Header, HTTPException, Request, UploadFile
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.database import get_db
from app.services.auth_service import AuthService

router = APIRouter(prefix="/api/v1/admin/config", tags=["admin-config"])

_CATEGORIES = {
    "branding": "Branding PDF",
    "branding_web": "Branding WEB",
    "planes": "Default de Planes",
    "comisiones": "Default de Comisiones",
    "email": "Email",
    "contacto": "Contacto",
}

_ALLOWED_TYPES = {
    "integer",
    "decimal",
    "boolean",
    "date",
    "input_text_plain",
    "textarea_plain",
    "html_plain",
    "input_text_translated",
    "textarea_translated",
    "html_translated",
    "email",
    "url",
    "phone",
    "color",
    "json",
    "model_reference",
    "enum",
    "file_plain",
    "file_translated",
}

_PLAIN_TEXT_TYPES = {
    "input_text_plain",
    "textarea_plain",
    "html_plain",
    "email",
    "url",
    "phone",
    "color",
    "json",
    "model_reference",
    "enum",
}

_TRANSLATED_TYPES = {
    "input_text_translated",
    "textarea_translated",
    "html_translated",
}


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


def _has_permission(auth_payload: dict, permission: str) -> bool:
    permissions = [str(item) for item in (auth_payload.get("permissions") or [])]
    return permission in permissions


def _require_permission(auth_payload: dict, permission: str) -> None:
    if not _has_permission(auth_payload, permission):
        raise HTTPException(status_code=403, detail="Forbidden")


def _validation_error(errors: dict[str, list[str]]) -> None:
    raise HTTPException(
        status_code=422,
        detail={
            "code": "API_VALIDATION_ERROR",
            "message": "The given data was invalid.",
            "errors": errors,
        },
    )


def _parse_json_object(raw_value, *, fallback: dict | None = None) -> dict:
    if isinstance(raw_value, dict):
        return raw_value
    if isinstance(raw_value, str):
        try:
            parsed = json.loads(raw_value)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return fallback or {}
    return fallback or {}


def _parse_json_value(raw_value):
    if raw_value is None:
        return None
    if isinstance(raw_value, (dict, list, int, float, bool)):
        return raw_value
    if isinstance(raw_value, str):
        value = raw_value.strip()
        if not value:
            return None
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return None


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


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
    raise HTTPException(status_code=422, detail={"message": "Disk de archivo no soportado.", "errors": {}})


def _serialize_file(row) -> dict | None:
    if not row:
        return None

    file_uuid = str(row.get("uuid") or "").strip()
    if not file_uuid:
        return None

    return {
        "id": int(row.get("id") or 0),
        "original_name": row.get("original_name"),
        "url": f"/api/v1/files/{file_uuid}",
        "temporary_url": f"/api/v1/files/{file_uuid}",
    }


def _fetch_file(db: Session, file_id: int | None):
    if not file_id:
        return None

    return db.execute(
        text(
            """
            SELECT id, uuid, original_name
            FROM files
            WHERE id = :file_id
            LIMIT 1
            """
        ),
        {"file_id": int(file_id)},
    ).mappings().first()


def _display_value(item: dict) -> object:
    item_type = str(item.get("type") or "")

    if item_type in {"integer", "decimal", "boolean"}:
        if item_type == "integer":
            return item.get("value_int")
        if item_type == "decimal":
            return item.get("value_decimal")
        value_int = item.get("value_int")
        return None if value_int is None else bool(value_int)

    if item_type == "date":
        return item.get("value_date")

    if item_type == "enum":
        value_text = item.get("value_text")
        config = _parse_json_object(item.get("config"), fallback={})
        options = config.get("options") if isinstance(config.get("options"), list) else []
        for option in options:
            if not isinstance(option, dict):
                continue
            if option.get("key") == value_text:
                return option.get("label") or value_text
        return value_text

    if item_type in _PLAIN_TEXT_TYPES:
        return item.get("value_text")

    if item_type in _TRANSLATED_TYPES:
        value_trans = _parse_json_object(item.get("value_trans"), fallback={})
        return value_trans.get("es") or value_trans.get("en")

    if item_type in {"file_plain", "file_translated"}:
        return "[archivo]"

    return None


def _serialize_item(row, db: Session) -> dict:
    value_trans = _parse_json_object(row.get("value_trans"), fallback={})
    config = _parse_json_object(row.get("config"), fallback={})

    file_plain = _serialize_file(_fetch_file(db, row.get("value_file_plain_id")))
    file_es = _serialize_file(_fetch_file(db, row.get("value_file_es_id")))
    file_en = _serialize_file(_fetch_file(db, row.get("value_file_en_id")))

    data = {
        "id": int(row["id"]),
        "category": str(row.get("category") or ""),
        "category_label": _CATEGORIES.get(str(row.get("category") or ""), str(row.get("category") or "")),
        "token": str(row.get("token") or ""),
        "name": str(row.get("name") or ""),
        "type": str(row.get("type") or ""),
        "config": config,
        "value_int": row.get("value_int"),
        "value_decimal": float(row.get("value_decimal")) if row.get("value_decimal") is not None else None,
        "value_text": row.get("value_text"),
        "value_translations": {
            "es": value_trans.get("es") if isinstance(value_trans, dict) else None,
            "en": value_trans.get("en") if isinstance(value_trans, dict) else None,
        },
        "value_date": str(row.get("value_date")) if row.get("value_date") is not None else None,
        "value_file_plain_id": row.get("value_file_plain_id"),
        "value_file_es_id": row.get("value_file_es_id"),
        "value_file_en_id": row.get("value_file_en_id"),
        "file_plain": file_plain,
        "file_es": file_es,
        "file_en": file_en,
    }

    display = _display_value(row)
    if isinstance(display, str) and len(display) > 80:
        display = f"{display[:77]}..."

    data["display_value"] = display
    return data


def _fetch_item(db: Session, item_id: int):
    return db.execute(
        text(
            """
            SELECT id, category, token, name, type, config,
                   value_int, value_decimal, value_text, value_trans,
                   value_file_plain_id, value_file_es_id, value_file_en_id,
                   value_date
            FROM config_items
            WHERE id = :item_id
            LIMIT 1
            """
        ),
        {"item_id": int(item_id)},
    ).mappings().first()


def _validate_definition_payload(payload: dict, *, item_id: int | None, db: Session) -> dict:
    category = _normalize_text(payload.get("category"))
    name = _normalize_text(payload.get("name"))
    token = _normalize_text(payload.get("token"))
    item_type = _normalize_text(payload.get("type"))
    config = payload.get("config")

    errors: dict[str, list[str]] = {}

    if category not in _CATEGORIES:
        errors.setdefault("category", []).append("Categoria invalida.")
    if not name:
        errors.setdefault("name", []).append("El nombre es obligatorio.")
    elif len(name) > 191:
        errors.setdefault("name", []).append("No puede superar 191 caracteres.")

    if not token:
        errors.setdefault("token", []).append("El token es obligatorio.")
    elif len(token) > 191:
        errors.setdefault("token", []).append("No puede superar 191 caracteres.")

    if item_type not in _ALLOWED_TYPES:
        errors.setdefault("type", []).append("Tipo de configuracion invalido.")

    if config is None:
        config = {}
    if not isinstance(config, dict):
        errors.setdefault("config", []).append("El campo config debe ser un objeto.")

    if not errors and category and token:
        duplicate = db.execute(
            text(
                """
                SELECT id
                FROM config_items
                WHERE category = :category AND token = :token
                LIMIT 1
                """
            ),
            {"category": category, "token": token},
        ).mappings().first()
        if duplicate and int(duplicate.get("id") or 0) != int(item_id or 0):
            errors.setdefault("token", []).append("Ya existe un item con esta categoria y token.")

    if errors:
        _validation_error(errors)

    return {
        "category": category,
        "name": name,
        "token": token,
        "type": item_type,
        "config": config,
    }


def _file_constraints(config: dict) -> dict:
    raw_exts = config.get("file_allowed_extensions") if isinstance(config, dict) else None
    exts: list[str] = []

    if isinstance(raw_exts, str):
        exts = [part.strip().lower().lstrip(".") for part in raw_exts.split(",") if part.strip()]
    elif isinstance(raw_exts, list):
        exts = [str(part or "").strip().lower().lstrip(".") for part in raw_exts if str(part or "").strip()]

    raw_max_kb = config.get("file_max_size_kb") if isinstance(config, dict) else None
    max_size_kb = None
    if isinstance(raw_max_kb, (int, float, str)):
        try:
            parsed = int(float(raw_max_kb))
            if parsed > 0:
                max_size_kb = parsed
        except ValueError:
            max_size_kb = None

    return {
        "extensions": exts,
        "max_size_kb": max_size_kb,
    }


def _permissions_map(auth_payload: dict) -> dict:
    return {
        "create": _has_permission(auth_payload, "admin.config.create"),
        "read": _has_permission(auth_payload, "admin.config.read"),
        "fill": _has_permission(auth_payload, "admin.config.fill"),
        "edit": _has_permission(auth_payload, "admin.config.edit"),
        "delete": _has_permission(auth_payload, "admin.config.delete"),
    }


@router.get("")
def index_config(
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request=request, authorization=authorization, db=db)
    _require_permission(auth_payload, "admin.config.read")

    rows = db.execute(
        text(
            """
            SELECT id, category, token, name, type, config,
                   value_int, value_decimal, value_text, value_trans,
                   value_file_plain_id, value_file_es_id, value_file_en_id,
                   value_date
            FROM config_items
            ORDER BY category, name
            """
        )
    ).mappings().all()

    return {
        "categories": _CATEGORIES,
        "permissions": _permissions_map(auth_payload),
        "items": [_serialize_item(row, db=db) for row in rows],
    }


@router.get("/{item_id:int}")
def show_config_item(
    item_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request=request, authorization=authorization, db=db)
    _require_permission(auth_payload, "admin.config.read")

    row = _fetch_item(db=db, item_id=int(item_id))
    if not row:
        raise HTTPException(status_code=404, detail="Not Found")

    return {"item": _serialize_item(row, db=db)}


@router.post("/items")
async def store_config_item(
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request=request, authorization=authorization, db=db)
    _require_permission(auth_payload, "admin.config.create")

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload invalido."]})

    validated = _validate_definition_payload(payload, item_id=None, db=db)

    db.execute(
        text(
            """
            INSERT INTO config_items (
                category,
                token,
                name,
                type,
                config,
                created_at,
                updated_at
            ) VALUES (
                :category,
                :token,
                :name,
                :type,
                :config,
                NOW(),
                NOW()
            )
            """
        ),
        {
            "category": validated["category"],
            "token": validated["token"],
            "name": validated["name"],
            "type": validated["type"],
            "config": json.dumps(validated["config"], ensure_ascii=False),
        },
    )

    inserted = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    db.commit()

    row = _fetch_item(db=db, item_id=int(inserted["id"])) if inserted else None
    if not row:
        raise HTTPException(status_code=500, detail="No se pudo recuperar el item creado.")

    return {
        "message": "Variable de configuración creada correctamente.",
        "item": _serialize_item(row, db=db),
    }


@router.put("/{item_id:int}/definition")
async def update_config_definition(
    item_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request=request, authorization=authorization, db=db)
    _require_permission(auth_payload, "admin.config.edit")

    existing = _fetch_item(db=db, item_id=int(item_id))
    if not existing:
        raise HTTPException(status_code=404, detail="Not Found")

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload invalido."]})

    validated = _validate_definition_payload(payload, item_id=int(item_id), db=db)

    db.execute(
        text(
            """
            UPDATE config_items
            SET category = :category,
                token = :token,
                name = :name,
                type = :type,
                config = :config,
                updated_at = NOW()
            WHERE id = :item_id
            """
        ),
        {
            "item_id": int(item_id),
            "category": validated["category"],
            "token": validated["token"],
            "name": validated["name"],
            "type": validated["type"],
            "config": json.dumps(validated["config"], ensure_ascii=False),
        },
    )
    db.commit()

    row = _fetch_item(db=db, item_id=int(item_id))
    if not row:
        raise HTTPException(status_code=404, detail="Not Found")

    return {
        "message": "Definición actualizada correctamente.",
        "item": _serialize_item(row, db=db),
    }


@router.put("/{item_id:int}/value")
async def update_config_value(
    item_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request=request, authorization=authorization, db=db)
    _require_permission(auth_payload, "admin.config.fill")

    row = _fetch_item(db=db, item_id=int(item_id))
    if not row:
        raise HTTPException(status_code=404, detail="Not Found")

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload invalido."]})

    item_type = str(row.get("type") or "")

    if item_type == "integer":
        if payload.get("value") is None:
            value = None
        else:
            try:
                value = int(payload.get("value"))
            except (TypeError, ValueError):
                _validation_error({"value": ["Debe ser un entero."]})

        db.execute(
            text(
                """
                UPDATE config_items
                SET value_int = :value,
                    updated_at = NOW()
                WHERE id = :item_id
                """
            ),
            {"item_id": int(item_id), "value": value},
        )

    elif item_type == "boolean":
        if payload.get("value") is None:
            value = None
        else:
            value = 1 if bool(payload.get("value")) else 0

        db.execute(
            text(
                """
                UPDATE config_items
                SET value_int = :value,
                    updated_at = NOW()
                WHERE id = :item_id
                """
            ),
            {"item_id": int(item_id), "value": value},
        )

    elif item_type == "decimal":
        if payload.get("value") is None:
            value = None
        else:
            try:
                value = float(payload.get("value"))
            except (TypeError, ValueError):
                _validation_error({"value": ["Debe ser numerico."]})

        db.execute(
            text(
                """
                UPDATE config_items
                SET value_decimal = :value,
                    updated_at = NOW()
                WHERE id = :item_id
                """
            ),
            {"item_id": int(item_id), "value": value},
        )

    elif item_type in _PLAIN_TEXT_TYPES:
        value = payload.get("value")
        if value is not None and not isinstance(value, str):
            _validation_error({"value": ["Debe ser texto."]})

        db.execute(
            text(
                """
                UPDATE config_items
                SET value_text = :value,
                    updated_at = NOW()
                WHERE id = :item_id
                """
            ),
            {"item_id": int(item_id), "value": value},
        )

    elif item_type in _TRANSLATED_TYPES:
        translations = payload.get("translations") or {}
        if not isinstance(translations, dict):
            _validation_error({"translations": ["Debe ser un objeto con llaves es/en."]})

        normalized = {
            "es": _normalize_text(translations.get("es")),
            "en": _normalize_text(translations.get("en")),
        }

        db.execute(
            text(
                """
                UPDATE config_items
                SET value_trans = :value,
                    updated_at = NOW()
                WHERE id = :item_id
                """
            ),
            {
                "item_id": int(item_id),
                "value": json.dumps(normalized, ensure_ascii=False),
            },
        )

    elif item_type == "date":
        value = payload.get("value")
        if value is not None and not isinstance(value, str):
            _validation_error({"value": ["Debe ser fecha valida."]})

        db.execute(
            text(
                """
                UPDATE config_items
                SET value_date = :value,
                    updated_at = NOW()
                WHERE id = :item_id
                """
            ),
            {"item_id": int(item_id), "value": value},
        )

    elif item_type == "file_plain":
        if "value_file_plain_id" not in payload or payload.get("value_file_plain_id") is not None:
            _validation_error({"value_file_plain_id": ["Para archivos, solo se admite null para limpiar."]})

        db.execute(
            text(
                """
                UPDATE config_items
                SET value_file_plain_id = NULL,
                    updated_at = NOW()
                WHERE id = :item_id
                """
            ),
            {"item_id": int(item_id)},
        )

    elif item_type == "file_translated":
        has_es = "value_file_es_id" in payload
        has_en = "value_file_en_id" in payload
        if not has_es and not has_en:
            _validation_error({"value_file_es_id": ["Debe enviar value_file_es_id o value_file_en_id en null."], "value_file_en_id": ["Debe enviar value_file_es_id o value_file_en_id en null."]})

        if has_es and payload.get("value_file_es_id") is not None:
            _validation_error({"value_file_es_id": ["Solo se admite null para limpiar."]})
        if has_en and payload.get("value_file_en_id") is not None:
            _validation_error({"value_file_en_id": ["Solo se admite null para limpiar."]})

        updates = []
        if has_es:
            updates.append("value_file_es_id = NULL")
        if has_en:
            updates.append("value_file_en_id = NULL")

        db.execute(
            text(
                f"""
                UPDATE config_items
                SET {', '.join(updates)},
                    updated_at = NOW()
                WHERE id = :item_id
                """
            ),
            {"item_id": int(item_id)},
        )

    else:
        _validation_error({"type": ["Tipo de configuración no soportado para actualizar valor."]})

    db.commit()

    updated = _fetch_item(db=db, item_id=int(item_id))
    if not updated:
        raise HTTPException(status_code=404, detail="Not Found")

    return {
        "message": "Valor actualizado correctamente.",
        "item": _serialize_item(updated, db=db),
    }


@router.post("/{item_id:int}/file")
async def upload_config_file(
    item_id: int,
    request: Request,
    file: UploadFile = File(...),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request=request, authorization=authorization, db=db)
    _require_permission(auth_payload, "admin.config.fill")

    row = _fetch_item(db=db, item_id=int(item_id))
    if not row:
        raise HTTPException(status_code=404, detail="Not Found")

    item_type = str(row.get("type") or "")
    if item_type not in {"file_plain", "file_translated"}:
        _validation_error({"file": ["Este item no admite archivos."]})

    form = await request.form()
    locale = _normalize_text(form.get("locale")).lower()

    config = _parse_json_object(row.get("config"), fallback={})
    constraints = _file_constraints(config)

    original_name = str(file.filename or "archivo.bin").strip() or "archivo.bin"
    suffix = Path(original_name).suffix.lower()
    extension = suffix.lstrip(".")

    allowed_exts = constraints["extensions"]
    if allowed_exts and extension not in allowed_exts:
        _validation_error({"file": [f"Extensión no permitida. Permitidas: {', '.join(allowed_exts)}."]})

    content = await file.read()
    size_bytes = len(content)
    max_size_kb = constraints["max_size_kb"]
    if max_size_kb and size_bytes > int(max_size_kb) * 1024:
        _validation_error({"file": [f"El archivo excede el máximo de {max_size_kb} KB."]})

    if item_type == "file_plain":
        field = "value_file_plain_id"
    else:
        effective_locale = locale or "es"
        if effective_locale not in {"es", "en"}:
            _validation_error({"locale": ["Locale invalido. Solo es/en."]})
        field = "value_file_es_id" if effective_locale == "es" else "value_file_en_id"

    file_uuid = str(uuid4())
    filename = f"{file_uuid}{suffix}" if suffix else file_uuid
    relative_path = f"ConfigItem/{int(item_id)}/{field}/{filename}"

    disk = "public"
    disk_root = _resolve_disk_root(disk)
    absolute_path = (disk_root / relative_path).resolve()
    absolute_path.parent.mkdir(parents=True, exist_ok=True)
    absolute_path.write_bytes(content)

    mime_type = str(file.content_type or "").strip() or (mimetypes.guess_type(original_name)[0] or "application/octet-stream")
    uploaded_by = int(auth_payload.get("id") or 0) or None

    meta = {
        "context": "config_item",
        "model": "App\\Models\\ConfigItem",
        "model_id": int(item_id),
        "field": field,
        "lang": locale if item_type == "file_translated" else None,
    }

    db.execute(
        text(
            """
            INSERT INTO files (uuid, disk, path, original_name, mime_type, size, uploaded_by, meta, created_at, updated_at)
            VALUES (:uuid, :disk, :path, :original_name, :mime_type, :size, :uploaded_by, :meta, NOW(), NOW())
            """
        ),
        {
            "uuid": file_uuid,
            "disk": disk,
            "path": relative_path,
            "original_name": original_name,
            "mime_type": mime_type,
            "size": size_bytes,
            "uploaded_by": uploaded_by,
            "meta": json.dumps(meta, ensure_ascii=False),
        },
    )

    inserted = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    new_file_id = int(inserted.get("id") or 0) if inserted else 0

    db.execute(
        text(
            f"""
            UPDATE config_items
            SET {field} = :file_id,
                updated_at = NOW()
            WHERE id = :item_id
            """
        ),
        {
            "item_id": int(item_id),
            "file_id": new_file_id,
        },
    )

    db.commit()

    updated = _fetch_item(db=db, item_id=int(item_id))
    if not updated:
        raise HTTPException(status_code=404, detail="Not Found")

    return {
        "message": "Archivo subido correctamente.",
        "item": _serialize_item(updated, db=db),
    }


@router.delete("/{item_id:int}")
def destroy_config_item(
    item_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request=request, authorization=authorization, db=db)
    _require_permission(auth_payload, "admin.config.delete")

    row = _fetch_item(db=db, item_id=int(item_id))
    if not row:
        raise HTTPException(status_code=404, detail="Not Found")

    db.execute(
        text(
            """
            DELETE FROM config_items
            WHERE id = :item_id
            """
        ),
        {"item_id": int(item_id)},
    )
    db.commit()

    return {"message": "Variable de configuración eliminada."}
