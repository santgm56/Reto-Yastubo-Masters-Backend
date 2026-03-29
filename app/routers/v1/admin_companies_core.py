from __future__ import annotations

import json
import mimetypes
import re
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Request, UploadFile
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.database import get_db
from app.services.auth_service import AuthService

router = APIRouter(prefix="/api/v1/admin/companies", tags=["admin-companies"])

_SHORT_CODE_PATTERN = re.compile(r"^[A-Za-z]{3,5}$")
_COLOR_PATTERN = re.compile(r"^#?[0-9A-Fa-f]{3}([0-9A-Fa-f]{3})?$")
_ALLOWED_STATUSES = {"active", "inactive", "archived"}


def _normalize_status_filter(status: str | None) -> str:
    normalized = str(status or "active").strip().lower()
    if normalized in {"active", "inactive", "archived", "all"}:
        return normalized
    return "active"


def _extract_bearer_token(authorization: str | None) -> str:
    if not authorization:
        return ""

    raw = authorization.strip()
    if not raw.lower().startswith("bearer "):
        return ""

    return raw[7:].strip()


def _require_admin(request: Request, authorization: str | None, db: Session) -> dict:
    token = _extract_bearer_token(authorization)
    if not token:
        token = str(request.cookies.get("yastubo_access_token") or "").strip()

    if not token:
        raise HTTPException(status_code=403, detail="Forbidden")

    auth_payload = AuthService(db).me(token)
    role = str(auth_payload.get("role") or "").upper()
    if role != "ADMIN":
        raise HTTPException(status_code=403, detail="Forbidden")

    return auth_payload


def _storage_root() -> Path:
    settings = get_settings()
    configured = Path(str(settings.frontend_storage_root or "").strip())
    if configured.is_absolute():
        return configured

    backend_root = Path(__file__).resolve().parents[3]
    return (backend_root / configured).resolve()


def _fetch_company_row(db: Session, company_id: int):
    return db.execute(
        text(
            """
            SELECT
                id,
                name,
                short_code,
                phone,
                email,
                description,
                status,
                commission_beneficiary_user_id,
                branding_logo_file_id,
                pdf_template_id,
                branding_text_dark,
                branding_bg_light,
                branding_text_light,
                branding_bg_dark
            FROM companies
            WHERE id = :company_id
            LIMIT 1
            """
        ),
        {"company_id": int(company_id)},
    ).mappings().first()


def _fetch_companies_for_index(db: Session, status: str, search: str) -> list[dict]:
    where_clauses = ["1 = 1"]
    params: dict[str, object] = {}

    if status != "all":
        where_clauses.append("status = :status")
        params["status"] = status

    if search:
        where_clauses.append(
            """
            (
                name LIKE :search
                OR short_code LIKE :search
                OR phone LIKE :search
                OR email LIKE :search
            )
            """
        )
        params["search"] = f"%{search}%"

    rows = db.execute(
        text(
            f"""
            SELECT
                id,
                name,
                short_code,
                phone,
                email,
                description,
                status,
                commission_beneficiary_user_id,
                branding_logo_file_id,
                pdf_template_id,
                branding_text_dark,
                branding_bg_light,
                branding_text_light,
                branding_bg_dark
            FROM companies
            WHERE {' AND '.join(where_clauses)}
            ORDER BY name ASC
            """
        ),
        params,
    ).mappings().all()

    payload: list[dict] = []
    for row in rows:
        users_ids = _fetch_company_user_ids(db=db, company_id=int(row["id"]))
        logo_payload = _fetch_logo_payload(db=db, branding_logo_file_id=row.get("branding_logo_file_id"))
        payload.append(_serialize_company(row, users_ids, logo_payload))

    return payload


def _fetch_company_user_ids(db: Session, company_id: int) -> list[int]:
    rows = db.execute(
        text(
            """
            SELECT user_id
            FROM company_user
            WHERE company_id = :company_id
            ORDER BY user_id ASC
            """
        ),
        {"company_id": int(company_id)},
    ).mappings().all()

    return [int(row["user_id"]) for row in rows]


def _fetch_assigned_users(db: Session, company_id: int) -> list[dict]:
    rows = db.execute(
        text(
            """
            SELECT u.id, u.email, u.first_name, u.last_name
            FROM users u
            INNER JOIN company_user cu ON cu.user_id = u.id
            WHERE cu.company_id = :company_id
            ORDER BY u.first_name ASC, u.last_name ASC, u.email ASC
            """
        ),
        {"company_id": int(company_id)},
    ).mappings().all()

    payload: list[dict] = []
    for row in rows:
        first_name = str(row.get("first_name") or "").strip()
        last_name = str(row.get("last_name") or "").strip()
        email = str(row.get("email") or "").strip()
        display_name = f"{first_name} {last_name}".strip() or email
        payload.append(
            {
                "id": int(row["id"]),
                "email": email,
                "display_name": display_name,
            }
        )

    return payload


def _fetch_beneficiary_users(db: Session) -> list[dict]:
    rows = db.execute(
        text(
            """
            SELECT id, email, first_name, last_name
            FROM users
            ORDER BY first_name ASC, last_name ASC, email ASC
            """
        )
    ).mappings().all()

    payload: list[dict] = []
    for row in rows:
        first_name = str(row.get("first_name") or "").strip()
        last_name = str(row.get("last_name") or "").strip()
        email = str(row.get("email") or "").strip()
        display_name = f"{first_name} {last_name}".strip() or email
        payload.append(
            {
                "id": int(row["id"]),
                "email": email,
                "display_name": display_name,
            }
        )

    return payload


def _fetch_pdf_templates(db: Session) -> list[dict]:
    rows = db.execute(
        text(
            """
            SELECT id, name
            FROM templates
            WHERE UPPER(type) = 'PDF'
              AND deleted_at IS NULL
            ORDER BY name ASC
            """
        )
    ).mappings().all()

    return [{"id": int(row["id"]), "name": row.get("name")} for row in rows]


def _fetch_logo_payload(db: Session, branding_logo_file_id: int | None) -> dict | None:
    if branding_logo_file_id is None:
        return None

    row = db.execute(
        text(
            """
            SELECT id, uuid, original_name
            FROM files
            WHERE id = :file_id
            LIMIT 1
            """
        ),
        {"file_id": int(branding_logo_file_id)},
    ).mappings().first()

    if not row:
        return None

    return {
        "id": int(row["id"]),
        "url": f"/api/v1/files/{row['uuid']}",
        "original_name": row.get("original_name"),
        "is_custom": True,
    }


def _serialize_company(row, users_ids: list[int], logo_payload: dict | None) -> dict:
    def color_with_hash(value: str | None, fallback: str) -> str:
        normalized = str(value or "").strip().lstrip("#")
        if not normalized:
            normalized = fallback
        return f"#{normalized}"

    return {
        "id": int(row["id"]),
        "name": row.get("name"),
        "short_code": row.get("short_code"),
        "phone": row.get("phone"),
        "email": row.get("email"),
        "description": row.get("description"),
        "status": row.get("status"),
        "status_label": row.get("status"),
        "users_ids": users_ids,
        "commission_beneficiary_user_id": row.get("commission_beneficiary_user_id"),
        "branding_logo_file_id": row.get("branding_logo_file_id"),
        "pdf_template_id": row.get("pdf_template_id"),
        "branding": {
            "text_dark": color_with_hash(row.get("branding_text_dark"), "000000"),
            "bg_light": color_with_hash(row.get("branding_bg_light"), "FFFFFF"),
            "text_light": color_with_hash(row.get("branding_text_light"), "FFFFFF"),
            "bg_dark": color_with_hash(row.get("branding_bg_dark"), "000000"),
            "custom_text_dark": row.get("branding_text_dark"),
            "custom_bg_light": row.get("branding_bg_light"),
            "custom_text_light": row.get("branding_text_light"),
            "custom_bg_dark": row.get("branding_bg_dark"),
            "logo": logo_payload,
        },
    }


def _require_company_exists(db: Session, company_id: int):
    row = _fetch_company_row(db=db, company_id=int(company_id))
    if not row:
        raise HTTPException(status_code=404, detail="Not Found")
    return row


def _validation_error(errors: dict[str, list[str]]) -> None:
    raise HTTPException(
        status_code=422,
        detail={
            "code": "API_VALIDATION_ERROR",
            "message": "The given data was invalid.",
            "errors": errors,
        },
    )


def _normalize_optional_string(payload: dict, field: str) -> str | None:
    if field not in payload:
        return None
    value = payload.get(field)
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _validate_short_code(db: Session, short_code: str, ignore_company_id: int | None = None) -> str:
    normalized = str(short_code or "").strip()
    if not _SHORT_CODE_PATTERN.fullmatch(normalized):
        _validation_error(
            {
                "short_code": [
                    "El código debe contener solo letras y tener entre 3 y 5 caracteres.",
                ]
            }
        )

    upper_short_code = normalized.upper()
    query_sql = """
        SELECT id
        FROM companies
        WHERE UPPER(short_code) = :short_code
    """
    params: dict[str, object] = {"short_code": upper_short_code}
    if ignore_company_id is not None:
        query_sql += " AND id != :company_id"
        params["company_id"] = int(ignore_company_id)
    query_sql += " LIMIT 1"

    existing = db.execute(text(query_sql), params).mappings().first()
    if existing:
        _validation_error({"short_code": ["El código ya está en uso."]})

    return upper_short_code


def _validate_company_payload(
    db: Session,
    payload: dict,
    *,
    require_name: bool,
    require_short_code: bool,
    ignore_company_id: int | None = None,
) -> dict:
    errors: dict[str, list[str]] = {}
    validated: dict[str, object] = {}

    if require_name or "name" in payload:
        name = str(payload.get("name") or "").strip()
        if not name:
            errors.setdefault("name", []).append("El nombre es obligatorio.")
        elif len(name) > 255:
            errors.setdefault("name", []).append("El nombre no puede superar 255 caracteres.")
        else:
            validated["name"] = name

    if require_short_code or "short_code" in payload:
        validated["short_code"] = _validate_short_code(
            db=db,
            short_code=str(payload.get("short_code") or ""),
            ignore_company_id=ignore_company_id,
        )

    for field in ["phone", "email", "description"]:
        if field in payload:
            validated[field] = _normalize_optional_string(payload, field)

    if "status" in payload:
        status = str(payload.get("status") or "").strip().lower()
        if status not in _ALLOWED_STATUSES:
            errors.setdefault("status", []).append("Estado inválido.")
        else:
            validated["status"] = status

    if "commission_beneficiary_user_id" in payload:
        raw_user_id = payload.get("commission_beneficiary_user_id")
        if raw_user_id in [None, "", 0, "0"]:
            validated["commission_beneficiary_user_id"] = None
        else:
            try:
                user_id = int(raw_user_id)
            except (TypeError, ValueError):
                errors.setdefault("commission_beneficiary_user_id", []).append("Usuario inválido.")
            else:
                user_row = db.execute(
                    text("SELECT id FROM users WHERE id = :user_id LIMIT 1"),
                    {"user_id": user_id},
                ).mappings().first()
                if not user_row:
                    errors.setdefault("commission_beneficiary_user_id", []).append("Usuario inválido.")
                else:
                    validated["commission_beneficiary_user_id"] = user_id

    for field in [
        "branding_text_dark",
        "branding_bg_light",
        "branding_text_light",
        "branding_bg_dark",
    ]:
        if field in payload:
            value = _normalize_optional_string(payload, field)
            if value is not None and not _COLOR_PATTERN.fullmatch(value):
                errors.setdefault(field, []).append("Color inválido.")
            else:
                validated[field] = value.lstrip("#") if value else None

    if "pdf_template_id" in payload:
        raw_template_id = payload.get("pdf_template_id")
        if raw_template_id in [None, "", 0, "0"]:
            validated["pdf_template_id"] = None
        else:
            try:
                template_id = int(raw_template_id)
            except (TypeError, ValueError):
                errors.setdefault("pdf_template_id", []).append("Plantilla inválida.")
            else:
                template_row = db.execute(
                    text(
                        """
                        SELECT id
                        FROM templates
                        WHERE id = :template_id
                          AND UPPER(type) = 'PDF'
                          AND deleted_at IS NULL
                        LIMIT 1
                        """
                    ),
                    {"template_id": template_id},
                ).mappings().first()
                if not template_row:
                    errors.setdefault("pdf_template_id", []).append("Plantilla inválida.")
                else:
                    validated["pdf_template_id"] = template_id

    if errors:
        _validation_error(errors)

    return validated


async def _extract_payload_for_update(request: Request) -> tuple[dict, UploadFile | None]:
    content_type = str(request.headers.get("content-type") or "").lower()

    if "application/json" in content_type:
        raw_payload = await request.json()
        if not isinstance(raw_payload, dict):
            _validation_error({"body": ["Formato de payload inválido."]})
        return raw_payload, None

    form = await request.form()
    payload: dict[str, object] = {}
    logo_file: UploadFile | None = None

    for key, value in form.multi_items():
        if key == "branding_logo" and isinstance(value, UploadFile):
            logo_file = value
            continue
        payload[key] = value

    if "branding_logo_remove" in payload:
        raw = str(payload.get("branding_logo_remove") or "").strip().lower()
        payload["branding_logo_remove"] = raw in {"1", "true", "yes", "on"}

    return payload, logo_file


async def _store_branding_logo(
    db: Session,
    company_id: int,
    uploaded_file: UploadFile,
    uploaded_by_user_id: int | None,
) -> int:
    original_name = str(uploaded_file.filename or "logo.png").strip() or "logo.png"
    suffix = Path(original_name).suffix.lower()
    if suffix not in {".jpg", ".jpeg", ".png"}:
        _validation_error({"branding_logo": ["Solo se permiten archivos JPG o PNG."]})

    raw_bytes = await uploaded_file.read()
    if not raw_bytes:
        _validation_error({"branding_logo": ["El archivo está vacío."]})

    max_size = 5 * 1024 * 1024
    if len(raw_bytes) > max_size:
        _validation_error({"branding_logo": ["El archivo supera el máximo de 5MB."]})

    file_uuid = str(uuid4())
    relative_path = f"companies/{int(company_id)}/branding_logo/{file_uuid}{suffix}"

    public_root = _storage_root() / "public"
    absolute_path = (public_root / relative_path).resolve()
    absolute_path.parent.mkdir(parents=True, exist_ok=True)
    absolute_path.write_bytes(raw_bytes)

    mime_type = str(uploaded_file.content_type or "").strip() or (mimetypes.guess_type(original_name)[0] or "application/octet-stream")
    meta_payload = json.dumps({"context": "company_branding_logo", "company_id": int(company_id)})

    insert_result = db.execute(
        text(
            """
            INSERT INTO files (uuid, disk, path, original_name, mime_type, size, uploaded_by, meta, created_at, updated_at)
            VALUES (:uuid, :disk, :path, :original_name, :mime_type, :size, :uploaded_by, :meta, NOW(), NOW())
            """
        ),
        {
            "uuid": file_uuid,
            "disk": "public",
            "path": relative_path,
            "original_name": original_name,
            "mime_type": mime_type,
            "size": int(len(raw_bytes)),
            "uploaded_by": int(uploaded_by_user_id) if uploaded_by_user_id else None,
            "meta": meta_payload,
        },
    )

    if insert_result.lastrowid is not None:
        return int(insert_result.lastrowid)

    row = db.execute(
        text("SELECT id FROM files WHERE uuid = :uuid LIMIT 1"),
        {"uuid": file_uuid},
    ).mappings().first()

    if not row:
        raise HTTPException(status_code=500, detail="No se pudo registrar el logo.")

    return int(row["id"])


def _build_company_response(db: Session, company_id: int) -> dict:
    company_row = _require_company_exists(db=db, company_id=int(company_id))
    users_ids = _fetch_company_user_ids(db=db, company_id=int(company_id))
    logo_payload = _fetch_logo_payload(db=db, branding_logo_file_id=company_row.get("branding_logo_file_id"))

    return {
        "data": _serialize_company(company_row, users_ids, logo_payload),
        "assigned_users": _fetch_assigned_users(db=db, company_id=int(company_id)),
        "beneficiary_users": _fetch_beneficiary_users(db=db),
        "branding_defaults": {
            "text_dark": "000000",
            "bg_light": "FFFFFF",
            "text_light": "FFFFFF",
            "bg_dark": "000000",
            "logo": None,
        },
        "pdf_templates": _fetch_pdf_templates(db=db),
    }


@router.get("")
def index_companies(
    request: Request,
    status: str = "active",
    search: str = "",
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin(request=request, authorization=authorization, db=db)

    normalized_status = _normalize_status_filter(status)
    normalized_search = str(search or "").strip()

    companies = _fetch_companies_for_index(
        db=db,
        status=normalized_status,
        search=normalized_search,
    )

    return {
        "companies": companies,
        "filters": {
            "status": normalized_status,
            "search": normalized_search,
        },
    }


@router.get("/{company_id}")
def show_company(
    company_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin(request=request, authorization=authorization, db=db)
    return _build_company_response(db=db, company_id=int(company_id))


@router.post("")
async def create_company(
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin(request=request, authorization=authorization, db=db)

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload inválido."]})

    validated = _validate_company_payload(
        db=db,
        payload=payload,
        require_name=True,
        require_short_code=True,
    )

    db.execute(
        text(
            """
            INSERT INTO companies (name, short_code, status, created_at, updated_at)
            VALUES (:name, :short_code, :status, NOW(), NOW())
            """
        ),
        {
            "name": str(validated["name"]),
            "short_code": str(validated["short_code"]),
            "status": "active",
        },
    )
    created_row = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    db.commit()

    if not created_row:
        raise HTTPException(status_code=500, detail="No se pudo crear la empresa.")

    return _build_company_response(db=db, company_id=int(created_row["id"]))


@router.put("/{company_id}")
async def update_company(
    company_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _require_admin(request=request, authorization=authorization, db=db)
    _require_company_exists(db=db, company_id=int(company_id))

    payload, logo_file = await _extract_payload_for_update(request)
    validated = _validate_company_payload(
        db=db,
        payload=payload,
        require_name=False,
        require_short_code=False,
        ignore_company_id=int(company_id),
    )

    if validated:
        set_fragments: list[str] = []
        params: dict[str, object] = {"company_id": int(company_id)}

        for field in [
            "name",
            "short_code",
            "phone",
            "email",
            "description",
            "status",
            "commission_beneficiary_user_id",
            "branding_text_dark",
            "branding_bg_light",
            "branding_text_light",
            "branding_bg_dark",
            "pdf_template_id",
        ]:
            if field in validated:
                set_fragments.append(f"{field} = :{field}")
                params[field] = validated[field]

        if set_fragments:
            db.execute(
                text(
                    f"""
                    UPDATE companies
                    SET {', '.join(set_fragments)}
                    WHERE id = :company_id
                    """
                ),
                params,
            )

    if payload.get("branding_logo_remove"):
        db.execute(
            text(
                """
                UPDATE companies
                SET branding_logo_file_id = NULL
                WHERE id = :company_id
                """
            ),
            {"company_id": int(company_id)},
        )

    if logo_file is not None:
        file_id = await _store_branding_logo(
            db=db,
            company_id=int(company_id),
            uploaded_file=logo_file,
            uploaded_by_user_id=int(auth_payload.get("id") or 0) or None,
        )
        db.execute(
            text(
                """
                UPDATE companies
                SET branding_logo_file_id = :file_id
                WHERE id = :company_id
                """
            ),
            {"company_id": int(company_id), "file_id": int(file_id)},
        )

    db.commit()

    return _build_company_response(db=db, company_id=int(company_id))
