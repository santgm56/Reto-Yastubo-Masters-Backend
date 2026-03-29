from __future__ import annotations

from pathlib import Path
from uuid import uuid4
import json
import mimetypes

from fastapi import APIRouter, Depends, File, Header, HTTPException, Query, Request, UploadFile
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.database import get_db
from app.services.auth_service import AuthService


router = APIRouter(prefix="/api/v1/admin/business-units", tags=["admin-business-units"])

_LEVEL_NONE = 0
_LEVEL_LOCAL = 1
_LEVEL_INHERITED = 2
_LEVEL_GLOBAL = 3

_ALLOWED_TYPES = {"consolidator", "office", "counter", "freelance"}
_ALLOWED_STATUS = {"active", "inactive"}

_PERMISSION_KEYS = {
    "unit.structure.view",
    "unit.structure.manage",
    "unit.basic.view",
    "unit.basic.edit",
    "unit.branding.view",
    "unit.branding.manage",
    "unit.members.view",
    "unit.members.invite",
    "unit.members.manage_roles",
    "unit.members.remove",
    "unit.manage_children",
    "unit.gsa.commission",
    "unit.products.sell",
}

_ABILITY_RULES = {
    "can_structure_view": ("unit.structure.view", _LEVEL_LOCAL),
    "can_basic_view": ("unit.basic.view", _LEVEL_LOCAL),
    "can_basic_edit": ("unit.basic.edit", _LEVEL_LOCAL),
    "can_branding_view": ("unit.branding.view", _LEVEL_LOCAL),
    "can_branding_manage": ("unit.branding.manage", _LEVEL_LOCAL),
    "can_members_view": ("unit.members.view", _LEVEL_LOCAL),
    "can_members_invite": ("unit.members.invite", _LEVEL_LOCAL),
    "can_members_manage_roles": ("unit.members.manage_roles", _LEVEL_LOCAL),
    "can_members_manage_roles_any": ("unit.members.manage_roles", _LEVEL_INHERITED),
    "can_members_remove": ("unit.members.remove", _LEVEL_INHERITED),
    "can_manage_children": ("unit.manage_children", _LEVEL_LOCAL),
    "can_toggle_status": ("unit.structure.manage", _LEVEL_INHERITED),
    "can_move": ("unit.structure.manage", _LEVEL_GLOBAL),
    "can_change_type": ("unit.structure.manage", _LEVEL_GLOBAL),
    "can_create": ("unit.structure.manage", _LEVEL_GLOBAL),
    "can_pick_active_users": ("unit.members.invite", _LEVEL_GLOBAL),
    "can_products_sell": ("unit.products.sell", _LEVEL_GLOBAL),
    "can_access": ("unit.structure.view", _LEVEL_LOCAL),
    "can_edit_gsa_commission": ("unit.gsa.commission", _LEVEL_GLOBAL),
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


def _json_422(message: str, errors: dict[str, list[str]] | None = None) -> JSONResponse:
    payload = {"message": message}
    if errors:
        payload["errors"] = errors
    return JSONResponse(status_code=422, content=payload)


def _parse_bool(value) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, int):
        if value == 1:
            return True
        if value == 0:
            return False
        return None

    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return None


def _normalize_page(value: int | None) -> int:
    if value is None or value < 1:
        return 1
    return int(value)


def _normalize_per_page(value: int | None, default: int) -> int:
    if value is None:
        return default
    if value < 1:
        return default
    return min(int(value), 200)


def _empty_permissions() -> dict[str, int]:
    return {key: _LEVEL_NONE for key in sorted(_PERMISSION_KEYS)}


def _permissions_set(auth_payload: dict) -> set[str]:
    return {str(item) for item in (auth_payload.get("permissions") or [])}


def _global_permission_levels(auth_payload: dict) -> dict[str, int]:
    owned = _permissions_set(auth_payload)
    result = _empty_permissions()
    for key in _PERMISSION_KEYS:
        if key in owned:
            result[key] = _LEVEL_GLOBAL
    return result


def _permission_level(levels: dict[str, int], key: str) -> int:
    return int(levels.get(key) or _LEVEL_NONE)


def _abilities_from_levels(levels: dict[str, int]) -> dict[str, bool]:
    out: dict[str, bool] = {}
    for ability, (perm, min_level) in _ABILITY_RULES.items():
        out[ability] = _permission_level(levels, perm) >= int(min_level)
    return out


def _fetch_unit(db: Session, unit_id: int):
    return db.execute(
        text(
            """
            SELECT id, type, name, status, parent_id,
                   branding_text_dark, branding_bg_light, branding_text_light, branding_bg_dark,
                   branding_logo_file_id
            FROM business_units
            WHERE id = :unit_id
            LIMIT 1
            """
        ),
        {"unit_id": int(unit_id)},
    ).mappings().first()


def _ancestor_chain(db: Session, unit_id: int) -> list[dict]:
    chain: list[dict] = []
    seen: set[int] = set()
    current_id = int(unit_id)

    while current_id > 0 and current_id not in seen:
        seen.add(current_id)
        row = _fetch_unit(db, current_id)
        if not row:
            break
        item = dict(row)
        chain.append(item)

        parent_id = item.get("parent_id")
        current_id = int(parent_id) if parent_id is not None else 0

    chain.reverse()
    return chain


def _local_permissions_by_unit(db: Session, user_id: int, unit_ids: list[int]) -> dict[int, set[str]]:
    if not unit_ids:
        return {}

    placeholders = ",".join(f":uid_{idx}" for idx, _ in enumerate(unit_ids))
    params = {f"uid_{idx}": int(value) for idx, value in enumerate(unit_ids)}
    params["user_id"] = int(user_id)

    rows = db.execute(
        text(
            f"""
            SELECT
                m.business_unit_id,
                p.name AS permission_name
            FROM memberships_business_unit m
            INNER JOIN roles r ON r.id = m.role_id
            LEFT JOIN role_has_permissions rp ON rp.role_id = r.id
            LEFT JOIN permissions p ON p.id = rp.permission_id
            WHERE m.user_id = :user_id
              AND m.business_unit_id IN ({placeholders})
            """
        ),
        params,
    ).mappings().all()

    out: dict[int, set[str]] = {int(unit_id): set() for unit_id in unit_ids}
    for row in rows:
        unit_id = int(row.get("business_unit_id") or 0)
        perm = str(row.get("permission_name") or "")
        if unit_id <= 0 or perm not in _PERMISSION_KEYS:
            continue
        out.setdefault(unit_id, set()).add(perm)

    return out


def _permission_levels_for_unit(db: Session, user_id: int, global_levels: dict[str, int], unit_id: int | None) -> dict[str, int]:
    if not unit_id:
        return dict(global_levels)

    chain = _ancestor_chain(db, int(unit_id))
    if not chain:
        return dict(global_levels)

    chain_ids = [int(item["id"]) for item in chain]
    local_by_unit = _local_permissions_by_unit(db, int(user_id), chain_ids)

    levels_by_unit: dict[int, dict[str, int]] = {}

    for index, unit in enumerate(chain):
        current_levels = dict(global_levels)
        current_id = int(unit["id"])

        for perm in local_by_unit.get(current_id, set()):
            if _permission_level(current_levels, perm) < _LEVEL_LOCAL:
                current_levels[perm] = _LEVEL_LOCAL

        if index > 0:
            for ancestor_index in range(index):
                ancestor_id = int(chain[ancestor_index]["id"])
                ancestor_levels = levels_by_unit.get(ancestor_id, dict(global_levels))
                manage_children_level = _permission_level(ancestor_levels, "unit.manage_children")
                if manage_children_level <= _LEVEL_NONE:
                    continue
                for perm in local_by_unit.get(ancestor_id, set()):
                    if _permission_level(current_levels, perm) < _LEVEL_INHERITED:
                        current_levels[perm] = _LEVEL_INHERITED

        levels_by_unit[current_id] = current_levels

    return levels_by_unit.get(int(unit_id), dict(global_levels))


def _pagination_meta(*, page: int, per_page: int, total: int) -> dict:
    page = max(1, int(page))
    per_page = max(1, int(per_page))
    last_page = max(1, (int(total) + per_page - 1) // per_page)
    return {
        "current_page": page,
        "last_page": last_page,
        "per_page": per_page,
        "total": int(total),
    }


def _display_name_from_user(row) -> str:
    display_name = str(row.get("display_name") or "").strip()
    if display_name:
        return display_name

    first_name = str(row.get("first_name") or "").strip()
    last_name = str(row.get("last_name") or "").strip()
    full = f"{first_name} {last_name}".strip()
    if full:
        return full

    return str(row.get("email") or "").strip()


def _role_name(row) -> str:
    label = str(row.get("label") or "").strip()
    if label:
        return label

    raw_name = str(row.get("name") or "")
    if not raw_name:
        return ""

    normalized = raw_name.replace(".", " - ").replace("_", " ")
    normalized = " ".join(normalized.split())
    if not normalized:
        return ""

    return normalized.lower().title()


def _fetch_file_url(db: Session, file_id: int | None) -> str | None:
    if not file_id:
        return None

    row = db.execute(
        text(
            """
            SELECT uuid
            FROM files
            WHERE id = :file_id
            LIMIT 1
            """
        ),
        {"file_id": int(file_id)},
    ).mappings().first()
    if not row:
        return None

    file_uuid = str(row.get("uuid") or "").strip()
    if not file_uuid:
        return None

    return f"/api/v1/files/{file_uuid}"


def _effective_branding(db: Session, unit_id: int) -> dict:
    chain = _ancestor_chain(db, int(unit_id))

    text_dark = ""
    bg_light = ""
    text_light = ""
    bg_dark = ""
    logo_file_id: int | None = None

    for row in chain:
        for field_name, target in (
            ("branding_text_dark", "text_dark"),
            ("branding_bg_light", "bg_light"),
            ("branding_text_light", "text_light"),
            ("branding_bg_dark", "bg_dark"),
        ):
            raw = str(row.get(field_name) or "").strip()
            if not raw:
                continue
            if target == "text_dark":
                text_dark = raw
            elif target == "bg_light":
                bg_light = raw
            elif target == "text_light":
                text_light = raw
            elif target == "bg_dark":
                bg_dark = raw

        if row.get("branding_logo_file_id") is not None:
            logo_file_id = int(row.get("branding_logo_file_id"))

    return {
        "text_dark": text_dark,
        "bg_light": bg_light,
        "text_light": text_light,
        "bg_dark": bg_dark,
        "logo_url": _fetch_file_url(db, logo_file_id),
    }


def _system_logo_constraints() -> dict:
    return {
        "max_size_kb": 2048,
        "allowed_mimes": ["image/png", "image/jpeg", "image/webp"],
    }


def _assert_parent_rules(unit_type: str, parent_row) -> str | None:
    if unit_type == "consolidator":
        if parent_row is not None:
            return "Un consolidator no puede tener padre."
        return None

    if unit_type == "freelance":
        if parent_row is not None:
            return "Un freelance no puede tener padre."
        return None

    if unit_type == "office":
        if parent_row is not None and str(parent_row.get("type") or "") != "consolidator":
            return "Una office solo puede tener como padre un consolidator (o ser independiente)."
        return None

    if unit_type == "counter":
        if parent_row is None:
            return "Un counter debe tener padre."
        parent_type = str(parent_row.get("type") or "")
        if parent_type not in {"office", "consolidator"}:
            return "Un counter debe tener como padre una office o un consolidator."
        return None

    return "Tipo de unidad invalido."


def _fetch_membership_for_user_unit(db: Session, *, user_id: int, unit_id: int):
    return db.execute(
        text(
            """
            SELECT m.id, m.user_id, m.business_unit_id, m.role_id, m.status,
                   r.id AS role_ref_id, r.level AS role_level
            FROM memberships_business_unit m
            LEFT JOIN roles r ON r.id = m.role_id
            WHERE m.user_id = :user_id
              AND m.business_unit_id = :unit_id
            LIMIT 1
            """
        ),
        {"user_id": int(user_id), "unit_id": int(unit_id)},
    ).mappings().first()


def _roles_manageable_for_unit(db: Session, *, actor_id: int, unit_id: int, levels: dict[str, int]) -> list[dict]:
    manage_level = _permission_level(levels, "unit.members.manage_roles")
    if manage_level <= _LEVEL_NONE:
        return []

    rows = db.execute(
        text(
            """
            SELECT id, name, guard_name, scope, level, label
            FROM roles
            WHERE guard_name = 'admin'
              AND scope = 'unit'
            ORDER BY name
            """
        )
    ).mappings().all()

    role_rows = [dict(item) for item in rows]

    if manage_level == _LEVEL_LOCAL:
        membership = _fetch_membership_for_user_unit(db, user_id=int(actor_id), unit_id=int(unit_id))
        if not membership or membership.get("role_level") is None:
            return []

        my_level = int(membership.get("role_level"))
        role_rows = [item for item in role_rows if int(item.get("level") or 0) >= my_level]

    return role_rows


def _would_create_unit_redundancy_cycle_for_unit_regalia(db: Session, *, beneficiary_user_id: int, unit_id: int) -> bool:
    existing_rows = db.execute(
        text(
            """
            SELECT source_id
            FROM regalias
            WHERE beneficiary_user_id = :beneficiary_user_id
              AND source_type = 'unit'
            """
        ),
        {"beneficiary_user_id": int(beneficiary_user_id)},
    ).mappings().all()

    existing_unit_ids = sorted({int(row.get("source_id") or 0) for row in existing_rows if int(row.get("source_id") or 0) > 0})
    if not existing_unit_ids:
        return False

    candidate_id = int(unit_id)
    if candidate_id in existing_unit_ids:
        return True

    candidate_chain_ids = {int(item.get("id") or 0) for item in _ancestor_chain(db, candidate_id) if int(item.get("id") or 0) > 0}

    for existing_id in existing_unit_ids:
        existing_chain_ids = {int(item.get("id") or 0) for item in _ancestor_chain(db, int(existing_id)) if int(item.get("id") or 0) > 0}

        if candidate_id in existing_chain_ids:
            return True

        if int(existing_id) in candidate_chain_ids:
            return True

    return False


def _storage_root() -> Path:
    settings = get_settings()
    configured = Path(str(settings.frontend_storage_root or "").strip())
    if configured.is_absolute():
        return configured

    backend_root = Path(__file__).resolve().parents[3]
    return (backend_root / configured).resolve()


async def _store_branding_logo(
    db: Session,
    *,
    unit_id: int,
    uploaded_file: UploadFile,
    uploaded_by_user_id: int | None,
) -> int:
    original_name = str(uploaded_file.filename or "logo.png").strip() or "logo.png"
    suffix = Path(original_name).suffix.lower() or ".png"
    if suffix not in {".jpg", ".jpeg", ".png", ".webp"}:
        raise HTTPException(status_code=422, detail={"message": "Solo se permiten imagenes JPG, PNG o WEBP.", "errors": {}})

    raw_bytes = await uploaded_file.read()
    if not raw_bytes:
        raise HTTPException(status_code=422, detail={"message": "El archivo esta vacio.", "errors": {}})

    max_size = 2 * 1024 * 1024
    if len(raw_bytes) > max_size:
        raise HTTPException(status_code=422, detail={"message": "El archivo supera el maximo de 2MB.", "errors": {}})

    file_uuid = str(uuid4())
    relative_path = f"business_units/branding/{int(unit_id)}/{file_uuid}{suffix}"

    public_root = _storage_root() / "public"
    absolute_path = (public_root / relative_path).resolve()
    absolute_path.parent.mkdir(parents=True, exist_ok=True)
    absolute_path.write_bytes(raw_bytes)

    mime_type = str(uploaded_file.content_type or "").strip() or (mimetypes.guess_type(original_name)[0] or "application/octet-stream")
    meta_payload = json.dumps({"context": "business_unit_branding_logo", "unit_id": int(unit_id)})

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
        raise HTTPException(status_code=500, detail="No se pudo registrar el archivo.")

    return int(row["id"])


def _unit_parent_payload(db: Session, parent_id: int | None) -> dict | None:
    if parent_id is None:
        return None

    row = _fetch_unit(db, int(parent_id))
    if not row:
        return None

    return {
        "id": int(row.get("id")),
        "name": str(row.get("name") or ""),
        "type": str(row.get("type") or ""),
        "status": str(row.get("status") or ""),
    }


@router.get("/units")
def list_units(
    request: Request,
    type: str = Query(...),
    status: str = Query("active"),
    root: str | None = Query("true"),
    q: str | None = Query(None),
    page: int | None = Query(1),
    per_page: int | None = Query(25),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request, authorization, db)
    user_id = int(auth_payload.get("id") or 0)

    if user_id <= 0:
        raise HTTPException(status_code=403, detail="Forbidden")

    unit_type = str(type or "").strip()
    if unit_type not in _ALLOWED_TYPES:
        return _json_422("The given data was invalid.", {"type": ["The selected type is invalid."]})

    status_value = str(status or "active").strip()
    if status_value not in {"active", "inactive", "all"}:
        return _json_422("The given data was invalid.", {"status": ["The selected status is invalid."]})

    root_parsed = _parse_bool(root)
    if root_parsed is None:
        return _json_422("The root field must be true or false.", {"root": ["The root field must be true or false."]})

    page_number = _normalize_page(page)
    page_size = _normalize_per_page(per_page, 25)
    offset = (page_number - 1) * page_size

    global_levels = _global_permission_levels(auth_payload)
    global_abilities = _abilities_from_levels(global_levels)
    if not bool(global_abilities.get("can_structure_view")):
        raise HTTPException(status_code=403, detail="Forbidden")

    where_parts = ["u.type = :unit_type"]
    params: dict[str, object] = {
        "unit_type": unit_type,
        "limit": page_size,
        "offset": offset,
    }

    if root_parsed:
        where_parts.append("u.parent_id IS NULL")

    if status_value != "all":
        where_parts.append("u.status = :status")
        params["status"] = status_value

    q_text = str(q or "").strip()
    is_freelance = unit_type == "freelance"

    if q_text:
        if is_freelance:
            where_parts.append(
                "EXISTS ("
                "SELECT 1 FROM memberships_business_unit mx "
                "INNER JOIN users ux ON ux.id = mx.user_id "
                "WHERE mx.business_unit_id = u.id "
                "AND (ux.first_name LIKE :q OR ux.last_name LIKE :q OR ux.email LIKE :q)"
                ")"
            )
            params["q"] = f"%{q_text}%"
        else:
            if q_text.isdigit():
                where_parts.append("(u.id = :q_id OR u.name LIKE :q_like)")
                params["q_id"] = int(q_text)
                params["q_like"] = f"%{q_text}%"
            else:
                where_parts.append("u.name LIKE :q_like")
                params["q_like"] = f"%{q_text}%"

    where_sql = " AND ".join(where_parts)

    total_row = db.execute(
        text(f"SELECT COUNT(*) AS c FROM business_units u WHERE {where_sql}"),
        params,
    ).mappings().first()
    total = int(total_row.get("c") or 0) if total_row else 0

    order_sql = "owner_email ASC, u.id ASC" if is_freelance else "u.name ASC, u.id ASC"

    rows = db.execute(
        text(
            f"""
            SELECT
                u.id,
                u.type,
                u.name,
                u.status,
                u.parent_id,
                p.id AS parent_ref_id,
                p.name AS parent_name,
                p.type AS parent_type,
                p.status AS parent_status,
                (SELECT COUNT(*) FROM business_units c WHERE c.parent_id = u.id) AS children_count,
                (SELECT COUNT(*) FROM memberships_business_unit m WHERE m.business_unit_id = u.id) AS members_count,
                (
                    SELECT ux.id
                    FROM memberships_business_unit mx
                    INNER JOIN users ux ON ux.id = mx.user_id
                    WHERE mx.business_unit_id = u.id
                    ORDER BY mx.id
                    LIMIT 1
                ) AS owner_user_id,
                (
                    SELECT ux.email
                    FROM memberships_business_unit mx
                    INNER JOIN users ux ON ux.id = mx.user_id
                    WHERE mx.business_unit_id = u.id
                    ORDER BY mx.id
                    LIMIT 1
                ) AS owner_email,
                (
                    SELECT ux.first_name
                    FROM memberships_business_unit mx
                    INNER JOIN users ux ON ux.id = mx.user_id
                    WHERE mx.business_unit_id = u.id
                    ORDER BY mx.id
                    LIMIT 1
                ) AS owner_first_name,
                (
                    SELECT ux.last_name
                    FROM memberships_business_unit mx
                    INNER JOIN users ux ON ux.id = mx.user_id
                    WHERE mx.business_unit_id = u.id
                    ORDER BY mx.id
                    LIMIT 1
                ) AS owner_last_name,
                (
                    SELECT ux.display_name
                    FROM memberships_business_unit mx
                    INNER JOIN users ux ON ux.id = mx.user_id
                    WHERE mx.business_unit_id = u.id
                    ORDER BY mx.id
                    LIMIT 1
                ) AS owner_display_name,
                (
                    SELECT ux.status
                    FROM memberships_business_unit mx
                    INNER JOIN users ux ON ux.id = mx.user_id
                    WHERE mx.business_unit_id = u.id
                    ORDER BY mx.id
                    LIMIT 1
                ) AS owner_status
            FROM business_units u
            LEFT JOIN business_units p ON p.id = u.parent_id
            WHERE {where_sql}
            ORDER BY {order_sql}
            LIMIT :limit OFFSET :offset
            """
        ),
        params,
    ).mappings().all()

    data: list[dict] = []
    for row in rows:
        unit_id = int(row.get("id") or 0)
        levels = _permission_levels_for_unit(db, user_id, global_levels, unit_id)
        abilities = _abilities_from_levels(levels)

        payload = {
            "id": unit_id,
            "type": str(row.get("type") or ""),
            "name": str(row.get("name") or ""),
            "status": str(row.get("status") or ""),
            "parent_id": int(row.get("parent_id")) if row.get("parent_id") is not None else None,
            "parent": {
                "id": int(row.get("parent_ref_id")),
                "name": str(row.get("parent_name") or ""),
                "type": str(row.get("parent_type") or ""),
                "status": str(row.get("parent_status") or ""),
            }
            if row.get("parent_ref_id") is not None
            else None,
            "children_count": int(row.get("children_count") or 0),
            "members_count": int(row.get("members_count") or 0),
            "abilities": abilities,
        }

        if is_freelance:
            owner_user_id = row.get("owner_user_id")
            payload["owner_user"] = (
                {
                    "id": int(owner_user_id),
                    "email": row.get("owner_email"),
                    "first_name": row.get("owner_first_name"),
                    "last_name": row.get("owner_last_name"),
                    "display_name": _display_name_from_user(row),
                    "status": row.get("owner_status"),
                }
                if owner_user_id is not None
                else None
            )

        data.append(payload)

    return {
        "data": data,
        "meta": {
            "pagination": _pagination_meta(page=page_number, per_page=page_size, total=total),
            "permissions": global_abilities,
        },
    }


@router.get("/units/{unit_id:int}")
def show_unit(
    unit_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request, authorization, db)
    user_id = int(auth_payload.get("id") or 0)

    row = _fetch_unit(db, int(unit_id))
    if not row:
        raise HTTPException(status_code=404, detail="Not Found")

    global_levels = _global_permission_levels(auth_payload)
    levels = _permission_levels_for_unit(db, user_id, global_levels, int(unit_id))
    abilities = _abilities_from_levels(levels)

    if not abilities.get("can_access"):
        raise HTTPException(status_code=403, detail="Forbidden")

    counts = db.execute(
        text(
            """
            SELECT
                (SELECT COUNT(*) FROM business_units c WHERE c.parent_id = :unit_id) AS children_count,
                (SELECT COUNT(*) FROM memberships_business_unit m WHERE m.business_unit_id = :unit_id) AS memberships_count
            """
        ),
        {"unit_id": int(unit_id)},
    ).mappings().first() or {}

    parent_payload = _unit_parent_payload(db, row.get("parent_id"))
    branding = {
        "branding_text_dark": row.get("branding_text_dark"),
        "branding_bg_light": row.get("branding_bg_light"),
        "branding_text_light": row.get("branding_text_light"),
        "branding_bg_dark": row.get("branding_bg_dark"),
        "branding_logo_file_id": row.get("branding_logo_file_id"),
        "branding_logo_url": _fetch_file_url(db, int(row.get("branding_logo_file_id")) if row.get("branding_logo_file_id") is not None else None),
    }

    return {
        "data": {
            "id": int(row.get("id")),
            "type": str(row.get("type") or ""),
            "name": str(row.get("name") or ""),
            "status": str(row.get("status") or ""),
            "parent_id": int(row.get("parent_id")) if row.get("parent_id") is not None else None,
            "parent": parent_payload,
            "children_count": int(counts.get("children_count") or 0),
            "memberships_count": int(counts.get("memberships_count") or 0),
            "branding": branding,
            "branding_effective": _effective_branding(db, int(unit_id)),
            "system_logo_constraints": _system_logo_constraints(),
            "abilities": abilities,
        }
    }


@router.get("/units/{unit_id:int}/children")
def unit_children(
    unit_id: int,
    request: Request,
    status: str = Query("active"),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request, authorization, db)
    user_id = int(auth_payload.get("id") or 0)

    unit_row = _fetch_unit(db, int(unit_id))
    if not unit_row:
        raise HTTPException(status_code=404, detail="Not Found")

    if status not in {"active", "inactive", "all"}:
        return _json_422("The given data was invalid.", {"status": ["The selected status is invalid."]})

    global_levels = _global_permission_levels(auth_payload)
    levels = _permission_levels_for_unit(db, user_id, global_levels, int(unit_id))
    abilities = _abilities_from_levels(levels)
    if not abilities.get("can_access"):
        raise HTTPException(status_code=403, detail="Forbidden")

    where_parts = ["u.parent_id = :unit_id"]
    params: dict[str, object] = {"unit_id": int(unit_id)}
    if status != "all":
        where_parts.append("u.status = :status")
        params["status"] = status

    where_sql = " AND ".join(where_parts)

    rows = db.execute(
        text(
            f"""
            SELECT
                u.id,
                u.type,
                u.name,
                u.status,
                u.parent_id,
                (SELECT COUNT(*) FROM business_units c WHERE c.parent_id = u.id) AS children_count,
                (SELECT COUNT(*) FROM memberships_business_unit m WHERE m.business_unit_id = u.id) AS members_count
            FROM business_units u
            WHERE {where_sql}
            ORDER BY u.name, u.id
            """
        ),
        params,
    ).mappings().all()

    data: list[dict] = []
    for row in rows:
        child_id = int(row.get("id") or 0)
        child_levels = _permission_levels_for_unit(db, user_id, global_levels, child_id)
        child_abilities = _abilities_from_levels(child_levels)
        data.append(
            {
                "id": child_id,
                "type": str(row.get("type") or ""),
                "name": str(row.get("name") or ""),
                "status": str(row.get("status") or ""),
                "parent_id": int(row.get("parent_id")) if row.get("parent_id") is not None else None,
                "children_count": int(row.get("children_count") or 0),
                "members_count": int(row.get("members_count") or 0),
                "abilities": child_abilities,
            }
        )

    return {"data": data}


@router.get("/roles/unit")
def roles_unit_scope(
    request: Request,
    unit_id: int = Query(...),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request, authorization, db)
    actor_id = int(auth_payload.get("id") or 0)

    unit = _fetch_unit(db, int(unit_id))
    if not unit:
        raise HTTPException(status_code=404, detail="Not Found")

    global_levels = _global_permission_levels(auth_payload)
    levels = _permission_levels_for_unit(db, actor_id, global_levels, int(unit_id))
    abilities = _abilities_from_levels(levels)
    if not abilities.get("can_access"):
        raise HTTPException(status_code=403, detail="Forbidden")

    roles = _roles_manageable_for_unit(db, actor_id=actor_id, unit_id=int(unit_id), levels=levels)
    data = [
        {
            "id": int(role.get("id") or 0),
            "name": str(role.get("name") or ""),
            "scope": str(role.get("scope") or ""),
            "level": int(role.get("level") or 0),
            "role_name": _role_name(role),
        }
        for role in roles
    ]

    return {"data": data}


@router.post("/units")
async def store_unit(
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    permissions = _permissions_set(auth_payload)
    actor_id = int(auth_payload.get("id") or 0)

    if "unit.structure.manage" not in permissions:
        raise HTTPException(status_code=403, detail="Forbidden")

    payload = await request.json()
    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.", {"body": ["Formato de payload invalido."]})

    unit_type = str(payload.get("type") or "").strip()
    if unit_type not in _ALLOWED_TYPES:
        return _json_422("The given data was invalid.", {"type": ["Tipo de unidad invalido."]})

    parent_id_raw = payload.get("parent_id")
    parent_id = int(parent_id_raw) if parent_id_raw not in [None, ""] else None
    parent_row = _fetch_unit(db, int(parent_id)) if parent_id is not None else None
    if parent_id is not None and not parent_row:
        return _json_422("The given data was invalid.", {"parent_id": ["Parent invalido."]})

    rule_error = _assert_parent_rules(unit_type, parent_row)
    if rule_error:
        return _json_422(rule_error)

    if unit_type == "freelance":
        mode = str(payload.get("mode") or "existing_user").strip()
        if mode not in {"new_user", "existing_user", "email_exact"}:
            return _json_422("The given data was invalid.", {"mode": ["Modo invalido."]})

        user_row = None
        if mode == "new_user":
            user_payload = payload.get("user") if isinstance(payload.get("user"), dict) else {}
            first_name = str((user_payload or {}).get("first_name") or "").strip()
            last_name = str((user_payload or {}).get("last_name") or "").strip()
            email = str((user_payload or {}).get("email") or "").strip().lower()
            if not first_name or not last_name or not email:
                return _json_422("Nombre, apellido y correo son requeridos.")

            existing_email = db.execute(
                text("SELECT id FROM users WHERE email = :email AND deleted_at IS NULL LIMIT 1"),
                {"email": email},
            ).mappings().first()
            if existing_email:
                return _json_422("Ya existe un usuario con ese email.")

            db.execute(
                text(
                    """
                    INSERT INTO users (
                        realm, email, password, force_password_change,
                        first_name, last_name, display_name, status,
                        locale, timezone, created_at, updated_at
                    ) VALUES (
                        'admin', :email, '', 0,
                        :first_name, :last_name, :display_name, 'active',
                        'es', 'America/Santiago', NOW(), NOW()
                    )
                    """
                ),
                {
                    "email": email,
                    "first_name": first_name,
                    "last_name": last_name,
                    "display_name": f"{first_name} {last_name}".strip(),
                },
            )
            user_id_row = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
            if not user_id_row:
                db.rollback()
                raise HTTPException(status_code=500, detail="No se pudo crear el usuario.")

            user_row = {
                "id": int(user_id_row["id"]),
                "status": "active",
                "email": email,
            }

        elif mode == "existing_user":
            existing_user_id = payload.get("existing_user_id")
            if existing_user_id in [None, ""]:
                return _json_422("existing_user_id requerido.")

            user_row = db.execute(
                text("SELECT id, status, email FROM users WHERE id = :id AND deleted_at IS NULL LIMIT 1"),
                {"id": int(existing_user_id)},
            ).mappings().first()
            if not user_row:
                return _json_422("Usuario invalido.")

            if str(user_row.get("status") or "") != "active":
                return _json_422("Usuario no activo.")

        else:
            email = str(payload.get("email") or "").strip().lower()
            if not email:
                return _json_422("Email requerido.")

            user_row = db.execute(
                text("SELECT id, status, email FROM users WHERE email = :email AND deleted_at IS NULL LIMIT 1"),
                {"email": email},
            ).mappings().first()
            if not user_row:
                return _json_422("No existe un usuario con ese email.")

            if str(user_row.get("status") or "") != "active":
                return _json_422("Usuario no activo.")

        user_id = int(user_row.get("id") or 0)

        already_has_freelance = db.execute(
            text(
                """
                SELECT 1
                FROM memberships_business_unit m
                INNER JOIN business_units bu ON bu.id = m.business_unit_id
                WHERE m.user_id = :user_id
                  AND bu.type = 'freelance'
                LIMIT 1
                """
            ),
            {"user_id": user_id},
        ).mappings().first()
        if already_has_freelance:
            db.rollback()
            return _json_422("El usuario ya tiene una unidad freelance asociada.")

        owner_role = db.execute(
            text(
                """
                SELECT id
                FROM roles
                WHERE name = 'unit.owner'
                  AND scope = 'unit'
                  AND guard_name = 'admin'
                LIMIT 1
                """
            )
        ).mappings().first()
        if not owner_role:
            db.rollback()
            return _json_422("No existe el rol unit.owner (scope=unit).")

        db.execute(
            text(
                """
                INSERT INTO business_units (type, parent_id, name, status, created_at, updated_at)
                VALUES ('freelance', NULL, '', 'active', NOW(), NOW())
                """
            )
        )
        unit_id_row = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
        if not unit_id_row:
            db.rollback()
            raise HTTPException(status_code=500, detail="No se pudo crear la unidad.")

        created_unit_id = int(unit_id_row["id"])

        db.execute(
            text(
                """
                INSERT INTO memberships_business_unit (business_unit_id, user_id, role_id, status, created_at, updated_at)
                VALUES (:business_unit_id, :user_id, :role_id, 'active', NOW(), NOW())
                """
            ),
            {
                "business_unit_id": created_unit_id,
                "user_id": user_id,
                "role_id": int(owner_role["id"]),
            },
        )

        db.commit()
        return {"message": "Unidad creada.", "data": {"id": created_unit_id}}

    name = str(payload.get("name") or "").strip()
    if not name:
        return _json_422("name requerido.")

    db.execute(
        text(
            """
            INSERT INTO business_units (type, parent_id, name, status, created_at, updated_at)
            VALUES (:type, :parent_id, :name, 'active', NOW(), NOW())
            """
        ),
        {
            "type": unit_type,
            "parent_id": int(parent_id) if parent_id is not None else None,
            "name": name,
        },
    )
    created = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    db.commit()

    if not created:
        raise HTTPException(status_code=500, detail="No se pudo crear la unidad.")

    return {"message": "Unidad creada.", "data": {"id": int(created["id"])}}


@router.patch("/units/{unit_id:int}/basic")
async def update_basic(
    unit_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    user_id = int(auth_payload.get("id") or 0)

    unit = _fetch_unit(db, int(unit_id))
    if not unit:
        raise HTTPException(status_code=404, detail="Not Found")

    levels = _permission_levels_for_unit(db, user_id, _global_permission_levels(auth_payload), int(unit_id))
    abilities = _abilities_from_levels(levels)

    if not abilities.get("can_access") or not abilities.get("can_basic_edit"):
        raise HTTPException(status_code=403, detail="Forbidden")

    payload = await request.json()
    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.", {"body": ["Formato de payload invalido."]})

    name = str(payload.get("name") or "").strip()
    if not name:
        return _json_422("The given data was invalid.", {"name": ["El nombre es obligatorio."]})

    db.execute(
        text("UPDATE business_units SET name = :name, updated_at = NOW() WHERE id = :unit_id"),
        {"name": name, "unit_id": int(unit_id)},
    )
    db.commit()

    return {"message": "Guardado."}


@router.patch("/units/{unit_id:int}/status")
async def update_status(
    unit_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    user_id = int(auth_payload.get("id") or 0)

    unit = _fetch_unit(db, int(unit_id))
    if not unit:
        raise HTTPException(status_code=404, detail="Not Found")

    levels = _permission_levels_for_unit(db, user_id, _global_permission_levels(auth_payload), int(unit_id))
    abilities = _abilities_from_levels(levels)

    if not abilities.get("can_access") or not abilities.get("can_toggle_status"):
        raise HTTPException(status_code=403, detail="Forbidden")

    payload = await request.json()
    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.", {"body": ["Formato de payload invalido."]})

    next_status = str(payload.get("status") or "").strip()
    if next_status not in _ALLOWED_STATUS:
        return _json_422("The given data was invalid.", {"status": ["Estado invalido."]})

    db.execute(
        text("UPDATE business_units SET status = :status, updated_at = NOW() WHERE id = :unit_id"),
        {"status": next_status, "unit_id": int(unit_id)},
    )
    db.commit()

    return {"message": "Guardado."}


@router.post("/units/{unit_id:int}/change-type")
async def change_type(
    unit_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    if "unit.structure.manage" not in _permissions_set(auth_payload):
        raise HTTPException(status_code=403, detail="Forbidden")

    unit = _fetch_unit(db, int(unit_id))
    if not unit:
        raise HTTPException(status_code=404, detail="Not Found")

    payload = await request.json()
    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.", {"body": ["Formato de payload invalido."]})

    target_type = str(payload.get("target_type") or "").strip()
    if target_type not in _ALLOWED_TYPES:
        return _json_422("The given data was invalid.", {"target_type": ["Tipo objetivo invalido."]})

    detach_parent = _parse_bool(payload.get("detach_parent")) is True

    from_type = str(unit.get("type") or "")
    new_type = from_type
    new_parent_id = int(unit.get("parent_id")) if unit.get("parent_id") is not None else None

    if from_type == "freelance" and target_type == "office":
        new_type = "office"
        new_parent_id = None
    elif from_type == "office" and target_type == "consolidator":
        if new_parent_id is not None:
            return _json_422("Solo una office sin padre puede convertirse en consolidator.")
        new_type = "consolidator"
        new_parent_id = None
    elif from_type == "office" and target_type == "office" and detach_parent:
        if new_parent_id is None:
            return _json_422("La office ya es independiente.")
        new_parent_id = None
    elif from_type == "counter" and target_type == "office":
        new_type = "office"
        new_parent_id = None
    else:
        return _json_422("Conversión no permitida.")

    parent_row = _fetch_unit(db, int(new_parent_id)) if new_parent_id is not None else None
    rule_error = _assert_parent_rules(new_type, parent_row)
    if rule_error:
        return _json_422(rule_error)

    db.execute(
        text(
            """
            UPDATE business_units
            SET type = :type, parent_id = :parent_id, updated_at = NOW()
            WHERE id = :unit_id
            """
        ),
        {
            "type": new_type,
            "parent_id": int(new_parent_id) if new_parent_id is not None else None,
            "unit_id": int(unit_id),
        },
    )
    db.commit()

    return {"message": "Tipo actualizado."}


@router.post("/units/{unit_id:int}/move")
async def move_unit(
    unit_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    if "unit.structure.manage" not in _permissions_set(auth_payload):
        raise HTTPException(status_code=403, detail="Forbidden")

    unit = _fetch_unit(db, int(unit_id))
    if not unit:
        raise HTTPException(status_code=404, detail="Not Found")

    payload = await request.json()
    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.", {"body": ["Formato de payload invalido."]})

    parent_id_raw = payload.get("parent_id")
    new_parent_id = int(parent_id_raw) if parent_id_raw not in [None, ""] else None

    if new_parent_id == int(unit_id):
        return _json_422("Parent inválido.")

    parent_row = _fetch_unit(db, int(new_parent_id)) if new_parent_id is not None else None
    if new_parent_id is not None and not parent_row:
        return _json_422("Parent inválido.")

    rule_error = _assert_parent_rules(str(unit.get("type") or ""), parent_row)
    if rule_error:
        return _json_422(rule_error)

    if new_parent_id is not None:
        seen: set[int] = set()
        current_id = int(new_parent_id)
        while current_id > 0 and current_id not in seen:
            if current_id == int(unit_id):
                return _json_422("Parent inválido (ciclo).")
            seen.add(current_id)
            current_row = _fetch_unit(db, current_id)
            if not current_row or current_row.get("parent_id") is None:
                break
            current_id = int(current_row.get("parent_id"))

    db.execute(
        text("UPDATE business_units SET parent_id = :parent_id, updated_at = NOW() WHERE id = :unit_id"),
        {
            "parent_id": int(new_parent_id) if new_parent_id is not None else None,
            "unit_id": int(unit_id),
        },
    )
    db.commit()

    return {"message": "Movida."}


@router.post("/units/{unit_id:int}/branding")
async def update_branding(
    unit_id: int,
    request: Request,
    logo: UploadFile | None = File(default=None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    user_id = int(auth_payload.get("id") or 0)

    unit = _fetch_unit(db, int(unit_id))
    if not unit:
        raise HTTPException(status_code=404, detail="Not Found")

    levels = _permission_levels_for_unit(db, user_id, _global_permission_levels(auth_payload), int(unit_id))
    abilities = _abilities_from_levels(levels)

    if not abilities.get("can_access") or not abilities.get("can_branding_manage"):
        raise HTTPException(status_code=403, detail="Forbidden")

    if str(unit.get("type") or "") == "freelance":
        return _json_422("Freelance no permite editar branding.")

    content_type = str(request.headers.get("content-type") or "").lower()
    payload: dict[str, object] = {}
    if "application/json" in content_type:
        raw_payload = await request.json()
        if isinstance(raw_payload, dict):
            payload = raw_payload
    else:
        form = await request.form()
        for key, value in form.multi_items():
            if key == "logo":
                continue
            payload[key] = value

    updates = {
        "branding_text_dark": payload.get("branding_text_dark"),
        "branding_bg_light": payload.get("branding_bg_light"),
        "branding_text_light": payload.get("branding_text_light"),
        "branding_bg_dark": payload.get("branding_bg_dark"),
    }

    normalized: dict[str, object] = {}
    for field_name, raw_value in updates.items():
        if raw_value is None and field_name not in payload:
            continue
        text_value = str(raw_value or "").strip()
        normalized[field_name] = text_value if text_value else None

    remove_logo = _parse_bool(payload.get("remove_logo")) is True
    if logo is not None:
        file_id = await _store_branding_logo(
            db,
            unit_id=int(unit_id),
            uploaded_file=logo,
            uploaded_by_user_id=user_id,
        )
        normalized["branding_logo_file_id"] = int(file_id)
    elif remove_logo:
        normalized["branding_logo_file_id"] = None

    if normalized:
        set_sql = ", ".join(f"{key} = :{key}" for key in normalized.keys())
        params = {**normalized, "unit_id": int(unit_id)}
        db.execute(
            text(f"UPDATE business_units SET {set_sql}, updated_at = NOW() WHERE id = :unit_id"),
            params,
        )
        db.commit()

    return {"message": "Guardado."}


@router.get("/units/{unit_id:int}/members")
def unit_members(
    unit_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request, authorization, db)
    actor_id = int(auth_payload.get("id") or 0)

    unit = _fetch_unit(db, int(unit_id))
    if not unit:
        raise HTTPException(status_code=404, detail="Not Found")

    levels = _permission_levels_for_unit(db, actor_id, _global_permission_levels(auth_payload), int(unit_id))
    abilities = _abilities_from_levels(levels)

    if not abilities.get("can_access") or not abilities.get("can_members_view"):
        raise HTTPException(status_code=403, detail="Forbidden")

    rows = db.execute(
        text(
            """
            SELECT
                m.id,
                m.user_id,
                m.role_id,
                m.status,
                u.email,
                u.first_name,
                u.last_name,
                u.display_name,
                u.status AS user_status,
                r.name AS role_name_raw,
                r.level AS role_level,
                r.label AS role_label
            FROM memberships_business_unit m
            INNER JOIN users u ON u.id = m.user_id
            LEFT JOIN roles r ON r.id = m.role_id
            WHERE m.business_unit_id = :unit_id
            ORDER BY m.id
            """
        ),
        {"unit_id": int(unit_id)},
    ).mappings().all()

    data: list[dict] = []
    current_membership = None

    for row in rows:
        role_payload = None
        if row.get("role_id") is not None:
            role_payload = {
                "id": int(row.get("role_id")),
                "name": str(row.get("role_name_raw") or ""),
                "level": int(row.get("role_level") or 0),
                "role_name": _role_name({"name": row.get("role_name_raw"), "label": row.get("role_label")}),
            }

        item = {
            "id": int(row.get("id")),
            "status": str(row.get("status") or "active"),
            "user": {
                "id": int(row.get("user_id")),
                "email": row.get("email"),
                "display_name": _display_name_from_user(row),
                "status": row.get("user_status"),
            },
            "role": role_payload,
        }

        data.append(item)

        if int(row.get("user_id") or 0) == actor_id:
            current_membership = item

    return {
        "data": data,
        "meta": {
            "can_pick_active_users": bool(abilities.get("can_pick_active_users")),
            "current_membership": current_membership,
        },
    }


@router.get("/users/active")
def users_active(
    request: Request,
    q: str | None = Query(None),
    page: int | None = Query(1),
    per_page: int | None = Query(20),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request, authorization, db)

    if "unit.members.invite" not in _permissions_set(auth_payload):
        raise HTTPException(status_code=403, detail="Forbidden")

    page_number = _normalize_page(page)
    page_size = _normalize_per_page(per_page, 20)
    offset = (page_number - 1) * page_size

    where_parts = ["realm = 'admin'", "status = 'active'", "deleted_at IS NULL"]
    params: dict[str, object] = {
        "limit": page_size,
        "offset": offset,
    }

    q_text = str(q or "").strip()
    if q_text:
        where_parts.append("(email LIKE :q OR first_name LIKE :q OR last_name LIKE :q OR display_name LIKE :q)")
        params["q"] = f"%{q_text}%"

    where_sql = " AND ".join(where_parts)

    total_row = db.execute(
        text(f"SELECT COUNT(*) AS c FROM users WHERE {where_sql}"),
        params,
    ).mappings().first()
    total = int(total_row.get("c") or 0) if total_row else 0

    rows = db.execute(
        text(
            f"""
            SELECT id, email, first_name, last_name, display_name, status
            FROM users
            WHERE {where_sql}
            ORDER BY email, id
            LIMIT :limit OFFSET :offset
            """
        ),
        params,
    ).mappings().all()

    data = [
        {
            "id": int(row.get("id")),
            "email": row.get("email"),
            "display_name": _display_name_from_user(row),
            "status": row.get("status"),
        }
        for row in rows
    ]

    return {
        "data": data,
        "meta": {
            "pagination": _pagination_meta(page=page_number, per_page=page_size, total=total),
        },
    }


@router.post("/units/{unit_id:int}/members")
async def member_link(
    unit_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    actor_id = int(auth_payload.get("id") or 0)

    unit = _fetch_unit(db, int(unit_id))
    if not unit:
        raise HTTPException(status_code=404, detail="Not Found")

    levels = _permission_levels_for_unit(db, actor_id, _global_permission_levels(auth_payload), int(unit_id))
    abilities = _abilities_from_levels(levels)
    if not abilities.get("can_access") or not abilities.get("can_members_invite"):
        raise HTTPException(status_code=403, detail="Forbidden")

    if str(unit.get("type") or "") == "freelance":
        return _json_422("Freelance no permite vincular miembros.")

    payload = await request.json()
    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.", {"body": ["Formato de payload invalido."]})

    mode = str(payload.get("mode") or "").strip()
    if mode not in {"email", "user_id"}:
        return _json_422("The given data was invalid.", {"mode": ["Modo invalido."]})

    role_id = payload.get("role_id")
    if role_id in [None, ""]:
        return _json_422("The given data was invalid.", {"role_id": ["Rol requerido."]})

    role = db.execute(
        text(
            """
            SELECT id
            FROM roles
            WHERE id = :role_id
              AND guard_name = 'admin'
              AND scope = 'unit'
            LIMIT 1
            """
        ),
        {"role_id": int(role_id)},
    ).mappings().first()
    if not role:
        return _json_422("Rol inválido.")

    manageable_roles = _roles_manageable_for_unit(db, actor_id=actor_id, unit_id=int(unit_id), levels=levels)
    if not manageable_roles:
        raise HTTPException(status_code=403, detail="No tienes permisos para asignar roles en esta unidad.")

    manageable_role_ids = {int(item.get("id") or 0) for item in manageable_roles}
    if int(role.get("id") or 0) not in manageable_role_ids:
        return _json_422("Rol inválido o fuera de alcance.")

    target_user = None
    if mode == "user_id":
        if not abilities.get("can_pick_active_users"):
            raise HTTPException(status_code=403, detail="Forbidden")

        user_id = payload.get("user_id")
        if user_id in [None, ""]:
            return _json_422("The given data was invalid.", {"user_id": ["user_id requerido."]})

        target_user = db.execute(
            text("SELECT id, status FROM users WHERE id = :id AND deleted_at IS NULL LIMIT 1"),
            {"id": int(user_id)},
        ).mappings().first()
    else:
        email = str(payload.get("email") or "").strip().lower()
        if not email:
            return _json_422("Email requerido.")

        target_user = db.execute(
            text("SELECT id, status FROM users WHERE email = :email AND deleted_at IS NULL LIMIT 1"),
            {"email": email},
        ).mappings().first()

    if not target_user:
        return _json_422("No existe un usuario con ese email.")

    if str(target_user.get("status") or "") != "active":
        return _json_422("Usuario no activo.")

    exists = db.execute(
        text(
            """
            SELECT 1
            FROM memberships_business_unit
            WHERE business_unit_id = :unit_id
              AND user_id = :user_id
            LIMIT 1
            """
        ),
        {"unit_id": int(unit_id), "user_id": int(target_user.get("id"))},
    ).mappings().first()

    if exists:
        return _json_422("El usuario ya es miembro.")

    db.execute(
        text(
            """
            INSERT INTO memberships_business_unit (business_unit_id, user_id, role_id, status, created_at, updated_at)
            VALUES (:unit_id, :user_id, :role_id, 'active', NOW(), NOW())
            """
        ),
        {
            "unit_id": int(unit_id),
            "user_id": int(target_user.get("id")),
            "role_id": int(role.get("id")),
        },
    )
    db.commit()

    return {"message": "Vinculado."}


@router.post("/units/{unit_id:int}/members/create-user")
async def member_create_user(
    unit_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    actor_id = int(auth_payload.get("id") or 0)

    unit = _fetch_unit(db, int(unit_id))
    if not unit:
        raise HTTPException(status_code=404, detail="Not Found")

    if str(unit.get("type") or "") == "freelance":
        return _json_422("Freelance no permite crear usuarios desde esta unidad.")

    levels = _permission_levels_for_unit(db, actor_id, _global_permission_levels(auth_payload), int(unit_id))
    abilities = _abilities_from_levels(levels)
    if not abilities.get("can_access"):
        raise HTTPException(status_code=403, detail="Forbidden")

    payload = await request.json()
    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.", {"body": ["Formato de payload invalido."]})

    first_name = str(payload.get("first_name") or "").strip()
    last_name = str(payload.get("last_name") or "").strip()
    email = str(payload.get("email") or "").strip().lower()
    role_id = payload.get("role_id")

    if not first_name or not last_name or not email or role_id in [None, ""]:
        return _json_422("The given data was invalid.")

    manageable_roles = _roles_manageable_for_unit(db, actor_id=actor_id, unit_id=int(unit_id), levels=levels)
    if not manageable_roles:
        raise HTTPException(status_code=403, detail="No tienes permisos para asignar roles en esta unidad.")

    role = next((item for item in manageable_roles if int(item.get("id") or 0) == int(role_id)), None)
    if not role:
        return _json_422("Rol inválido o fuera de alcance.")

    existing_email = db.execute(
        text("SELECT id FROM users WHERE email = :email AND deleted_at IS NULL LIMIT 1"),
        {"email": email},
    ).mappings().first()
    if existing_email:
        return _json_422("Ya existe un usuario con ese email.")

    db.execute(
        text(
            """
            INSERT INTO users (
                realm, email, password, force_password_change,
                first_name, last_name, display_name, status,
                locale, timezone, created_at, updated_at
            ) VALUES (
                'admin', :email, '', 0,
                :first_name, :last_name, :display_name, 'active',
                'es', 'America/Santiago', NOW(), NOW()
            )
            """
        ),
        {
            "email": email,
            "first_name": first_name,
            "last_name": last_name,
            "display_name": f"{first_name} {last_name}".strip(),
        },
    )
    user_id_row = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    if not user_id_row:
        db.rollback()
        raise HTTPException(status_code=500, detail="No se pudo crear el usuario.")

    new_user_id = int(user_id_row.get("id"))

    db.execute(
        text(
            """
            INSERT INTO memberships_business_unit (business_unit_id, user_id, role_id, status, created_at, updated_at)
            VALUES (:unit_id, :user_id, :role_id, 'active', NOW(), NOW())
            """
        ),
        {
            "unit_id": int(unit_id),
            "user_id": new_user_id,
            "role_id": int(role.get("id")),
        },
    )
    membership_id_row = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    db.commit()

    role_payload = {
        "id": int(role.get("id") or 0),
        "name": str(role.get("name") or ""),
        "level": int(role.get("level") or 0),
        "role_name": _role_name(role),
    }

    return {
        "message": "Usuario creado y vinculado.",
        "data": {
            "membership": {
                "id": int(membership_id_row.get("id") or 0) if membership_id_row else None,
                "status": "active",
                "user": {
                    "id": new_user_id,
                    "email": email,
                    "display_name": f"{first_name} {last_name}".strip(),
                    "status": "active",
                },
                "role": role_payload,
            }
        },
    }


@router.patch("/units/{unit_id:int}/members/{membership_id:int}")
async def member_update_role(
    unit_id: int,
    membership_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    actor_id = int(auth_payload.get("id") or 0)

    unit = _fetch_unit(db, int(unit_id))
    if not unit:
        raise HTTPException(status_code=404, detail="Not Found")

    if str(unit.get("type") or "") == "freelance":
        return _json_422("Freelance no permite cambiar roles.")

    membership = db.execute(
        text(
            """
            SELECT m.id, m.business_unit_id, m.user_id, m.role_id,
                   r.level AS target_role_level
            FROM memberships_business_unit m
            LEFT JOIN roles r ON r.id = m.role_id
            WHERE m.id = :membership_id
            LIMIT 1
            """
        ),
        {"membership_id": int(membership_id)},
    ).mappings().first()

    if not membership or int(membership.get("business_unit_id") or 0) != int(unit_id):
        raise HTTPException(status_code=404, detail="Not Found")

    levels = _permission_levels_for_unit(db, actor_id, _global_permission_levels(auth_payload), int(unit_id))
    abilities = _abilities_from_levels(levels)
    if not abilities.get("can_access"):
        raise HTTPException(status_code=403, detail="Forbidden")

    manage_level = _permission_level(levels, "unit.members.manage_roles")
    if manage_level <= _LEVEL_NONE:
        raise HTTPException(status_code=403, detail="Forbidden")

    payload = await request.json()
    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.")

    role_id = payload.get("role_id")
    if role_id in [None, ""]:
        return _json_422("The given data was invalid.", {"role_id": ["Rol requerido."]})

    new_role = db.execute(
        text(
            """
            SELECT id, level
            FROM roles
            WHERE id = :role_id
              AND guard_name = 'admin'
              AND scope = 'unit'
            LIMIT 1
            """
        ),
        {"role_id": int(role_id)},
    ).mappings().first()

    if not new_role:
        return _json_422("Rol inválido.")

    if manage_level == _LEVEL_LOCAL:
        if int(membership.get("user_id") or 0) == actor_id:
            return _json_422("No puedes cambiar tu rol con permisos solo locales.")

        my_membership = _fetch_membership_for_user_unit(db, user_id=actor_id, unit_id=int(unit_id))
        if not my_membership or my_membership.get("role_level") is None:
            return _json_422("Membresía inválida.")

        my_level = int(my_membership.get("role_level") or 0)
        target_level = int(membership.get("target_role_level") or 999999)
        new_level = int(new_role.get("level") or 999999)

        if target_level < my_level:
            return _json_422("No puedes administrar un usuario con rol más importante que el tuyo.")

        if new_level < my_level:
            return _json_422("No puedes asignar un rol más importante que el tuyo.")

    db.execute(
        text("UPDATE memberships_business_unit SET role_id = :role_id, updated_at = NOW() WHERE id = :membership_id"),
        {"role_id": int(new_role.get("id")), "membership_id": int(membership_id)},
    )
    db.commit()

    return {"message": "Rol actualizado."}


@router.patch("/units/{unit_id:int}/members/{membership_id:int}/status")
async def member_update_status(
    unit_id: int,
    membership_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    actor_id = int(auth_payload.get("id") or 0)

    unit = _fetch_unit(db, int(unit_id))
    if not unit:
        raise HTTPException(status_code=404, detail="Not Found")

    if str(unit.get("type") or "") == "freelance":
        return _json_422("Freelance no permite cambiar estado de membresías.")

    membership = db.execute(
        text(
            """
            SELECT m.id, m.business_unit_id, m.user_id, m.role_id, m.status,
                   r.level AS target_role_level
            FROM memberships_business_unit m
            LEFT JOIN roles r ON r.id = m.role_id
            WHERE m.id = :membership_id
            LIMIT 1
            """
        ),
        {"membership_id": int(membership_id)},
    ).mappings().first()

    if not membership or int(membership.get("business_unit_id") or 0) != int(unit_id):
        raise HTTPException(status_code=404, detail="Not Found")

    levels = _permission_levels_for_unit(db, actor_id, _global_permission_levels(auth_payload), int(unit_id))
    abilities = _abilities_from_levels(levels)
    if not abilities.get("can_access"):
        raise HTTPException(status_code=403, detail="Forbidden")

    manage_level = _permission_level(levels, "unit.members.manage_roles")
    if manage_level <= _LEVEL_NONE:
        raise HTTPException(status_code=403, detail="Forbidden")

    payload = await request.json()
    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.")

    next_status = str(payload.get("status") or "").strip()
    if next_status not in _ALLOWED_STATUS:
        return _json_422("The given data was invalid.", {"status": ["Estado invalido."]})

    if manage_level == _LEVEL_LOCAL:
        if int(membership.get("user_id") or 0) == actor_id:
            return _json_422("No puedes cambiar el estado de tu propia membresía con permisos solo locales.")

        my_membership = _fetch_membership_for_user_unit(db, user_id=actor_id, unit_id=int(unit_id))
        if my_membership and my_membership.get("role_level") is not None and membership.get("target_role_level") is not None:
            my_level = int(my_membership.get("role_level") or 0)
            target_level = int(membership.get("target_role_level") or 0)
            if target_level < my_level:
                return _json_422("No puedes administrar un usuario con rol más importante que el tuyo.")

    db.execute(
        text("UPDATE memberships_business_unit SET status = :status, updated_at = NOW() WHERE id = :membership_id"),
        {"status": next_status, "membership_id": int(membership_id)},
    )
    db.commit()

    label = "inactiva" if next_status == "inactive" else "activa"
    return {"message": f"Membresía marcada como {label}."}


@router.delete("/units/{unit_id:int}/members/{membership_id:int}")
def member_remove(
    unit_id: int,
    membership_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    actor_id = int(auth_payload.get("id") or 0)

    unit = _fetch_unit(db, int(unit_id))
    if not unit:
        raise HTTPException(status_code=404, detail="Not Found")

    if str(unit.get("type") or "") == "freelance":
        return _json_422("Freelance no permite desvincular miembros.")

    membership = db.execute(
        text(
            """
            SELECT m.id, m.business_unit_id, m.user_id, m.role_id,
                   r.level AS target_role_level
            FROM memberships_business_unit m
            LEFT JOIN roles r ON r.id = m.role_id
            WHERE m.id = :membership_id
            LIMIT 1
            """
        ),
        {"membership_id": int(membership_id)},
    ).mappings().first()

    if not membership or int(membership.get("business_unit_id") or 0) != int(unit_id):
        raise HTTPException(status_code=404, detail="Not Found")

    levels = _permission_levels_for_unit(db, actor_id, _global_permission_levels(auth_payload), int(unit_id))
    abilities = _abilities_from_levels(levels)
    if not abilities.get("can_access"):
        raise HTTPException(status_code=403, detail="Forbidden")

    remove_level = _permission_level(levels, "unit.members.remove")
    if remove_level <= _LEVEL_NONE:
        raise HTTPException(status_code=403, detail="Forbidden")

    if int(membership.get("user_id") or 0) == actor_id and remove_level <= _LEVEL_LOCAL:
        return _json_422("No puedes removerte con permisos solo locales.")

    if remove_level == _LEVEL_LOCAL:
        my_membership = _fetch_membership_for_user_unit(db, user_id=actor_id, unit_id=int(unit_id))
        if my_membership and my_membership.get("role_level") is not None and membership.get("target_role_level") is not None:
            my_level = int(my_membership.get("role_level") or 0)
            target_level = int(membership.get("target_role_level") or 0)
            if target_level < my_level:
                return _json_422("No puedes administrar un usuario con rol más importante que el tuyo.")

    db.execute(text("DELETE FROM memberships_business_unit WHERE id = :membership_id"), {"membership_id": int(membership_id)})
    db.commit()

    return {"message": "Membresía eliminada."}


@router.get("/units/{unit_id:int}/gsa-commissions")
def gsa_commissions(
    unit_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request, authorization, db)
    actor_id = int(auth_payload.get("id") or 0)

    unit = _fetch_unit(db, int(unit_id))
    if not unit:
        raise HTTPException(status_code=404, detail="Not Found")

    levels = _permission_levels_for_unit(db, actor_id, _global_permission_levels(auth_payload), int(unit_id))
    abilities = _abilities_from_levels(levels)
    if not abilities.get("can_edit_gsa_commission"):
        raise HTTPException(status_code=403, detail="Forbidden")

    rows = db.execute(
        text(
            """
            SELECT id, beneficiary_user_id, commission
            FROM regalias
            WHERE source_type = 'unit'
              AND source_id = :unit_id
            ORDER BY id
            """
        ),
        {"unit_id": int(unit_id)},
    ).mappings().all()

    beneficiary_ids = [int(row.get("beneficiary_user_id")) for row in rows if row.get("beneficiary_user_id") is not None]

    users_by_id: dict[int, dict] = {}
    if beneficiary_ids:
        placeholders = ",".join(f":uid_{idx}" for idx, _ in enumerate(beneficiary_ids))
        params = {f"uid_{idx}": int(value) for idx, value in enumerate(beneficiary_ids)}
        user_rows = db.execute(
            text(
                f"""
                SELECT id, email, first_name, last_name, display_name, status
                FROM users
                WHERE id IN ({placeholders})
                """
            ),
            params,
        ).mappings().all()
        for user_row in user_rows:
            uid = int(user_row.get("id"))
            users_by_id[uid] = {
                "id": uid,
                "email": user_row.get("email"),
                "display_name": _display_name_from_user(user_row),
                "status": user_row.get("status"),
            }

    data: list[dict] = []
    for row in rows:
        beneficiary_id = int(row.get("beneficiary_user_id") or 0)
        data.append(
            {
                "id": int(row.get("id") or 0),
                "business_unit_id": int(unit_id),
                "user_id": beneficiary_id,
                "commission": float(row.get("commission") or 0),
                "user": users_by_id.get(beneficiary_id),
            }
        )

    return {"data": data}


@router.get("/units/{unit_id:int}/gsa-commissions/available")
def gsa_commissions_available(
    unit_id: int,
    request: Request,
    q: str | None = Query(None),
    page: int | None = Query(1),
    per_page: int | None = Query(20),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request, authorization, db)
    actor_id = int(auth_payload.get("id") or 0)

    unit = _fetch_unit(db, int(unit_id))
    if not unit:
        raise HTTPException(status_code=404, detail="Not Found")

    levels = _permission_levels_for_unit(db, actor_id, _global_permission_levels(auth_payload), int(unit_id))
    abilities = _abilities_from_levels(levels)
    if not abilities.get("can_edit_gsa_commission"):
        raise HTTPException(status_code=403, detail="Forbidden")

    page_number = _normalize_page(page)
    page_size = _normalize_per_page(per_page, 20)
    offset = (page_number - 1) * page_size

    where_parts = ["realm = 'admin'", "status = 'active'", "deleted_at IS NULL"]
    params: dict[str, object] = {"limit": page_size, "offset": offset}

    q_text = str(q or "").strip()
    if q_text:
        where_parts.append("(email LIKE :q OR first_name LIKE :q OR last_name LIKE :q OR display_name LIKE :q)")
        params["q"] = f"%{q_text}%"

    where_sql = " AND ".join(where_parts)

    total_row = db.execute(
        text(f"SELECT COUNT(*) AS c FROM users WHERE {where_sql}"),
        params,
    ).mappings().first()
    total = int(total_row.get("c") or 0) if total_row else 0

    users = db.execute(
        text(
            f"""
            SELECT id, email, first_name, last_name, display_name, status
            FROM users
            WHERE {where_sql}
            ORDER BY email, id
            LIMIT :limit OFFSET :offset
            """
        ),
        params,
    ).mappings().all()

    user_ids = [int(row.get("id")) for row in users]
    existing_map: dict[int, dict] = {}
    if user_ids:
        placeholders = ",".join(f":uid_{idx}" for idx, _ in enumerate(user_ids))
        query_params = {f"uid_{idx}": int(value) for idx, value in enumerate(user_ids)}
        query_params["unit_id"] = int(unit_id)

        existing_rows = db.execute(
            text(
                f"""
                SELECT id, beneficiary_user_id, commission
                FROM regalias
                WHERE source_type = 'unit'
                  AND source_id = :unit_id
                  AND beneficiary_user_id IN ({placeholders})
                """
            ),
            query_params,
        ).mappings().all()

        for row in existing_rows:
            existing_map[int(row.get("beneficiary_user_id"))] = dict(row)

    data: list[dict] = []
    for user_row in users:
        uid = int(user_row.get("id"))
        existing = existing_map.get(uid)
        data.append(
            {
                "id": uid,
                "email": user_row.get("email"),
                "display_name": _display_name_from_user(user_row),
                "status": user_row.get("status"),
                "is_assigned": existing is not None,
                "commission_user_id": int(existing.get("id")) if existing else None,
                "commission": float(existing.get("commission") or 0) if existing else None,
            }
        )

    return {
        "data": data,
        "meta": {
            "pagination": {
                **_pagination_meta(page=page_number, per_page=page_size, total=total),
                "from": (offset + 1) if total > 0 and data else 0,
                "to": (offset + len(data)) if total > 0 and data else 0,
            }
        },
    }


@router.post("/units/{unit_id:int}/gsa-commissions")
async def gsa_commissions_store(
    unit_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    actor_id = int(auth_payload.get("id") or 0)

    unit = _fetch_unit(db, int(unit_id))
    if not unit:
        raise HTTPException(status_code=404, detail="Not Found")

    levels = _permission_levels_for_unit(db, actor_id, _global_permission_levels(auth_payload), int(unit_id))
    abilities = _abilities_from_levels(levels)
    if not abilities.get("can_edit_gsa_commission"):
        raise HTTPException(status_code=403, detail="Forbidden")

    payload = await request.json()
    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.")

    user_id = payload.get("user_id")
    if user_id in [None, ""]:
        return _json_422("The given data was invalid.", {"user_id": ["user_id requerido."]})

    target_user = db.execute(
        text("SELECT id, status, email, first_name, last_name, display_name FROM users WHERE id = :id AND deleted_at IS NULL LIMIT 1"),
        {"id": int(user_id)},
    ).mappings().first()
    if not target_user:
        return _json_422("Usuario inválido.")

    if str(target_user.get("status") or "") != "active":
        return _json_422("Usuario no activo.")

    exists = db.execute(
        text(
            """
            SELECT 1
            FROM regalias
            WHERE source_type = 'unit'
              AND source_id = :unit_id
              AND beneficiary_user_id = :beneficiary_user_id
            LIMIT 1
            """
        ),
        {"unit_id": int(unit_id), "beneficiary_user_id": int(target_user.get("id"))},
    ).mappings().first()

    if exists:
        return _json_422("Ya existe una regalía para este beneficiario y esta unidad.")

    if _would_create_unit_redundancy_cycle_for_unit_regalia(
        db,
        beneficiary_user_id=int(target_user.get("id")),
        unit_id=int(unit_id),
    ):
        return _json_422("La unidad seleccionada genera una redundancia en la jerarquía de unidades para este beneficiario y no es válida.")

    db.execute(
        text(
            """
            INSERT INTO regalias (beneficiary_user_id, source_type, source_id, commission, created_at, updated_at)
            VALUES (:beneficiary_user_id, 'unit', :source_id, 0, NOW(), NOW())
            """
        ),
        {
            "beneficiary_user_id": int(target_user.get("id")),
            "source_id": int(unit_id),
        },
    )
    regalia_row = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    db.commit()

    regalia_id = int(regalia_row.get("id") or 0) if regalia_row else 0

    return {
        "message": "Usuario añadido a las regalias de la unidad.",
        "data": {
            "id": regalia_id,
            "business_unit_id": int(unit_id),
            "user_id": int(target_user.get("id")),
            "commission": 0.0,
            "user": {
                "id": int(target_user.get("id")),
                "email": target_user.get("email"),
                "display_name": _display_name_from_user(target_user),
                "status": target_user.get("status"),
            },
        },
    }


@router.patch("/units/{unit_id:int}/gsa-commissions/{commission_user_id:int}")
async def gsa_commissions_update(
    unit_id: int,
    commission_user_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    actor_id = int(auth_payload.get("id") or 0)

    unit = _fetch_unit(db, int(unit_id))
    if not unit:
        raise HTTPException(status_code=404, detail="Not Found")

    levels = _permission_levels_for_unit(db, actor_id, _global_permission_levels(auth_payload), int(unit_id))
    abilities = _abilities_from_levels(levels)
    if not abilities.get("can_edit_gsa_commission"):
        raise HTTPException(status_code=403, detail="Forbidden")

    regalia = db.execute(
        text(
            """
            SELECT id, beneficiary_user_id, source_type, source_id, commission
            FROM regalias
            WHERE id = :id
            LIMIT 1
            """
        ),
        {"id": int(commission_user_id)},
    ).mappings().first()

    if not regalia or str(regalia.get("source_type") or "") != "unit" or int(regalia.get("source_id") or 0) != int(unit_id):
        raise HTTPException(status_code=404, detail="Not Found")

    payload = await request.json()
    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.")

    commission_raw = payload.get("commission")
    if commission_raw in [None, ""]:
        commission_value = 0.0
    else:
        try:
            commission_value = float(commission_raw)
        except (TypeError, ValueError):
            return _json_422("The given data was invalid.", {"commission": ["Comision invalida."]})

    commission_value = max(0.0, min(100.0, commission_value))

    db.execute(
        text("UPDATE regalias SET commission = :commission, updated_at = NOW() WHERE id = :id"),
        {"commission": commission_value, "id": int(commission_user_id)},
    )
    db.commit()

    beneficiary = db.execute(
        text("SELECT id, email, first_name, last_name, display_name, status FROM users WHERE id = :id LIMIT 1"),
        {"id": int(regalia.get("beneficiary_user_id") or 0)},
    ).mappings().first()

    return {
        "message": "Comisión actualizada.",
        "data": {
            "id": int(commission_user_id),
            "business_unit_id": int(unit_id),
            "user_id": int(regalia.get("beneficiary_user_id") or 0),
            "commission": float(commission_value),
            "user": {
                "id": int(beneficiary.get("id")),
                "email": beneficiary.get("email"),
                "display_name": _display_name_from_user(beneficiary),
                "status": beneficiary.get("status"),
            }
            if beneficiary
            else None,
        },
    }


@router.delete("/units/{unit_id:int}/gsa-commissions/{commission_user_id:int}")
def gsa_commissions_destroy(
    unit_id: int,
    commission_user_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    actor_id = int(auth_payload.get("id") or 0)

    unit = _fetch_unit(db, int(unit_id))
    if not unit:
        raise HTTPException(status_code=404, detail="Not Found")

    levels = _permission_levels_for_unit(db, actor_id, _global_permission_levels(auth_payload), int(unit_id))
    abilities = _abilities_from_levels(levels)
    if not abilities.get("can_edit_gsa_commission"):
        raise HTTPException(status_code=403, detail="Forbidden")

    regalia = db.execute(
        text(
            """
            SELECT id, beneficiary_user_id, source_type, source_id
            FROM regalias
            WHERE id = :id
            LIMIT 1
            """
        ),
        {"id": int(commission_user_id)},
    ).mappings().first()

    if not regalia or str(regalia.get("source_type") or "") != "unit" or int(regalia.get("source_id") or 0) != int(unit_id):
        raise HTTPException(status_code=404, detail="Not Found")

    payload = {
        "id": int(regalia.get("id") or 0),
        "business_unit_id": int(unit_id),
        "user_id": int(regalia.get("beneficiary_user_id") or 0),
    }

    db.execute(text("DELETE FROM regalias WHERE id = :id"), {"id": int(commission_user_id)})
    db.commit()

    return {
        "message": "Usuario removido de las regalias de la unidad.",
        "data": payload,
    }
