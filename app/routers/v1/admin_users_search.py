import secrets
import string
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.database import get_db
from app.routers.v1.auth_cookies import (
    ACCESS_COOKIE_MAX_AGE_SECONDS,
    ACCESS_COOKIE_NAME,
    IMPERSONATION_COOKIE_MAX_AGE_SECONDS,
    IMPERSONATION_META_COOKIE_NAME,
    IMPERSONATOR_ACCESS_COOKIE_NAME,
    IMPERSONATOR_REFRESH_COOKIE_NAME,
    REFRESH_COOKIE_MAX_AGE_SECONDS,
    REFRESH_COOKIE_NAME,
    encode_impersonation_meta,
    set_auth_cookie,
)
from app.services.auth_service import AuthService, pwd_context

router = APIRouter(prefix="/api/v1/admin/users", tags=["admin-users"])
settings = get_settings()

_STATUSES = [
    {"value": "active", "label": "Activo"},
    {"value": "suspended", "label": "Suspendido"},
    {"value": "locked", "label": "Bloqueado"},
]


def _extract_bearer_token(authorization: str | None) -> str:
    if not authorization:
        return ""

    raw = authorization.strip()
    if not raw.lower().startswith("bearer "):
        return ""

    return raw[7:].strip()


def _json_422(message: str, errors: dict[str, list[str]] | None = None) -> JSONResponse:
    payload = {"message": message}
    if errors:
        payload["errors"] = errors
    return JSONResponse(status_code=422, content=payload)


def _auth_payload(request: Request, authorization: str | None, db: Session) -> dict:
    token = _extract_bearer_token(authorization)
    if not token:
        token = str(request.cookies.get("yastubo_access_token") or "").strip()

    if not token:
        raise HTTPException(status_code=403, detail="Forbidden")

    return AuthService(db).me(token)


def _permissions(auth_payload: dict) -> list[str]:
    return [str(item) for item in (auth_payload.get("permissions") or []) if item]


def _require_any_permission(auth_payload: dict, *required: str) -> list[str]:
    permissions = _permissions(auth_payload)
    if not any(permission in permissions for permission in required):
        raise HTTPException(status_code=403, detail="Forbidden")
    return permissions


def _resolve_frontend_origin(request: Request) -> str:
    origin = str(request.headers.get("origin") or "").strip().rstrip("/")
    if origin:
        return origin

    referer = str(request.headers.get("referer") or "").strip()
    if referer:
        parts = urlsplit(referer)
        if parts.scheme and parts.netloc:
            return f"{parts.scheme}://{parts.netloc}"

    fallback = str(settings.frontend_admin_legacy_base_url or "").strip().rstrip("/")
    if fallback:
        return fallback

    return ""


def _format_datetime(value) -> str | None:
    if value is None:
        return None

    if hasattr(value, "isoformat"):
        return value.isoformat()

    raw = str(value).strip()
    return raw or None


def _load_available_roles(db: Session) -> list[dict]:
    rows = db.execute(
        text(
            """
            SELECT
                id,
                name,
                COALESCE(NULLIF(label, ''), name) AS label
            FROM roles
            WHERE guard_name = 'admin'
            ORDER BY COALESCE(NULLIF(label, ''), name) ASC, name ASC
            """
        )
    ).mappings().all()

    return [
        {
            "id": int(row["id"]),
            "name": str(row.get("name") or ""),
            "label": str(row.get("label") or row.get("name") or ""),
        }
        for row in rows
    ]


def _load_user_roles(db: Session, user_id: int) -> list[dict]:
    rows = db.execute(
        text(
            """
            SELECT
                r.id,
                r.name,
                COALESCE(NULLIF(r.label, ''), r.name) AS label
            FROM roles r
            INNER JOIN model_has_roles mhr
                ON mhr.role_id = r.id
               AND mhr.model_type LIKE :model_type_suffix
               AND mhr.model_id = :user_id
            WHERE r.guard_name = 'admin'
            ORDER BY COALESCE(NULLIF(r.label, ''), r.name) ASC, r.name ASC
            """
        ),
        {"model_type_suffix": "%User", "user_id": user_id},
    ).mappings().all()

    return [
        {
            "id": int(row["id"]),
            "name": str(row.get("name") or ""),
            "label": str(row.get("label") or row.get("name") or ""),
        }
        for row in rows
    ]


def _load_user_role_names(db: Session, user_id: int) -> set[str]:
    return {
        str(item.get("name") or "").strip().lower()
        for item in _load_user_roles(db, user_id)
        if item.get("name")
    }


def _load_user_permissions(db: Session, user_id: int) -> list[str]:
    rows = db.execute(
        text(
            """
            SELECT DISTINCT p.name
            FROM permissions p
            INNER JOIN model_has_permissions mhp
                ON mhp.permission_id = p.id
               AND mhp.model_type LIKE :model_type_suffix
               AND mhp.model_id = :user_id

            UNION

            SELECT DISTINCT p2.name
            FROM permissions p2
            INNER JOIN role_has_permissions rhp
                ON rhp.permission_id = p2.id
            INNER JOIN model_has_roles mhr
                ON mhr.role_id = rhp.role_id
               AND mhr.model_type LIKE :model_type_suffix
               AND mhr.model_id = :user_id

            ORDER BY name ASC
            """
        ),
        {"model_type_suffix": "%User", "user_id": user_id},
    ).mappings().all()

    return [str(row.get("name") or "") for row in rows if row.get("name")]


def _actor_capabilities(permissions: list[str]) -> dict:
    return {
        "can_view_any": "users.viewAny" in permissions or "users.view" in permissions,
        "can_update_email": "users.email.update" in permissions,
        "can_update_status": "users.status.update" in permissions,
        "can_assign_roles": "users.roles.assign" in permissions,
        "can_edit_commissions": "users.commissions.edit" in permissions,
        "can_revoke_sessions": "users.sessions.revoke" in permissions,
        "can_impersonate": "impersonate" in permissions or "users.impersonate" in permissions,
    }


def _target_capabilities(permissions: list[str]) -> dict:
    return {
        "can_regular_sales": "sales.regular.use" in permissions,
        "can_capitados_sales": "sales.capitados.use" in permissions,
    }


def _require_permission(auth_payload: dict, permission: str) -> list[str]:
    permissions = _permissions(auth_payload)
    if permission not in permissions:
        raise HTTPException(status_code=403, detail="Forbidden")
    return permissions


def _fetch_user_row(db: Session, user_id: int, *, include_deleted: bool = False):
    deleted_clause = "" if include_deleted else "AND u.deleted_at IS NULL"
    return db.execute(
        text(
            f"""
            SELECT
                u.id,
                u.realm,
                u.first_name,
                u.last_name,
                u.display_name,
                u.email,
                u.status,
                u.force_password_change,
                u.last_login_at,
                u.deleted_at,
                sp.work_phone,
                sp.notes_admin,
                sp.commission_regular_first_year_pct,
                sp.commission_regular_renewal_pct,
                sp.commission_capitados_pct
            FROM users u
            LEFT JOIN staff_profiles sp ON sp.user_id = u.id
            WHERE u.id = :user_id
              AND u.realm = 'admin'
              {deleted_clause}
            LIMIT 1
            """
        ),
        {"user_id": user_id},
    ).mappings().first()


def _email_exists(db: Session, email: str, *, exclude_user_id: int | None = None) -> bool:
    where_extra = ""
    params: dict[str, object] = {"email": email.strip().lower()}
    if exclude_user_id is not None:
        where_extra = "AND id != :exclude_user_id"
        params["exclude_user_id"] = int(exclude_user_id)

    row = db.execute(
        text(
            f"""
            SELECT id
            FROM users
            WHERE realm = 'admin'
              AND deleted_at IS NULL
              AND email = :email
              {where_extra}
            LIMIT 1
            """
        ),
        params,
    ).mappings().first()
    return bool(row)


def _make_temp_password(length: int = 12) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(max(8, length)))


def _hash_password(plain_password: str) -> str:
    try:
        return str(pwd_context.hash(plain_password))
    except Exception:
        import bcrypt

        return bcrypt.hashpw(plain_password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _allowed_admin_roles(db: Session) -> set[str]:
    return {item["name"] for item in _load_available_roles(db) if item.get("name")}


def _validate_roles_payload(
    db: Session,
    permissions: list[str],
    raw_roles,
) -> tuple[list[str], JSONResponse | None]:
    if raw_roles is None:
        return [], None

    if not isinstance(raw_roles, list):
        return [], _json_422("The given data was invalid.", {"roles": ["Formato invalido."]})

    if "users.roles.assign" not in permissions:
        return [], _json_422("No tienes permisos para asignar roles.")

    normalized_roles = []
    for item in raw_roles:
        role_name = str(item or "").strip()
        if role_name:
            normalized_roles.append(role_name)

    allowed_roles = _allowed_admin_roles(db)
    invalid_roles = [role for role in normalized_roles if role not in allowed_roles]
    if invalid_roles:
        return [], _json_422("The given data was invalid.", {"roles": ["Uno o más roles son inválidos."]})

    if "superadmin" in normalized_roles and "users.roles.assign-superadmin" not in permissions:
        return [], _json_422("No tienes permisos para asignar el rol superadmin.")

    return normalized_roles, None


def _validate_status_value(raw_status: object) -> tuple[str | None, JSONResponse | None]:
    status = str(raw_status or "").strip().lower()
    if not status:
        return None, _json_422("The given data was invalid.", {"status": ["Estado requerido."]})
    if status not in {"active", "suspended", "locked"}:
        return None, _json_422("The given data was invalid.", {"status": ["Estado invalido."]})
    return status, None


def _validate_commissions(payload: dict, roles: list[str], *, actor_can_edit: bool) -> JSONResponse | None:
    commission_fields = [
        "commission_regular_first_year_pct",
        "commission_regular_renewal_pct",
        "commission_capitados_pct",
    ]
    if not actor_can_edit:
        return None

    errors: dict[str, list[str]] = {}
    for field in commission_fields:
        raw = payload.get(field)
        if raw in (None, ""):
            continue
        try:
            numeric = float(raw)
        except (TypeError, ValueError):
            errors.setdefault(field, []).append("Debe ser un número válido.")
            continue
        if numeric < 0 or numeric > 100:
            errors.setdefault(field, []).append("Debe estar entre 0 y 100.")

    if "vendedor_regular" in roles:
        if payload.get("commission_regular_first_year_pct") in (None, ""):
            errors.setdefault("commission_regular_first_year_pct", []).append("Obligatorio para vendedor regular.")
        if payload.get("commission_regular_renewal_pct") in (None, ""):
            errors.setdefault("commission_regular_renewal_pct", []).append("Obligatorio para vendedor regular.")

    if "vendedor_capitados" in roles and payload.get("commission_capitados_pct") in (None, ""):
        errors.setdefault("commission_capitados_pct", []).append("Obligatorio para vendedor capitados.")

    if errors:
        return _json_422("The given data was invalid.", errors)
    return None


def _sync_user_roles(db: Session, user_id: int, roles: list[str]) -> None:
    db.execute(
        text(
            """
            DELETE FROM model_has_roles
            WHERE model_id = :user_id
              AND model_type LIKE :model_type_suffix
            """
        ),
        {"user_id": user_id, "model_type_suffix": "%User"},
    )

    if not roles:
        return

    for role_name in roles:
        row = db.execute(
            text(
                """
                SELECT id
                FROM roles
                WHERE guard_name = 'admin'
                  AND name = :role_name
                LIMIT 1
                """
            ),
            {"role_name": role_name},
        ).mappings().first()
        if not row:
            continue
        db.execute(
            text(
                """
                INSERT INTO model_has_roles (role_id, model_type, model_id)
                VALUES (:role_id, :model_type, :user_id)
                """
            ),
            {"role_id": int(row["id"]), "model_type": "App\\Models\\User", "user_id": user_id},
        )


def _upsert_staff_profile(db: Session, user_id: int, payload: dict, *, actor_can_edit_commissions: bool) -> None:
    exists = db.execute(
        text("SELECT user_id FROM staff_profiles WHERE user_id = :user_id LIMIT 1"),
        {"user_id": user_id},
    ).mappings().first()

    work_phone = payload.get("work_phone")
    notes_admin = payload.get("notes_admin")
    regular_first = payload.get("commission_regular_first_year_pct") if actor_can_edit_commissions else None
    regular_renewal = payload.get("commission_regular_renewal_pct") if actor_can_edit_commissions else None
    capitados = payload.get("commission_capitados_pct") if actor_can_edit_commissions else None

    if exists:
        if not actor_can_edit_commissions:
            db.execute(
                text(
                    """
                    UPDATE staff_profiles
                    SET work_phone = :work_phone,
                        notes_admin = :notes_admin,
                        updated_at = NOW()
                    WHERE user_id = :user_id
                    """
                ),
                {
                    "user_id": user_id,
                    "work_phone": work_phone,
                    "notes_admin": notes_admin,
                },
            )
            return

        db.execute(
            text(
                """
                UPDATE staff_profiles
                SET work_phone = :work_phone,
                    notes_admin = :notes_admin,
                    commission_regular_first_year_pct = :regular_first,
                    commission_regular_renewal_pct = :regular_renewal,
                    commission_capitados_pct = :capitados,
                    updated_at = NOW()
                WHERE user_id = :user_id
                """
            ),
            {
                "user_id": user_id,
                "work_phone": work_phone,
                "notes_admin": notes_admin,
                "regular_first": regular_first,
                "regular_renewal": regular_renewal,
                "capitados": capitados,
            },
        )
        return

    db.execute(
        text(
            """
            INSERT INTO staff_profiles (
                user_id,
                work_phone,
                notes_admin,
                commission_regular_first_year_pct,
                commission_regular_renewal_pct,
                commission_capitados_pct,
                created_at,
                updated_at
            ) VALUES (
                :user_id,
                :work_phone,
                :notes_admin,
                :regular_first,
                :regular_renewal,
                :capitados,
                NOW(),
                NOW()
            )
            """
        ),
        {
            "user_id": user_id,
            "work_phone": work_phone,
            "notes_admin": notes_admin,
            "regular_first": regular_first,
            "regular_renewal": regular_renewal,
            "capitados": capitados,
        },
    )


def _serialize_user_detail(db: Session, user_id: int, permissions: list[str], *, include_deleted: bool = False) -> dict:
    row = _fetch_user_row(db, user_id, include_deleted=include_deleted)
    if not row:
        raise HTTPException(status_code=404, detail="Not Found")

    assigned_roles = _load_user_roles(db, user_id)
    target_permissions = _load_user_permissions(db, user_id)

    return {
        "user": {
            "id": int(row["id"]),
            "realm": str(row.get("realm") or "admin"),
            "first_name": str(row.get("first_name") or ""),
            "last_name": str(row.get("last_name") or ""),
            "display_name": str(row.get("display_name") or "").strip(),
            "email": row.get("email"),
            "status": row.get("status"),
            "last_login_at": _format_datetime(row.get("last_login_at")),
        },
        "staff_profile": {
            "work_phone": row.get("work_phone"),
            "notes_admin": row.get("notes_admin"),
            "commission_regular_first_year_pct": row.get("commission_regular_first_year_pct"),
            "commission_regular_renewal_pct": row.get("commission_regular_renewal_pct"),
            "commission_capitados_pct": row.get("commission_capitados_pct"),
        },
        "assigned_roles": assigned_roles,
        "available_roles": _load_available_roles(db),
        "target_capabilities": _target_capabilities(target_permissions),
        "actor_capabilities": _actor_capabilities(permissions),
    }


@router.get("")
def list_users(
    request: Request,
    page: int = Query(1, ge=1),
    per_page: int = Query(15, ge=1, le=100),
    q: str = Query(""),
    status: str | None = Query(None),
    role: str | None = Query(None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request, authorization, db)
    permissions = _require_any_permission(auth_payload, "users.viewAny", "users.view")

    normalized_q = (q or "").strip()
    normalized_status = (status or "").strip()
    normalized_role = (role or "").strip()

    where_parts = ["u.realm = 'admin'", "u.deleted_at IS NULL"]
    params: dict[str, object] = {}

    if normalized_status:
        where_parts.append("u.status = :status")
        params["status"] = normalized_status

    if normalized_q:
        where_parts.append(
            "(" \
            "u.first_name LIKE :q " \
            "OR u.last_name LIKE :q " \
            "OR u.display_name LIKE :q " \
            "OR u.email LIKE :q" \
            ")"
        )
        params["q"] = f"%{normalized_q}%"

    role_join = ""
    if normalized_role:
        role_join = (
            " INNER JOIN model_has_roles mhr_filter"
            " ON mhr_filter.model_id = u.id"
            " AND mhr_filter.model_type LIKE :model_type_suffix"
            " INNER JOIN roles r_filter"
            " ON r_filter.id = mhr_filter.role_id"
            " AND r_filter.guard_name = 'admin'"
        )
        where_parts.append("r_filter.name = :role")
        params["role"] = normalized_role
        params["model_type_suffix"] = "%User"

    where_sql = " AND ".join(where_parts)

    total_row = db.execute(
        text(f"SELECT COUNT(DISTINCT u.id) AS c FROM users u{role_join} WHERE {where_sql}"),
        params,
    ).mappings().first()
    total = int(total_row["c"] if total_row else 0)

    offset = (page - 1) * per_page
    rows = db.execute(
        text(
            f"""
            SELECT DISTINCT
                u.id,
                u.first_name,
                u.last_name,
                u.display_name,
                u.email,
                u.status,
                u.last_login_at,
                CASE
                    WHEN u.display_name IS NOT NULL AND TRIM(u.display_name) <> '' THEN u.display_name
                    ELSE TRIM(CONCAT(COALESCE(u.first_name, ''), ' ', COALESCE(u.last_name, '')))
                END AS resolved_display_name
            FROM users u
            {role_join}
            WHERE {where_sql}
            ORDER BY resolved_display_name ASC, u.id ASC
            LIMIT :limit OFFSET :offset
            """
        ),
        {**params, "limit": per_page, "offset": offset},
    ).mappings().all()

    last_page = max(1, (total + per_page - 1) // per_page)
    from_item = (offset + 1) if total > 0 else 0
    to_item = min(offset + len(rows), total) if total > 0 else 0

    data = []
    for row in rows:
        user_id = int(row["id"])
        data.append(
            {
                "id": user_id,
                "first_name": str(row.get("first_name") or ""),
                "last_name": str(row.get("last_name") or ""),
                "display_name": str(row.get("resolved_display_name") or "").strip(),
                "email": row.get("email"),
                "status": row.get("status"),
                "last_login_at": _format_datetime(row.get("last_login_at")),
                "roles": _load_user_roles(db, user_id),
            }
        )

    return {
        "data": data,
        "meta": {
            "pagination": {
                "current_page": page,
                "last_page": last_page,
                "per_page": per_page,
                "total": total,
                "from": from_item,
                "to": to_item,
            },
            "filters": {
                "q": normalized_q,
                "status": normalized_status or None,
                "role": normalized_role or None,
            },
        },
        "catalogs": {
            "roles": _load_available_roles(db),
            "statuses": _STATUSES,
        },
        "actor_capabilities": _actor_capabilities(permissions),
    }


@router.get("/bootstrap")
def users_bootstrap(
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request, authorization, db)
    permissions = _require_any_permission(auth_payload, "users.viewAny", "users.view")

    return {
        "data": {
            "roles": _load_available_roles(db),
            "statuses": _STATUSES,
            "actor_capabilities": _actor_capabilities(permissions),
        }
    }


@router.get("/search")
def search_users(
    request: Request,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    q: str = Query(""),
    status: str | None = Query(None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request, authorization, db)
    _require_any_permission(auth_payload, "users.viewAny", "users.view")

    normalized_q = (q or "").strip()
    normalized_status = (status or "").strip()

    where_parts = ["realm = 'admin'"]
    params: dict[str, object] = {}

    if normalized_status:
        where_parts.append("status = :status")
        params["status"] = normalized_status

    if normalized_q:
        where_parts.append(
            "(" \
            "first_name LIKE :q " \
            "OR last_name LIKE :q " \
            "OR display_name LIKE :q " \
            "OR email LIKE :q" \
            ")"
        )
        params["q"] = f"%{normalized_q}%"

    where_sql = " AND ".join(where_parts)

    total_row = db.execute(
        text(f"SELECT COUNT(*) AS c FROM users WHERE {where_sql}"),
        params,
    ).mappings().first()
    total = int(total_row["c"] if total_row else 0)

    offset = (page - 1) * per_page
    rows = db.execute(
        text(
            f"""
            SELECT
                id,
                CASE
                    WHEN display_name IS NOT NULL AND TRIM(display_name) <> '' THEN display_name
                    ELSE TRIM(CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, '')))
                END AS display_name,
                email,
                status
            FROM users
            WHERE {where_sql}
            ORDER BY display_name ASC, id ASC
            LIMIT :limit OFFSET :offset
            """
        ),
        {**params, "limit": per_page, "offset": offset},
    ).mappings().all()

    last_page = max(1, (total + per_page - 1) // per_page)
    from_item = (offset + 1) if total > 0 else 0
    to_item = min(offset + len(rows), total) if total > 0 else 0

    data = [
        {
            "id": int(row["id"]),
            "display_name": str(row["display_name"] or "").strip(),
            "email": row["email"],
            "status": row["status"],
        }
        for row in rows
    ]

    return {
        "data": data,
        "meta": {
            "pagination": {
                "current_page": page,
                "last_page": last_page,
                "per_page": per_page,
                "total": total,
                "from": from_item,
                "to": to_item,
            }
        },
    }


@router.get("/{user_id:int}")
def show_user(
    user_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request, authorization, db)
    permissions = _require_any_permission(auth_payload, "users.viewAny", "users.view")

    return {"data": _serialize_user_detail(db, user_id, permissions)}


@router.post("")
async def create_user(
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request, authorization, db)
    permissions = _require_permission(auth_payload, "users.create")

    payload = await request.json()
    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.", {"body": ["Formato de payload invalido."]})

    first_name = str(payload.get("first_name") or "").strip()
    last_name = str(payload.get("last_name") or "").strip()
    display_name = str(payload.get("display_name") or "").strip()
    email = str(payload.get("email") or "").strip().lower()
    if not first_name:
        return _json_422("The given data was invalid.", {"first_name": ["Nombre requerido."]})
    if not email:
        return _json_422("The given data was invalid.", {"email": ["Email requerido."]})

    status, status_error = _validate_status_value(payload.get("status") or "active")
    if status_error:
        return status_error

    if _email_exists(db, email):
        return _json_422("The given data was invalid.", {"email": ["Ya existe un usuario con ese email."]})

    roles, roles_error = _validate_roles_payload(db, permissions, payload.get("roles"))
    if roles_error:
        return roles_error

    commissions_error = _validate_commissions(
        payload,
        roles,
        actor_can_edit="users.commissions.edit" in permissions,
    )
    if commissions_error:
        return commissions_error

    temp_password = _make_temp_password()
    password_hash = _hash_password(temp_password)
    resolved_display_name = display_name or f"{first_name} {last_name}".strip()

    db.execute(
        text(
            """
            INSERT INTO users (
                realm,
                email,
                password,
                force_password_change,
                first_name,
                last_name,
                display_name,
                status,
                created_at,
                updated_at
            ) VALUES (
                'admin',
                :email,
                :password,
                1,
                :first_name,
                :last_name,
                :display_name,
                :status,
                NOW(),
                NOW()
            )
            """
        ),
        {
            "email": email,
            "password": password_hash,
            "first_name": first_name,
            "last_name": last_name or None,
            "display_name": resolved_display_name or None,
            "status": status,
        },
    )
    user_id_row = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    user_id = int((user_id_row or {}).get("id") or 0)

    _sync_user_roles(db, user_id, roles)
    _upsert_staff_profile(
        db,
        user_id,
        payload,
        actor_can_edit_commissions="users.commissions.edit" in permissions,
    )
    db.commit()

    return {
        "message": "Usuario creado correctamente.",
        "data": {
            **_serialize_user_detail(db, user_id, permissions),
            "temporary_password": temp_password,
        },
    }


@router.put("/{user_id:int}")
async def update_user(
    user_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request, authorization, db)
    permissions = _require_permission(auth_payload, "users.update")

    current_row = _fetch_user_row(db, user_id)
    if not current_row:
        raise HTTPException(status_code=404, detail="Not Found")

    payload = await request.json()
    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.", {"body": ["Formato de payload invalido."]})

    first_name = str(payload.get("first_name") or current_row.get("first_name") or "").strip()
    last_name = str(payload.get("last_name") or "").strip()
    if not first_name:
        return _json_422("The given data was invalid.", {"first_name": ["Nombre requerido."]})
    if not last_name:
        return _json_422("The given data was invalid.", {"last_name": ["Apellido requerido."]})

    email = str(payload.get("email") or current_row.get("email") or "").strip().lower()
    if "users.email.update" not in permissions:
        email = str(current_row.get("email") or "").strip().lower()
    elif not email:
        return _json_422("The given data was invalid.", {"email": ["Email requerido."]})

    if _email_exists(db, email, exclude_user_id=user_id):
        return _json_422("The given data was invalid.", {"email": ["Ya existe un usuario con ese email."]})

    status = str(current_row.get("status") or "active")
    if "users.status.update" in permissions and "status" in payload:
        status, status_error = _validate_status_value(payload.get("status"))
        if status_error:
            return status_error

    roles = [item["name"] for item in _load_user_roles(db, user_id)]
    if "roles" in payload:
        roles, roles_error = _validate_roles_payload(db, permissions, payload.get("roles"))
        if roles_error:
            return roles_error

    commissions_error = _validate_commissions(
        payload,
        roles,
        actor_can_edit="users.commissions.edit" in permissions,
    )
    if commissions_error:
        return commissions_error

    display_name = str(payload.get("display_name") or current_row.get("display_name") or "").strip()

    db.execute(
        text(
            """
            UPDATE users
            SET first_name = :first_name,
                last_name = :last_name,
                display_name = :display_name,
                email = :email,
                status = :status,
                updated_at = NOW()
            WHERE id = :user_id
              AND realm = 'admin'
              AND deleted_at IS NULL
            """
        ),
        {
            "user_id": user_id,
            "first_name": first_name,
            "last_name": last_name,
            "display_name": display_name or None,
            "email": email,
            "status": status,
        },
    )

    if "users.roles.assign" in permissions and "roles" in payload:
        _sync_user_roles(db, user_id, roles)

    _upsert_staff_profile(
        db,
        user_id,
        {
            "work_phone": payload.get("work_phone", current_row.get("work_phone")),
            "notes_admin": payload.get("notes_admin", current_row.get("notes_admin")),
            "commission_regular_first_year_pct": payload.get(
                "commission_regular_first_year_pct",
                current_row.get("commission_regular_first_year_pct"),
            ),
            "commission_regular_renewal_pct": payload.get(
                "commission_regular_renewal_pct",
                current_row.get("commission_regular_renewal_pct"),
            ),
            "commission_capitados_pct": payload.get(
                "commission_capitados_pct",
                current_row.get("commission_capitados_pct"),
            ),
        },
        actor_can_edit_commissions="users.commissions.edit" in permissions,
    )

    revoked_sessions = 0
    if bool(payload.get("revoke_sessions")) and "users.sessions.revoke" in permissions:
        revoked_sessions = db.execute(
            text("DELETE FROM sessions WHERE user_id = :user_id"),
            {"user_id": user_id},
        ).rowcount or 0

    db.commit()

    return {
        "message": "Usuario actualizado correctamente.",
        "data": {
            **_serialize_user_detail(db, user_id, permissions),
            "revoked_sessions": int(revoked_sessions),
        },
    }


@router.delete("/{user_id:int}")
def delete_user(
    user_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request, authorization, db)
    _require_permission(auth_payload, "users.delete")

    actor_id = int(auth_payload.get("id") or 0)
    if actor_id == user_id:
        return _json_422("No puedes eliminar tu propia cuenta.")

    row = _fetch_user_row(db, user_id)
    if not row:
        raise HTTPException(status_code=404, detail="Not Found")

    db.execute(
        text(
            """
            UPDATE users
            SET deleted_at = NOW(),
                updated_at = NOW()
            WHERE id = :user_id
              AND realm = 'admin'
              AND deleted_at IS NULL
            """
        ),
        {"user_id": user_id},
    )
    db.commit()

    return {"message": "Usuario eliminado.", "data": {"id": user_id, "deleted": True}}


@router.post("/{user_id:int}/restore")
def restore_user(
    user_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request, authorization, db)
    permissions = _require_permission(auth_payload, "users.restore")

    row = _fetch_user_row(db, user_id, include_deleted=True)
    if not row:
        raise HTTPException(status_code=404, detail="Not Found")

    db.execute(
        text(
            """
            UPDATE users
            SET deleted_at = NULL,
                updated_at = NOW()
            WHERE id = :user_id
              AND realm = 'admin'
            """
        ),
        {"user_id": user_id},
    )
    db.commit()

    return {"message": "Usuario restaurado.", "data": _serialize_user_detail(db, user_id, permissions)}


@router.post("/{user_id:int}/sessions/revoke")
def revoke_user_sessions(
    user_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request, authorization, db)
    _require_permission(auth_payload, "users.sessions.revoke")
    _require_permission(auth_payload, "users.update")

    row = _fetch_user_row(db, user_id)
    if not row:
        raise HTTPException(status_code=404, detail="Not Found")

    revoked = db.execute(
        text("DELETE FROM sessions WHERE user_id = :user_id"),
        {"user_id": user_id},
    ).rowcount or 0
    db.commit()

    return {
        "ok": True,
        "message": f"Se revocaron {int(revoked)} sesiones del usuario.",
        "revoked": int(revoked),
    }


@router.post("/{user_id:int}/send-reset")
def send_user_reset_link(
    user_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request, authorization, db)
    _require_permission(auth_payload, "users.update")

    row = _fetch_user_row(db, user_id)
    if not row:
        raise HTTPException(status_code=404, detail="Not Found")

    frontend_origin = _resolve_frontend_origin(request)
    data = AuthService(db).send_admin_reset_link(user_id, frontend_origin)

    return {
        "ok": True,
        "message": "Correo de reset enviado.",
        "data": data,
    }


@router.post("/{user_id:int}/impersonate", response_model=None)
def impersonate_user(
    user_id: int,
    request: Request,
    response: Response,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_any_permission(auth_payload, "users.impersonate", "impersonate")

    actor_id = int(auth_payload.get("id") or 0)
    if actor_id <= 0:
        raise HTTPException(status_code=403, detail="Forbidden")

    if actor_id == int(user_id):
        return _json_422("No puedes impersonarte a ti mismo.")

    if str(request.cookies.get(IMPERSONATOR_REFRESH_COOKIE_NAME) or "").strip():
        return _json_422("Ya existe una impersonación activa. Debes salir antes de iniciar otra.")

    target_row = _fetch_user_row(db, user_id)
    if not target_row:
        raise HTTPException(status_code=404, detail="Not Found")

    if str(target_row.get("realm") or "").strip().lower() != "admin":
        raise HTTPException(status_code=403, detail="Solo se permite impersonar usuarios del realm admin.")

    if str(target_row.get("status") or "").strip().lower() != "active":
        return _json_422("No puedes impersonar a un usuario que no está activo.")

    actor_roles = _load_user_role_names(db, actor_id)
    target_roles = _load_user_role_names(db, int(user_id))
    if not actor_roles.intersection({"admin", "superadmin"}):
        raise HTTPException(status_code=403, detail="No autorizado para impersonar.")

    if "superadmin" in target_roles and "superadmin" not in actor_roles:
        raise HTTPException(status_code=403, detail="Solo un superadmin puede impersonar a otro superadmin.")

    original_refresh = str(request.cookies.get(REFRESH_COOKIE_NAME) or "").strip()
    original_access = str(request.cookies.get(ACCESS_COOKIE_NAME) or _extract_bearer_token(authorization) or "").strip()
    if not original_refresh or not original_access:
        return _json_422("No se pudo preservar la sesión original para la impersonación.")

    service = AuthService(db)
    token_data = service.issue_tokens_for_user_id(int(user_id))

    new_access = str(token_data.get("access_token") or "").strip()
    new_refresh = str(token_data.get("refresh_token") or "").strip()
    set_auth_cookie(response, ACCESS_COOKIE_NAME, new_access, ACCESS_COOKIE_MAX_AGE_SECONDS)
    set_auth_cookie(response, REFRESH_COOKIE_NAME, new_refresh, REFRESH_COOKIE_MAX_AGE_SECONDS)
    set_auth_cookie(response, IMPERSONATOR_ACCESS_COOKIE_NAME, original_access, ACCESS_COOKIE_MAX_AGE_SECONDS)
    set_auth_cookie(response, IMPERSONATOR_REFRESH_COOKIE_NAME, original_refresh, REFRESH_COOKIE_MAX_AGE_SECONDS)

    meta = {
        "actor_id": actor_id,
        "actor_email": str(auth_payload.get("email") or ""),
        "target_id": int(user_id),
        "target_email": str(target_row.get("email") or ""),
        "started_at": _format_datetime(target_row.get("updated_at")) or _format_datetime(target_row.get("created_at")),
    }
    set_auth_cookie(
        response,
        IMPERSONATION_META_COOKIE_NAME,
        encode_impersonation_meta(meta),
        IMPERSONATION_COOKIE_MAX_AGE_SECONDS,
    )

    frontend_origin = _resolve_frontend_origin(request)
    redirect_to = f"{frontend_origin}/admin" if frontend_origin else "/admin"

    return {
        "ok": True,
        "message": f"Ahora estás impersonando a {target_row.get('email') or 'otro usuario'}.",
        "redirect_to": redirect_to,
        "data": {
            "impersonation": meta,
        },
    }
