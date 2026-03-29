from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.services.auth_service import AuthService


router = APIRouter(prefix="/api/v1/admin/acl", tags=["admin-acl"])

_ALLOWED_GUARDS = {"admin", "customer"}


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


def _permissions_set(auth_payload: dict) -> set[str]:
    return {str(item) for item in (auth_payload.get("permissions") or [])}


def _require_system_roles(auth_payload: dict) -> None:
    if "system.roles" not in _permissions_set(auth_payload):
        raise HTTPException(status_code=403, detail="Forbidden")


def _normalize_guard(guard: str) -> str:
    value = str(guard or "").strip()
    if value not in _ALLOWED_GUARDS:
        raise HTTPException(status_code=404, detail="Not Found")
    return value


def _json_422(message: str, errors: dict[str, list[str]] | None = None) -> JSONResponse:
    payload = {"message": message}
    if errors:
        payload["errors"] = errors
    return JSONResponse(status_code=422, content=payload)


@router.get("/roles/{guard}/matrix")
def matrix_data(
    guard: str,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    auth_payload = _auth_payload(request, authorization, db)
    _require_system_roles(auth_payload)
    guard_name = _normalize_guard(guard)

    roles_rows = db.execute(
        text(
            """
            SELECT id, name, guard_name, scope, label, level
            FROM roles
            WHERE guard_name = :guard_name
            ORDER BY name, id
            """
        ),
        {"guard_name": guard_name},
    ).mappings().all()

    permissions_rows = db.execute(
        text(
            """
            SELECT id, name, guard_name, description
            FROM permissions
            WHERE guard_name = :guard_name
            ORDER BY name, id
            """
        ),
        {"guard_name": guard_name},
    ).mappings().all()

    roles = [dict(item) for item in roles_rows]
    permissions = [dict(item) for item in permissions_rows]

    matrix: dict[int, list[int]] = {int(role.get("id")): [] for role in roles}

    role_ids = [int(role.get("id")) for role in roles]
    permission_ids = [int(perm.get("id")) for perm in permissions]

    if role_ids and permission_ids:
        role_placeholders = ",".join(f":rid_{idx}" for idx, _ in enumerate(role_ids))
        perm_placeholders = ",".join(f":pid_{idx}" for idx, _ in enumerate(permission_ids))
        params: dict[str, int] = {}
        for idx, role_id in enumerate(role_ids):
            params[f"rid_{idx}"] = int(role_id)
        for idx, permission_id in enumerate(permission_ids):
            params[f"pid_{idx}"] = int(permission_id)

        pivots = db.execute(
            text(
                f"""
                SELECT role_id, permission_id
                FROM role_has_permissions
                WHERE role_id IN ({role_placeholders})
                  AND permission_id IN ({perm_placeholders})
                """
            ),
            params,
        ).mappings().all()

        for row in pivots:
            role_id = int(row.get("role_id") or 0)
            permission_id = int(row.get("permission_id") or 0)
            matrix.setdefault(role_id, []).append(permission_id)

    return {
        "roles": roles,
        "permissions": permissions,
        "matrix": matrix,
    }


@router.post("/roles/{guard}/roles")
async def store_role(
    guard: str,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_system_roles(auth_payload)
    guard_name = _normalize_guard(guard)

    payload = await request.json()
    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.")

    name = str(payload.get("name") or "").strip()
    if not name:
        return _json_422("The given data was invalid.", {"name": ["The name field is required."]})

    exists = db.execute(
        text(
            """
            SELECT id
            FROM roles
            WHERE guard_name = :guard_name
              AND name = :name
            LIMIT 1
            """
        ),
        {"guard_name": guard_name, "name": name},
    ).mappings().first()
    if exists:
        return _json_422("The given data was invalid.", {"name": ["The name has already been taken."]})

    scope = payload.get("scope")
    scope_value = str(scope).strip() if scope not in [None, ""] else None

    label_value = payload.get("label")
    if label_value is not None and not isinstance(label_value, dict):
        return _json_422("The given data was invalid.", {"label": ["The label must be an object."]})

    try:
        db.execute(
            text(
                """
                INSERT INTO roles (name, guard_name, scope, label, created_at, updated_at)
                VALUES (:name, :guard_name, :scope, :label, NOW(), NOW())
                """
            ),
            {
                "name": name,
                "guard_name": guard_name,
                "scope": scope_value,
                "label": label_value,
            },
        )
        role_id_row = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
        db.commit()
    except IntegrityError:
        db.rollback()
        return _json_422("The given data was invalid.", {"name": ["The name has already been taken."]})

    role_id = int(role_id_row.get("id") or 0) if role_id_row else 0
    role = db.execute(
        text(
            """
            SELECT id, name, guard_name, scope, label, level
            FROM roles
            WHERE id = :role_id
            LIMIT 1
            """
        ),
        {"role_id": role_id},
    ).mappings().first()

    return {
        "role": dict(role) if role else {"id": role_id, "name": name, "guard_name": guard_name, "scope": scope_value, "label": label_value},
        "message": "Rol creado correctamente.",
    }


@router.put("/roles/{guard}/roles/{role_id:int}")
async def update_role(
    guard: str,
    role_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_system_roles(auth_payload)
    guard_name = _normalize_guard(guard)

    role = db.execute(
        text(
            """
            SELECT id, name, guard_name, scope, label, level
            FROM roles
            WHERE id = :role_id
            LIMIT 1
            """
        ),
        {"role_id": int(role_id)},
    ).mappings().first()

    if not role or str(role.get("guard_name") or "") != guard_name:
        raise HTTPException(status_code=404, detail="Not Found")

    payload = await request.json()
    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.")

    if "name" in payload:
        name = str(payload.get("name") or "").strip()
        if not name:
            return _json_422("The given data was invalid.", {"name": ["The name field is required."]})

        duplicate = db.execute(
            text(
                """
                SELECT id
                FROM roles
                WHERE guard_name = :guard_name
                  AND name = :name
                  AND id <> :role_id
                LIMIT 1
                """
            ),
            {
                "guard_name": guard_name,
                "name": name,
                "role_id": int(role_id),
            },
        ).mappings().first()
        if duplicate:
            return _json_422("The given data was invalid.", {"name": ["The name has already been taken."]})

    if "label" in payload and payload.get("label") is not None and not isinstance(payload.get("label"), dict):
        return _json_422("The given data was invalid.", {"label": ["The label must be an object."]})

    updates = {
        "name": str(payload.get("name") or role.get("name") or "").strip() if "name" in payload else role.get("name"),
        "scope": str(payload.get("scope")).strip() if "scope" in payload and payload.get("scope") not in [None, ""] else (None if "scope" in payload else role.get("scope")),
        "label": payload.get("label") if "label" in payload else role.get("label"),
    }

    db.execute(
        text(
            """
            UPDATE roles
            SET name = :name,
                scope = :scope,
                label = :label,
                updated_at = NOW()
            WHERE id = :role_id
            """
        ),
        {
            "name": updates["name"],
            "scope": updates["scope"],
            "label": updates["label"],
            "role_id": int(role_id),
        },
    )
    db.commit()

    refreshed = db.execute(
        text(
            """
            SELECT id, name, guard_name, scope, label, level
            FROM roles
            WHERE id = :role_id
            LIMIT 1
            """
        ),
        {"role_id": int(role_id)},
    ).mappings().first()

    return {
        "role": dict(refreshed) if refreshed else {"id": int(role_id), **updates, "guard_name": guard_name},
        "message": "Rol actualizado correctamente.",
    }


@router.post("/roles/{guard}/permissions")
async def store_permission(
    guard: str,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_system_roles(auth_payload)
    guard_name = _normalize_guard(guard)

    payload = await request.json()
    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.")

    name = str(payload.get("name") or "").strip()
    if not name:
        return _json_422("The given data was invalid.", {"name": ["The name field is required."]})

    duplicate = db.execute(
        text(
            """
            SELECT id
            FROM permissions
            WHERE guard_name = :guard_name
              AND name = :name
            LIMIT 1
            """
        ),
        {"guard_name": guard_name, "name": name},
    ).mappings().first()
    if duplicate:
        return _json_422("The given data was invalid.", {"name": ["The name has already been taken."]})

    description = payload.get("description")
    description_value = str(description).strip() if description not in [None, ""] else None

    try:
        db.execute(
            text(
                """
                INSERT INTO permissions (name, guard_name, description, created_at, updated_at)
                VALUES (:name, :guard_name, :description, NOW(), NOW())
                """
            ),
            {
                "name": name,
                "guard_name": guard_name,
                "description": description_value,
            },
        )
        permission_id_row = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
        db.commit()
    except IntegrityError:
        db.rollback()
        return _json_422("The given data was invalid.", {"name": ["The name has already been taken."]})

    permission_id = int(permission_id_row.get("id") or 0) if permission_id_row else 0
    permission = db.execute(
        text(
            """
            SELECT id, name, guard_name, description
            FROM permissions
            WHERE id = :permission_id
            LIMIT 1
            """
        ),
        {"permission_id": permission_id},
    ).mappings().first()

    return {
        "permission": dict(permission) if permission else {"id": permission_id, "name": name, "guard_name": guard_name, "description": description_value},
        "message": "Permiso creado correctamente.",
    }


@router.put("/roles/{guard}/permissions/{permission_id:int}")
async def update_permission(
    guard: str,
    permission_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_system_roles(auth_payload)
    guard_name = _normalize_guard(guard)

    permission = db.execute(
        text(
            """
            SELECT id, name, guard_name, description
            FROM permissions
            WHERE id = :permission_id
            LIMIT 1
            """
        ),
        {"permission_id": int(permission_id)},
    ).mappings().first()

    if not permission or str(permission.get("guard_name") or "") != guard_name:
        raise HTTPException(status_code=404, detail="Not Found")

    payload = await request.json()
    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.")

    if "name" in payload:
        name = str(payload.get("name") or "").strip()
        if not name:
            return _json_422("The given data was invalid.", {"name": ["The name field is required."]})

        duplicate = db.execute(
            text(
                """
                SELECT id
                FROM permissions
                WHERE guard_name = :guard_name
                  AND name = :name
                  AND id <> :permission_id
                LIMIT 1
                """
            ),
            {
                "guard_name": guard_name,
                "name": name,
                "permission_id": int(permission_id),
            },
        ).mappings().first()
        if duplicate:
            return _json_422("The given data was invalid.", {"name": ["The name has already been taken."]})

    updates = {
        "name": str(payload.get("name") or permission.get("name") or "").strip() if "name" in payload else permission.get("name"),
        "description": (str(payload.get("description") or "").strip() or None) if "description" in payload else permission.get("description"),
    }

    db.execute(
        text(
            """
            UPDATE permissions
            SET name = :name,
                description = :description,
                updated_at = NOW()
            WHERE id = :permission_id
            """
        ),
        {
            "name": updates["name"],
            "description": updates["description"],
            "permission_id": int(permission_id),
        },
    )
    db.commit()

    refreshed = db.execute(
        text(
            """
            SELECT id, name, guard_name, description
            FROM permissions
            WHERE id = :permission_id
            LIMIT 1
            """
        ),
        {"permission_id": int(permission_id)},
    ).mappings().first()

    return {
        "permission": dict(refreshed) if refreshed else {"id": int(permission_id), **updates, "guard_name": guard_name},
        "message": "Permiso actualizado correctamente.",
    }


@router.post("/roles/{guard}/toggle")
async def toggle_assignment(
    guard: str,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    auth_payload = _auth_payload(request, authorization, db)
    _require_system_roles(auth_payload)
    guard_name = _normalize_guard(guard)

    payload = await request.json()
    if not isinstance(payload, dict):
        return _json_422("The given data was invalid.")

    role_id = payload.get("role_id")
    permission_id = payload.get("permission_id")
    if role_id in [None, ""] or permission_id in [None, ""]:
        return _json_422("The given data was invalid.", {"role_id": ["role_id requerido."], "permission_id": ["permission_id requerido."]})

    value_raw = payload.get("value")
    if isinstance(value_raw, bool):
        value = value_raw
    elif isinstance(value_raw, int) and value_raw in {0, 1}:
        value = bool(value_raw)
    else:
        return _json_422("The given data was invalid.", {"value": ["The value field must be true or false."]})

    role = db.execute(
        text("SELECT id, guard_name FROM roles WHERE id = :id LIMIT 1"),
        {"id": int(role_id)},
    ).mappings().first()
    if not role or str(role.get("guard_name") or "") != guard_name:
        raise HTTPException(status_code=404, detail="Not Found")

    permission = db.execute(
        text("SELECT id, guard_name FROM permissions WHERE id = :id LIMIT 1"),
        {"id": int(permission_id)},
    ).mappings().first()
    if not permission or str(permission.get("guard_name") or "") != guard_name:
        raise HTTPException(status_code=404, detail="Not Found")

    if value:
        db.execute(
            text(
                """
                INSERT IGNORE INTO role_has_permissions (permission_id, role_id)
                VALUES (:permission_id, :role_id)
                """
            ),
            {
                "permission_id": int(permission_id),
                "role_id": int(role_id),
            },
        )
    else:
        db.execute(
            text(
                """
                DELETE FROM role_has_permissions
                WHERE permission_id = :permission_id
                  AND role_id = :role_id
                """
            ),
            {
                "permission_id": int(permission_id),
                "role_id": int(role_id),
            },
        )

    db.commit()

    return {"message": "Asignación rol/permisos actualizada correctamente."}
