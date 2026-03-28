from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.database import get_db
from app.services.auth_service import AuthService


router = APIRouter(prefix="/api/v1/admin/regalias", tags=["admin-regalias"])


def _extract_bearer_token(authorization: str | None) -> str:
    if not authorization:
        return ""

    raw = authorization.strip()
    if not raw.lower().startswith("bearer "):
        return ""

    return raw[7:].strip()


def _normalize_per_page(value: int | None) -> int:
    if value is None:
        return 20
    if value <= 0:
        return 20
    return min(value, 100)


def _pagination_meta(*, page: int, per_page: int, total: int, count: int) -> dict:
    last_page = max(1, (total + per_page - 1) // per_page)
    offset = (page - 1) * per_page
    from_item = (offset + 1) if total > 0 else 0
    to_item = min(offset + count, total) if total > 0 else 0
    return {
        "current_page": page,
        "last_page": last_page,
        "per_page": per_page,
        "total": total,
        "from": from_item,
        "to": to_item,
    }


def _require_permissions(request: Request, db: Session, authorization: str | None, required: str) -> None:
    token = _extract_bearer_token(authorization)
    if not token:
        token = str(request.cookies.get("yastubo_access_token") or "").strip()

    if not token:
        raise HTTPException(status_code=403, detail="Forbidden")

    auth_payload = AuthService(db).me(token)
    permissions = [str(item) for item in (auth_payload.get("permissions") or [])]
    if required not in permissions:
        raise HTTPException(status_code=403, detail="Forbidden")


def _allowed_source_types() -> list[str]:
    settings = get_settings()
    raw = str(getattr(settings, "app_regalias", "") or "").strip()
    if raw == "":
        return []

    parts = [part.strip().lower() for part in raw.split(",") if part.strip()]
    if not parts:
        raise HTTPException(status_code=500, detail="APP_REGALIAS esta mal formateado.")

    supported = {"user", "unit"}
    normalized: list[str] = []
    for item in parts:
        if not item.replace("_", "").replace("-", "").isalnum():
            raise HTTPException(status_code=500, detail=f"APP_REGALIAS contiene tipo invalido: {item}")
        if item not in supported:
            raise HTTPException(status_code=500, detail=f"APP_REGALIAS no soportado por backend: {item}")
        if item not in normalized:
            normalized.append(item)

    return normalized


def _unit_ancestor_ids(db: Session, unit_id: int) -> list[int]:
    chain: list[int] = []
    seen: set[int] = set()
    current = unit_id
    while current and current not in seen:
        seen.add(current)
        chain.append(current)
        row = db.execute(
            text("SELECT parent_id FROM business_units WHERE id = :unit_id LIMIT 1"),
            {"unit_id": current},
        ).mappings().first()
        if not row:
            break
        parent_id = row.get("parent_id")
        current = int(parent_id) if parent_id is not None else 0
    chain.reverse()
    return chain


def _would_create_user_cycle(db: Session, beneficiary_id: int, source_id: int) -> bool:
    if beneficiary_id == source_id:
        return True

    edges = db.execute(
        text(
            """
            SELECT beneficiary_user_id, source_id
            FROM regalias
            WHERE source_type = 'user'
            """
        )
    ).mappings().all()

    adjacency: dict[int, set[int]] = {}
    for edge in edges:
        frm = int(edge.get("beneficiary_user_id") or 0)
        to = int(edge.get("source_id") or 0)
        if frm <= 0 or to <= 0:
            continue
        adjacency.setdefault(frm, set()).add(to)

    stack = [source_id]
    visited = {source_id}
    while stack:
        current = stack.pop()
        if current == beneficiary_id:
            return True
        for neighbor in adjacency.get(current, set()):
            if neighbor not in visited:
                visited.add(neighbor)
                stack.append(neighbor)

    return False


def _would_create_unit_redundancy(db: Session, beneficiary_id: int, unit_id: int) -> bool:
    rows = db.execute(
        text(
            """
            SELECT source_id
            FROM regalias
            WHERE beneficiary_user_id = :beneficiary_id
              AND source_type = 'unit'
            """
        ),
        {"beneficiary_id": beneficiary_id},
    ).mappings().all()

    existing_ids = sorted({int(row.get("source_id") or 0) for row in rows if int(row.get("source_id") or 0) > 0})
    if not existing_ids:
        return False
    if unit_id in existing_ids:
        return True

    candidate_chain = _unit_ancestor_ids(db, unit_id)

    for existing_id in existing_ids:
        existing_chain = _unit_ancestor_ids(db, existing_id)
        if unit_id in existing_chain:
            return True
        if existing_id in candidate_chain:
            return True

    return False


class RegaliaCreateRequest(BaseModel):
    beneficiary_user_id: int = Field(..., ge=1)
    source_type: str
    source_id: int = Field(..., ge=1)


class RegaliaUpdateRequest(BaseModel):
    commission: float | None = Field(default=None, ge=0, le=100)


@router.get("/beneficiaries")
def beneficiaries_index(
    request: Request,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    q: str = Query(""),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_permissions(request, db, authorization, "regalia.users.read")

    normalized_q = (q or "").strip()
    normalized_per_page = _normalize_per_page(per_page)
    offset = (page - 1) * normalized_per_page

    where_parts = [
        "u.realm = 'admin'",
        "EXISTS (SELECT 1 FROM regalias r0 WHERE r0.beneficiary_user_id = u.id)",
    ]
    params: dict[str, object] = {}
    if normalized_q:
        where_parts.append(
            "(" 
            "u.first_name LIKE :q OR u.last_name LIKE :q OR u.display_name LIKE :q OR u.email LIKE :q" 
            ")"
        )
        params["q"] = f"%{normalized_q}%"

    where_sql = " AND ".join(where_parts)

    total_row = db.execute(
        text(f"SELECT COUNT(*) AS c FROM users u WHERE {where_sql}"),
        params,
    ).mappings().first()
    total = int(total_row["c"] if total_row else 0)

    beneficiaries = db.execute(
        text(
            f"""
            SELECT
                u.id,
                CASE
                    WHEN u.display_name IS NOT NULL AND TRIM(u.display_name) <> '' THEN u.display_name
                    ELSE TRIM(CONCAT(COALESCE(u.first_name, ''), ' ', COALESCE(u.last_name, '')))
                END AS display_name,
                u.email,
                u.status
            FROM users u
            WHERE {where_sql}
            ORDER BY display_name ASC, u.id ASC
            LIMIT :limit OFFSET :offset
            """
        ),
        {**params, "limit": normalized_per_page, "offset": offset},
    ).mappings().all()

    beneficiary_ids = [int(row["id"]) for row in beneficiaries]

    regalias_rows = []
    if beneficiary_ids:
        placeholders = ",".join(f":bid_{idx}" for idx, _ in enumerate(beneficiary_ids))
        id_params = {f"bid_{idx}": value for idx, value in enumerate(beneficiary_ids)}
        regalias_rows = db.execute(
            text(
                f"""
                SELECT id, source_type, source_id, beneficiary_user_id, commission
                FROM regalias
                WHERE beneficiary_user_id IN ({placeholders})
                """
            ),
            id_params,
        ).mappings().all()

    user_origin_ids = sorted(
        {int(row["source_id"]) for row in regalias_rows if str(row.get("source_type") or "") == "user"}
    )
    unit_origin_ids = sorted(
        {int(row["source_id"]) for row in regalias_rows if str(row.get("source_type") or "") == "unit"}
    )

    origin_users: dict[int, dict] = {}
    if user_origin_ids:
        placeholders = ",".join(f":uid_{idx}" for idx, _ in enumerate(user_origin_ids))
        params_users = {f"uid_{idx}": value for idx, value in enumerate(user_origin_ids)}
        user_rows = db.execute(
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
                WHERE id IN ({placeholders})
                """
            ),
            params_users,
        ).mappings().all()
        origin_users = {
            int(row["id"]): {
                "id": int(row["id"]),
                "display_name": str(row.get("display_name") or "").strip(),
                "email": row.get("email"),
                "status": row.get("status"),
            }
            for row in user_rows
        }

    origin_units: dict[int, dict] = {}
    if unit_origin_ids:
        placeholders = ",".join(f":uid_{idx}" for idx, _ in enumerate(unit_origin_ids))
        params_units = {f"uid_{idx}": value for idx, value in enumerate(unit_origin_ids)}
        unit_rows = db.execute(
            text(
                f"""
                SELECT id, name, type, status
                FROM business_units
                WHERE id IN ({placeholders})
                """
            ),
            params_units,
        ).mappings().all()

        for row in unit_rows:
            unit_id = int(row["id"])
            display_name = str(row.get("name") or "").strip()
            if str(row.get("type") or "") == "freelance":
                members = db.execute(
                    text(
                        """
                        SELECT
                            u.display_name,
                            u.first_name,
                            u.last_name
                        FROM memberships_business_unit m
                        INNER JOIN users u ON u.id = m.user_id
                        WHERE m.business_unit_id = :unit_id
                        """
                    ),
                    {"unit_id": unit_id},
                ).mappings().all()
                if len(members) == 1:
                    member = members[0]
                    candidate = str(member.get("display_name") or "").strip()
                    if not candidate:
                        first_name = str(member.get("first_name") or "").strip()
                        last_name = str(member.get("last_name") or "").strip()
                        candidate = f"{first_name} {last_name}".strip()
                    if candidate:
                        display_name = candidate

            origin_units[unit_id] = {
                "id": unit_id,
                "name": display_name,
                "status": row.get("status"),
                "type": row.get("type"),
            }

    rows_by_beneficiary: dict[int, list[dict]] = {}
    for reg in regalias_rows:
        beneficiary_id = int(reg["beneficiary_user_id"])
        source_type = str(reg.get("source_type") or "")
        source_id = int(reg.get("source_id") or 0)
        item = {
            "id": int(reg["id"]),
            "source_type": source_type,
            "source_id": source_id,
            "beneficiary_user_id": beneficiary_id,
            "commission": float(reg.get("commission") or 0),
            "origin_user": origin_users.get(source_id) if source_type == "user" else None,
            "origin_unit": origin_units.get(source_id) if source_type == "unit" else None,
        }
        rows_by_beneficiary.setdefault(beneficiary_id, []).append(item)

    data = []
    for row in beneficiaries:
        beneficiary_id = int(row["id"])
        data.append(
            {
                "beneficiary": {
                    "id": beneficiary_id,
                    "display_name": str(row.get("display_name") or "").strip(),
                    "email": row.get("email"),
                    "status": row.get("status"),
                },
                "regalias": rows_by_beneficiary.get(beneficiary_id, []),
            }
        )

    return {
        "data": data,
        "meta": {
            "pagination": _pagination_meta(
                page=page,
                per_page=normalized_per_page,
                total=total,
                count=len(beneficiaries),
            ),
            "regalias_sources": _allowed_source_types(),
        },
    }


@router.get("/beneficiaries/{beneficiary_id}/origins/users/available")
def available_origin_users(
    beneficiary_id: int,
    request: Request,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    q: str = Query(""),
    status: str | None = Query(None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_permissions(request, db, authorization, "regalia.users.edit")

    exists = db.execute(
        text("SELECT id FROM users WHERE realm = 'admin' AND id = :id LIMIT 1"),
        {"id": beneficiary_id},
    ).mappings().first()
    if not exists:
        raise HTTPException(status_code=404, detail="Not Found")

    normalized_q = (q or "").strip()
    normalized_status = (status or "").strip()
    normalized_per_page = _normalize_per_page(per_page)
    offset = (page - 1) * normalized_per_page

    where_parts = ["realm = 'admin'"]
    params: dict[str, object] = {}

    if normalized_status:
        where_parts.append("status = :status")
        params["status"] = normalized_status

    if normalized_q:
        where_parts.append(
            "(" 
            "first_name LIKE :q OR last_name LIKE :q OR display_name LIKE :q OR email LIKE :q" 
            ")"
        )
        params["q"] = f"%{normalized_q}%"

    where_sql = " AND ".join(where_parts)
    total_row = db.execute(
        text(f"SELECT COUNT(*) AS c FROM users WHERE {where_sql}"),
        params,
    ).mappings().first()
    total = int(total_row["c"] if total_row else 0)

    users = db.execute(
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
        {**params, "limit": normalized_per_page, "offset": offset},
    ).mappings().all()

    user_ids = [int(row["id"]) for row in users]
    existing_map: dict[int, dict] = {}
    if user_ids:
        placeholders = ",".join(f":uid_{idx}" for idx, _ in enumerate(user_ids))
        bind = {f"uid_{idx}": value for idx, value in enumerate(user_ids)}
        assigned = db.execute(
            text(
                f"""
                SELECT id, source_id, commission
                FROM regalias
                WHERE beneficiary_user_id = :beneficiary_id
                  AND source_type = 'user'
                  AND source_id IN ({placeholders})
                """
            ),
            {"beneficiary_id": beneficiary_id, **bind},
        ).mappings().all()
        existing_map = {int(row["source_id"]): dict(row) for row in assigned}

    data = []
    for row in users:
        user_id = int(row["id"])
        assigned = existing_map.get(user_id)
        data.append(
            {
                "id": user_id,
                "display_name": str(row.get("display_name") or "").strip(),
                "email": row.get("email"),
                "status": row.get("status"),
                "is_assigned": assigned is not None,
                "regalia_id": int(assigned["id"]) if assigned else None,
                "commission": float(assigned.get("commission") or 0) if assigned else None,
            }
        )

    return {
        "data": data,
        "meta": {
            "pagination": _pagination_meta(
                page=page,
                per_page=normalized_per_page,
                total=total,
                count=len(users),
            )
        },
    }


@router.get("/beneficiaries/{beneficiary_id}/origins/units/available")
def available_origin_units(
    beneficiary_id: int,
    request: Request,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    q: str = Query(""),
    status: str | None = Query(None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_permissions(request, db, authorization, "regalia.users.edit")

    exists = db.execute(
        text("SELECT id FROM users WHERE realm = 'admin' AND id = :id LIMIT 1"),
        {"id": beneficiary_id},
    ).mappings().first()
    if not exists:
        raise HTTPException(status_code=404, detail="Not Found")

    normalized_q = (q or "").strip()
    normalized_status = (status or "").strip()
    normalized_per_page = _normalize_per_page(per_page)
    offset = (page - 1) * normalized_per_page

    where_parts = ["1=1"]
    params: dict[str, object] = {}
    if normalized_status and normalized_status != "all":
        where_parts.append("status = :status")
        params["status"] = normalized_status

    if normalized_q:
        where_parts.append(
            "(" 
            "name LIKE :q OR EXISTS (" 
            "SELECT 1 FROM memberships_business_unit m " 
            "INNER JOIN users u ON u.id = m.user_id " 
            "WHERE m.business_unit_id = business_units.id " 
            "AND (u.display_name LIKE :q OR u.first_name LIKE :q OR u.last_name LIKE :q)" 
            ")" 
            ")"
        )
        params["q"] = f"%{normalized_q}%"

    where_sql = " AND ".join(where_parts)

    total_row = db.execute(
        text(f"SELECT COUNT(*) AS c FROM business_units WHERE {where_sql}"),
        params,
    ).mappings().first()
    total = int(total_row["c"] if total_row else 0)

    units = db.execute(
        text(
            f"""
            SELECT id, name, status, type
            FROM business_units
            WHERE {where_sql}
            ORDER BY name ASC, id ASC
            LIMIT :limit OFFSET :offset
            """
        ),
        {**params, "limit": normalized_per_page, "offset": offset},
    ).mappings().all()

    unit_ids = [int(row["id"]) for row in units]
    existing_map: dict[int, dict] = {}
    if unit_ids:
        placeholders = ",".join(f":uid_{idx}" for idx, _ in enumerate(unit_ids))
        bind = {f"uid_{idx}": value for idx, value in enumerate(unit_ids)}
        assigned = db.execute(
            text(
                f"""
                SELECT id, source_id, commission
                FROM regalias
                WHERE beneficiary_user_id = :beneficiary_id
                  AND source_type = 'unit'
                  AND source_id IN ({placeholders})
                """
            ),
            {"beneficiary_id": beneficiary_id, **bind},
        ).mappings().all()
        existing_map = {int(row["source_id"]): dict(row) for row in assigned}

    data = []
    for row in units:
        unit_id = int(row["id"])
        display_name = str(row.get("name") or "").strip()
        if str(row.get("type") or "") == "freelance":
            members = db.execute(
                text(
                    """
                    SELECT u.display_name, u.first_name, u.last_name
                    FROM memberships_business_unit m
                    INNER JOIN users u ON u.id = m.user_id
                    WHERE m.business_unit_id = :unit_id
                    """
                ),
                {"unit_id": unit_id},
            ).mappings().all()
            if len(members) == 1:
                member = members[0]
                candidate = str(member.get("display_name") or "").strip()
                if not candidate:
                    first_name = str(member.get("first_name") or "").strip()
                    last_name = str(member.get("last_name") or "").strip()
                    candidate = f"{first_name} {last_name}".strip()
                if candidate:
                    display_name = candidate

        assigned = existing_map.get(unit_id)
        data.append(
            {
                "id": unit_id,
                "name": display_name,
                "status": row.get("status"),
                "type": row.get("type"),
                "is_assigned": assigned is not None,
                "regalia_id": int(assigned["id"]) if assigned else None,
                "commission": float(assigned.get("commission") or 0) if assigned else None,
            }
        )

    return {
        "data": data,
        "meta": {
            "pagination": _pagination_meta(
                page=page,
                per_page=normalized_per_page,
                total=total,
                count=len(units),
            )
        },
    }


@router.post("/regalias", status_code=201)
def create_regalia(
    payload: RegaliaCreateRequest,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_permissions(request, db, authorization, "regalia.users.edit")

    allowed_types = _allowed_source_types()
    source_type = str(payload.source_type or "").strip().lower()
    if source_type not in allowed_types:
        raise HTTPException(status_code=422, detail={"message": "El tipo de origen no esta permitido."})

    beneficiary = db.execute(
        text("SELECT id FROM users WHERE id = :id AND realm = 'admin' LIMIT 1"),
        {"id": payload.beneficiary_user_id},
    ).mappings().first()
    if not beneficiary:
        raise HTTPException(status_code=422, detail={"message": "Beneficiario invalido."})

    if source_type == "user":
        source_exists = db.execute(
            text("SELECT id FROM users WHERE id = :id AND realm = 'admin' LIMIT 1"),
            {"id": payload.source_id},
        ).mappings().first()
        if not source_exists:
            raise HTTPException(status_code=422, detail={"message": "Origen usuario no existe."})
        if _would_create_user_cycle(db, payload.beneficiary_user_id, payload.source_id):
            raise HTTPException(status_code=422, detail={"message": "La relacion de regalias entre usuarios genera un ciclo y no es valida."})
    elif source_type == "unit":
        source_exists = db.execute(
            text("SELECT id FROM business_units WHERE id = :id LIMIT 1"),
            {"id": payload.source_id},
        ).mappings().first()
        if not source_exists:
            raise HTTPException(status_code=422, detail={"message": "Origen unidad no existe."})
        if _would_create_unit_redundancy(db, payload.beneficiary_user_id, payload.source_id):
            raise HTTPException(status_code=422, detail={"message": "La unidad seleccionada genera una redundancia en la jerarquia de unidades para este beneficiario y no es valida."})

    duplicate = db.execute(
        text(
            """
            SELECT id
            FROM regalias
            WHERE beneficiary_user_id = :beneficiary_user_id
              AND source_type = :source_type
              AND source_id = :source_id
            LIMIT 1
            """
        ),
        {
            "beneficiary_user_id": payload.beneficiary_user_id,
            "source_type": source_type,
            "source_id": payload.source_id,
        },
    ).mappings().first()
    if duplicate:
        raise HTTPException(status_code=422, detail={"message": "Ya existe una regalia para este beneficiario y origen."})

    now = datetime.utcnow()
    try:
        db.execute(
            text(
                """
                INSERT INTO regalias (
                    source_type,
                    source_id,
                    beneficiary_user_id,
                    commission,
                    created_at,
                    updated_at
                ) VALUES (
                    :source_type,
                    :source_id,
                    :beneficiary_user_id,
                    :commission,
                    :created_at,
                    :updated_at
                )
                """
            ),
            {
                "source_type": source_type,
                "source_id": payload.source_id,
                "beneficiary_user_id": payload.beneficiary_user_id,
                "commission": 0.0,
                "created_at": now,
                "updated_at": now,
            },
        )
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=422, detail={"message": "Ya existe una regalia para este beneficiario y origen."}) from exc

    created = db.execute(
        text(
            """
            SELECT id, beneficiary_user_id, source_type, source_id, commission
            FROM regalias
            WHERE beneficiary_user_id = :beneficiary_user_id
              AND source_type = :source_type
              AND source_id = :source_id
            ORDER BY id DESC
            LIMIT 1
            """
        ),
        {
            "beneficiary_user_id": payload.beneficiary_user_id,
            "source_type": source_type,
            "source_id": payload.source_id,
        },
    ).mappings().first()
    if not created:
        raise HTTPException(status_code=500, detail="No fue posible crear la regalia.")

    return {
        "data": {
            "id": int(created["id"]),
            "beneficiary_user_id": int(created["beneficiary_user_id"]),
            "source_type": str(created["source_type"]),
            "source_id": int(created["source_id"]),
            "commission": float(created.get("commission") or 0),
        },
        "message": "Regalia creada.",
    }


@router.patch("/regalias/{regalia_id}")
def update_regalia(
    regalia_id: int,
    payload: RegaliaUpdateRequest,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_permissions(request, db, authorization, "regalia.users.edit")

    row = db.execute(
        text(
            """
            SELECT id, beneficiary_user_id, source_type, source_id, commission
            FROM regalias
            WHERE id = :id
            LIMIT 1
            """
        ),
        {"id": regalia_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Not Found")

    commission = float(payload.commission if payload.commission is not None else 0)
    commission = max(0.0, min(100.0, commission))

    db.execute(
        text("UPDATE regalias SET commission = :commission, updated_at = :updated_at WHERE id = :id"),
        {
            "commission": commission,
            "updated_at": datetime.utcnow(),
            "id": regalia_id,
        },
    )
    db.commit()

    return {
        "data": {
            "id": int(row["id"]),
            "beneficiary_user_id": int(row["beneficiary_user_id"]),
            "source_type": str(row["source_type"]),
            "source_id": int(row["source_id"]),
            "commission": commission,
        },
        "message": "Regalia actualizada.",
    }


@router.delete("/regalias/{regalia_id}")
def delete_regalia(
    regalia_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_permissions(request, db, authorization, "regalia.users.edit")

    row = db.execute(
        text(
            """
            SELECT id, beneficiary_user_id, source_type, source_id
            FROM regalias
            WHERE id = :id
            LIMIT 1
            """
        ),
        {"id": regalia_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Not Found")

    db.execute(text("DELETE FROM regalias WHERE id = :id"), {"id": regalia_id})
    db.commit()

    return {
        "data": {
            "id": int(row["id"]),
            "beneficiary_user_id": int(row["beneficiary_user_id"]),
            "source_type": str(row["source_type"]),
            "source_id": int(row["source_id"]),
        },
        "message": "Regalia eliminada.",
    }