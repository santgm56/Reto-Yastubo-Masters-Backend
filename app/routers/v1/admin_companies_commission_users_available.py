from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.services.auth_service import AuthService

router = APIRouter(prefix="/api/v1/admin/companies", tags=["admin-companies"])


class CommissionUserStorePayload(BaseModel):
    user_id: int = Field(ge=1)


class CommissionUserUpdatePayload(BaseModel):
    commission: float = Field(ge=0, le=100)


def _extract_bearer_token(authorization: str | None) -> str:
    if not authorization:
        return ""

    raw = authorization.strip()
    if not raw.lower().startswith("bearer "):
        return ""

    return raw[7:].strip()


def _require_admin(request: Request, authorization: str | None, db: Session) -> None:
    token = _extract_bearer_token(authorization)
    if not token:
        token = str(request.cookies.get("yastubo_access_token") or "").strip()

    if not token:
        raise HTTPException(status_code=403, detail="Forbidden")

    auth_payload = AuthService(db).me(token)
    role = str(auth_payload.get("role") or "").upper()
    if role != "ADMIN":
        raise HTTPException(status_code=403, detail="Forbidden")


def _fetch_commission_user_row(db: Session, commission_user_id: int):
    return db.execute(
        text(
            """
            SELECT
                ccu.id,
                ccu.user_id,
                ccu.company_id,
                ccu.commission,
                u.id AS user_ref_id,
                u.email,
                CASE
                    WHEN u.display_name IS NOT NULL AND TRIM(u.display_name) <> '' THEN u.display_name
                    ELSE TRIM(CONCAT(COALESCE(u.first_name, ''), ' ', COALESCE(u.last_name, '')))
                END AS user_display_name
            FROM company_commission_users ccu
            LEFT JOIN users u ON u.id = ccu.user_id
            WHERE ccu.id = :commission_user_id
            LIMIT 1
            """
        ),
        {"commission_user_id": int(commission_user_id)},
    ).mappings().first()


def _serialize_commission_user_row(row) -> dict:
    commission_number = float(row.get("commission") or 0)
    return {
        "id": int(row["id"]),
        "user_id": int(row["user_id"]),
        "commission": f"{commission_number:.2f}",
        "user": {
            "id": int(row["user_ref_id"]),
            "email": row.get("email"),
            "display_name": str(row.get("user_display_name") or "").strip(),
        }
        if row.get("user_ref_id") is not None
        else None,
    }


@router.get("/{company_id}/commission-users")
def commission_users_index(
    company_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin(request=request, authorization=authorization, db=db)

    rows = db.execute(
        text(
            """
            SELECT
                ccu.id,
                ccu.user_id,
                ccu.commission,
                u.id AS user_ref_id,
                u.email,
                CASE
                    WHEN u.display_name IS NOT NULL AND TRIM(u.display_name) <> '' THEN u.display_name
                    ELSE TRIM(CONCAT(COALESCE(u.first_name, ''), ' ', COALESCE(u.last_name, '')))
                END AS user_display_name
            FROM company_commission_users ccu
            LEFT JOIN users u ON u.id = ccu.user_id
            WHERE ccu.company_id = :company_id
            ORDER BY ccu.id ASC
            """
        ),
        {"company_id": int(company_id)},
    ).mappings().all()

    data = []
    for row in rows:
        data.append(_serialize_commission_user_row(row))

    return {"data": data}


@router.post("/{company_id}/commission-users")
def store_commission_user(
    company_id: int,
    payload: CommissionUserStorePayload,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin(request=request, authorization=authorization, db=db)

    user_id = int(payload.user_id)
    user_exists = db.execute(
        text(
            """
            SELECT id
            FROM users
            WHERE id = :user_id
            LIMIT 1
            """
        ),
        {"user_id": user_id},
    ).mappings().first()
    if not user_exists:
        raise HTTPException(status_code=422, detail="Validation Error")

    already = db.execute(
        text(
            """
            SELECT id
            FROM company_commission_users
            WHERE company_id = :company_id AND user_id = :user_id
            LIMIT 1
            """
        ),
        {"company_id": int(company_id), "user_id": user_id},
    ).mappings().first()

    if already:
        raise HTTPException(
            status_code=422,
            detail={
                "message": "El usuario ya esta asociado como beneficiario de comisiones en esta empresa.",
                "toast": {
                    "type": "info",
                    "message": "Este usuario ya esta en la lista de comisiones.",
                },
            },
        )

    db.execute(
        text(
            """
            INSERT INTO company_commission_users (company_id, user_id, commission)
            VALUES (:company_id, :user_id, :commission)
            """
        ),
        {"company_id": int(company_id), "user_id": user_id, "commission": 0},
    )

    created = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    db.commit()

    row = _fetch_commission_user_row(db, int(created["id"]))
    return {
        "data": _serialize_commission_user_row(row),
        "toast": {
            "type": "success",
            "message": "Usuario anadido a la lista de comisiones.",
        },
    }


@router.get("/{company_id}/commission-users/available")
def available_commission_users(
    company_id: int,
    request: Request,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    q: str = Query(""),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin(request=request, authorization=authorization, db=db)

    normalized_q = (q or "").strip()

    where_parts = ["u.status = 'active'"]
    params: dict[str, object] = {"company_id": int(company_id)}

    if normalized_q:
        where_parts.append(
            "(" \
            "u.email LIKE :q " \
            "OR (:q_is_numeric = 1 AND u.id = :q_id)" \
            ")"
        )
        params["q"] = f"%{normalized_q}%"
        if normalized_q.isdigit():
            params["q_is_numeric"] = 1
            params["q_id"] = int(normalized_q)
        else:
            params["q_is_numeric"] = 0
            params["q_id"] = -1

    where_sql = " AND ".join(where_parts)

    total_row = db.execute(
        text(
            f"""
            SELECT COUNT(*) AS c
            FROM users u
            WHERE {where_sql}
            """
        ),
        params,
    ).mappings().first()
    total = int(total_row["c"] if total_row else 0)

    offset = (page - 1) * per_page
    rows = db.execute(
        text(
            f"""
            SELECT
                u.id,
                u.email,
                CASE
                    WHEN u.display_name IS NOT NULL AND TRIM(u.display_name) <> '' THEN u.display_name
                    ELSE TRIM(CONCAT(COALESCE(u.first_name, ''), ' ', COALESCE(u.last_name, '')))
                END AS display_name,
                ccu.id AS commission_user_id
            FROM users u
            LEFT JOIN company_commission_users ccu
                ON ccu.user_id = u.id
               AND ccu.company_id = :company_id
            WHERE {where_sql}
            ORDER BY u.first_name ASC, u.last_name ASC, u.email ASC
            LIMIT :limit OFFSET :offset
            """
        ),
        {**params, "limit": per_page, "offset": offset},
    ).mappings().all()

    data = [
        {
            "id": int(row["id"]),
            "email": row["email"],
            "display_name": str(row["display_name"] or "").strip(),
            "attached": row["commission_user_id"] is not None,
            "commission_user_id": int(row["commission_user_id"]) if row["commission_user_id"] is not None else None,
        }
        for row in rows
    ]

    last_page = max(1, (total + per_page - 1) // per_page)
    return {
        "data": data,
        "meta": {
            "current_page": page,
            "last_page": last_page,
            "per_page": per_page,
            "total": total,
        },
    }


@router.delete("/{company_id}/commission-users/{commission_user_id}")
def destroy_commission_user(
    company_id: int,
    commission_user_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin(request=request, authorization=authorization, db=db)

    row = db.execute(
        text(
            """
            SELECT id, company_id
            FROM company_commission_users
            WHERE id = :commission_user_id
            LIMIT 1
            """
        ),
        {"commission_user_id": int(commission_user_id)},
    ).mappings().first()

    if not row or int(row.get("company_id") or 0) != int(company_id):
        raise HTTPException(status_code=404, detail="Not Found")

    db.execute(
        text(
            """
            DELETE FROM company_commission_users
            WHERE id = :commission_user_id
            """
        ),
        {"commission_user_id": int(commission_user_id)},
    )
    db.commit()

    return {
        "toast": {
            "type": "success",
            "message": "Usuario eliminado de la lista de comisiones.",
        }
    }


@router.patch("/{company_id}/commission-users/{commission_user_id}")
def update_commission_user(
    company_id: int,
    commission_user_id: int,
    payload: CommissionUserUpdatePayload,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin(request=request, authorization=authorization, db=db)

    row = _fetch_commission_user_row(db, int(commission_user_id))
    if not row or int(row.get("company_id") or 0) != int(company_id):
        raise HTTPException(status_code=404, detail="Not Found")

    db.execute(
        text(
            """
            UPDATE company_commission_users
            SET commission = :commission
            WHERE id = :commission_user_id
            """
        ),
        {
            "commission": float(payload.commission),
            "commission_user_id": int(commission_user_id),
        },
    )
    db.commit()

    updated = _fetch_commission_user_row(db, int(commission_user_id))
    return {
        "data": _serialize_commission_user_row(updated),
        "toast": {
            "type": "success",
            "message": "Comision actualizada correctamente.",
        },
    }
