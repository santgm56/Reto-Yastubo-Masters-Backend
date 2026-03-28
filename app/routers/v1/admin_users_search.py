from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.services.auth_service import AuthService

router = APIRouter(prefix="/api/v1/admin/users", tags=["admin-users"])


def _extract_bearer_token(authorization: str | None) -> str:
    if not authorization:
        return ""

    raw = authorization.strip()
    if not raw.lower().startswith("bearer "):
        return ""

    return raw[7:].strip()


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
    token = _extract_bearer_token(authorization)
    if not token:
        token = str(request.cookies.get("yastubo_access_token") or "").strip()

    if not token:
        raise HTTPException(status_code=403, detail="Forbidden")

    auth_payload = AuthService(db).me(token)
    permissions = [str(item) for item in (auth_payload.get("permissions") or [])]
    if "users.viewAny" not in permissions:
        raise HTTPException(status_code=403, detail="Forbidden")

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
