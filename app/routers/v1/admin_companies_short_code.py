from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.services.auth_service import AuthService

router = APIRouter(prefix="/api/v1/admin/companies", tags=["admin-companies"])


def _extract_bearer_token(authorization: str | None) -> str:
    if not authorization:
        return ""

    raw = authorization.strip()
    if not raw.lower().startswith("bearer "):
        return ""

    return raw[7:].strip()


@router.get("/check-short-code")
def check_short_code(
    request: Request,
    short_code: str = Query(""),
    company_id: int | None = Query(default=None),
    ignore_id: int | None = Query(default=None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    token = _extract_bearer_token(authorization)
    if not token:
        token = str(request.cookies.get("yastubo_access_token") or "").strip()

    if not token:
        raise HTTPException(status_code=403, detail="Forbidden")

    auth_payload = AuthService(db).me(token)
    role = str(auth_payload.get("role") or "").upper()
    if role != "ADMIN":
        raise HTTPException(status_code=403, detail="Forbidden")

    normalized_short_code = str(short_code or "").strip().upper()
    if normalized_short_code == "":
        return {
            "short_code": normalized_short_code,
            "is_available": False,
            "reason": "empty",
        }

    ignored_company_id = company_id or ignore_id

    query_sql = """
        SELECT 1
        FROM companies
        WHERE UPPER(short_code) = :short_code
    """
    params: dict[str, object] = {"short_code": normalized_short_code}

    if ignored_company_id is not None:
        query_sql += " AND id != :company_id"
        params["company_id"] = int(ignored_company_id)

    query_sql += " LIMIT 1"

    existing = db.execute(text(query_sql), params).mappings().first()
    is_available = existing is None

    return {
        "short_code": normalized_short_code,
        "is_available": is_available,
        "reason": None if is_available else "taken",
    }
