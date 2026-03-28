from uuid import uuid4

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.services.audit_service import AuditService

router = APIRouter(prefix="/api/v1/admin/audit", tags=["admin-audit"])


def _request_id() -> str:
    return f"req_{uuid4().hex}"


@router.get("")
def index(
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    action: str | None = Query(None),
    realm: str | None = Query(None),
    actor_user_id: int | None = Query(None),
    from_date: str | None = Query(None, alias="from"),
    to_date: str | None = Query(None, alias="to"),
    db: Session = Depends(get_db),
) -> dict:
    service = AuditService(db)
    data = service.list_events(
        page=page,
        per_page=per_page,
        action=action,
        realm=realm,
        actor_user_id=actor_user_id,
        from_date=from_date,
        to_date=to_date,
    )
    return {"data": data, "request_id": str(uuid4())}
