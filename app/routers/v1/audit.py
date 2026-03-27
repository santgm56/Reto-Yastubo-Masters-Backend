from uuid import uuid4

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.schemas.common import ApiResponse
from app.services.audit_service import AuditService

router = APIRouter(prefix="/api/v1/admin/audit", tags=["admin-audit"])


def _request_id() -> str:
    return f"req_{uuid4().hex}"


@router.get("", response_model=ApiResponse)
def index(
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    action: str | None = Query(None),
    realm: str | None = Query(None),
    db: Session = Depends(get_db),
) -> ApiResponse:
    service = AuditService(db)
    data = service.list_events(page=page, per_page=per_page, action=action, realm=realm)
    return ApiResponse(ok=True, message="Auditoria obtenida", data=data, request_id=_request_id())
