from __future__ import annotations

from uuid import uuid4

from fastapi import APIRouter, Header, HTTPException, Query
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.schemas.cancellation import CancellationCreateRequest
from app.schemas.common import ApiResponse
from app.services.cancellation_service import CancellationService
from fastapi import Depends

router = APIRouter(prefix="/api/v1/cancellations", tags=["cancellations"])


def _request_id() -> str:
    return f"req_{uuid4().hex}"


@router.get("", response_model=ApiResponse)
def index(limit: int = Query(120, ge=1, le=300), db: Session = Depends(get_db)) -> ApiResponse:
    service = CancellationService(db)
    rows = service.list_rows(limit=limit)
    return ApiResponse(
        ok=True,
        message="Cancellations obtenidas",
        data={
            "rows": rows,
            "total": len(rows),
        },
        request_id=_request_id(),
    )


@router.post("", response_model=ApiResponse)
def store(
    payload: CancellationCreateRequest,
    db: Session = Depends(get_db),
    x_frontend_user_id: str | None = Header(default=None),
) -> ApiResponse:
    service = CancellationService(db)

    try:
        data = service.request_cancellation(
            contract_id=payload.contract_id,
            reason=payload.reason,
            actor_id=(x_frontend_user_id or "").strip(),
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return ApiResponse(ok=True, message="Anulacion solicitada", data=data, request_id=_request_id())