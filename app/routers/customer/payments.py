from uuid import uuid4

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.schemas.common import ApiResponse
from app.services.payment_service import PaymentService

router = APIRouter(prefix="/api/customer/payments", tags=["customer-payments"])


def _request_id() -> str:
    return f"req_{uuid4().hex}"


@router.get("", response_model=ApiResponse)
def index(db: Session = Depends(get_db)) -> ApiResponse:
    service = PaymentService(db)
    rows = service.customer_history()
    return ApiResponse(
        ok=True,
        message="Historial de pagos",
        data={
            "transactions": rows,
            "rows": rows,
            "total": len(rows),
        },
        request_id=_request_id(),
    )


@router.get("/status", response_model=ApiResponse)
def status(db: Session = Depends(get_db)) -> ApiResponse:
    service = PaymentService(db)
    return ApiResponse(ok=True, message="Estado de pago", data=service.customer_status(), request_id=_request_id())


@router.get("/history", response_model=ApiResponse)
def history_alias(db: Session = Depends(get_db)) -> ApiResponse:
    return index(db=db)
