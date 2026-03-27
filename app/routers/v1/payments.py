from uuid import uuid4

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.schemas.common import ApiResponse
from app.schemas.payment import PaymentCheckoutRequest, PaymentSubscribeRequest, PaymentWebhookRequest
from app.services.payment_service import PaymentService

router = APIRouter(prefix="/api/v1/payments", tags=["payments"])


def _request_id() -> str:
    return f"req_{uuid4().hex}"


@router.get("", response_model=ApiResponse)
def index(limit: int = Query(100, ge=1, le=300), db: Session = Depends(get_db)) -> ApiResponse:
    service = PaymentService(db)
    rows = service.list_payments(limit=limit)
    return ApiResponse(
        ok=True,
        message="Pagos obtenidos",
        data={
            "rows": rows,
            "total": len(rows),
        },
        request_id=_request_id(),
    )


@router.post("/{monthly_record_id}/checkout", response_model=ApiResponse)
def checkout(monthly_record_id: int, db: Session = Depends(get_db)) -> ApiResponse:
    service = PaymentService(db)
    data = service.register_payment_event(monthly_record_id, "payment.checkout.started", {"channel": "manual"})
    return ApiResponse(ok=True, message="Checkout iniciado", data=data, request_id=_request_id())


@router.post("/{monthly_record_id}/subscribe", response_model=ApiResponse)
def subscribe(monthly_record_id: int, db: Session = Depends(get_db)) -> ApiResponse:
    service = PaymentService(db)
    data = service.register_payment_event(monthly_record_id, "payment.subscribe.started", {"channel": "stripe"})
    return ApiResponse(ok=True, message="Suscripcion iniciada", data={**data, "checkout_url": "/customer/pagos-pendientes?status=processing"}, request_id=_request_id())


@router.post("/{monthly_record_id}/retry", response_model=ApiResponse)
def retry(monthly_record_id: int, db: Session = Depends(get_db)) -> ApiResponse:
    service = PaymentService(db)
    data = service.register_payment_event(monthly_record_id, "payment.retry.started", {"channel": "stripe"})
    return ApiResponse(ok=True, message="Reintento iniciado", data=data, request_id=_request_id())


@router.post("/webhook", response_model=ApiResponse)
def webhook(event_id: str = Query(""), outcome: str = Query("success"), monthly_record_id: int = Query(...), db: Session = Depends(get_db)) -> ApiResponse:
    service = PaymentService(db)
    result = service.register_webhook_event(monthly_record_id=monthly_record_id, outcome=outcome, event_id=event_id)
    message = "Webhook duplicado ignorado" if result.get("idempotent") else "Webhook procesado"
    return ApiResponse(ok=True, message=message, data=result, request_id=_request_id())


@router.post("/checkout", response_model=ApiResponse)
def checkout_compat(payload: PaymentCheckoutRequest, db: Session = Depends(get_db)) -> ApiResponse:
    return checkout(monthly_record_id=payload.monthly_record_id, db=db)


@router.post("/subscribe", response_model=ApiResponse)
def subscribe_compat(payload: PaymentSubscribeRequest, db: Session = Depends(get_db)) -> ApiResponse:
    return subscribe(monthly_record_id=payload.monthly_record_id, db=db)


@router.post("/webhooks/stripe", response_model=ApiResponse)
def webhook_compat(payload: PaymentWebhookRequest, db: Session = Depends(get_db)) -> ApiResponse:
    return webhook(
        event_id=(payload.event_id or "").strip(),
        outcome=payload.outcome,
        monthly_record_id=payload.monthly_record_id,
        db=db,
    )
