from __future__ import annotations

from uuid import uuid4

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from fastapi import Depends

from app.db.database import get_db
from app.schemas.common import ApiResponse
from app.services.customer_portal_service import CustomerPortalService
from app.services.payment_service import PaymentService

router = APIRouter(prefix="/api/customer", tags=["customer-portal"])

service = CustomerPortalService()


def _request_id() -> str:
    return f"req_{uuid4().hex}"


class BeneficiaryCreateRequest(BaseModel):
    nombre: str = Field(min_length=3, max_length=120)
    documento: str = Field(min_length=5, max_length=40)
    parentesco: str = Field(min_length=3, max_length=80)
    estado: str = Field(pattern="^(activo|incompleto|bloqueado)$")


class DeathReportCreateRequest(BaseModel):
    nombreReportante: str = Field(min_length=3, max_length=80)
    documentoReportante: str = Field(min_length=5, max_length=20)
    nombreFallecido: str = Field(min_length=3, max_length=80)
    documentoFallecido: str = Field(min_length=5, max_length=20)
    fechaFallecimiento: str = Field(min_length=10, max_length=10)
    observacion: str = Field(min_length=10, max_length=300)
    canalContacto: str = Field(pattern="^(email|telefono)$")


class PaymentMethodUpsertRequest(BaseModel):
    reference: str = Field(min_length=6, max_length=60)
    brand: str | None = Field(default="CARD", max_length=20)


@router.get("/portal/modules", response_model=ApiResponse)
def modules(x_frontend_user_id: str | None = Header(default=None)) -> ApiResponse:
    data = service.modules(x_frontend_user_id)
    return ApiResponse(ok=True, message="Modulo customer portal", data=data, request_id=_request_id())


@router.get("/beneficiaries", response_model=ApiResponse)
def beneficiaries_index(x_frontend_user_id: str | None = Header(default=None)) -> ApiResponse:
    data = service.beneficiaries_index(x_frontend_user_id)
    return ApiResponse(ok=True, message="Beneficiarios obtenidos", data=data, request_id=_request_id())


@router.post("/beneficiaries", response_model=ApiResponse)
def beneficiaries_store(payload: BeneficiaryCreateRequest, x_frontend_user_id: str | None = Header(default=None)) -> ApiResponse:
    item, duplicate_error = service.beneficiaries_store(payload.model_dump(), x_frontend_user_id)
    if duplicate_error:
        raise HTTPException(status_code=422, detail=duplicate_error)

    return ApiResponse(ok=True, message="Beneficiario creado", data={"item": item}, request_id=_request_id())


@router.get("/death-report", response_model=ApiResponse)
def death_report_show(x_frontend_user_id: str | None = Header(default=None)) -> ApiResponse:
    data = service.death_report_show(x_frontend_user_id)
    return ApiResponse(ok=True, message="Reporte de fallecimiento", data=data, request_id=_request_id())


@router.post("/death-report", response_model=ApiResponse)
def death_report_store(payload: DeathReportCreateRequest, x_frontend_user_id: str | None = Header(default=None)) -> ApiResponse:
    data = service.death_report_store(payload.model_dump(), x_frontend_user_id)
    return ApiResponse(ok=True, message="Reporte registrado", data=data, request_id=_request_id())


@router.get("/payment-method", response_model=ApiResponse)
def payment_method_show(x_frontend_user_id: str | None = Header(default=None)) -> ApiResponse:
    data = service.payment_method_show(x_frontend_user_id)
    return ApiResponse(ok=True, message="Metodo de pago obtenido", data=data, request_id=_request_id())


@router.post("/payment-method", response_model=ApiResponse)
def payment_method_upsert(payload: PaymentMethodUpsertRequest, x_frontend_user_id: str | None = Header(default=None)) -> ApiResponse:
    data = service.payment_method_upsert(payload.model_dump(), x_frontend_user_id)
    return ApiResponse(ok=True, message="Metodo de pago actualizado", data=data, request_id=_request_id())


@router.delete("/payment-method", response_model=ApiResponse)
def payment_method_delete(x_frontend_user_id: str | None = Header(default=None)) -> ApiResponse:
    data = service.payment_method_delete(x_frontend_user_id)
    return ApiResponse(ok=True, message="Metodo de pago eliminado", data=data, request_id=_request_id())


@router.get("/payment-history", response_model=ApiResponse)
def payment_history(db: Session = Depends(get_db)) -> ApiResponse:
    rows = PaymentService(db).customer_history()
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


@router.get("/payments/status", response_model=ApiResponse)
def payment_status(db: Session = Depends(get_db)) -> ApiResponse:
    data = PaymentService(db).customer_status()
    return ApiResponse(ok=True, message="Estado de pago", data=data, request_id=_request_id())