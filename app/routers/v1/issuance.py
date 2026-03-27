from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.schemas.common import ApiResponse
from app.schemas.issuance import IssuanceCreateRequest, IssuanceQuoteRequest
from app.services.issuance_service import IssuanceService

router = APIRouter(prefix="/api/v1/issuances", tags=["issuance"])


def _request_id() -> str:
    return f"req_{uuid4().hex}"


class IssuanceSendEmailRequest(BaseModel):
    email: EmailStr | None = None


@router.post("/quote", response_model=ApiResponse)
def quote(payload: IssuanceQuoteRequest, db: Session = Depends(get_db)) -> ApiResponse:
    service = IssuanceService(db)
    data = service.quote(payload)
    return ApiResponse(ok=True, message="Cotizacion generada", data=data, request_id=_request_id())


@router.post("", response_model=ApiResponse)
def store(payload: IssuanceCreateRequest, db: Session = Depends(get_db)) -> ApiResponse:
    service = IssuanceService(db)
    data = service.store(payload)
    return ApiResponse(ok=True, message="Emision creada", data=data, request_id=_request_id())


@router.get("/{contract_id}", response_model=ApiResponse)
def show(contract_id: int, db: Session = Depends(get_db)) -> ApiResponse:
    service = IssuanceService(db)
    data = service.show(contract_id)
    if not data:
        raise HTTPException(status_code=404, detail="Contrato no encontrado")

    return ApiResponse(ok=True, message="Contrato obtenido", data=data, request_id=_request_id())


@router.get("", response_model=ApiResponse)
def index(
    status: str = Query("all"),
    term: str = Query(""),
    sort: str = Query("newest"),
    per_page: int = Query(15),
    page: int = Query(1),
    db: Session = Depends(get_db),
) -> ApiResponse:
    service = IssuanceService(db)
    data = service.index(
        status=status,
        term=term,
        sort=sort,
        per_page=per_page,
        page=page,
    )
    return ApiResponse(ok=True, message="Listado de emisiones", data=data, request_id=_request_id())


@router.get("/{contract_id}/pdf", response_model=ApiResponse)
def pdf(contract_id: int, db: Session = Depends(get_db)) -> ApiResponse:
    service = IssuanceService(db)
    data = service.show(contract_id)
    if not data:
        raise HTTPException(status_code=404, detail="Contrato no encontrado")

    return ApiResponse(
        ok=True,
        message="PDF disponible",
        data={
            "issuance_id": data.get("issuance_id"),
            "status": "PDF_READY",
            "download_url": f"/api/v1/issuances/{contract_id}/pdf",
        },
        request_id=_request_id(),
    )


@router.post("/{contract_id}/send-email", response_model=ApiResponse)
def send_email(contract_id: int, payload: IssuanceSendEmailRequest, db: Session = Depends(get_db)) -> ApiResponse:
    service = IssuanceService(db)
    data = service.show(contract_id)
    if not data:
        raise HTTPException(status_code=404, detail="Contrato no encontrado")

    return ApiResponse(
        ok=True,
        message="Email encolado",
        data={
            "issuance_id": data.get("issuance_id"),
            "status": "EMAIL_QUEUED",
            "recipient": payload.email,
        },
        request_id=_request_id(),
    )
