from uuid import uuid4

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.schemas.common import ApiResponse
from app.services.seller_dashboard_service import SellerDashboardService

router = APIRouter(prefix="/api/v1/seller", tags=["seller-dashboard"])


def _request_id() -> str:
    return f"req_{uuid4().hex}"


@router.get("/dashboard-summary", response_model=ApiResponse)
def dashboard_summary(db: Session = Depends(get_db)) -> ApiResponse:
    service = SellerDashboardService(db)
    data = service.summary()
    return ApiResponse(ok=True, message="Dashboard seller obtenido", data=data, request_id=_request_id())


@router.get("/customers", response_model=ApiResponse)
def customers(limit: int = Query(50, ge=1, le=200), db: Session = Depends(get_db)) -> ApiResponse:
    service = SellerDashboardService(db)
    rows = service.customers(limit=limit)
    return ApiResponse(
        ok=True,
        message="Clientes seller obtenidos",
        data={"rows": rows, "total": len(rows)},
        request_id=_request_id(),
    )


@router.get("/sales", response_model=ApiResponse)
def sales(limit: int = Query(80, ge=1, le=300), db: Session = Depends(get_db)) -> ApiResponse:
    service = SellerDashboardService(db)
    rows = service.sales(limit=limit)
    return ApiResponse(
        ok=True,
        message="Ventas seller obtenidas",
        data={"rows": rows, "total": len(rows)},
        request_id=_request_id(),
    )
