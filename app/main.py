from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.core.config import get_settings
from app.routers.customer import payments as customer_payments_router
from app.routers.customer import portal as customer_portal_router
from app.routers.v1 import auth as auth_router
from app.routers.v1 import audit as audit_router
from app.routers.v1 import admin_companies_core as admin_companies_core_router
from app.routers.v1 import admin_companies_capitated_batches as admin_companies_capitated_batches_router
from app.routers.v1 import admin_companies_capitated_contracts as admin_companies_capitated_contracts_router
from app.routers.v1 import admin_companies_capitated_monthly_reports as admin_companies_capitated_monthly_reports_router
from app.routers.v1 import admin_companies_short_code as admin_companies_short_code_router
from app.routers.v1 import admin_companies_commission_users_available as admin_companies_commission_users_available_router
from app.routers.v1 import admin_companies_users as admin_companies_users_router
from app.routers.v1 import admin_companies_status as admin_companies_status_router
from app.routers.v1 import admin_regalias as admin_regalias_router
from app.routers.v1 import admin_users_search as admin_users_search_router
from app.routers.v1 import admin_products as admin_products_router
from app.routers.v1 import admin_plans as admin_plans_router
from app.routers.v1 import cancellations as cancellations_router
from app.routers.v1 import frontend_bootstrap as frontend_bootstrap_router
from app.routers.v1 import issuance as issuance_router
from app.routers.v1 import payments as payments_router
from app.routers.v1 import public_files as public_files_router
from app.routers.v1 import public_capitated_contracts as public_capitated_contracts_router
from app.routers.v1 import seller_dashboard as seller_dashboard_router
from app.routers.web import backoffice_shell, customer_shell as customer_shell_router

settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    description="Migracion real de contratos API desde Laravel a FastAPI",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.parsed_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(issuance_router.router)
app.include_router(payments_router.router)
app.include_router(cancellations_router.router)
app.include_router(auth_router.router)
app.include_router(frontend_bootstrap_router.router)
app.include_router(public_files_router.router)
app.include_router(public_capitated_contracts_router.router)
app.include_router(customer_payments_router.router)
app.include_router(customer_portal_router.router)
app.include_router(audit_router.router)
app.include_router(admin_companies_short_code_router.router)
app.include_router(admin_companies_commission_users_available_router.router)
app.include_router(admin_companies_users_router.router)
app.include_router(admin_companies_status_router.router)
app.include_router(admin_companies_capitated_batches_router.router)
app.include_router(admin_companies_capitated_contracts_router.router)
app.include_router(admin_companies_capitated_monthly_reports_router.router)
app.include_router(admin_companies_core_router.router)
app.include_router(admin_regalias_router.router)
app.include_router(admin_users_search_router.router)
app.include_router(admin_products_router.router)
app.include_router(admin_plans_router.router)
app.include_router(seller_dashboard_router.router)
app.include_router(customer_shell_router.router)
app.include_router(backoffice_shell.router)


def _request_id() -> str:
    return f"req_{uuid4().hex}"


@app.exception_handler(HTTPException)
async def http_exception_handler(_: Request, exc: HTTPException) -> JSONResponse:
    detail = exc.detail

    if isinstance(detail, dict):
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "code": str(detail.get("code") or "API_HTTP_ERROR"),
                "message": str(detail.get("message") or "Solicitud no valida."),
                "errors": detail.get("errors") if isinstance(detail.get("errors"), dict) else {},
                "details": detail.get("details") if isinstance(detail.get("details"), dict) else None,
                "request_id": str(detail.get("request_id") or _request_id()),
            },
        )

    return JSONResponse(
        status_code=exc.status_code,
        content={
            "code": "API_HTTP_ERROR",
            "message": str(detail or "Solicitud no valida."),
            "errors": {},
            "details": None,
            "request_id": _request_id(),
        },
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    field_errors: dict[str, list[str]] = {}

    for item in exc.errors():
        path = item.get("loc") or []
        field = ".".join(str(token) for token in path[1:]) if len(path) > 1 else "body"
        message = str(item.get("msg") or "Dato invalido")
        field_errors.setdefault(field, []).append(message)

    return JSONResponse(
        status_code=422,
        content={
            "code": "API_VALIDATION_ERROR",
            "message": "La solicitud no cumple las reglas de validacion.",
            "errors": field_errors,
            "details": {"origin": "request_validation"},
            "request_id": _request_id(),
        },
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"ok": "true", "service": settings.app_name}
