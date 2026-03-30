from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from app.core.config import get_settings
from app.core.shell_metrics import get_snapshot as get_shell_metrics_snapshot
from app.routers import include_all_routers

settings = get_settings()
frontend_public_path = (Path(__file__).resolve().parents[2] / ".." / "frontend-yastubo" / "public").resolve()

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

if frontend_public_path.exists():
    app.mount("/assets", StaticFiles(directory=str(frontend_public_path / "assets")), name="frontend-assets")

include_all_routers(app)


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


@app.get("/health/shells")
def health_shells() -> dict[str, object]:
    return {
        "ok": "true",
        "service": settings.app_name,
        "shells": get_shell_metrics_snapshot(),
    }
