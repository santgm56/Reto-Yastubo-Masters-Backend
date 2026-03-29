from __future__ import annotations

import json

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.services.auth_service import AuthService

router = APIRouter(prefix="/api/v1/admin/products", tags=["admin-products"])

_ALLOWED_PRODUCT_TYPES = {"plan_regular", "plan_capitado"}
_ALLOWED_STATUSES = {"active", "inactive"}


def _extract_bearer_token(authorization: str | None) -> str:
    if not authorization:
        return ""

    raw = authorization.strip()
    if not raw.lower().startswith("bearer "):
        return ""

    return raw[7:].strip()


def _require_admin_products_manage(request: Request, authorization: str | None, db: Session) -> None:
    token = _extract_bearer_token(authorization)
    if not token:
        token = str(request.cookies.get("yastubo_access_token") or "").strip()

    if not token:
        raise HTTPException(status_code=403, detail="Forbidden")

    auth_payload = AuthService(db).me(token)
    permissions = [str(item) for item in (auth_payload.get("permissions") or [])]
    if "admin.products.manage" not in permissions:
        raise HTTPException(status_code=403, detail="Forbidden")


def _validation_error(errors: dict[str, list[str]]) -> None:
    raise HTTPException(
        status_code=422,
        detail={
            "code": "API_VALIDATION_ERROR",
            "message": "The given data was invalid.",
            "errors": errors,
        },
    )


def _parse_json_field(raw_value, *, fallback: dict | None = None) -> dict:
    if isinstance(raw_value, dict):
        return raw_value
    if isinstance(raw_value, str):
        try:
            parsed = json.loads(raw_value)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return fallback or {}
    return fallback or {}


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _validate_translatable(payload: dict, field: str, *, required_es: bool) -> dict:
    value = payload.get(field)
    if value is None:
        return {"es": "", "en": ""}

    if not isinstance(value, dict):
        _validation_error({field: ["El campo debe ser un objeto con llaves es/en."]})

    es = _normalize_text(value.get("es"))
    en = _normalize_text(value.get("en"))

    field_errors: dict[str, list[str]] = {}
    if required_es and not es:
        field_errors[f"{field}.es"] = ["El nombre en español es obligatorio."]
    if len(es) > 255:
        field_errors.setdefault(f"{field}.es", []).append("No puede superar 255 caracteres.")
    if len(en) > 255:
        field_errors.setdefault(f"{field}.en", []).append("No puede superar 255 caracteres.")

    if field_errors:
        _validation_error(field_errors)

    return {"es": es, "en": en}


def _validate_store_payload(payload: dict, db: Session) -> dict:
    name = _validate_translatable(payload, "name", required_es=True)
    description = _validate_translatable(payload, "description", required_es=False)

    product_type = _normalize_text(payload.get("product_type"))
    if product_type not in _ALLOWED_PRODUCT_TYPES:
        _validation_error({"product_type": ["Tipo de producto invalido."]})

    raw_company_id = payload.get("company_id")
    company_id: int | None = None
    if raw_company_id not in [None, "", 0, "0"]:
        try:
            company_id = int(raw_company_id)
        except (TypeError, ValueError):
            _validation_error({"company_id": ["Empresa invalida."]})

    if product_type == "plan_capitado" and company_id is None:
        _validation_error({"company_id": ["Los productos capitados deben tener empresa asociada."]})

    if product_type != "plan_capitado" and company_id is not None:
        _validation_error({"company_id": ["Solo productos capitados pueden asociarse a empresa."]})

    if company_id is not None:
        company_exists = db.execute(
            text("SELECT id FROM companies WHERE id = :company_id LIMIT 1"),
            {"company_id": company_id},
        ).mappings().first()
        if not company_exists:
            _validation_error({"company_id": ["Empresa invalida."]})

    show_in_widget = bool(payload.get("show_in_widget", False))

    return {
        "name": name,
        "description": description,
        "product_type": product_type,
        "company_id": company_id,
        "show_in_widget": show_in_widget,
        "status": "inactive",
    }


def _validate_update_payload(payload: dict) -> dict:
    name = _validate_translatable(payload, "name", required_es=True)
    description = _validate_translatable(payload, "description", required_es=False)

    status = _normalize_text(payload.get("status"))
    if status not in _ALLOWED_STATUSES:
        _validation_error({"status": ["Estado invalido."]})

    show_in_widget = bool(payload.get("show_in_widget", False))

    return {
        "name": name,
        "description": description,
        "show_in_widget": show_in_widget,
        "status": status,
    }


def _serialize_product(row) -> dict:
    return {
        "id": int(row["id"]),
        "company_id": int(row["company_id"]) if row.get("company_id") is not None else None,
        "status": str(row.get("status") or "inactive"),
        "product_type": str(row.get("product_type") or ""),
        "show_in_widget": bool(row.get("show_in_widget")),
        "name": _parse_json_field(row.get("name"), fallback={"es": "", "en": ""}),
        "description": _parse_json_field(row.get("description"), fallback={"es": "", "en": ""}),
    }


def _product_type_label(product_type: str) -> str:
    return {
        "plan_regular": "Plan regular",
        "plan_capitado": "Plan capitado",
    }.get(product_type, product_type.replace("_", " ").strip().title())


def _serialize_product_type_options() -> list[dict]:
    return [
        {
            "value": product_type,
            "label": _product_type_label(product_type),
        }
        for product_type in sorted(_ALLOWED_PRODUCT_TYPES)
    ]


def _fetch_product(db: Session, product_id: int):
    return db.execute(
        text(
            """
            SELECT id, company_id, status, product_type, show_in_widget, name, description
            FROM products
            WHERE id = :product_id
            LIMIT 1
            """
        ),
        {"product_id": int(product_id)},
    ).mappings().first()


@router.get("")
def index_products(
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)

    rows = db.execute(
        text(
            """
            SELECT id, company_id, status, product_type, show_in_widget, name, description
            FROM products
            WHERE product_type = 'plan_regular'
            ORDER BY id DESC
            """
        )
    ).mappings().all()

    return {
        "data": [_serialize_product(row) for row in rows],
        "meta": {
            "total": len(rows),
            "product_types": _serialize_product_type_options(),
        },
    }


@router.get("/{product_id:int}")
def show_product(
    product_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)

    row = _fetch_product(db=db, product_id=int(product_id))
    if not row:
        raise HTTPException(status_code=404, detail="Not Found")

    return {"data": _serialize_product(row)}


@router.post("")
async def store_product(
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload invalido."]})

    validated = _validate_store_payload(payload=payload, db=db)

    db.execute(
        text(
            """
            INSERT INTO products (
                company_id,
                status,
                name,
                description,
                product_type,
                show_in_widget,
                created_at,
                updated_at
            ) VALUES (
                :company_id,
                :status,
                CAST(:name AS JSON),
                CAST(:description AS JSON),
                :product_type,
                :show_in_widget,
                NOW(),
                NOW()
            )
            """
        ),
        {
            "company_id": validated["company_id"],
            "status": validated["status"],
            "name": json.dumps(validated["name"], ensure_ascii=False),
            "description": json.dumps(validated["description"], ensure_ascii=False),
            "product_type": validated["product_type"],
            "show_in_widget": 1 if validated["show_in_widget"] else 0,
        },
    )

    created = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    db.commit()

    if not created:
        raise HTTPException(status_code=500, detail="No se pudo crear el producto.")

    product = _fetch_product(db=db, product_id=int(created["id"]))
    if not product:
        raise HTTPException(status_code=500, detail="No se pudo cargar el producto creado.")

    return {
        "data": _serialize_product(product),
        "message": "Producto creado correctamente.",
    }


@router.put("/{product_id:int}")
async def update_product(
    product_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)

    existing = _fetch_product(db=db, product_id=int(product_id))
    if not existing:
        raise HTTPException(status_code=404, detail="Not Found")

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload invalido."]})

    validated = _validate_update_payload(payload)

    db.execute(
        text(
            """
            UPDATE products
            SET
                name = CAST(:name AS JSON),
                description = CAST(:description AS JSON),
                show_in_widget = :show_in_widget,
                status = :status,
                updated_at = NOW()
            WHERE id = :product_id
            """
        ),
        {
            "name": json.dumps(validated["name"], ensure_ascii=False),
            "description": json.dumps(validated["description"], ensure_ascii=False),
            "show_in_widget": 1 if validated["show_in_widget"] else 0,
            "status": validated["status"],
            "product_id": int(product_id),
        },
    )
    db.commit()

    refreshed = _fetch_product(db=db, product_id=int(product_id))
    return {
        "data": _serialize_product(refreshed),
        "message": "Producto actualizado correctamente.",
    }
