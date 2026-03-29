from __future__ import annotations

import json

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.services.auth_service import AuthService

router = APIRouter(prefix="/api/v1/admin/products", tags=["admin-plans"])

_ALLOWED_PRODUCT_TYPES = ("plan_capitado", "plan_regular")


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


def _fetch_product_row(db: Session, product_id: int):
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


def _serialize_product(row) -> dict:
    return {
        "id": int(row["id"]),
        "company_id": int(row.get("company_id")) if row.get("company_id") is not None else None,
        "status": str(row.get("status") or "inactive"),
        "product_type": str(row.get("product_type") or ""),
        "show_in_widget": bool(row.get("show_in_widget")),
        "name": _serialize_translatable(row.get("name")),
        "description": _serialize_translatable(row.get("description")),
    }


def _serialize_plan_version(row) -> dict:
    return {
        "id": int(row["id"]),
        "product_id": int(row.get("product_id") or 0),
        "name": str(row.get("name") or ""),
        "status": str(row.get("status") or "inactive"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "is_deletable": True,
    }


def _ensure_product_exists(db: Session, product_id: int) -> None:
    found = db.execute(
        text(
            """
            SELECT id
            FROM products
            WHERE id = :product_id
            LIMIT 1
            """
        ),
        {"product_id": int(product_id)},
    ).mappings().first()
    if not found:
        raise HTTPException(status_code=404, detail="Not Found")


def _fetch_plan_version(db: Session, product_id: int, plan_version_id: int):
    return db.execute(
        text(
            """
            SELECT id, product_id, name, status, created_at, updated_at
            FROM plan_versions
            WHERE id = :plan_version_id
              AND product_id = :product_id
            LIMIT 1
            """
        ),
        {
            "plan_version_id": int(plan_version_id),
            "product_id": int(product_id),
        },
    ).mappings().first()


def _fetch_plan_version_detail(db: Session, product_id: int, plan_version_id: int):
    return db.execute(
        text(
            """
            SELECT
                id,
                product_id,
                name,
                status,
                max_entry_age,
                max_renewal_age,
                wtime_suicide,
                wtime_preexisting_conditions,
                wtime_accident,
                country_id,
                zone_id,
                price_1,
                price_2,
                price_3,
                price_4
            FROM plan_versions
            WHERE id = :plan_version_id
              AND product_id = :product_id
            LIMIT 1
            """
        ),
        {
            "plan_version_id": int(plan_version_id),
            "product_id": int(product_id),
        },
    ).mappings().first()


def _serialize_plan_version_detail(row) -> dict:
    return {
        "id": int(row["id"]),
        "product_id": int(row.get("product_id") or 0),
        "name": str(row.get("name") or ""),
        "status": str(row.get("status") or "inactive"),
        "max_entry_age": int(row.get("max_entry_age")) if row.get("max_entry_age") is not None else None,
        "max_renewal_age": int(row.get("max_renewal_age")) if row.get("max_renewal_age") is not None else None,
        "wtime_suicide": int(row.get("wtime_suicide")) if row.get("wtime_suicide") is not None else None,
        "wtime_preexisting_conditions": int(row.get("wtime_preexisting_conditions")) if row.get("wtime_preexisting_conditions") is not None else None,
        "wtime_accident": int(row.get("wtime_accident")) if row.get("wtime_accident") is not None else None,
        "country_id": int(row.get("country_id")) if row.get("country_id") is not None else None,
        "zone_id": int(row.get("zone_id")) if row.get("zone_id") is not None else None,
        "price_1": float(row.get("price_1")) if row.get("price_1") is not None else None,
        "price_2": float(row.get("price_2")) if row.get("price_2") is not None else None,
        "price_3": float(row.get("price_3")) if row.get("price_3") is not None else None,
        "price_4": float(row.get("price_4")) if row.get("price_4") is not None else None,
        "can_be_activated": True,
    }


def _fetch_plan_version_coverage_rows(db: Session, plan_version_id: int):
    return db.execute(
        text(
            """
            SELECT
                pvc.id,
                pvc.plan_version_id,
                pvc.coverage_id,
                pvc.sort_order,
                pvc.value_int,
                pvc.value_decimal,
                pvc.value_text,
                pvc.notes,
                c.name AS coverage_name,
                c.description AS coverage_description,
                u.name AS unit_name,
                u.measure_type AS unit_measure_type,
                cc.id AS category_id,
                cc.name AS category_name,
                cc.description AS category_description,
                cc.sort_order AS category_sort_order
            FROM plan_version_coverages pvc
            INNER JOIN coverages c ON c.id = pvc.coverage_id
            LEFT JOIN units_of_measure u ON u.id = c.unit_id
            LEFT JOIN coverage_categories cc ON cc.id = c.category_id
            WHERE pvc.plan_version_id = :plan_version_id
            ORDER BY cc.sort_order, cc.id, pvc.sort_order, pvc.id
            """
        ),
        {"plan_version_id": int(plan_version_id)},
    ).mappings().all()


def _group_coverage_categories(rows) -> list[dict]:
    categories: list[dict] = []
    by_category_id: dict[int, dict] = {}

    for row in rows:
        category_id = int(row.get("category_id") or 0)
        if category_id <= 0:
            continue

        category = by_category_id.get(category_id)
        if category is None:
            category = {
                "id": category_id,
                "name": _serialize_translatable(row.get("category_name")),
                "description": _serialize_translatable(row.get("category_description")),
                "sort_order": int(row.get("category_sort_order") or 0),
                "coverages": [],
            }
            by_category_id[category_id] = category
            categories.append(category)

        category["coverages"].append(_serialize_plan_version_coverage_row(row))

    return categories


@router.get("/{product_id:int}/plans")
def index_plan_versions(
    product_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)
    _ensure_product_exists(db=db, product_id=int(product_id))

    rows = db.execute(
        text(
            """
            SELECT id, product_id, name, status, created_at, updated_at
            FROM plan_versions
            WHERE product_id = :product_id
            ORDER BY id DESC
            """
        ),
        {"product_id": int(product_id)},
    ).mappings().all()

    return {
        "data": [_serialize_plan_version(row) for row in rows],
        "meta": {
            "total": len(rows),
            "product": _serialize_product(_fetch_product_row(db=db, product_id=int(product_id))),
            "product_types": _serialize_product_type_options(),
        },
    }


@router.get("/{product_id:int}/plans/{plan_version_id:int}")
def show_plan_version_bootstrap(
    product_id: int,
    plan_version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)

    product = _fetch_product_row(db=db, product_id=int(product_id))
    if not product:
        raise HTTPException(status_code=404, detail="Not Found")

    plan_version = _fetch_plan_version_detail(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))
    if not plan_version:
        raise HTTPException(status_code=404, detail="Not Found")

    coverage_rows = _fetch_plan_version_coverage_rows(db=db, plan_version_id=int(plan_version_id))

    return {
        "data": {
            "product": _serialize_product(product),
            "plan_version": _serialize_plan_version_detail(plan_version),
            "coverage_categories": _group_coverage_categories(coverage_rows),
        },
        "meta": {
            "product_types": _serialize_product_type_options(),
        },
    }


@router.post("/{product_id:int}/plans")
async def store_plan_version(
    product_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)
    _ensure_product_exists(db=db, product_id=int(product_id))

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload invalido."]})

    name = str(payload.get("name") or "").strip()
    if not name:
        _validation_error({"name": ["El nombre es obligatorio."]})
    if len(name) > 255:
        _validation_error({"name": ["No puede superar 255 caracteres."]})

    db.execute(
        text(
            """
            INSERT INTO plan_versions (product_id, name, status, created_at, updated_at)
            VALUES (:product_id, :name, 'inactive', NOW(), NOW())
            """
        ),
        {"product_id": int(product_id), "name": name},
    )

    created = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    db.commit()

    if not created:
        raise HTTPException(status_code=500, detail="No se pudo crear la version.")

    plan = _fetch_plan_version(db=db, product_id=int(product_id), plan_version_id=int(created["id"]))
    if not plan:
        raise HTTPException(status_code=500, detail="No se pudo cargar la version creada.")

    redirect_url = f"/admin/products/{int(product_id)}/plans/{int(plan['id'])}/edit"

    return {
        "data": _serialize_plan_version(plan),
        "redirect_url": redirect_url,
        "message": "Version creada.",
    }


@router.put("/{product_id:int}/plans/{plan_version_id:int}")
async def update_plan_version(
    product_id: int,
    plan_version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)

    exists = _fetch_plan_version(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))
    if not exists:
        raise HTTPException(status_code=404, detail="Not Found")

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload invalido."]})

    sets = []
    params: dict[str, object] = {
        "plan_version_id": int(plan_version_id),
        "product_id": int(product_id),
    }

    allowed_fields = {
        "name",
        "status",
        "max_entry_age",
        "max_renewal_age",
        "wtime_suicide",
        "wtime_preexisting_conditions",
        "wtime_accident",
        "country_id",
        "zone_id",
        "price_1",
        "price_2",
        "price_3",
        "price_4",
    }

    int_fields = {
        "max_entry_age",
        "max_renewal_age",
        "wtime_suicide",
        "wtime_preexisting_conditions",
        "wtime_accident",
        "country_id",
        "zone_id",
    }
    float_fields = {"price_1", "price_2", "price_3", "price_4"}

    for key, value in payload.items():
        if key not in allowed_fields:
            continue

        if key == "name":
            parsed = str(value or "").strip()
            if not parsed:
                _validation_error({"name": ["El nombre es obligatorio."]})
            params[key] = parsed
        elif key == "status":
            parsed = str(value or "").strip()
            if parsed not in {"inactive", "active", "archived"}:
                _validation_error({"status": ["Estado invalido."]})
            params[key] = parsed
        elif key in int_fields:
            params[key] = None if value in [None, ""] else int(value)
        elif key in float_fields:
            params[key] = None if value in [None, ""] else float(value)
        else:
            params[key] = value

        sets.append(f"{key} = :{key}")

    if sets:
        clause = ",\n                ".join(sets + ["updated_at = NOW()"])
        db.execute(
            text(
                f"""
                UPDATE plan_versions
                SET {clause}
                WHERE id = :plan_version_id
                  AND product_id = :product_id
                """
            ),
            params,
        )
        db.commit()

    refreshed = db.execute(
        text(
            """
            SELECT
                id,
                product_id,
                name,
                status,
                max_entry_age,
                max_renewal_age,
                wtime_suicide,
                wtime_preexisting_conditions,
                wtime_accident,
                country_id,
                zone_id,
                price_1,
                price_2,
                price_3,
                price_4
            FROM plan_versions
            WHERE id = :plan_version_id
              AND product_id = :product_id
            LIMIT 1
            """
        ),
        {
            "plan_version_id": int(plan_version_id),
            "product_id": int(product_id),
        },
    ).mappings().first()

    return {
        "data": _serialize_plan_version_detail(refreshed),
        "message": "Version actualizada.",
    }


@router.post("/{product_id:int}/plans/{plan_version_id:int}/clone")
async def clone_plan_version(
    product_id: int,
    plan_version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)

    source = _fetch_plan_version(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))
    if not source:
        raise HTTPException(status_code=404, detail="Not Found")

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload invalido."]})

    name = str(payload.get("name") or "").strip()
    if not name:
        _validation_error({"name": ["El nombre es obligatorio."]})
    if len(name) > 255:
        _validation_error({"name": ["No puede superar 255 caracteres."]})

    db.execute(
        text(
            """
            INSERT INTO plan_versions (
                product_id,
                name,
                status,
                max_entry_age,
                max_renewal_age,
                wtime_suicide,
                wtime_preexisting_conditions,
                wtime_accident,
                country_id,
                zone_id,
                price_1,
                price_2,
                price_3,
                price_4,
                terms_file_es_id,
                terms_file_en_id,
                terms_html,
                created_at,
                updated_at
            )
            SELECT
                product_id,
                :name,
                'inactive',
                max_entry_age,
                max_renewal_age,
                wtime_suicide,
                wtime_preexisting_conditions,
                wtime_accident,
                country_id,
                zone_id,
                price_1,
                price_2,
                price_3,
                price_4,
                terms_file_es_id,
                terms_file_en_id,
                terms_html,
                NOW(),
                NOW()
            FROM plan_versions
            WHERE id = :source_id
              AND product_id = :product_id
            LIMIT 1
            """
        ),
        {
            "name": name,
            "source_id": int(plan_version_id),
            "product_id": int(product_id),
        },
    )

    created = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    if not created:
        raise HTTPException(status_code=500, detail="No se pudo clonar la version.")

    db.execute(
        text(
            """
            INSERT INTO plan_version_coverages (
                plan_version_id,
                coverage_id,
                sort_order,
                value_int,
                value_decimal,
                value_text,
                notes,
                created_at,
                updated_at
            )
            SELECT
                :new_plan_version_id,
                coverage_id,
                sort_order,
                value_int,
                value_decimal,
                value_text,
                notes,
                NOW(),
                NOW()
            FROM plan_version_coverages
            WHERE plan_version_id = :source_id
            """
        ),
        {
            "new_plan_version_id": int(created["id"]),
            "source_id": int(plan_version_id),
        },
    )
    db.commit()

    cloned = _fetch_plan_version(db=db, product_id=int(product_id), plan_version_id=int(created["id"]))
    if not cloned:
        raise HTTPException(status_code=500, detail="No se pudo cargar la version clonada.")

    redirect_url = f"/admin/products/{int(product_id)}/plans/{int(cloned['id'])}/edit"

    return {
        "data": _serialize_plan_version(cloned),
        "redirect_url": redirect_url,
        "message": "Version clonada.",
    }


@router.delete("/{product_id:int}/plans/{plan_version_id:int}")
def destroy_plan_version(
    product_id: int,
    plan_version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)

    found = _fetch_plan_version(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))
    if not found:
        raise HTTPException(status_code=404, detail="Not Found")

    db.execute(
        text(
            """
            DELETE FROM plan_versions
            WHERE id = :plan_version_id
              AND product_id = :product_id
            """
        ),
        {
            "plan_version_id": int(plan_version_id),
            "product_id": int(product_id),
        },
    )
    db.commit()

    return {
        "message": "Version eliminada correctamente.",
    }


def _parse_terms_html(raw_value) -> dict:
    if isinstance(raw_value, dict):
        return {
            "es": raw_value.get("es"),
            "en": raw_value.get("en"),
        }

    if isinstance(raw_value, str) and raw_value.strip():
        try:
            decoded = json.loads(raw_value)
            if isinstance(decoded, dict):
                return {
                    "es": decoded.get("es"),
                    "en": decoded.get("en"),
                }
        except json.JSONDecodeError:
            return {
                "es": raw_value,
                "en": None,
            }

    return {
        "es": None,
        "en": None,
    }


_CONTINENT_OPTIONS = {
    "EU": "Europa",
    "AF": "Africa",
    "AS": "Asia",
    "OC": "Oceania",
    "AN": "Antartida",
    "NA": "Norteamerica",
    "CA": "Centroamerica y Caribe",
    "SA": "Sudamerica",
}


def _parse_translatable_name(raw_value) -> dict:
    if isinstance(raw_value, dict):
        return {
            "es": raw_value.get("es"),
            "en": raw_value.get("en"),
        }

    if isinstance(raw_value, str) and raw_value.strip():
        try:
            decoded = json.loads(raw_value)
            if isinstance(decoded, dict):
                return {
                    "es": decoded.get("es"),
                    "en": decoded.get("en"),
                }
        except json.JSONDecodeError:
            return {
                "es": raw_value,
                "en": None,
            }

    return {
        "es": None,
        "en": None,
    }


def _serialize_country_base(row) -> dict:
    continent_code = str(row.get("continent_code") or "")
    return {
        "id": int(row["id"]),
        "name": _parse_translatable_name(row.get("name")),
        "iso2": row.get("iso2"),
        "iso3": row.get("iso3"),
        "continent_code": continent_code,
        "continent_label": _CONTINENT_OPTIONS.get(continent_code, continent_code),
        "phone_code": row.get("phone_code"),
        "is_active": bool(row.get("is_active")),
    }


def _serialize_plan_country(row, *, attached: bool | None = None) -> dict:
    base = _serialize_country_base(row)

    if attached is not None:
        base["attached"] = attached

    if "price" in row:
        raw_price = row.get("price")
        base["price"] = float(raw_price) if raw_price is not None else None

    return base


def _serialize_translatable(raw_value, *, empty_default: str = "") -> dict:
    parsed = _parse_translatable_name(raw_value)
    return {
        "es": parsed.get("es") if parsed.get("es") is not None else empty_default,
        "en": parsed.get("en") if parsed.get("en") is not None else empty_default,
    }


def _serialize_plan_version_coverage_row(row) -> dict:
    return {
        "id": int(row["id"]),
        "plan_version_id": int(row.get("plan_version_id") or 0),
        "coverage_id": int(row.get("coverage_id") or 0),
        "sort_order": int(row.get("sort_order") or 0),
        "value_int": int(row.get("value_int")) if row.get("value_int") is not None else None,
        "value_decimal": float(row.get("value_decimal")) if row.get("value_decimal") is not None else None,
        "value_text": _serialize_translatable(row.get("value_text")),
        "notes": _serialize_translatable(row.get("notes")),
        "coverage_name": _serialize_translatable(row.get("coverage_name")),
        "coverage_description": _serialize_translatable(row.get("coverage_description")),
        "unit_name": _serialize_translatable(row.get("unit_name")),
        "unit_measure_type": str(row.get("unit_measure_type") or "none"),
        "category_id": int(row.get("category_id") or 0) if row.get("category_id") is not None else None,
        "category_name": _serialize_translatable(row.get("category_name")),
        "category_description": _serialize_translatable(row.get("category_description")),
        "category_sort_order": int(row.get("category_sort_order") or 0),
    }


def _fetch_plan_version_coverage_row(db: Session, plan_version_id: int, pvc_id: int):
    return db.execute(
        text(
            """
            SELECT
                pvc.id,
                pvc.plan_version_id,
                pvc.coverage_id,
                pvc.sort_order,
                pvc.value_int,
                pvc.value_decimal,
                pvc.value_text,
                pvc.notes,
                c.name AS coverage_name,
                c.description AS coverage_description,
                u.name AS unit_name,
                u.measure_type AS unit_measure_type,
                cc.id AS category_id,
                cc.name AS category_name,
                cc.description AS category_description,
                cc.sort_order AS category_sort_order
            FROM plan_version_coverages pvc
            INNER JOIN coverages c ON c.id = pvc.coverage_id
            LEFT JOIN units_of_measure u ON u.id = c.unit_id
            LEFT JOIN coverage_categories cc ON cc.id = c.category_id
            WHERE pvc.plan_version_id = :plan_version_id
              AND pvc.id = :pvc_id
            LIMIT 1
            """
        ),
        {
            "plan_version_id": int(plan_version_id),
            "pvc_id": int(pvc_id),
        },
    ).mappings().first()


def _fetch_plan_version_coverage_by_catalog(db: Session, plan_version_id: int, coverage_id: int):
    return db.execute(
        text(
            """
            SELECT id
            FROM plan_version_coverages
            WHERE plan_version_id = :plan_version_id
              AND coverage_id = :coverage_id
            LIMIT 1
            """
        ),
        {
            "plan_version_id": int(plan_version_id),
            "coverage_id": int(coverage_id),
        },
    ).mappings().first()


def _ensure_plan_belongs(db: Session, product_id: int, plan_version_id: int) -> None:
    found = db.execute(
        text(
            """
            SELECT id
            FROM plan_versions
            WHERE id = :plan_version_id
              AND product_id = :product_id
            LIMIT 1
            """
        ),
        {
            "plan_version_id": int(plan_version_id),
            "product_id": int(product_id),
        },
    ).mappings().first()

    if not found:
        raise HTTPException(status_code=404, detail="Not Found")


def _normalize_id_list(raw_values) -> list[int]:
    values = raw_values if isinstance(raw_values, list) else []
    result: list[int] = []

    for value in values:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue

        if parsed > 0 and parsed not in result:
            result.append(parsed)

    return result


def _zone_country_ids(db: Session, zone_id: int) -> list[int]:
    rows = db.execute(
        text(
            """
            SELECT country_id
            FROM country_zone
            WHERE zone_id = :zone_id
            """
        ),
        {"zone_id": int(zone_id)},
    ).mappings().all()
    return [int(row["country_id"]) for row in rows if row.get("country_id") is not None]


def _countries_by_ids(db: Session, country_ids: list[int]):
    if not country_ids:
        return []

    placeholders = ", ".join(f":id_{idx}" for idx in range(len(country_ids)))
    params = {f"id_{idx}": int(value) for idx, value in enumerate(country_ids)}

    return db.execute(
        text(
            f"""
            SELECT id, name, iso2, iso3, continent_code, phone_code, is_active
            FROM countries
            WHERE id IN ({placeholders})
            """
        ),
        params,
    ).mappings().all()


@router.get("/{product_id:int}/plans/{plan_version_id:int}/terms-html")
def show_terms_html(
    product_id: int,
    plan_version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)

    found = _fetch_plan_version(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))
    if not found:
        raise HTTPException(status_code=404, detail="Not Found")

    terms_raw = db.execute(
        text(
            """
            SELECT terms_html
            FROM plan_versions
            WHERE id = :plan_version_id
              AND product_id = :product_id
            LIMIT 1
            """
        ),
        {
            "plan_version_id": int(plan_version_id),
            "product_id": int(product_id),
        },
    ).mappings().first()

    if not terms_raw:
        raise HTTPException(status_code=404, detail="Not Found")

    return {
        "data": {
            "terms_html": _parse_terms_html(terms_raw.get("terms_html")),
        }
    }


@router.patch("/{product_id:int}/plans/{plan_version_id:int}/terms-html")
async def update_terms_html(
    product_id: int,
    plan_version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)

    found = _fetch_plan_version(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))
    if not found:
        raise HTTPException(status_code=404, detail="Not Found")

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload invalido."]})

    locale = str(payload.get("locale") or "").strip().lower()
    if locale not in {"es", "en"}:
        _validation_error({"locale": ["Debe ser es o en."]})

    html = payload.get("html")
    if html is not None and not isinstance(html, str):
        _validation_error({"html": ["Debe ser string o null."]})

    current_raw = db.execute(
        text(
            """
            SELECT terms_html
            FROM plan_versions
            WHERE id = :plan_version_id
              AND product_id = :product_id
            LIMIT 1
            """
        ),
        {
            "plan_version_id": int(plan_version_id),
            "product_id": int(product_id),
        },
    ).mappings().first()

    current = _parse_terms_html(current_raw.get("terms_html") if current_raw else None)
    current[locale] = html

    db.execute(
        text(
            """
            UPDATE plan_versions
            SET terms_html = CAST(:terms_html AS JSON),
                updated_at = NOW()
            WHERE id = :plan_version_id
              AND product_id = :product_id
            """
        ),
        {
            "terms_html": json.dumps(current, ensure_ascii=False),
            "plan_version_id": int(plan_version_id),
            "product_id": int(product_id),
        },
    )
    db.commit()

    updated_message = "Términos (EN) actualizados." if locale == "en" else "Términos (ES) actualizados."

    return {
        "data": {
            "terms_html": current,
        },
        "message": updated_message,
    }


@router.get("/{product_id:int}/plans/{plan_version_id:int}/countries")
def index_plan_countries(
    product_id: int,
    plan_version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)
    _ensure_plan_belongs(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))

    plan_rows = db.execute(
        text(
            """
            SELECT c.id, c.name, c.iso2, c.iso3, c.continent_code, c.phone_code, c.is_active, pvc.price
            FROM plan_version_countries pvc
            INNER JOIN countries c ON c.id = pvc.country_id
            WHERE pvc.plan_version_id = :plan_version_id
            ORDER BY c.name
            """
        ),
        {"plan_version_id": int(plan_version_id)},
    ).mappings().all()

    all_rows = db.execute(
        text(
            """
            SELECT c.id, c.name, c.iso2, c.iso3, c.continent_code, c.phone_code, c.is_active,
                   CASE WHEN pvc.country_id IS NULL THEN 0 ELSE 1 END AS attached,
                   pvc.price
            FROM countries c
            LEFT JOIN plan_version_countries pvc
              ON pvc.country_id = c.id AND pvc.plan_version_id = :plan_version_id
            ORDER BY c.name
            """
        ),
        {"plan_version_id": int(plan_version_id)},
    ).mappings().all()

    zones = db.execute(
        text(
            """
            SELECT z.id, z.name, COUNT(cz.country_id) AS countries_count
            FROM zones z
            LEFT JOIN country_zone cz ON cz.zone_id = z.id
            WHERE z.is_active = 1
            GROUP BY z.id, z.name
            ORDER BY z.name
            """
        )
    ).mappings().all()

    return {
        "data": {
            "plan_countries": [_serialize_plan_country(row) for row in plan_rows],
            "countries": [
                _serialize_plan_country(row, attached=bool(row.get("attached")))
                for row in all_rows
            ],
            "zones": [
                {
                    "id": int(row["id"]),
                    "name": row.get("name"),
                    "countries_count": int(row.get("countries_count") or 0),
                }
                for row in zones
            ],
            "continents": _CONTINENT_OPTIONS,
        }
    }


@router.post("/{product_id:int}/plans/{plan_version_id:int}/countries")
async def store_plan_countries(
    product_id: int,
    plan_version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)
    _ensure_plan_belongs(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload invalido."]})

    country_ids = _normalize_id_list(payload.get("country_ids"))
    if not country_ids:
        _validation_error({"country_ids": ["No se recibieron paises validos."]})

    existing_rows = db.execute(
        text(
            """
            SELECT country_id
            FROM plan_version_countries
            WHERE plan_version_id = :plan_version_id
            """
        ),
        {"plan_version_id": int(plan_version_id)},
    ).mappings().all()
    already = {int(row["country_id"]) for row in existing_rows if row.get("country_id") is not None}
    new_ids = [country_id for country_id in country_ids if country_id not in already]

    if not new_ids:
        return {
            "message": "Los paises seleccionados ya estaban asociados a la version.",
            "data": {"countries": []},
        }

    for country_id in new_ids:
        db.execute(
            text(
                """
                INSERT INTO plan_version_countries (plan_version_id, country_id, price, created_at, updated_at)
                VALUES (:plan_version_id, :country_id, NULL, NOW(), NOW())
                """
            ),
            {
                "plan_version_id": int(plan_version_id),
                "country_id": int(country_id),
            },
        )
    db.commit()

    countries = _countries_by_ids(db=db, country_ids=new_ids)
    return {
        "toast": {
            "message": "Paises anadidos correctamente a la version.",
            "type": "success",
        },
        "data": {
            "countries": [_serialize_plan_country(row, attached=True) for row in countries],
        },
    }


@router.post("/{product_id:int}/plans/{plan_version_id:int}/countries/attach-zone")
async def attach_zone_plan_countries(
    product_id: int,
    plan_version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)
    _ensure_plan_belongs(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))

    payload = await request.json()
    zone_id = int((payload or {}).get("zone_id") or 0)
    if zone_id <= 0:
        _validation_error({"zone_id": ["Zona no valida."]})

    zone = db.execute(
        text("SELECT id FROM zones WHERE id = :zone_id AND is_active = 1 LIMIT 1"),
        {"zone_id": zone_id},
    ).mappings().first()
    if not zone:
        raise HTTPException(status_code=404, detail="Zona no encontrada o inactiva.")

    zone_country_ids = _zone_country_ids(db=db, zone_id=zone_id)
    if not zone_country_ids:
        _validation_error({"zone_id": ["La zona seleccionada no tiene paises asociados."]})

    existing_rows = db.execute(
        text(
            """
            SELECT country_id
            FROM plan_version_countries
            WHERE plan_version_id = :plan_version_id
            """
        ),
        {"plan_version_id": int(plan_version_id)},
    ).mappings().all()
    already = {int(row["country_id"]) for row in existing_rows if row.get("country_id") is not None}
    new_ids = [country_id for country_id in zone_country_ids if country_id not in already]

    if not new_ids:
        return {
            "message": "Todos los paises de la zona ya estan asociados a la version.",
            "data": {"countries": []},
        }

    for country_id in new_ids:
        db.execute(
            text(
                """
                INSERT INTO plan_version_countries (plan_version_id, country_id, price, created_at, updated_at)
                VALUES (:plan_version_id, :country_id, NULL, NOW(), NOW())
                """
            ),
            {
                "plan_version_id": int(plan_version_id),
                "country_id": int(country_id),
            },
        )
    db.commit()

    countries = _countries_by_ids(db=db, country_ids=new_ids)
    return {
        "toast": {
            "message": "Zona anadida correctamente a la version.",
            "type": "success",
        },
        "data": {
            "countries": [_serialize_plan_country(row, attached=True) for row in countries],
        },
    }


@router.patch("/{product_id:int}/plans/{plan_version_id:int}/countries/{country_id:int}")
async def update_plan_country(
    product_id: int,
    plan_version_id: int,
    country_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)
    _ensure_plan_belongs(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))

    found = db.execute(
        text(
            """
            SELECT country_id
            FROM plan_version_countries
            WHERE plan_version_id = :plan_version_id
              AND country_id = :country_id
            LIMIT 1
            """
        ),
        {
            "plan_version_id": int(plan_version_id),
            "country_id": int(country_id),
        },
    ).mappings().first()
    if not found:
        raise HTTPException(status_code=404, detail="Not Found")

    payload = await request.json()
    raw_price = (payload or {}).get("price")
    price_value = None
    if raw_price not in [None, ""]:
        try:
            price_value = float(raw_price)
        except (TypeError, ValueError):
            _validation_error({"price": ["El precio debe ser numerico o vacio."]})
        if price_value < 0:
            _validation_error({"price": ["El precio debe ser positivo o vacio."]})

    db.execute(
        text(
            """
            UPDATE plan_version_countries
            SET price = :price,
                updated_at = NOW()
            WHERE plan_version_id = :plan_version_id
              AND country_id = :country_id
            """
        ),
        {
            "price": price_value,
            "plan_version_id": int(plan_version_id),
            "country_id": int(country_id),
        },
    )
    db.commit()

    country_rows = _countries_by_ids(db=db, country_ids=[int(country_id)])
    country_payload = _serialize_plan_country(country_rows[0], attached=True) if country_rows else {"id": int(country_id)}
    country_payload["price"] = price_value

    return {
        "toast": {
            "message": "Precio por pais actualizado correctamente.",
            "type": "success",
        },
        "data": country_payload,
    }


@router.delete("/{product_id:int}/plans/{plan_version_id:int}/countries/{country_id:int}")
def destroy_plan_country(
    product_id: int,
    plan_version_id: int,
    country_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)
    _ensure_plan_belongs(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))

    found = db.execute(
        text(
            """
            SELECT country_id
            FROM plan_version_countries
            WHERE plan_version_id = :plan_version_id
              AND country_id = :country_id
            LIMIT 1
            """
        ),
        {
            "plan_version_id": int(plan_version_id),
            "country_id": int(country_id),
        },
    ).mappings().first()

    if not found:
        return {
            "message": "El pais no esta asociado a esta version.",
            "data": {"countries": []},
        }

    db.execute(
        text(
            """
            DELETE FROM plan_version_countries
            WHERE plan_version_id = :plan_version_id
              AND country_id = :country_id
            """
        ),
        {
            "plan_version_id": int(plan_version_id),
            "country_id": int(country_id),
        },
    )
    db.commit()

    country_rows = _countries_by_ids(db=db, country_ids=[int(country_id)])
    payload = _serialize_plan_country(country_rows[0]) if country_rows else {"id": int(country_id)}
    return {
        "toast": {
            "message": "Pais quitado correctamente de la version.",
            "type": "success",
        },
        "data": {
            "countries": [payload],
        },
    }


@router.post("/{product_id:int}/plans/{plan_version_id:int}/countries/detach-by-zone")
async def detach_zone_plan_countries(
    product_id: int,
    plan_version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)
    _ensure_plan_belongs(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))

    payload = await request.json()
    zone_id = int((payload or {}).get("zone_id") or 0)
    if zone_id <= 0:
        _validation_error({"zone_id": ["Zona no valida."]})

    zone = db.execute(
        text("SELECT id FROM zones WHERE id = :zone_id LIMIT 1"),
        {"zone_id": zone_id},
    ).mappings().first()
    if not zone:
        raise HTTPException(status_code=404, detail="Zona no encontrada.")

    zone_country_ids = _zone_country_ids(db=db, zone_id=zone_id)
    if not zone_country_ids:
        _validation_error({"zone_id": ["La zona seleccionada no tiene paises asociados."]})

    placeholders = ", ".join(f":cid_{idx}" for idx in range(len(zone_country_ids)))
    params = {"plan_version_id": int(plan_version_id)}
    params.update({f"cid_{idx}": int(value) for idx, value in enumerate(zone_country_ids)})

    attached_rows = db.execute(
        text(
            f"""
            SELECT country_id
            FROM plan_version_countries
            WHERE plan_version_id = :plan_version_id
              AND country_id IN ({placeholders})
            """
        ),
        params,
    ).mappings().all()
    attached_ids = [int(row["country_id"]) for row in attached_rows if row.get("country_id") is not None]

    if not attached_ids:
        return {
            "message": "Ninguno de los paises de la zona esta asociado a esta version.",
            "data": {"countries": []},
        }

    delete_placeholders = ", ".join(f":did_{idx}" for idx in range(len(attached_ids)))
    delete_params = {"plan_version_id": int(plan_version_id)}
    delete_params.update({f"did_{idx}": int(value) for idx, value in enumerate(attached_ids)})

    db.execute(
        text(
            f"""
            DELETE FROM plan_version_countries
            WHERE plan_version_id = :plan_version_id
              AND country_id IN ({delete_placeholders})
            """
        ),
        delete_params,
    )
    db.commit()

    countries = _countries_by_ids(db=db, country_ids=attached_ids)
    return {
        "toast": {
            "message": "Paises de la zona quitados correctamente de la version.",
            "type": "success",
        },
        "data": {
            "countries": [_serialize_plan_country(row) for row in countries],
        },
    }


@router.get("/{product_id:int}/plans/{plan_version_id:int}/repatriation-countries")
def index_repatriation_countries(
    product_id: int,
    plan_version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)
    _ensure_plan_belongs(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))

    plan_rows = db.execute(
        text(
            """
            SELECT c.id, c.name, c.iso2, c.iso3, c.continent_code, c.phone_code, c.is_active
            FROM plan_version_repatriation_countries pvr
            INNER JOIN countries c ON c.id = pvr.country_id
            WHERE pvr.plan_version_id = :plan_version_id
            ORDER BY c.name
            """
        ),
        {"plan_version_id": int(plan_version_id)},
    ).mappings().all()

    all_rows = db.execute(
        text(
            """
            SELECT c.id, c.name, c.iso2, c.iso3, c.continent_code, c.phone_code, c.is_active,
                   CASE WHEN pvr.country_id IS NULL THEN 0 ELSE 1 END AS attached
            FROM countries c
            LEFT JOIN plan_version_repatriation_countries pvr
              ON pvr.country_id = c.id AND pvr.plan_version_id = :plan_version_id
            ORDER BY c.name
            """
        ),
        {"plan_version_id": int(plan_version_id)},
    ).mappings().all()

    zones = db.execute(
        text(
            """
            SELECT z.id, z.name, COUNT(cz.country_id) AS countries_count
            FROM zones z
            LEFT JOIN country_zone cz ON cz.zone_id = z.id
            WHERE z.is_active = 1
            GROUP BY z.id, z.name
            ORDER BY z.name
            """
        )
    ).mappings().all()

    return {
        "data": {
            "plan_countries": [_serialize_plan_country(row) for row in plan_rows],
            "countries": [
                _serialize_plan_country(row, attached=bool(row.get("attached")))
                for row in all_rows
            ],
            "zones": [
                {
                    "id": int(row["id"]),
                    "name": row.get("name"),
                    "countries_count": int(row.get("countries_count") or 0),
                }
                for row in zones
            ],
            "continents": _CONTINENT_OPTIONS,
        }
    }


@router.post("/{product_id:int}/plans/{plan_version_id:int}/repatriation-countries")
async def store_repatriation_countries(
    product_id: int,
    plan_version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)
    _ensure_plan_belongs(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))

    payload = await request.json()
    country_ids = _normalize_id_list((payload or {}).get("country_ids"))
    if not country_ids:
        _validation_error({"country_ids": ["No se recibieron paises validos."]})

    existing_rows = db.execute(
        text(
            """
            SELECT country_id
            FROM plan_version_repatriation_countries
            WHERE plan_version_id = :plan_version_id
            """
        ),
        {"plan_version_id": int(plan_version_id)},
    ).mappings().all()
    already = {int(row["country_id"]) for row in existing_rows if row.get("country_id") is not None}
    new_ids = [country_id for country_id in country_ids if country_id not in already]

    if not new_ids:
        return {
            "message": "Los paises seleccionados ya estaban asociados como permitidos para repatriacion.",
            "data": {"countries": []},
        }

    for country_id in new_ids:
        db.execute(
            text(
                """
                INSERT INTO plan_version_repatriation_countries (plan_version_id, country_id, created_at, updated_at)
                VALUES (:plan_version_id, :country_id, NOW(), NOW())
                """
            ),
            {
                "plan_version_id": int(plan_version_id),
                "country_id": int(country_id),
            },
        )
    db.commit()

    countries = _countries_by_ids(db=db, country_ids=new_ids)
    return {
        "toast": {
            "message": "Paises anadidos correctamente como permitidos para repatriacion.",
            "type": "success",
        },
        "data": {
            "countries": [_serialize_plan_country(row, attached=True) for row in countries],
        },
    }


@router.post("/{product_id:int}/plans/{plan_version_id:int}/repatriation-countries/attach-zone")
async def attach_zone_repatriation_countries(
    product_id: int,
    plan_version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)
    _ensure_plan_belongs(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))

    payload = await request.json()
    zone_id = int((payload or {}).get("zone_id") or 0)
    if zone_id <= 0:
        _validation_error({"zone_id": ["Zona no valida."]})

    zone = db.execute(
        text("SELECT id FROM zones WHERE id = :zone_id AND is_active = 1 LIMIT 1"),
        {"zone_id": zone_id},
    ).mappings().first()
    if not zone:
        raise HTTPException(status_code=404, detail="Zona no encontrada o inactiva.")

    zone_country_ids = _zone_country_ids(db=db, zone_id=zone_id)
    if not zone_country_ids:
        _validation_error({"zone_id": ["La zona seleccionada no tiene paises asociados."]})

    existing_rows = db.execute(
        text(
            """
            SELECT country_id
            FROM plan_version_repatriation_countries
            WHERE plan_version_id = :plan_version_id
            """
        ),
        {"plan_version_id": int(plan_version_id)},
    ).mappings().all()
    already = {int(row["country_id"]) for row in existing_rows if row.get("country_id") is not None}
    new_ids = [country_id for country_id in zone_country_ids if country_id not in already]

    if not new_ids:
        return {
            "message": "Todos los paises de la zona ya estan asociados como permitidos para repatriacion en esta version.",
            "data": {"countries": []},
        }

    for country_id in new_ids:
        db.execute(
            text(
                """
                INSERT INTO plan_version_repatriation_countries (plan_version_id, country_id, created_at, updated_at)
                VALUES (:plan_version_id, :country_id, NOW(), NOW())
                """
            ),
            {
                "plan_version_id": int(plan_version_id),
                "country_id": int(country_id),
            },
        )
    db.commit()

    countries = _countries_by_ids(db=db, country_ids=new_ids)
    return {
        "toast": {
            "message": "Zona anadida correctamente como permitida para repatriacion.",
            "type": "success",
        },
        "data": {
            "countries": [_serialize_plan_country(row, attached=True) for row in countries],
        },
    }


@router.delete("/{product_id:int}/plans/{plan_version_id:int}/repatriation-countries/{country_id:int}")
def destroy_repatriation_country(
    product_id: int,
    plan_version_id: int,
    country_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)
    _ensure_plan_belongs(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))

    found = db.execute(
        text(
            """
            SELECT country_id
            FROM plan_version_repatriation_countries
            WHERE plan_version_id = :plan_version_id
              AND country_id = :country_id
            LIMIT 1
            """
        ),
        {
            "plan_version_id": int(plan_version_id),
            "country_id": int(country_id),
        },
    ).mappings().first()

    if not found:
        return {
            "message": "El pais no esta asociado como permitido para repatriacion en esta version.",
            "data": {"countries": []},
        }

    db.execute(
        text(
            """
            DELETE FROM plan_version_repatriation_countries
            WHERE plan_version_id = :plan_version_id
              AND country_id = :country_id
            """
        ),
        {
            "plan_version_id": int(plan_version_id),
            "country_id": int(country_id),
        },
    )
    db.commit()

    country_rows = _countries_by_ids(db=db, country_ids=[int(country_id)])
    payload = _serialize_plan_country(country_rows[0]) if country_rows else {"id": int(country_id)}
    return {
        "toast": {
            "message": "Pais quitado correctamente de los permitidos para repatriacion.",
            "type": "success",
        },
        "data": {
            "countries": [payload],
        },
    }


@router.post("/{product_id:int}/plans/{plan_version_id:int}/repatriation-countries/detach-by-zone")
async def detach_zone_repatriation_countries(
    product_id: int,
    plan_version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)
    _ensure_plan_belongs(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))

    payload = await request.json()
    zone_id = int((payload or {}).get("zone_id") or 0)
    if zone_id <= 0:
        _validation_error({"zone_id": ["Zona no valida."]})

    zone = db.execute(
        text("SELECT id FROM zones WHERE id = :zone_id LIMIT 1"),
        {"zone_id": zone_id},
    ).mappings().first()
    if not zone:
        raise HTTPException(status_code=404, detail="Zona no encontrada.")

    zone_country_ids = _zone_country_ids(db=db, zone_id=zone_id)
    if not zone_country_ids:
        _validation_error({"zone_id": ["La zona seleccionada no tiene paises asociados."]})

    placeholders = ", ".join(f":cid_{idx}" for idx in range(len(zone_country_ids)))
    params = {"plan_version_id": int(plan_version_id)}
    params.update({f"cid_{idx}": int(value) for idx, value in enumerate(zone_country_ids)})

    attached_rows = db.execute(
        text(
            f"""
            SELECT country_id
            FROM plan_version_repatriation_countries
            WHERE plan_version_id = :plan_version_id
              AND country_id IN ({placeholders})
            """
        ),
        params,
    ).mappings().all()
    attached_ids = [int(row["country_id"]) for row in attached_rows if row.get("country_id") is not None]

    if not attached_ids:
        return {
            "message": "Ninguno de los paises de la zona esta asociado como permitido para repatriacion en esta version.",
            "data": {"countries": []},
        }

    delete_placeholders = ", ".join(f":did_{idx}" for idx in range(len(attached_ids)))
    delete_params = {"plan_version_id": int(plan_version_id)}
    delete_params.update({f"did_{idx}": int(value) for idx, value in enumerate(attached_ids)})

    db.execute(
        text(
            f"""
            DELETE FROM plan_version_repatriation_countries
            WHERE plan_version_id = :plan_version_id
              AND country_id IN ({delete_placeholders})
            """
        ),
        delete_params,
    )
    db.commit()

    countries = _countries_by_ids(db=db, country_ids=attached_ids)
    return {
        "toast": {
            "message": "Paises de la zona quitados correctamente de los permitidos para repatriacion.",
            "type": "success",
        },
        "data": {
            "countries": [_serialize_plan_country(row) for row in countries],
        },
    }


@router.get("/{product_id:int}/plans/{plan_version_id:int}/coverages/available")
def available_coverages(
    product_id: int,
    plan_version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)
    _ensure_plan_belongs(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))

    attached_rows = db.execute(
        text(
            """
            SELECT id, coverage_id
            FROM plan_version_coverages
            WHERE plan_version_id = :plan_version_id
            """
        ),
        {"plan_version_id": int(plan_version_id)},
    ).mappings().all()
    attached_map = {int(row["coverage_id"]): int(row["id"]) for row in attached_rows if row.get("coverage_id") is not None}

    categories = db.execute(
        text(
            """
            SELECT id, name, description, sort_order
            FROM coverage_categories
            WHERE status = 'active'
            ORDER BY sort_order, id
            """
        )
    ).mappings().all()

    result = []
    for category in categories:
        coverage_rows = db.execute(
            text(
                """
                SELECT c.id, c.name, c.description, u.id AS unit_id, u.name AS unit_name, u.measure_type AS unit_measure_type
                FROM coverages c
                LEFT JOIN units_of_measure u ON u.id = c.unit_id
                WHERE c.category_id = :category_id
                  AND c.status = 'active'
                ORDER BY c.sort_order, c.id
                """
            ),
            {"category_id": int(category["id"])},
        ).mappings().all()

        result.append(
            {
                "id": int(category["id"]),
                "name": _serialize_translatable(category.get("name")),
                "description": _serialize_translatable(category.get("description")),
                "coverages": [
                    {
                        "id": int(row["id"]),
                        "name": _serialize_translatable(row.get("name")),
                        "description": _serialize_translatable(row.get("description")),
                        "unit": {
                            "id": int(row.get("unit_id") or 0),
                            "name": _serialize_translatable(row.get("unit_name")),
                            "measure_type": row.get("unit_measure_type"),
                        }
                        if row.get("unit_id") is not None
                        else None,
                        "attached": int(row["id"]) in attached_map,
                        "plan_version_coverage_id": attached_map.get(int(row["id"])),
                    }
                    for row in coverage_rows
                ],
            }
        )

    return {"data": result}


@router.post("/{product_id:int}/plans/{plan_version_id:int}/coverages")
async def store_coverage(
    product_id: int,
    plan_version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)
    _ensure_plan_belongs(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))

    payload = await request.json()
    coverage_id = int((payload or {}).get("coverage_id") or 0)
    if coverage_id <= 0:
        _validation_error({"coverage_id": ["Cobertura invalida."]})

    existing = _fetch_plan_version_coverage_by_catalog(db=db, plan_version_id=int(plan_version_id), coverage_id=coverage_id)
    if existing:
        row = _fetch_plan_version_coverage_row(db=db, plan_version_id=int(plan_version_id), pvc_id=int(existing["id"]))
        return {"data": _serialize_plan_version_coverage_row(row)}

    max_sort_row = db.execute(
        text(
            """
            SELECT MAX(pvc.sort_order) AS max_sort
            FROM plan_version_coverages pvc
            INNER JOIN coverages c ON c.id = pvc.coverage_id
            WHERE pvc.plan_version_id = :plan_version_id
              AND c.category_id = (SELECT category_id FROM coverages WHERE id = :coverage_id LIMIT 1)
            """
        ),
        {
            "plan_version_id": int(plan_version_id),
            "coverage_id": int(coverage_id),
        },
    ).mappings().first()
    next_sort = int(max_sort_row.get("max_sort") or 0) + 1

    db.execute(
        text(
            """
            INSERT INTO plan_version_coverages (
                plan_version_id,
                coverage_id,
                sort_order,
                value_int,
                value_decimal,
                value_text,
                notes,
                created_at,
                updated_at
            ) VALUES (
                :plan_version_id,
                :coverage_id,
                :sort_order,
                NULL,
                NULL,
                CAST(:value_text AS JSON),
                CAST(:notes AS JSON),
                NOW(),
                NOW()
            )
            """
        ),
        {
            "plan_version_id": int(plan_version_id),
            "coverage_id": int(coverage_id),
            "sort_order": next_sort,
            "value_text": json.dumps({"es": None, "en": None}, ensure_ascii=False),
            "notes": json.dumps({"es": None, "en": None}, ensure_ascii=False),
        },
    )
    created = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    db.commit()

    row = _fetch_plan_version_coverage_row(db=db, plan_version_id=int(plan_version_id), pvc_id=int(created["id"]))
    return {"data": _serialize_plan_version_coverage_row(row)}


@router.patch("/{product_id:int}/plans/{plan_version_id:int}/coverages/{pvc_id:int}")
async def update_coverage_value(
    product_id: int,
    plan_version_id: int,
    pvc_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)
    _ensure_plan_belongs(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))

    row = _fetch_plan_version_coverage_row(db=db, plan_version_id=int(plan_version_id), pvc_id=int(pvc_id))
    if not row:
        raise HTTPException(status_code=404, detail="Not Found")

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload invalido."]})

    sets = []
    params: dict[str, object] = {"pvc_id": int(pvc_id), "plan_version_id": int(plan_version_id)}

    if "value_int" in payload:
        raw = payload.get("value_int")
        params["value_int"] = None if raw in [None, ""] else int(raw)
        sets.append("value_int = :value_int")

    if "value_decimal" in payload:
        raw = payload.get("value_decimal")
        params["value_decimal"] = None if raw in [None, ""] else float(raw)
        sets.append("value_decimal = :value_decimal")

    if "value_text" in payload:
        value_text = payload.get("value_text") if isinstance(payload.get("value_text"), dict) else None
        params["value_text"] = json.dumps(value_text, ensure_ascii=False) if value_text is not None else None
        sets.append("value_text = CAST(:value_text AS JSON)")

    if "notes" in payload:
        notes = payload.get("notes") if isinstance(payload.get("notes"), dict) else None
        params["notes"] = json.dumps(notes, ensure_ascii=False) if notes is not None else None
        sets.append("notes = CAST(:notes AS JSON)")

    if sets:
        set_clause = ",\n                ".join(sets + ["updated_at = NOW()"])
        db.execute(
            text(
                f"""
                UPDATE plan_version_coverages
                SET {set_clause}
                WHERE id = :pvc_id
                  AND plan_version_id = :plan_version_id
                """
            ),
            params,
        )
        db.commit()

    refreshed = _fetch_plan_version_coverage_row(db=db, plan_version_id=int(plan_version_id), pvc_id=int(pvc_id))
    return {"data": _serialize_plan_version_coverage_row(refreshed)}


@router.delete("/{product_id:int}/plans/{plan_version_id:int}/coverages/{pvc_id:int}")
def destroy_coverage(
    product_id: int,
    plan_version_id: int,
    pvc_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)
    _ensure_plan_belongs(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))

    row = _fetch_plan_version_coverage_row(db=db, plan_version_id=int(plan_version_id), pvc_id=int(pvc_id))
    if not row:
        raise HTTPException(status_code=404, detail="Not Found")

    db.execute(
        text(
            """
            DELETE FROM plan_version_coverages
            WHERE id = :pvc_id
              AND plan_version_id = :plan_version_id
            """
        ),
        {
            "pvc_id": int(pvc_id),
            "plan_version_id": int(plan_version_id),
        },
    )
    db.commit()

    return {
        "status": "ok",
        "message": "Cobertura eliminada de la version.",
    }


@router.post("/{product_id:int}/plans/{plan_version_id:int}/coverages/reorder")
async def reorder_coverages(
    product_id: int,
    plan_version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)
    _ensure_plan_belongs(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))

    payload = await request.json()
    coverage_ids = payload.get("coverage_ids") if isinstance(payload, dict) else None
    if not isinstance(coverage_ids, list):
        _validation_error({"coverage_ids": ["Formato invalido."]})

    for index, coverage_id in enumerate(coverage_ids):
        try:
            pvc_id = int(coverage_id)
        except (TypeError, ValueError):
            continue

        db.execute(
            text(
                """
                UPDATE plan_version_coverages
                SET sort_order = :sort_order,
                    updated_at = NOW()
                WHERE id = :pvc_id
                  AND plan_version_id = :plan_version_id
                """
            ),
            {
                "sort_order": int(index + 1),
                "pvc_id": pvc_id,
                "plan_version_id": int(plan_version_id),
            },
        )
    db.commit()

    return {"status": "ok"}


@router.get("/{product_id:int}/plans/{plan_version_id:int}/age-surcharges")
def index_age_surcharges(
    product_id: int,
    plan_version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)
    _ensure_plan_belongs(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))

    rows = db.execute(
        text(
            """
            SELECT id, plan_version_id, age_from, age_to, surcharge_percent
            FROM plan_version_age_surcharges
            WHERE plan_version_id = :plan_version_id
            ORDER BY age_from, age_to
            """
        ),
        {"plan_version_id": int(plan_version_id)},
    ).mappings().all()

    return {
        "data": [
            {
                "id": int(row["id"]),
                "plan_version_id": int(row.get("plan_version_id") or 0),
                "age_from": int(row.get("age_from")) if row.get("age_from") is not None else None,
                "age_to": int(row.get("age_to")) if row.get("age_to") is not None else None,
                "surcharge_percent": float(row.get("surcharge_percent")) if row.get("surcharge_percent") is not None else None,
            }
            for row in rows
        ]
    }


@router.post("/{product_id:int}/plans/{plan_version_id:int}/age-surcharges")
async def store_age_surcharge(
    product_id: int,
    plan_version_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)
    _ensure_plan_belongs(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload invalido."]})

    age_from = payload.get("age_from")
    age_to = payload.get("age_to")
    surcharge = payload.get("surcharge_percent")

    age_from_val = None if age_from in [None, ""] else int(age_from)
    age_to_val = None if age_to in [None, ""] else int(age_to)
    surcharge_val = None if surcharge in [None, ""] else float(surcharge)

    db.execute(
        text(
            """
            INSERT INTO plan_version_age_surcharges (
                plan_version_id,
                age_from,
                age_to,
                surcharge_percent,
                created_at,
                updated_at
            ) VALUES (
                :plan_version_id,
                :age_from,
                :age_to,
                :surcharge_percent,
                NOW(),
                NOW()
            )
            """
        ),
        {
            "plan_version_id": int(plan_version_id),
            "age_from": age_from_val,
            "age_to": age_to_val,
            "surcharge_percent": surcharge_val,
        },
    )
    created = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    db.commit()

    row = db.execute(
        text(
            """
            SELECT id, plan_version_id, age_from, age_to, surcharge_percent
            FROM plan_version_age_surcharges
            WHERE id = :id
            LIMIT 1
            """
        ),
        {"id": int(created["id"])},
    ).mappings().first()

    return {
        "data": {
            "id": int(row["id"]),
            "plan_version_id": int(row.get("plan_version_id") or 0),
            "age_from": int(row.get("age_from")) if row.get("age_from") is not None else None,
            "age_to": int(row.get("age_to")) if row.get("age_to") is not None else None,
            "surcharge_percent": float(row.get("surcharge_percent")) if row.get("surcharge_percent") is not None else None,
        },
        "message": "Rango de edad creado.",
    }


@router.patch("/{product_id:int}/plans/{plan_version_id:int}/age-surcharges/{age_surcharge_id:int}")
async def update_age_surcharge(
    product_id: int,
    plan_version_id: int,
    age_surcharge_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)
    _ensure_plan_belongs(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))

    exists = db.execute(
        text(
            """
            SELECT id
            FROM plan_version_age_surcharges
            WHERE id = :id
              AND plan_version_id = :plan_version_id
            LIMIT 1
            """
        ),
        {
            "id": int(age_surcharge_id),
            "plan_version_id": int(plan_version_id),
        },
    ).mappings().first()
    if not exists:
        raise HTTPException(status_code=404, detail="Not Found")

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload invalido."]})

    sets = []
    params: dict[str, object] = {"id": int(age_surcharge_id), "plan_version_id": int(plan_version_id)}

    if "age_from" in payload:
        value = payload.get("age_from")
        params["age_from"] = None if value in [None, ""] else int(value)
        sets.append("age_from = :age_from")

    if "age_to" in payload:
        value = payload.get("age_to")
        params["age_to"] = None if value in [None, ""] else int(value)
        sets.append("age_to = :age_to")

    if "surcharge_percent" in payload:
        value = payload.get("surcharge_percent")
        params["surcharge_percent"] = None if value in [None, ""] else float(value)
        sets.append("surcharge_percent = :surcharge_percent")

    if sets:
        clause = ",\n                ".join(sets + ["updated_at = NOW()"])
        db.execute(
            text(
                f"""
                UPDATE plan_version_age_surcharges
                SET {clause}
                WHERE id = :id
                  AND plan_version_id = :plan_version_id
                """
            ),
            params,
        )
        db.commit()

    row = db.execute(
        text(
            """
            SELECT id, plan_version_id, age_from, age_to, surcharge_percent
            FROM plan_version_age_surcharges
            WHERE id = :id
            LIMIT 1
            """
        ),
        {"id": int(age_surcharge_id)},
    ).mappings().first()

    return {
        "data": {
            "id": int(row["id"]),
            "plan_version_id": int(row.get("plan_version_id") or 0),
            "age_from": int(row.get("age_from")) if row.get("age_from") is not None else None,
            "age_to": int(row.get("age_to")) if row.get("age_to") is not None else None,
            "surcharge_percent": float(row.get("surcharge_percent")) if row.get("surcharge_percent") is not None else None,
        },
        "message": "Rango de edad actualizado.",
    }


@router.delete("/{product_id:int}/plans/{plan_version_id:int}/age-surcharges/{age_surcharge_id:int}")
def destroy_age_surcharge(
    product_id: int,
    plan_version_id: int,
    age_surcharge_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_products_manage(request=request, authorization=authorization, db=db)
    _ensure_plan_belongs(db=db, product_id=int(product_id), plan_version_id=int(plan_version_id))

    exists = db.execute(
        text(
            """
            SELECT id
            FROM plan_version_age_surcharges
            WHERE id = :id
              AND plan_version_id = :plan_version_id
            LIMIT 1
            """
        ),
        {
            "id": int(age_surcharge_id),
            "plan_version_id": int(plan_version_id),
        },
    ).mappings().first()
    if not exists:
        raise HTTPException(status_code=404, detail="Not Found")

    db.execute(
        text(
            """
            DELETE FROM plan_version_age_surcharges
            WHERE id = :id
              AND plan_version_id = :plan_version_id
            """
        ),
        {
            "id": int(age_surcharge_id),
            "plan_version_id": int(plan_version_id),
        },
    )
    db.commit()

    return {
        "data": {"id": int(age_surcharge_id)},
        "message": "Rango de edad eliminado.",
    }