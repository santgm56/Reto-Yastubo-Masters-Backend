from __future__ import annotations

import json

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.services.auth_service import AuthService

router = APIRouter(prefix="/api/v1/admin/coverages", tags=["admin-coverages"])


def _extract_bearer_token(authorization: str | None) -> str:
    if not authorization:
        return ""

    raw = authorization.strip()
    if not raw.lower().startswith("bearer "):
        return ""

    return raw[7:].strip()


def _require_admin_coverages_manage(request: Request, authorization: str | None, db: Session) -> None:
    token = _extract_bearer_token(authorization)
    if not token:
        token = str(request.cookies.get("yastubo_access_token") or "").strip()

    if not token:
        raise HTTPException(status_code=403, detail="Forbidden")

    auth_payload = AuthService(db).me(token)
    permissions = [str(item) for item in (auth_payload.get("permissions") or [])]
    if "admin.coverages.manage" not in permissions:
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


def _serialize_unit(row) -> dict:
    return {
        "id": int(row["id"]),
        "name": _parse_json_field(row.get("name"), fallback={"es": "", "en": ""}),
        "description": _parse_json_field(row.get("description"), fallback={"es": "", "en": ""}),
        "measure_type": str(row.get("measure_type") or "none"),
        "status": str(row.get("status") or "inactive"),
    }


def _serialize_coverage(row, unit_by_id: dict[int, dict]) -> dict:
    unit_id = int(row.get("unit_id") or 0) if row.get("unit_id") is not None else None
    return {
        "id": int(row["id"]),
        "category_id": int(row.get("category_id") or 0),
        "unit_id": unit_id,
        "name": _parse_json_field(row.get("name"), fallback={"es": "", "en": ""}),
        "description": _parse_json_field(row.get("description"), fallback={"es": "", "en": ""}),
        "status": str(row.get("status") or "active"),
        "sort_order": int(row.get("sort_order") or 0),
        "unit": unit_by_id.get(unit_id or 0),
    }


def _serialize_category(row, *, coverages: list[dict] | None = None) -> dict:
    data = {
        "id": int(row["id"]),
        "name": _parse_json_field(row.get("name"), fallback={"es": "", "en": ""}),
        "description": _parse_json_field(row.get("description"), fallback={"es": "", "en": ""}),
        "status": str(row.get("status") or "active"),
        "sort_order": int(row.get("sort_order") or 0),
    }
    if coverages is not None:
        data["coverages"] = list(coverages)
    return data


def _fetch_units_active(db: Session) -> list[dict]:
    rows = db.execute(
        text(
            """
            SELECT id, name, description, measure_type, status
            FROM units_of_measure
            WHERE status = 'active'
            ORDER BY id
            """
        )
    ).mappings().all()
    return [_serialize_unit(row) for row in rows]


def _fetch_coverages_by_category(db: Session, category_id: int, *, unit_by_id: dict[int, dict]) -> list[dict]:
    rows = db.execute(
        text(
            """
            SELECT id, category_id, unit_id, name, description, status, sort_order
            FROM coverages
            WHERE category_id = :category_id
            ORDER BY sort_order, id
            """
        ),
        {"category_id": int(category_id)},
    ).mappings().all()
    return [_serialize_coverage(row, unit_by_id=unit_by_id) for row in rows]


def _fetch_category(db: Session, category_id: int):
    return db.execute(
        text(
            """
            SELECT id, name, description, status, sort_order
            FROM coverage_categories
            WHERE id = :category_id
            LIMIT 1
            """
        ),
        {"category_id": int(category_id)},
    ).mappings().first()


def _fetch_coverage(db: Session, coverage_id: int):
    return db.execute(
        text(
            """
            SELECT id, category_id, unit_id, name, description, status, sort_order
            FROM coverages
            WHERE id = :coverage_id
            LIMIT 1
            """
        ),
        {"coverage_id": int(coverage_id)},
    ).mappings().first()


def _decode_localized_name(raw_value) -> str | None:
    parsed = _parse_json_field(raw_value, fallback={})
    if not parsed:
        return str(raw_value) if raw_value not in [None, ""] else None
    return str(parsed.get("es") or parsed.get("en") or "") or None


@router.get("/bootstrap")
def bootstrap_coverages(
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_coverages_manage(request=request, authorization=authorization, db=db)

    units = _fetch_units_active(db)
    unit_by_id = {int(unit["id"]): unit for unit in units}

    category_rows = db.execute(
        text(
            """
            SELECT id, name, description, status, sort_order
            FROM coverage_categories
            WHERE status = 'active'
            ORDER BY sort_order, id
            """
        )
    ).mappings().all()

    categories: list[dict] = []
    for row in category_rows:
        category_id = int(row["id"])
        coverages = _fetch_coverages_by_category(db, category_id, unit_by_id=unit_by_id)
        categories.append(_serialize_category(row, coverages=coverages))

    return {
        "data": {
            "categories": categories,
            "units": units,
        }
    }


@router.post("/categories")
async def store_category(
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_coverages_manage(request=request, authorization=authorization, db=db)

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload invalido."]})

    name = _validate_translatable(payload, "name", required_es=True)
    description = _validate_translatable(payload, "description", required_es=False)

    max_row = db.execute(text("SELECT COALESCE(MAX(sort_order), 0) AS max_order FROM coverage_categories")).mappings().first()
    next_order = int(max_row.get("max_order") or 0) + 1 if max_row else 1

    db.execute(
        text(
            """
            INSERT INTO coverage_categories (
                name,
                description,
                status,
                sort_order,
                created_at,
                updated_at
            ) VALUES (
                CAST(:name AS JSON),
                CAST(:description AS JSON),
                'active',
                :sort_order,
                NOW(),
                NOW()
            )
            """
        ),
        {
            "name": json.dumps(name, ensure_ascii=False),
            "description": json.dumps(description, ensure_ascii=False),
            "sort_order": next_order,
        },
    )
    created = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    db.commit()

    if not created:
        raise HTTPException(status_code=500, detail="No se pudo crear la categoria.")

    row = _fetch_category(db=db, category_id=int(created["id"]))
    return {"data": _serialize_category(row)}


@router.put("/categories/{category_id:int}")
async def update_category(
    category_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_coverages_manage(request=request, authorization=authorization, db=db)

    existing = _fetch_category(db=db, category_id=int(category_id))
    if not existing:
        raise HTTPException(status_code=404, detail="Not Found")

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload invalido."]})

    name = _validate_translatable(payload, "name", required_es=True)
    description = _validate_translatable(payload, "description", required_es=False)

    db.execute(
        text(
            """
            UPDATE coverage_categories
            SET
                name = CAST(:name AS JSON),
                description = CAST(:description AS JSON),
                updated_at = NOW()
            WHERE id = :category_id
            """
        ),
        {
            "category_id": int(category_id),
            "name": json.dumps(name, ensure_ascii=False),
            "description": json.dumps(description, ensure_ascii=False),
        },
    )
    db.commit()

    refreshed = _fetch_category(db=db, category_id=int(category_id))
    return {"data": _serialize_category(refreshed)}


@router.post("/categories/{category_id:int}/archive")
def archive_category(
    category_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_coverages_manage(request=request, authorization=authorization, db=db)

    existing = _fetch_category(db=db, category_id=int(category_id))
    if not existing:
        raise HTTPException(status_code=404, detail="Not Found")

    db.execute(
        text(
            """
            UPDATE coverage_categories
            SET status = 'archived', updated_at = NOW()
            WHERE id = :category_id
            """
        ),
        {"category_id": int(category_id)},
    )
    db.commit()

    return {"message": "Categoria archivada correctamente."}


@router.post("/categories/{category_id:int}/restore")
def restore_category(
    category_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_coverages_manage(request=request, authorization=authorization, db=db)

    existing = _fetch_category(db=db, category_id=int(category_id))
    if not existing:
        raise HTTPException(status_code=404, detail="Not Found")

    db.execute(
        text(
            """
            UPDATE coverage_categories
            SET status = 'active', updated_at = NOW()
            WHERE id = :category_id
            """
        ),
        {"category_id": int(category_id)},
    )
    db.commit()

    units = _fetch_units_active(db)
    unit_by_id = {int(unit["id"]): unit for unit in units}
    refreshed = _fetch_category(db=db, category_id=int(category_id))
    coverages = _fetch_coverages_by_category(db, int(category_id), unit_by_id=unit_by_id)

    return {"data": _serialize_category(refreshed, coverages=coverages)}


@router.get("/categories/archived")
def archived_categories(
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_coverages_manage(request=request, authorization=authorization, db=db)

    rows = db.execute(
        text(
            """
            SELECT id, name, description, status, sort_order
            FROM coverage_categories
            WHERE status = 'archived'
            ORDER BY sort_order, id
            """
        )
    ).mappings().all()

    return {"data": [_serialize_category(row) for row in rows]}


@router.post("/categories/{category_id:int}/reorder")
async def reorder_coverages(
    category_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_coverages_manage(request=request, authorization=authorization, db=db)

    category = _fetch_category(db=db, category_id=int(category_id))
    if not category:
        raise HTTPException(status_code=404, detail="Not Found")

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload invalido."]})

    items = payload.get("items")
    if not isinstance(items, list) or not items:
        _validation_error({"items": ["Debe enviar un arreglo de items."]})

    errors: dict[str, list[str]] = {}
    normalized_items: list[tuple[int, int]] = []
    seen_ids: set[int] = set()

    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            errors[f"items.{idx}"] = ["Formato invalido."]
            continue

        try:
            coverage_id = int(item.get("id"))
            sort_order = int(item.get("sort_order"))
        except (TypeError, ValueError):
            errors[f"items.{idx}"] = ["id y sort_order deben ser numericos."]
            continue

        if coverage_id in seen_ids:
            errors[f"items.{idx}.id"] = ["No se permiten ids duplicados."]
            continue

        seen_ids.add(coverage_id)
        normalized_items.append((coverage_id, sort_order))

    if errors:
        _validation_error(errors)

    for coverage_id, sort_order in normalized_items:
        db.execute(
            text(
                """
                UPDATE coverages
                SET sort_order = :sort_order, updated_at = NOW()
                WHERE id = :coverage_id
                  AND category_id = :category_id
                """
            ),
            {
                "coverage_id": int(coverage_id),
                "category_id": int(category_id),
                "sort_order": int(sort_order),
            },
        )
    db.commit()

    return {"message": "Orden actualizado."}


@router.post("/items")
async def store_coverage(
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_coverages_manage(request=request, authorization=authorization, db=db)

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload invalido."]})

    try:
        category_id = int(payload.get("category_id"))
    except (TypeError, ValueError):
        _validation_error({"category_id": ["Categoria invalida."]})

    try:
        unit_id = int(payload.get("unit_id"))
    except (TypeError, ValueError):
        _validation_error({"unit_id": ["Unidad invalida."]})

    if not _fetch_category(db=db, category_id=category_id):
        _validation_error({"category_id": ["Categoria invalida."]})

    unit_row = db.execute(
        text("SELECT id FROM units_of_measure WHERE id = :unit_id LIMIT 1"),
        {"unit_id": int(unit_id)},
    ).mappings().first()
    if not unit_row:
        _validation_error({"unit_id": ["Unidad invalida."]})

    name = _validate_translatable(payload, "name", required_es=True)
    description = _validate_translatable(payload, "description", required_es=False)

    max_row = db.execute(
        text("SELECT COALESCE(MAX(sort_order), 0) AS max_order FROM coverages WHERE category_id = :category_id"),
        {"category_id": int(category_id)},
    ).mappings().first()
    next_order = int(max_row.get("max_order") or 0) + 1 if max_row else 1

    db.execute(
        text(
            """
            INSERT INTO coverages (
                category_id,
                unit_id,
                name,
                description,
                status,
                sort_order,
                created_at,
                updated_at
            ) VALUES (
                :category_id,
                :unit_id,
                CAST(:name AS JSON),
                CAST(:description AS JSON),
                'active',
                :sort_order,
                NOW(),
                NOW()
            )
            """
        ),
        {
            "category_id": int(category_id),
            "unit_id": int(unit_id),
            "name": json.dumps(name, ensure_ascii=False),
            "description": json.dumps(description, ensure_ascii=False),
            "sort_order": int(next_order),
        },
    )
    created = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    db.commit()

    if not created:
        raise HTTPException(status_code=500, detail="No se pudo crear la cobertura.")

    units = _fetch_units_active(db)
    unit_by_id = {int(unit["id"]): unit for unit in units}
    row = _fetch_coverage(db=db, coverage_id=int(created["id"]))
    return {"data": _serialize_coverage(row, unit_by_id=unit_by_id)}


@router.put("/items/{coverage_id:int}")
async def update_coverage(
    coverage_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_coverages_manage(request=request, authorization=authorization, db=db)

    existing = _fetch_coverage(db=db, coverage_id=int(coverage_id))
    if not existing:
        raise HTTPException(status_code=404, detail="Not Found")

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload invalido."]})

    try:
        category_id = int(payload.get("category_id"))
    except (TypeError, ValueError):
        _validation_error({"category_id": ["Categoria invalida."]})

    try:
        unit_id = int(payload.get("unit_id"))
    except (TypeError, ValueError):
        _validation_error({"unit_id": ["Unidad invalida."]})

    if not _fetch_category(db=db, category_id=category_id):
        _validation_error({"category_id": ["Categoria invalida."]})

    unit_row = db.execute(
        text("SELECT id FROM units_of_measure WHERE id = :unit_id LIMIT 1"),
        {"unit_id": int(unit_id)},
    ).mappings().first()
    if not unit_row:
        _validation_error({"unit_id": ["Unidad invalida."]})

    name = _validate_translatable(payload, "name", required_es=True)
    description = _validate_translatable(payload, "description", required_es=False)

    db.execute(
        text(
            """
            UPDATE coverages
            SET
                category_id = :category_id,
                unit_id = :unit_id,
                name = CAST(:name AS JSON),
                description = CAST(:description AS JSON),
                updated_at = NOW()
            WHERE id = :coverage_id
            """
        ),
        {
            "coverage_id": int(coverage_id),
            "category_id": int(category_id),
            "unit_id": int(unit_id),
            "name": json.dumps(name, ensure_ascii=False),
            "description": json.dumps(description, ensure_ascii=False),
        },
    )
    db.commit()

    units = _fetch_units_active(db)
    unit_by_id = {int(unit["id"]): unit for unit in units}
    refreshed = _fetch_coverage(db=db, coverage_id=int(coverage_id))

    return {"data": _serialize_coverage(refreshed, unit_by_id=unit_by_id)}


@router.post("/items/{coverage_id:int}/archive")
def archive_coverage(
    coverage_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_coverages_manage(request=request, authorization=authorization, db=db)

    existing = _fetch_coverage(db=db, coverage_id=int(coverage_id))
    if not existing:
        raise HTTPException(status_code=404, detail="Not Found")

    db.execute(
        text(
            """
            UPDATE coverages
            SET status = 'archived', updated_at = NOW()
            WHERE id = :coverage_id
            """
        ),
        {"coverage_id": int(coverage_id)},
    )
    db.commit()

    return {"message": "Cobertura archivada correctamente."}


@router.post("/items/{coverage_id:int}/restore")
def restore_coverage(
    coverage_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_coverages_manage(request=request, authorization=authorization, db=db)

    existing = _fetch_coverage(db=db, coverage_id=int(coverage_id))
    if not existing:
        raise HTTPException(status_code=404, detail="Not Found")

    db.execute(
        text(
            """
            UPDATE coverages
            SET status = 'active', updated_at = NOW()
            WHERE id = :coverage_id
            """
        ),
        {"coverage_id": int(coverage_id)},
    )
    db.commit()

    units = _fetch_units_active(db)
    unit_by_id = {int(unit["id"]): unit for unit in units}
    refreshed = _fetch_coverage(db=db, coverage_id=int(coverage_id))

    return {"data": _serialize_coverage(refreshed, unit_by_id=unit_by_id)}


@router.delete("/items/{coverage_id:int}")
def destroy_coverage(
    coverage_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_coverages_manage(request=request, authorization=authorization, db=db)

    existing = _fetch_coverage(db=db, coverage_id=int(coverage_id))
    if not existing:
        raise HTTPException(status_code=404, detail="Not Found")

    usage = db.execute(
        text(
            """
            SELECT 1
            FROM plan_version_coverages
            WHERE coverage_id = :coverage_id
            LIMIT 1
            """
        ),
        {"coverage_id": int(coverage_id)},
    ).mappings().first()
    if usage:
        raise HTTPException(
            status_code=409,
            detail="La cobertura esta en uso en uno o mas planes/versiones. Archivala en lugar de eliminarla.",
        )

    db.execute(text("DELETE FROM coverages WHERE id = :coverage_id"), {"coverage_id": int(coverage_id)})
    db.commit()

    return {"message": "Cobertura eliminada."}


@router.get("/items/{coverage_id:int}/usages")
def coverage_usages(
    coverage_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_coverages_manage(request=request, authorization=authorization, db=db)

    existing = _fetch_coverage(db=db, coverage_id=int(coverage_id))
    if not existing:
        raise HTTPException(status_code=404, detail="Not Found")

    rows = db.execute(
        text(
            """
            SELECT
                pvc.plan_version_id AS product_version_id,
                pv.id AS version_id,
                pv.product_id,
                p.name AS product_name
            FROM plan_version_coverages pvc
            INNER JOIN plan_versions pv ON pv.id = pvc.plan_version_id
            INNER JOIN products p ON p.id = pv.product_id
            WHERE pvc.coverage_id = :coverage_id
            ORDER BY p.id, pv.id
            """
        ),
        {"coverage_id": int(coverage_id)},
    ).mappings().all()

    usages = [
        {
            "product_version_id": int(row.get("product_version_id") or 0),
            "version_id": int(row.get("version_id") or 0),
            "product_id": int(row.get("product_id") or 0),
            "product_name": _decode_localized_name(row.get("product_name")),
            "product_link": None,
            "version_link": None,
        }
        for row in rows
    ]

    return {"data": usages}
