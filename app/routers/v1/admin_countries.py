from __future__ import annotations

import json

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.services.auth_service import AuthService

router = APIRouter(prefix="/api/v1/admin/countries", tags=["admin-countries"])

_CONTINENT_OPTIONS = {
    "EU": "Europa",
    "AF": "África",
    "AS": "Asia",
    "OC": "Oceanía",
    "AN": "Antártida",
    "NA": "Norteamérica",
    "CA": "Centroamérica y Caribe",
    "SA": "Sudamérica",
}


def _extract_bearer_token(authorization: str | None) -> str:
    if not authorization:
        return ""

    raw = authorization.strip()
    if not raw.lower().startswith("bearer "):
        return ""

    return raw[7:].strip()


def _require_admin_countries_manage(request: Request, authorization: str | None, db: Session) -> None:
    token = _extract_bearer_token(authorization)
    if not token:
        token = str(request.cookies.get("yastubo_access_token") or "").strip()

    if not token:
        raise HTTPException(status_code=403, detail="Forbidden")

    auth_payload = AuthService(db).me(token)
    permissions = [str(item) for item in (auth_payload.get("permissions") or [])]
    if "admin.countries.manage" not in permissions:
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


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


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


def _serialize_country(row) -> dict:
    continent_code = str(row.get("continent_code") or "")
    return {
        "id": int(row["id"]),
        "name": _parse_json_field(row.get("name"), fallback={"es": None, "en": None}),
        "iso2": str(row.get("iso2") or ""),
        "iso3": str(row.get("iso3") or ""),
        "continent_code": continent_code,
        "continent_label": _CONTINENT_OPTIONS.get(continent_code, continent_code),
        "phone_code": str(row.get("phone_code")) if row.get("phone_code") not in [None, ""] else None,
        "is_active": bool(row.get("is_active")),
    }


def _fetch_country(db: Session, country_id: int):
    return db.execute(
        text(
            """
            SELECT id, name, iso2, iso3, continent_code, phone_code, is_active
            FROM countries
            WHERE id = :country_id
            LIMIT 1
            """
        ),
        {"country_id": int(country_id)},
    ).mappings().first()


def _validate_country_payload(payload: dict) -> dict:
    if not isinstance(payload.get("name"), dict):
        _validation_error({"name": ["El campo debe ser un objeto con llaves es/en."]})

    name_es = _normalize_text(payload["name"].get("es"))
    name_en = _normalize_text(payload["name"].get("en"))
    if not name_es:
        _validation_error({"name.es": ["El nombre en español es obligatorio."]})
    if not name_en:
        _validation_error({"name.en": ["El nombre en inglés es obligatorio."]})

    iso2 = _normalize_text(payload.get("iso2")).upper()
    iso3 = _normalize_text(payload.get("iso3")).upper()
    continent_code = _normalize_text(payload.get("continent_code")).upper()
    phone_code = _normalize_text(payload.get("phone_code"))

    errors: dict[str, list[str]] = {}
    if len(iso2) != 2:
        errors["iso2"] = ["ISO2 debe tener 2 caracteres."]
    if len(iso3) != 3:
        errors["iso3"] = ["ISO3 debe tener 3 caracteres."]
    if continent_code not in _CONTINENT_OPTIONS:
        errors["continent_code"] = ["Continente invalido."]
    if phone_code and not phone_code.isdigit():
        errors["phone_code"] = ["El código telefónico solo admite dígitos."]

    if errors:
        _validation_error(errors)

    return {
        "name": {"es": name_es, "en": name_en},
        "iso2": iso2,
        "iso3": iso3,
        "continent_code": continent_code,
        "phone_code": phone_code or None,
    }


@router.get("")
def index_countries(
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_countries_manage(request=request, authorization=authorization, db=db)

    search = _normalize_text(request.query_params.get("search"))
    continent = _normalize_text(request.query_params.get("continent")).upper()
    status = _normalize_text(request.query_params.get("status") or "active")

    where_clauses = ["1 = 1"]
    params: dict[str, object] = {}

    if search:
        where_clauses.append(
            """
            (
                name LIKE :search
                OR phone_code LIKE :search
                OR iso2 LIKE :search
                OR iso3 LIKE :search
            )
            """
        )
        params["search"] = f"%{search}%"

    if continent:
        where_clauses.append("continent_code = :continent")
        params["continent"] = continent

    if status == "active":
        where_clauses.append("is_active = 1")
    elif status == "inactive":
        where_clauses.append("is_active = 0")

    rows = db.execute(
        text(
            f"""
            SELECT id, name, iso2, iso3, continent_code, phone_code, is_active
            FROM countries
            WHERE {' AND '.join(where_clauses)}
            ORDER BY name
            """
        ),
        params,
    ).mappings().all()

    return {
        "countries": [_serialize_country(row) for row in rows],
        "filters": {
            "search": search,
            "continent": continent or "",
            "status": status or "active",
        },
        "continents": _CONTINENT_OPTIONS,
    }


@router.get("/{country_id:int}")
def show_country(
    country_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_countries_manage(request=request, authorization=authorization, db=db)

    row = _fetch_country(db=db, country_id=int(country_id))
    if not row:
        raise HTTPException(status_code=404, detail="Not Found")

    return {"data": _serialize_country(row)}


@router.post("")
async def store_country(
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_countries_manage(request=request, authorization=authorization, db=db)

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload invalido."]})

    validated = _validate_country_payload(payload)

    duplicate = db.execute(
        text(
            """
            SELECT id
            FROM countries
            WHERE iso2 = :iso2 OR iso3 = :iso3
            LIMIT 1
            """
        ),
        {"iso2": validated["iso2"], "iso3": validated["iso3"]},
    ).mappings().first()
    if duplicate:
        _validation_error({"iso2": ["ISO2 o ISO3 ya existe."], "iso3": ["ISO2 o ISO3 ya existe."]})

    db.execute(
        text(
            """
            INSERT INTO countries (
                name,
                iso2,
                iso3,
                continent_code,
                phone_code,
                is_active,
                created_at,
                updated_at
            ) VALUES (
                CAST(:name AS JSON),
                :iso2,
                :iso3,
                :continent_code,
                :phone_code,
                1,
                NOW(),
                NOW()
            )
            """
        ),
        {
            "name": json.dumps(validated["name"], ensure_ascii=False),
            "iso2": validated["iso2"],
            "iso3": validated["iso3"],
            "continent_code": validated["continent_code"],
            "phone_code": validated["phone_code"],
        },
    )
    created = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    db.commit()

    if not created:
        raise HTTPException(status_code=500, detail="No se pudo crear el pais.")

    country = _fetch_country(db=db, country_id=int(created["id"]))
    return {
        "message": "País creado correctamente.",
        "data": _serialize_country(country),
    }


@router.put("/{country_id:int}")
async def update_country(
    country_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_countries_manage(request=request, authorization=authorization, db=db)

    existing = _fetch_country(db=db, country_id=int(country_id))
    if not existing:
        raise HTTPException(status_code=404, detail="Not Found")

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload invalido."]})

    validated = _validate_country_payload(payload)

    duplicate = db.execute(
        text(
            """
            SELECT id
            FROM countries
            WHERE id != :country_id
              AND (iso2 = :iso2 OR iso3 = :iso3)
            LIMIT 1
            """
        ),
        {
            "country_id": int(country_id),
            "iso2": validated["iso2"],
            "iso3": validated["iso3"],
        },
    ).mappings().first()
    if duplicate:
        _validation_error({"iso2": ["ISO2 o ISO3 ya existe."], "iso3": ["ISO2 o ISO3 ya existe."]})

    db.execute(
        text(
            """
            UPDATE countries
            SET
                name = CAST(:name AS JSON),
                iso2 = :iso2,
                iso3 = :iso3,
                continent_code = :continent_code,
                phone_code = :phone_code,
                updated_at = NOW()
            WHERE id = :country_id
            """
        ),
        {
            "country_id": int(country_id),
            "name": json.dumps(validated["name"], ensure_ascii=False),
            "iso2": validated["iso2"],
            "iso3": validated["iso3"],
            "continent_code": validated["continent_code"],
            "phone_code": validated["phone_code"],
        },
    )
    db.commit()

    refreshed = _fetch_country(db=db, country_id=int(country_id))
    return {
        "message": "País actualizado correctamente.",
        "data": _serialize_country(refreshed),
    }


@router.put("/{country_id:int}/toggle-active")
def toggle_country_active(
    country_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_countries_manage(request=request, authorization=authorization, db=db)

    existing = _fetch_country(db=db, country_id=int(country_id))
    if not existing:
        raise HTTPException(status_code=404, detail="Not Found")

    next_active = 0 if bool(existing.get("is_active")) else 1

    db.execute(
        text(
            """
            UPDATE countries
            SET is_active = :is_active, updated_at = NOW()
            WHERE id = :country_id
            """
        ),
        {
            "country_id": int(country_id),
            "is_active": int(next_active),
        },
    )
    db.commit()

    refreshed = _fetch_country(db=db, country_id=int(country_id))
    return {
        "message": "País activado correctamente." if next_active else "País desactivado correctamente.",
        "data": _serialize_country(refreshed),
    }
