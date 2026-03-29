from __future__ import annotations

import json

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.services.auth_service import AuthService

router = APIRouter(prefix="/api/v1/admin/zones", tags=["admin-zones"])

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


def _serialize_country_for_zone(row, *, attached: bool | None = None) -> dict:
    continent_code = str(row.get("continent_code") or "")
    data = {
        "id": int(row["id"]),
        "name": _parse_json_field(row.get("name"), fallback={"es": None, "en": None}),
        "continent_code": continent_code,
        "continent_label": _CONTINENT_OPTIONS.get(continent_code, continent_code),
        "phone_code": str(row.get("phone_code")) if row.get("phone_code") not in [None, ""] else None,
        "is_active": bool(row.get("is_active")),
    }
    if attached is not None:
        data["attached"] = bool(attached)
    return data


def _fetch_zone(db: Session, zone_id: int):
    return db.execute(
        text(
            """
            SELECT id, name, description, is_active
            FROM zones
            WHERE id = :zone_id
            LIMIT 1
            """
        ),
        {"zone_id": int(zone_id)},
    ).mappings().first()


def _fetch_zone_countries(db: Session, zone_id: int) -> list[dict]:
    rows = db.execute(
        text(
            """
            SELECT c.id, c.name, c.continent_code, c.phone_code, c.is_active
            FROM countries c
            INNER JOIN country_zone cz ON cz.country_id = c.id
            WHERE cz.zone_id = :zone_id
            ORDER BY c.name
            """
        ),
        {"zone_id": int(zone_id)},
    ).mappings().all()

    return [_serialize_country_for_zone(row) for row in rows]


def _serialize_zone(db: Session, row) -> dict:
    countries = _fetch_zone_countries(db, int(row["id"]))
    return {
        "id": int(row["id"]),
        "name": str(row.get("name") or ""),
        "description": str(row.get("description")) if row.get("description") not in [None, ""] else None,
        "is_active": bool(row.get("is_active")),
        "countries": countries,
        "countries_count": len(countries),
    }


@router.get("")
def index_zones(
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_countries_manage(request=request, authorization=authorization, db=db)

    status = _normalize_text(request.query_params.get("status") or "active")

    where_clauses = ["1 = 1"]
    if status == "active":
        where_clauses.append("is_active = 1")
    elif status == "inactive":
        where_clauses.append("is_active = 0")

    rows = db.execute(
        text(
            f"""
            SELECT id, name, description, is_active
            FROM zones
            WHERE {' AND '.join(where_clauses)}
            ORDER BY name
            """
        )
    ).mappings().all()

    return {
        "zones": [_serialize_zone(db, row) for row in rows],
        "filters": {"status": status or "active"},
        "continents": _CONTINENT_OPTIONS,
    }


@router.get("/{zone_id:int}")
def show_zone(
    zone_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_countries_manage(request=request, authorization=authorization, db=db)

    row = _fetch_zone(db=db, zone_id=int(zone_id))
    if not row:
        raise HTTPException(status_code=404, detail="Not Found")

    return {
        "data": {
            "id": int(row["id"]),
            "name": str(row.get("name") or ""),
            "description": str(row.get("description")) if row.get("description") not in [None, ""] else None,
            "is_active": bool(row.get("is_active")),
        }
    }


@router.post("")
async def store_zone(
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_countries_manage(request=request, authorization=authorization, db=db)

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload invalido."]})

    name = _normalize_text(payload.get("name"))
    description = _normalize_text(payload.get("description"))

    if not name:
        _validation_error({"name": ["El nombre es obligatorio."]})

    db.execute(
        text(
            """
            INSERT INTO zones (
                name,
                description,
                is_active,
                created_at,
                updated_at
            ) VALUES (
                :name,
                :description,
                1,
                NOW(),
                NOW()
            )
            """
        ),
        {
            "name": name,
            "description": description or None,
        },
    )
    created = db.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()
    db.commit()

    if not created:
        raise HTTPException(status_code=500, detail="No se pudo crear la zona.")

    zone = _fetch_zone(db=db, zone_id=int(created["id"]))
    return {
        "message": "Zona creada correctamente.",
        "data": _serialize_zone(db, zone),
    }


@router.put("/{zone_id:int}")
async def update_zone(
    zone_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_countries_manage(request=request, authorization=authorization, db=db)

    existing = _fetch_zone(db=db, zone_id=int(zone_id))
    if not existing:
        raise HTTPException(status_code=404, detail="Not Found")

    payload = await request.json()
    if not isinstance(payload, dict):
        _validation_error({"body": ["Formato de payload invalido."]})

    name = _normalize_text(payload.get("name"))
    description = _normalize_text(payload.get("description"))

    if not name:
        _validation_error({"name": ["El nombre es obligatorio."]})

    db.execute(
        text(
            """
            UPDATE zones
            SET
                name = :name,
                description = :description,
                updated_at = NOW()
            WHERE id = :zone_id
            """
        ),
        {
            "zone_id": int(zone_id),
            "name": name,
            "description": description or None,
        },
    )
    db.commit()

    refreshed = _fetch_zone(db=db, zone_id=int(zone_id))
    return {
        "message": "Zona actualizada correctamente.",
        "data": _serialize_zone(db, refreshed),
    }


@router.put("/{zone_id:int}/toggle-active")
def toggle_zone_active(
    zone_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_countries_manage(request=request, authorization=authorization, db=db)

    existing = _fetch_zone(db=db, zone_id=int(zone_id))
    if not existing:
        raise HTTPException(status_code=404, detail="Not Found")

    next_active = 0 if bool(existing.get("is_active")) else 1

    db.execute(
        text(
            """
            UPDATE zones
            SET is_active = :is_active, updated_at = NOW()
            WHERE id = :zone_id
            """
        ),
        {
            "zone_id": int(zone_id),
            "is_active": int(next_active),
        },
    )
    db.commit()

    refreshed = _fetch_zone(db=db, zone_id=int(zone_id))
    return {
        "message": "Zona activada correctamente." if next_active else "Zona desactivada correctamente.",
        "data": _serialize_zone(db, refreshed),
    }


@router.get("/{zone_id:int}/countries")
def zone_countries(
    zone_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_countries_manage(request=request, authorization=authorization, db=db)

    zone = _fetch_zone(db=db, zone_id=int(zone_id))
    if not zone:
        raise HTTPException(status_code=404, detail="Not Found")

    countries = _fetch_zone_countries(db, int(zone_id))
    return {
        "zone_id": int(zone_id),
        "countries": countries,
    }


@router.get("/{zone_id:int}/countries/available")
def zone_available_countries(
    zone_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_countries_manage(request=request, authorization=authorization, db=db)

    zone = _fetch_zone(db=db, zone_id=int(zone_id))
    if not zone:
        raise HTTPException(status_code=404, detail="Not Found")

    search = _normalize_text(request.query_params.get("search"))
    continent = _normalize_text(request.query_params.get("continent")).upper()
    status = _normalize_text(request.query_params.get("status") or "active")

    where_clauses = ["1 = 1"]
    params: dict[str, object] = {}

    if search:
        where_clauses.append("name LIKE :search")
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
            SELECT id, name, continent_code, phone_code, is_active
            FROM countries
            WHERE {' AND '.join(where_clauses)}
            ORDER BY name
            """
        ),
        params,
    ).mappings().all()

    attached_rows = db.execute(
        text(
            """
            SELECT country_id
            FROM country_zone
            WHERE zone_id = :zone_id
            """
        ),
        {"zone_id": int(zone_id)},
    ).mappings().all()
    attached_ids = {int(item["country_id"]) for item in attached_rows}

    countries = [
        _serialize_country_for_zone(row, attached=int(row["id"]) in attached_ids)
        for row in rows
    ]

    return {
        "zone_id": int(zone_id),
        "countries": countries,
        "filters": {
            "search": search,
            "continent": continent or "",
            "status": status or "active",
        },
        "continents": _CONTINENT_OPTIONS,
    }


@router.post("/{zone_id:int}/countries/{country_id:int}")
def attach_country_to_zone(
    zone_id: int,
    country_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_countries_manage(request=request, authorization=authorization, db=db)

    zone = _fetch_zone(db=db, zone_id=int(zone_id))
    if not zone:
        raise HTTPException(status_code=404, detail="Not Found")

    country = db.execute(
        text("SELECT id FROM countries WHERE id = :country_id LIMIT 1"),
        {"country_id": int(country_id)},
    ).mappings().first()
    if not country:
        raise HTTPException(status_code=404, detail="Not Found")

    db.execute(
        text(
            """
            INSERT INTO country_zone (zone_id, country_id, created_at, updated_at)
            VALUES (:zone_id, :country_id, NOW(), NOW())
            ON DUPLICATE KEY UPDATE updated_at = NOW()
            """
        ),
        {
            "zone_id": int(zone_id),
            "country_id": int(country_id),
        },
    )
    db.commit()

    return {"message": "País añadido a la zona."}


@router.delete("/{zone_id:int}/countries/{country_id:int}")
def detach_country_from_zone(
    zone_id: int,
    country_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin_countries_manage(request=request, authorization=authorization, db=db)

    zone = _fetch_zone(db=db, zone_id=int(zone_id))
    if not zone:
        raise HTTPException(status_code=404, detail="Not Found")

    db.execute(
        text(
            """
            DELETE FROM country_zone
            WHERE zone_id = :zone_id
              AND country_id = :country_id
            """
        ),
        {
            "zone_id": int(zone_id),
            "country_id": int(country_id),
        },
    )
    db.commit()

    return {"message": "País quitado de la zona."}
