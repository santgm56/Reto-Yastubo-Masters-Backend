from __future__ import annotations

import math

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.services.auth_service import AuthService

router = APIRouter(prefix="/api/v1/admin/companies", tags=["admin-companies-capitated"])


ALLOWED_STATUSES = {"active", "expired", "voided", "rolled_back"}


def _extract_bearer_token(authorization: str | None) -> str:
    if not authorization:
        return ""

    raw = authorization.strip()
    if not raw.lower().startswith("bearer "):
        return ""

    return raw[7:].strip()


def _require_admin(request: Request, authorization: str | None, db: Session) -> None:
    token = _extract_bearer_token(authorization)
    if not token:
        token = str(request.cookies.get("yastubo_access_token") or "").strip()

    if not token:
        raise HTTPException(status_code=403, detail="Forbidden")

    auth_payload = AuthService(db).me(token)
    role = str(auth_payload.get("role") or "").upper()
    if role != "ADMIN":
        raise HTTPException(status_code=403, detail="Forbidden")


def _require_company_exists(db: Session, company_id: int) -> None:
    row = db.execute(
        text(
            """
            SELECT id
            FROM companies
            WHERE id = :company_id
            LIMIT 1
            """
        ),
        {"company_id": int(company_id)},
    ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Not Found")


def _build_filters(status: str | None, product_id: int | None, q: str) -> tuple[list[str], dict[str, object]]:
    clauses = ["c.company_id = :company_id"]
    params: dict[str, object] = {}

    normalized_status = (status or "active").strip().lower()
    if normalized_status in ALLOWED_STATUSES:
        clauses.append("LOWER(c.status) = :status")
        params["status"] = normalized_status

    if product_id is not None:
        clauses.append("c.product_id = :product_id")
        params["product_id"] = int(product_id)

    search = (q or "").strip().lower()
    if search:
        params["q_like"] = f"%{search}%"
        params["q_exact"] = search

        clauses.append(
            """
            (
                CAST(c.id AS CHAR) = :q_exact
                OR LOWER(pi.full_name) LIKE :q_like
                OR LOWER(pi.document_number) LIKE :q_like
                OR EXISTS (
                    SELECT 1
                    FROM capitados_monthly_records mr
                    LEFT JOIN countries rc ON rc.id = mr.residence_country_id
                    LEFT JOIN countries rep ON rep.id = mr.repatriation_country_id
                    WHERE mr.contract_id = c.id
                      AND mr.id = (
                          SELECT m2.id
                          FROM capitados_monthly_records m2
                          WHERE m2.contract_id = c.id
                          ORDER BY m2.coverage_month DESC, m2.id DESC
                          LIMIT 1
                      )
                      AND (
                          LOWER(COALESCE(rc.name, '')) LIKE :q_like
                          OR LOWER(COALESCE(rep.name, '')) LIKE :q_like
                      )
                )
            )
            """
        )

    return clauses, params


@router.get("/{company_id}/capitated/contracts")
def contracts_index(
    company_id: int,
    request: Request,
    status: str = Query(default="active"),
    product_id: int | None = Query(default=None),
    q: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=15, ge=1, le=250),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin(request=request, authorization=authorization, db=db)
    _require_company_exists(db=db, company_id=int(company_id))

    where_clauses, where_params = _build_filters(status=status, product_id=product_id, q=q)
    where_sql = " AND ".join(where_clauses)

    query_params: dict[str, object] = {
        "company_id": int(company_id),
        "limit": int(per_page),
        "offset": int((page - 1) * per_page),
    }
    query_params.update(where_params)

    count_row = db.execute(
        text(
            f"""
            SELECT COUNT(1) AS total
            FROM capitados_contracts c
            INNER JOIN capitados_product_insureds pi ON pi.id = c.person_id
            WHERE {where_sql}
            """
        ),
        query_params,
    ).mappings().first()

    total = int((count_row or {}).get("total") or 0)
    last_page = max(1, int(math.ceil(total / per_page)) if total > 0 else 1)

    rows = db.execute(
        text(
            f"""
            SELECT
                c.id,
                c.uuid,
                c.company_id,
                c.product_id,
                c.person_id,
                c.status,
                c.entry_date,
                c.valid_until,
                c.entry_age,
                c.created_at,
                c.updated_at,
                pi.full_name AS person_full_name,
                pi.document_number AS person_document_number,
                pi.sex AS person_sex,
                p.name AS product_name
            FROM capitados_contracts c
            INNER JOIN capitados_product_insureds pi ON pi.id = c.person_id
            LEFT JOIN products p ON p.id = c.product_id
            WHERE {where_sql}
            ORDER BY c.id DESC
            LIMIT :limit OFFSET :offset
            """
        ),
        query_params,
    ).mappings().all()

    data = []
    for row in rows:
        data.append(
            {
                "id": int(row["id"]),
                "uuid": row.get("uuid"),
                "company_id": int(row.get("company_id") or 0),
                "product_id": int(row.get("product_id") or 0),
                "person_id": int(row.get("person_id") or 0),
                "status": row.get("status"),
                "entry_date": row.get("entry_date"),
                "valid_until": row.get("valid_until"),
                "entry_age": row.get("entry_age"),
                "created_at": row.get("created_at"),
                "updated_at": row.get("updated_at"),
                "person": {
                    "id": int(row.get("person_id") or 0),
                    "full_name": row.get("person_full_name"),
                    "document_number": row.get("person_document_number"),
                    "sex": row.get("person_sex"),
                },
                "product": {
                    "id": int(row.get("product_id") or 0),
                    "name": row.get("product_name"),
                },
            }
        )

    return {
        "data": data,
        "meta": {
            "current_page": int(page),
            "last_page": int(last_page),
            "per_page": int(per_page),
            "total": int(total),
        },
    }


@router.get("/{company_id}/capitated/contracts/{contract_id}")
def contracts_show(
    company_id: int,
    contract_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin(request=request, authorization=authorization, db=db)
    _require_company_exists(db=db, company_id=int(company_id))

    contract_row = db.execute(
        text(
            """
            SELECT
                c.id,
                c.uuid,
                c.company_id,
                c.product_id,
                c.person_id,
                c.status,
                c.entry_date,
                c.valid_until,
                c.entry_age,
                c.created_at,
                c.updated_at,
                pi.full_name AS person_full_name,
                pi.document_number AS person_document_number,
                pi.sex AS person_sex,
                p.name AS product_name
            FROM capitados_contracts c
            INNER JOIN capitados_product_insureds pi ON pi.id = c.person_id
            LEFT JOIN products p ON p.id = c.product_id
            WHERE c.id = :contract_id
              AND c.company_id = :company_id
            LIMIT 1
            """
        ),
        {"company_id": int(company_id), "contract_id": int(contract_id)},
    ).mappings().first()

    if not contract_row:
        raise HTTPException(status_code=404, detail="Not Found")

    monthly_row = db.execute(
        text(
            """
            SELECT
                mr.id,
                mr.company_id,
                mr.product_id,
                mr.person_id,
                mr.contract_id,
                mr.coverage_month,
                mr.plan_version_id,
                mr.load_batch_id,
                mr.full_name,
                mr.sex,
                mr.age_reported,
                mr.price_base,
                mr.price_source,
                mr.age_surcharge_rule_id,
                mr.age_surcharge_percent,
                mr.age_surcharge_amount,
                mr.price_final,
                mr.status,
                mr.created_at,
                mr.updated_at,
                rc.id AS residence_country_id,
                rc.iso2 AS residence_country_iso2,
                rc.iso3 AS residence_country_iso3,
                rc.name AS residence_country_name,
                rep.id AS repatriation_country_id,
                rep.iso2 AS repatriation_country_iso2,
                rep.iso3 AS repatriation_country_iso3,
                rep.name AS repatriation_country_name
            FROM capitados_monthly_records mr
            LEFT JOIN countries rc ON rc.id = mr.residence_country_id
            LEFT JOIN countries rep ON rep.id = mr.repatriation_country_id
            WHERE mr.contract_id = :contract_id
            ORDER BY mr.coverage_month DESC, mr.id DESC
            LIMIT 1
            """
        ),
        {"contract_id": int(contract_id)},
    ).mappings().first()

    contract_payload = {
        "id": int(contract_row["id"]),
        "uuid": contract_row.get("uuid"),
        "company_id": int(contract_row.get("company_id") or 0),
        "product_id": int(contract_row.get("product_id") or 0),
        "person_id": int(contract_row.get("person_id") or 0),
        "status": contract_row.get("status"),
        "entry_date": contract_row.get("entry_date"),
        "valid_until": contract_row.get("valid_until"),
        "entry_age": contract_row.get("entry_age"),
        "created_at": contract_row.get("created_at"),
        "updated_at": contract_row.get("updated_at"),
        "person": {
            "id": int(contract_row.get("person_id") or 0),
            "full_name": contract_row.get("person_full_name"),
            "document_number": contract_row.get("person_document_number"),
            "sex": contract_row.get("person_sex"),
        },
        "product": {
            "id": int(contract_row.get("product_id") or 0),
            "name": contract_row.get("product_name"),
        },
    }

    monthly_payload = None
    if monthly_row:
        monthly_payload = {
            "id": int(monthly_row["id"]),
            "company_id": int(monthly_row.get("company_id") or 0),
            "product_id": int(monthly_row.get("product_id") or 0),
            "person_id": int(monthly_row.get("person_id") or 0),
            "contract_id": int(monthly_row.get("contract_id") or 0),
            "coverage_month": monthly_row.get("coverage_month"),
            "plan_version_id": monthly_row.get("plan_version_id"),
            "load_batch_id": monthly_row.get("load_batch_id"),
            "full_name": monthly_row.get("full_name"),
            "sex": monthly_row.get("sex"),
            "age_reported": monthly_row.get("age_reported"),
            "price_base": monthly_row.get("price_base"),
            "price_source": monthly_row.get("price_source"),
            "age_surcharge_rule_id": monthly_row.get("age_surcharge_rule_id"),
            "age_surcharge_percent": monthly_row.get("age_surcharge_percent"),
            "age_surcharge_amount": monthly_row.get("age_surcharge_amount"),
            "price_final": monthly_row.get("price_final"),
            "status": monthly_row.get("status"),
            "created_at": monthly_row.get("created_at"),
            "updated_at": monthly_row.get("updated_at"),
            "residence_country": {
                "id": monthly_row.get("residence_country_id"),
                "iso2": monthly_row.get("residence_country_iso2"),
                "iso3": monthly_row.get("residence_country_iso3"),
                "name": monthly_row.get("residence_country_name"),
            }
            if monthly_row.get("residence_country_id") is not None
            else None,
            "repatriation_country": {
                "id": monthly_row.get("repatriation_country_id"),
                "iso2": monthly_row.get("repatriation_country_iso2"),
                "iso3": monthly_row.get("repatriation_country_iso3"),
                "name": monthly_row.get("repatriation_country_name"),
            }
            if monthly_row.get("repatriation_country_id") is not None
            else None,
        }

    return {
        "contract": contract_payload,
        "last_monthly_record": monthly_payload,
    }
