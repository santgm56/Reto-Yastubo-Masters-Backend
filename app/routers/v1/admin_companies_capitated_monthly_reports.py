from __future__ import annotations

from datetime import datetime
from io import BytesIO

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import StreamingResponse
from openpyxl import Workbook
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.services.auth_service import AuthService

router = APIRouter(prefix="/api/v1/admin/companies", tags=["admin-companies-capitated"])


def _extract_bearer_token(authorization: str | None) -> str:
    if not authorization:
        return ""

    raw = authorization.strip()
    if not raw.lower().startswith("bearer "):
        return ""

    return raw[7:].strip()


def _require_capitated_monthly_permission(request: Request, authorization: str | None, db: Session) -> None:
    token = _extract_bearer_token(authorization)
    if not token:
        token = str(request.cookies.get("yastubo_access_token") or "").strip()

    if not token:
        raise HTTPException(status_code=403, detail="Forbidden")

    auth_payload = AuthService(db).me(token)
    role = str(auth_payload.get("role") or "").upper()
    if role != "ADMIN":
        raise HTTPException(status_code=403, detail="Forbidden")

    permissions = {str(item or "").strip() for item in (auth_payload.get("permissions") or [])}
    if "capitados.reporte.mensual" not in permissions:
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


@router.get("/{company_id}/capitated/reports/monthly/months")
def monthly_report_months(
    company_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_capitated_monthly_permission(request=request, authorization=authorization, db=db)
    _require_company_exists(db=db, company_id=int(company_id))

    rows = db.execute(
        text(
            """
            SELECT
                DATE_FORMAT(coverage_month, '%Y-%m-01') AS month,
                SUM(CASE WHEN UPPER(status) = 'ACTIVE' THEN 1 ELSE 0 END) AS active_count,
                SUM(CASE WHEN UPPER(status) = 'ACTIVE' THEN price_final ELSE 0 END) AS active_total
            FROM capitated_monthly_records
            WHERE company_id = :company_id
            GROUP BY DATE_FORMAT(coverage_month, '%Y-%m-01')
            ORDER BY month DESC
            """
        ),
        {"company_id": int(company_id)},
    ).mappings().all()

    months = [
        {
            "month": row.get("month"),
            "active_count": int(row.get("active_count") or 0),
            "active_total": float(row.get("active_total") or 0),
        }
        for row in rows
    ]

    return {"months": months}


@router.get("/{company_id}/capitated/reports/monthly/{month}/download")
def monthly_report_download(
    company_id: int,
    month: str,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> StreamingResponse:
    _require_capitated_monthly_permission(request=request, authorization=authorization, db=db)
    _require_company_exists(db=db, company_id=int(company_id))

    try:
        normalized_month = datetime.fromisoformat(str(month)).replace(day=1)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Invalid month") from exc

    rows = db.execute(
        text(
            """
            SELECT
                cmr.id,
                cmr.contract_id,
                cmr.coverage_month,
                cmr.person_id,
                cmr.full_name,
                cmr.sex,
                cmr.age_reported,
                rc.iso3 AS residence_iso3,
                rc.iso2 AS residence_iso2,
                rc.name AS residence_name,
                rep.iso3 AS repatriation_iso3,
                rep.iso2 AS repatriation_iso2,
                rep.name AS repatriation_name,
                cmr.price_source,
                cmr.price_base,
                cmr.age_surcharge_percent,
                cmr.price_final
            FROM capitated_monthly_records cmr
            LEFT JOIN countries rc ON rc.id = cmr.residence_country_id
            LEFT JOIN countries rep ON rep.id = cmr.repatriation_country_id
            WHERE cmr.company_id = :company_id
              AND DATE_FORMAT(cmr.coverage_month, '%Y-%m-01') = :coverage_month
            ORDER BY cmr.product_id ASC, cmr.id ASC
            """
        ),
        {
            "company_id": int(company_id),
            "coverage_month": normalized_month.strftime("%Y-%m-01"),
        },
    ).mappings().all()

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Reporte mensual"

    headers = [
        "ID",
        "# Contrato",
        "Mes",
        "ID Persona",
        "Persona",
        "Genero",
        "Edad reportada",
        "Residencia ISO3",
        "Residencia ISO2",
        "Residencia",
        "Repatriacion ISO3",
        "Repatriacion ISO2",
        "Repatriacion",
        "Fuente del precio",
        "Precio base",
        "Recargo por edad",
        "Precio total",
    ]
    sheet.append(headers)

    for row in rows:
        coverage_value = row.get("coverage_month")
        if hasattr(coverage_value, "strftime"):
            month_label = coverage_value.strftime("%m/%Y")
        else:
            month_label = str(coverage_value or "")

        sheet.append(
            [
                row.get("id"),
                row.get("contract_id"),
                month_label,
                row.get("person_id"),
                row.get("full_name"),
                row.get("sex"),
                row.get("age_reported"),
                row.get("residence_iso3"),
                row.get("residence_iso2"),
                row.get("residence_name"),
                row.get("repatriation_iso3"),
                row.get("repatriation_iso2"),
                row.get("repatriation_name"),
                row.get("price_source"),
                float(row.get("price_base") or 0),
                float(row.get("age_surcharge_percent") or 0),
                float(row.get("price_final") or 0),
            ]
        )

    buffer = BytesIO()
    workbook.save(buffer)
    buffer.seek(0)

    filename = f"capitados_reporte_{normalized_month.strftime('%Y_%m')}.xlsx"

    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
