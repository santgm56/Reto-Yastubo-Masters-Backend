from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.database import get_db

router = APIRouter(prefix="/api/v1/public/capitated/contracts", tags=["public-capitated-contracts"])


def _escape_pdf_text(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _build_simple_pdf(lines: list[str]) -> bytes:
    sanitized = [str(line or "").strip() for line in lines if str(line or "").strip()]
    if not sanitized:
        sanitized = ["Contract"]

    content_parts = ["BT", "/F1 12 Tf", "50 780 Td", "14 TL"]
    first = _escape_pdf_text(sanitized[0])
    content_parts.append(f"({first}) Tj")
    for line in sanitized[1:]:
        content_parts.append("T*")
        content_parts.append(f"({_escape_pdf_text(line)}) Tj")
    content_parts.append("ET")

    stream = "\n".join(content_parts).encode("latin-1", errors="replace")

    objects = [
        b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
        b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>\nendobj\n",
        b"4 0 obj\n<< /Length " + str(len(stream)).encode("ascii") + b" >>\nstream\n" + stream + b"\nendstream\nendobj\n",
        b"5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n",
    ]

    pdf = b"%PDF-1.4\n"
    offsets = [0]
    for obj in objects:
        offsets.append(len(pdf))
        pdf += obj

    xref_start = len(pdf)
    xref = [f"xref\n0 {len(objects) + 1}\n", "0000000000 65535 f \n"]
    for offset in offsets[1:]:
        xref.append(f"{offset:010d} 00000 n \n")
    pdf += "".join(xref).encode("ascii")
    pdf += (
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_start}\n%%EOF\n"
    ).encode("ascii")
    return pdf


@router.get("/{contract_uuid}/pdf")
def contract_pdf_by_uuid(contract_uuid: str, db: Session = Depends(get_db)) -> Response:
    contract_row = db.execute(
        text(
            """
            SELECT
                c.id,
                c.uuid,
                c.status,
                c.entry_date,
                c.valid_until,
                c.company_id,
                c.product_id,
                pi.full_name AS person_full_name,
                pi.document_number AS person_document_number,
                p.name AS product_name,
                co.short_code AS company_short_code,
                co.name AS company_name
            FROM capitados_contracts c
            INNER JOIN capitados_product_insureds pi ON pi.id = c.person_id
            LEFT JOIN products p ON p.id = c.product_id
            LEFT JOIN companies co ON co.id = c.company_id
            WHERE c.uuid = :contract_uuid
              AND LOWER(c.status) IN ('active', 'expired')
            LIMIT 1
            """
        ),
        {"contract_uuid": str(contract_uuid or "").strip()},
    ).mappings().first()

    if not contract_row:
        raise HTTPException(status_code=404, detail="Contrato no encontrado.")

    monthly_row = db.execute(
        text(
            """
            SELECT
                mr.id,
                mr.coverage_month,
                mr.price_final,
                rc.name AS residence_country_name,
                rep.name AS repatriation_country_name
            FROM capitados_monthly_records mr
            LEFT JOIN countries rc ON rc.id = mr.residence_country_id
            LEFT JOIN countries rep ON rep.id = mr.repatriation_country_id
            WHERE mr.contract_id = :contract_id
            ORDER BY mr.coverage_month DESC, mr.id DESC
            LIMIT 1
            """
        ),
        {"contract_id": int(contract_row.get("id") or 0)},
    ).mappings().first()

    if not monthly_row:
        raise HTTPException(status_code=404, detail="Registros mensuales no encontrados.")

    code_prefix = str(contract_row.get("company_short_code") or "CAP")
    contract_code = f"{code_prefix}-{int(contract_row.get('id') or 0):05d}"
    lines = [
        "Capitated Contract",
        f"Contract: {contract_code}",
        f"UUID: {contract_row.get('uuid')}",
        f"Company: {contract_row.get('company_name') or '-'}",
        f"Person: {contract_row.get('person_full_name') or '-'}",
        f"Document: {contract_row.get('person_document_number') or '-'}",
        f"Product: {contract_row.get('product_name') or '-'}",
        f"Status: {contract_row.get('status') or '-'}",
        f"Coverage month: {monthly_row.get('coverage_month') or '-'}",
        f"Residence: {monthly_row.get('residence_country_name') or '-'}",
        f"Repatriation: {monthly_row.get('repatriation_country_name') or '-'}",
        f"Final price: {monthly_row.get('price_final') or '-'}",
    ]

    pdf_bytes = _build_simple_pdf(lines)
    filename = f"capitated_contract_{contract_row.get('uuid')}.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{filename}"'},
    )
