from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.services.auth_service import AuthService

router = APIRouter(prefix="/api/v1/admin/companies", tags=["admin-companies"])


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


def _fetch_company_row(db: Session, company_id: int):
    return db.execute(
        text(
            """
            SELECT
                id,
                name,
                short_code,
                phone,
                email,
                description,
                status,
                commission_beneficiary_user_id,
                branding_logo_file_id,
                pdf_template_id,
                branding_text_dark,
                branding_bg_light,
                branding_text_light,
                branding_bg_dark
            FROM companies
            WHERE id = :company_id
            LIMIT 1
            """
        ),
        {"company_id": int(company_id)},
    ).mappings().first()


def _fetch_company_user_ids(db: Session, company_id: int) -> list[int]:
    rows = db.execute(
        text(
            """
            SELECT user_id
            FROM company_user
            WHERE company_id = :company_id
            ORDER BY user_id ASC
            """
        ),
        {"company_id": int(company_id)},
    ).mappings().all()

    return [int(row["user_id"]) for row in rows]


def _fetch_logo_payload(db: Session, branding_logo_file_id: int | None) -> dict | None:
    if branding_logo_file_id is None:
        return None

    row = db.execute(
        text(
            """
            SELECT id, uuid, original_name
            FROM files
            WHERE id = :file_id
            LIMIT 1
            """
        ),
        {"file_id": int(branding_logo_file_id)},
    ).mappings().first()

    if not row:
        return None

    return {
        "id": int(row["id"]),
        "url": f"/api/v1/files/{row['uuid']}",
        "original_name": row.get("original_name"),
        "is_custom": True,
    }


def _serialize_company(row, users_ids: list[int], logo_payload: dict | None) -> dict:
    def color_with_hash(value: str | None, fallback: str) -> str:
        normalized = str(value or "").strip().lstrip("#")
        if not normalized:
            normalized = fallback
        return f"#{normalized}"

    return {
        "id": int(row["id"]),
        "name": row.get("name"),
        "short_code": row.get("short_code"),
        "phone": row.get("phone"),
        "email": row.get("email"),
        "description": row.get("description"),
        "status": row.get("status"),
        "status_label": row.get("status"),
        "users_ids": users_ids,
        "commission_beneficiary_user_id": row.get("commission_beneficiary_user_id"),
        "branding_logo_file_id": row.get("branding_logo_file_id"),
        "pdf_template_id": row.get("pdf_template_id"),
        "branding": {
            "text_dark": color_with_hash(row.get("branding_text_dark"), "000000"),
            "bg_light": color_with_hash(row.get("branding_bg_light"), "FFFFFF"),
            "text_light": color_with_hash(row.get("branding_text_light"), "FFFFFF"),
            "bg_dark": color_with_hash(row.get("branding_bg_dark"), "000000"),
            "custom_text_dark": row.get("branding_text_dark"),
            "custom_bg_light": row.get("branding_bg_light"),
            "custom_text_light": row.get("branding_text_light"),
            "custom_bg_dark": row.get("branding_bg_dark"),
            "logo": logo_payload,
        },
    }


def _require_company_exists(db: Session, company_id: int):
    row = _fetch_company_row(db=db, company_id=int(company_id))
    if not row:
        raise HTTPException(status_code=404, detail="Not Found")
    return row


def _response_for_company(db: Session, company_id: int, toast_message: str) -> dict:
    row = _fetch_company_row(db=db, company_id=int(company_id))
    users_ids = _fetch_company_user_ids(db=db, company_id=int(company_id))
    logo_payload = _fetch_logo_payload(db=db, branding_logo_file_id=row.get("branding_logo_file_id"))

    return {
        "data": _serialize_company(row, users_ids, logo_payload),
        "toast": {
            "type": "success",
            "message": toast_message,
        },
    }


@router.put("/{company_id}/suspend")
def suspend_company(
    company_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin(request=request, authorization=authorization, db=db)

    row = _require_company_exists(db=db, company_id=int(company_id))
    if str(row.get("status") or "") != "archived":
        db.execute(
            text(
                """
                UPDATE companies
                SET status = :status
                WHERE id = :company_id
                """
            ),
            {"status": "inactive", "company_id": int(company_id)},
        )
        db.commit()

    return _response_for_company(db=db, company_id=int(company_id), toast_message="Empresa suspendida correctamente.")


@router.put("/{company_id}/archive")
def archive_company(
    company_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin(request=request, authorization=authorization, db=db)
    _require_company_exists(db=db, company_id=int(company_id))

    db.execute(
        text(
            """
            UPDATE companies
            SET status = :status
            WHERE id = :company_id
            """
        ),
        {"status": "archived", "company_id": int(company_id)},
    )
    db.commit()

    return _response_for_company(db=db, company_id=int(company_id), toast_message="Empresa archivada correctamente.")


@router.put("/{company_id}/activate")
def activate_company(
    company_id: int,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    _require_admin(request=request, authorization=authorization, db=db)
    _require_company_exists(db=db, company_id=int(company_id))

    db.execute(
        text(
            """
            UPDATE companies
            SET status = :status
            WHERE id = :company_id
            """
        ),
        {"status": "active", "company_id": int(company_id)},
    )
    db.commit()

    return _response_for_company(db=db, company_id=int(company_id), toast_message="Empresa activada correctamente.")
