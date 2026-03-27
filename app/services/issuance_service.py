from __future__ import annotations

import json
from datetime import date
from uuid import uuid4

from itsdangerous import BadSignature, URLSafeSerializer
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import get_settings

settings = get_settings()
quote_serializer = URLSafeSerializer(settings.quote_secret, salt="issuance-quote")


class IssuanceService:
    def __init__(self, db: Session):
        self.db = db

    def build_quote(self, payload: dict) -> dict:
        plan_version_id = int(payload["plan_version_id"])
        customer = payload["customer"]
        age = int(customer["age"])

        plan_row = self.db.execute(
            text(
                """
                SELECT pv.id, pv.product_id, pv.price_1, pv.max_entry_age, pv.name,
                       p.company_id
                FROM plan_versions pv
                INNER JOIN products p ON p.id = pv.product_id
                WHERE pv.id = :plan_version_id
                LIMIT 1
                """
            ),
            {"plan_version_id": plan_version_id},
        ).mappings().first()

        if not plan_row:
            raise ValueError("Plan version not found")

        residence_country_id = int(customer["residence_country_id"])
        repatriation_country_id = int(customer["repatriation_country_id"])

        country_price_row = self.db.execute(
            text(
                """
                SELECT price
                FROM plan_version_countries
                WHERE plan_version_id = :plan_version_id
                  AND country_id = :country_id
                LIMIT 1
                """
            ),
            {"plan_version_id": plan_version_id, "country_id": residence_country_id},
        ).mappings().first()

        base_price = float(country_price_row["price"]) if country_price_row else float(plan_row["price_1"] or 0)

        surcharge_row = self.db.execute(
            text(
                """
                SELECT id, surcharge_percent
                FROM plan_version_age_surcharges
                WHERE plan_version_id = :plan_version_id
                  AND (age_from IS NULL OR age_from <= :age)
                  AND (age_to IS NULL OR age_to >= :age)
                ORDER BY age_from DESC
                LIMIT 1
                """
            ),
            {"plan_version_id": plan_version_id, "age": age},
        ).mappings().first()

        surcharge_percent = float(surcharge_row["surcharge_percent"]) if surcharge_row else 0.0
        surcharge_amount = round(base_price * (surcharge_percent / 100.0), 2)
        total_price = round(base_price + surcharge_amount, 2)

        repat_count_row = self.db.execute(
            text("SELECT COUNT(*) AS c FROM plan_version_repatriation_countries WHERE plan_version_id = :plan_version_id"),
            {"plan_version_id": plan_version_id},
        ).mappings().first()
        has_repatriation_restrictions = int(repat_count_row["c"] or 0) > 0

        repat_allowed = True
        if has_repatriation_restrictions:
            allowed_row = self.db.execute(
                text(
                    """
                    SELECT 1
                    FROM plan_version_repatriation_countries
                    WHERE plan_version_id = :plan_version_id
                      AND country_id = :country_id
                    LIMIT 1
                    """
                ),
                {"plan_version_id": plan_version_id, "country_id": repatriation_country_id},
            ).mappings().first()
            repat_allowed = bool(allowed_row)

        max_entry_age = int(plan_row["max_entry_age"] or 0)
        is_age_eligible = max_entry_age <= 0 or age <= max_entry_age

        eligible = is_age_eligible and repat_allowed
        reasons: list[str] = []
        if not is_age_eligible:
            reasons.append("AGE_NOT_ELIGIBLE")
        if not repat_allowed:
            reasons.append("REPATRIATION_COUNTRY_NOT_ALLOWED")

        quote_data = {
            "plan_version_id": plan_version_id,
            "product_id": int(plan_row["product_id"]),
            "company_id": int(plan_row["company_id"]),
            "customer": {
                "document_number": str(customer["document_number"]),
                "full_name": str(customer["full_name"]),
                "age": age,
                "sex": str(customer.get("sex") or "M"),
                "residence_country_id": residence_country_id,
                "repatriation_country_id": repatriation_country_id,
            },
            "pricing": {
                "base_price": base_price,
                "surcharge_percent": surcharge_percent,
                "surcharge_amount": surcharge_amount,
                "total_price": total_price,
            },
            "eligibility": {
                "eligible": eligible,
                "reasons": reasons,
            },
        }

        quote_id = quote_serializer.dumps(quote_data)

        return {
            "quote_id": quote_id,
            "eligible": eligible,
            "pricing": quote_data["pricing"],
            "reasons": reasons,
            "plan": {
                "plan_version_id": plan_version_id,
                "name": plan_row["name"],
                "max_entry_age": max_entry_age,
            },
        }

    def quote(self, payload) -> dict:
        payload_dict = payload.model_dump() if hasattr(payload, "model_dump") else dict(payload)
        return self.build_quote(payload_dict)

    def decode_quote(self, quote_id: str) -> dict:
        try:
            decoded = quote_serializer.loads(quote_id)
            return decoded if isinstance(decoded, dict) else {}
        except BadSignature as exc:
            raise ValueError("Invalid quote id") from exc

    def store_issuance(self, quote_payload: dict, start_date: str | None = None) -> dict:
        if not quote_payload.get("eligibility", {}).get("eligible", False):
            raise ValueError("Quote is not eligible")

        customer = quote_payload["customer"]
        pricing = quote_payload["pricing"]
        plan_version_id = int(quote_payload["plan_version_id"])
        product_id = int(quote_payload["product_id"])
        company_id = int(quote_payload["company_id"])

        entry_date = date.fromisoformat(start_date) if start_date else date.today()
        coverage_month = entry_date.replace(day=1)
        valid_until = date(entry_date.year, entry_date.month, 28)
        if entry_date.month in (1, 3, 5, 7, 8, 10, 12):
            valid_until = valid_until.replace(day=31)
        elif entry_date.month in (4, 6, 9, 11):
            valid_until = valid_until.replace(day=30)

        with self.db.begin():
            person_row = self.db.execute(
                text(
                    """
                    SELECT id
                    FROM capitados_product_insureds
                    WHERE company_id = :company_id
                      AND product_id = :product_id
                      AND document_number = :document_number
                    LIMIT 1
                    """
                ),
                {
                    "company_id": company_id,
                    "product_id": product_id,
                    "document_number": customer["document_number"],
                },
            ).mappings().first()

            if person_row:
                person_id = int(person_row["id"])
                self.db.execute(
                    text(
                        """
                        UPDATE capitados_product_insureds
                        SET full_name = :full_name,
                            sex = :sex,
                            age_reported = :age_reported,
                            residence_country_id = :residence_country_id,
                            repatriation_country_id = :repatriation_country_id,
                            status = 'active',
                            updated_at = NOW()
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": person_id,
                        "full_name": customer["full_name"],
                        "sex": str(customer.get("sex") or "M").upper(),
                        "age_reported": int(customer["age"]),
                        "residence_country_id": int(customer["residence_country_id"]),
                        "repatriation_country_id": int(customer["repatriation_country_id"]),
                    },
                )
            else:
                result = self.db.execute(
                    text(
                        """
                        INSERT INTO capitados_product_insureds
                        (company_id, product_id, document_number, full_name, sex, age_reported,
                         residence_country_id, repatriation_country_id, status, created_at, updated_at)
                        VALUES
                        (:company_id, :product_id, :document_number, :full_name, :sex, :age_reported,
                         :residence_country_id, :repatriation_country_id, 'active', NOW(), NOW())
                        """
                    ),
                    {
                        "company_id": company_id,
                        "product_id": product_id,
                        "document_number": customer["document_number"],
                        "full_name": customer["full_name"],
                        "sex": str(customer.get("sex") or "M").upper(),
                        "age_reported": int(customer["age"]),
                        "residence_country_id": int(customer["residence_country_id"]),
                        "repatriation_country_id": int(customer["repatriation_country_id"]),
                    },
                )
                person_id = int(result.lastrowid)

            contract_uuid = str(uuid4())
            contract_result = self.db.execute(
                text(
                    """
                    INSERT INTO capitados_contracts
                    (company_id, product_id, person_id, status, entry_date, valid_until, entry_age, uuid, created_at, updated_at)
                    VALUES
                    (:company_id, :product_id, :person_id, 'active', :entry_date, :valid_until, :entry_age, :uuid, NOW(), NOW())
                    """
                ),
                {
                    "company_id": company_id,
                    "product_id": product_id,
                    "person_id": person_id,
                    "entry_date": entry_date.isoformat(),
                    "valid_until": valid_until.isoformat(),
                    "entry_age": int(customer["age"]),
                    "uuid": contract_uuid,
                },
            )
            contract_id = int(contract_result.lastrowid)

            monthly_row = self.db.execute(
                text(
                    """
                    SELECT id
                    FROM capitados_monthly_records
                    WHERE company_id = :company_id
                      AND product_id = :product_id
                      AND person_id = :person_id
                      AND coverage_month = :coverage_month
                    LIMIT 1
                    """
                ),
                {
                    "company_id": company_id,
                    "product_id": product_id,
                    "person_id": person_id,
                    "coverage_month": coverage_month.isoformat(),
                },
            ).mappings().first()

            if monthly_row:
                monthly_id = int(monthly_row["id"])
                self.db.execute(
                    text(
                        """
                        UPDATE capitados_monthly_records
                        SET contract_id = :contract_id,
                            plan_version_id = :plan_version_id,
                            load_batch_id = 0,
                            full_name = :full_name,
                            sex = :sex,
                            age_reported = :age_reported,
                            residence_country_id = :residence_country_id,
                            repatriation_country_id = :repatriation_country_id,
                            price_base = :price_base,
                            price_source = 'quote',
                            age_surcharge_percent = :age_surcharge_percent,
                            age_surcharge_amount = :age_surcharge_amount,
                            price_final = :price_final,
                            status = 'active',
                            updated_at = NOW()
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": monthly_id,
                        "contract_id": contract_id,
                        "plan_version_id": plan_version_id,
                        "full_name": customer["full_name"],
                        "sex": str(customer.get("sex") or "M").upper(),
                        "age_reported": int(customer["age"]),
                        "residence_country_id": int(customer["residence_country_id"]),
                        "repatriation_country_id": int(customer["repatriation_country_id"]),
                        "price_base": float(pricing.get("base_price") or 0),
                        "age_surcharge_percent": float(pricing.get("surcharge_percent") or 0),
                        "age_surcharge_amount": float(pricing.get("surcharge_amount") or 0),
                        "price_final": float(pricing.get("total_price") or 0),
                    },
                )
            else:
                monthly_result = self.db.execute(
                    text(
                        """
                        INSERT INTO capitados_monthly_records
                        (company_id, product_id, person_id, contract_id, coverage_month, plan_version_id,
                         load_batch_id, full_name, sex, age_reported, residence_country_id, repatriation_country_id,
                         price_base, price_source, age_surcharge_percent, age_surcharge_amount, price_final,
                         status, created_at, updated_at)
                        VALUES
                        (:company_id, :product_id, :person_id, :contract_id, :coverage_month, :plan_version_id,
                         0, :full_name, :sex, :age_reported, :residence_country_id, :repatriation_country_id,
                         :price_base, 'quote', :age_surcharge_percent, :age_surcharge_amount, :price_final,
                         'active', NOW(), NOW())
                        """
                    ),
                    {
                        "company_id": company_id,
                        "product_id": product_id,
                        "person_id": person_id,
                        "contract_id": contract_id,
                        "coverage_month": coverage_month.isoformat(),
                        "plan_version_id": plan_version_id,
                        "full_name": customer["full_name"],
                        "sex": str(customer.get("sex") or "M").upper(),
                        "age_reported": int(customer["age"]),
                        "residence_country_id": int(customer["residence_country_id"]),
                        "repatriation_country_id": int(customer["repatriation_country_id"]),
                        "price_base": float(pricing.get("base_price") or 0),
                        "age_surcharge_percent": float(pricing.get("surcharge_percent") or 0),
                        "age_surcharge_amount": float(pricing.get("surcharge_amount") or 0),
                        "price_final": float(pricing.get("total_price") or 0),
                    },
                )
                monthly_id = int(monthly_result.lastrowid)

            self.db.execute(
                text(
                    """
                    INSERT INTO audit_logs
                    (actor_user_id, target_user_id, realm, action, context_json, ip, user_agent, created_at)
                    VALUES (NULL, NULL, 'admin', 'issuance.completed', :context_json, NULL, 'fastapi-migration', NOW())
                    """
                ),
                {
                    "context_json": json.dumps(
                        {
                            "contract_id": contract_id,
                            "contract_uuid": contract_uuid,
                            "monthly_record_id": monthly_id,
                            "plan_version_id": plan_version_id,
                            "quote_price": float(pricing.get("total_price") or 0),
                        },
                        ensure_ascii=False,
                    )
                },
            )

        return {
            "issuance_id": contract_uuid,
            "contract_id": contract_id,
            "monthly_record_id": monthly_id,
            "status": "PENDING_PAYMENT",
            "amount": float(pricing.get("total_price") or 0),
        }

    def store(self, payload) -> dict:
        payload_dict = payload.model_dump() if hasattr(payload, "model_dump") else dict(payload)
        decoded_quote = self.decode_quote(str(payload_dict.get("quote_id") or ""))
        start_date = payload_dict.get("start_date")
        return self.store_issuance(decoded_quote, start_date=start_date)

    def show_issuance(self, issuance_id: str) -> dict | None:
        query = text(
            """
            SELECT c.id, c.uuid, c.status, c.entry_date, c.valid_until,
                   p.document_number, p.full_name, p.age_reported,
                   mr.price_final, mr.price_base
            FROM capitados_contracts c
            LEFT JOIN capitados_product_insureds p ON p.id = c.person_id
            LEFT JOIN capitados_monthly_records mr ON mr.contract_id = c.id
            WHERE c.uuid = :issuance_id OR c.id = :numeric_id
            ORDER BY mr.coverage_month DESC, mr.id DESC
            LIMIT 1
            """
        )

        numeric_id = int(issuance_id) if issuance_id.isdigit() else -1
        row = self.db.execute(query, {"issuance_id": issuance_id, "numeric_id": numeric_id}).mappings().first()
        if not row:
            return None

        return {
            "issuance_id": row["uuid"],
            "contract_id": int(row["id"]),
            "status": str(row["status"] or "").upper(),
            "entry_date": row["entry_date"].isoformat() if row["entry_date"] else None,
            "valid_until": row["valid_until"].isoformat() if row["valid_until"] else None,
            "customer": {
                "document_number": row["document_number"],
                "full_name": row["full_name"],
                "age": row["age_reported"],
            },
            "pricing": {
                "amount": float(row["price_final"] or 0),
                "base": float(row["price_base"] or 0),
            },
        }

    def show(self, issuance_id: int | str) -> dict | None:
        return self.show_issuance(str(issuance_id))

    def index(self, status: str = "all", term: str = "", sort: str = "newest", per_page: int = 15, page: int = 1) -> dict:
        page = max(1, int(page))
        per_page = max(1, min(int(per_page), 100))
        offset = (page - 1) * per_page

        where_parts = []
        params: dict[str, object] = {}

        normalized_status = (status or "all").strip().lower()
        if normalized_status and normalized_status != "all":
            where_parts.append("LOWER(c.status) = :status")
            params["status"] = normalized_status

        if term.strip():
            where_parts.append("(p.full_name LIKE :term OR p.document_number LIKE :term OR c.uuid LIKE :term)")
            params["term"] = f"%{term.strip()}%"

        where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

        order_sql = "c.id DESC" if sort != "oldest" else "c.id ASC"

        total_row = self.db.execute(
            text(
                f"""
                SELECT COUNT(*) AS c
                FROM capitados_contracts c
                LEFT JOIN capitados_product_insureds p ON p.id = c.person_id
                {where_sql}
                """
            ),
            params,
        ).mappings().first()
        total = int(total_row["c"] if total_row else 0)

        rows = self.db.execute(
            text(
                f"""
                SELECT c.id, c.uuid, c.status, c.entry_date, c.valid_until,
                       p.document_number, p.full_name,
                       mr.price_final, mr.coverage_month
                FROM capitados_contracts c
                LEFT JOIN capitados_product_insureds p ON p.id = c.person_id
                LEFT JOIN capitados_monthly_records mr ON mr.contract_id = c.id
                {where_sql}
                ORDER BY {order_sql}
                LIMIT :limit OFFSET :offset
                """
            ),
            {**params, "limit": per_page, "offset": offset},
        ).mappings().all()

        items = [
            {
                "id": int(row["id"]),
                "issuance_id": row["uuid"],
                "status": row["status"],
                "entry_date": row["entry_date"].isoformat() if row["entry_date"] else None,
                "valid_until": row["valid_until"].isoformat() if row["valid_until"] else None,
                "customer": {
                    "document_number": row["document_number"],
                    "full_name": row["full_name"],
                },
                "amount": float(row["price_final"] or 0),
                "coverage_month": row["coverage_month"].isoformat() if row["coverage_month"] else None,
            }
            for row in rows
        ]

        last_page = max(1, (total + per_page - 1) // per_page)
        return {
            "rows": items,
            "pagination": {
                "current_page": page,
                "last_page": last_page,
                "per_page": per_page,
                "total": total,
            },
        }
