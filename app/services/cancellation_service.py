from __future__ import annotations

import json

from sqlalchemy import text
from sqlalchemy.orm import Session


class CancellationService:
    def __init__(self, db: Session):
        self.db = db

    def list_rows(self, limit: int = 120) -> list[dict]:
        rows = self.db.execute(
            text(
                """
                SELECT c.id, c.uuid, c.status, c.entry_date, p.full_name
                FROM capitados_contracts c
                LEFT JOIN capitados_product_insureds p ON p.id = c.person_id
                ORDER BY c.id DESC
                LIMIT :limit
                """
            ),
            {"limit": max(1, min(limit, 300))},
        ).mappings().all()

        result = []
        for row in rows:
            status = str(row["status"] or "").upper()
            normalized_status = "CANCELED" if status in {"VOIDED", "CANCELLED", "CANCELED"} else "ACTIVE"
            result.append(
                {
                    "contract_id": int(row["id"]),
                    "issuance_id": row["uuid"],
                    "customer_name": row["full_name"],
                    "entry_date": row["entry_date"].isoformat() if row["entry_date"] else None,
                    "status": normalized_status,
                }
            )

        return result

    def request_cancellation(self, contract_id: int, reason: str, actor_id: str = "") -> dict:
        contract = self.db.execute(
            text(
                """
                SELECT id, uuid, status
                FROM capitados_contracts
                WHERE id = :contract_id
                LIMIT 1
                """
            ),
            {"contract_id": contract_id},
        ).mappings().first()

        if not contract:
            raise ValueError("Contrato no encontrado")

        current_status = str(contract["status"] or "").upper()
        if current_status in {"VOIDED", "CANCELED", "CANCELLED"}:
            return {
                "contract_id": contract_id,
                "issuance_id": contract["uuid"],
                "status": "CANCELED",
                "reason": reason,
                "already_canceled": True,
            }

        self.db.execute(
            text(
                """
                UPDATE capitados_contracts
                SET status = 'voided', updated_at = NOW()
                WHERE id = :contract_id
                """
            ),
            {"contract_id": contract_id},
        )
        self.db.execute(
            text(
                """
                UPDATE capitados_monthly_records
                SET status = 'voided', updated_at = NOW()
                WHERE contract_id = :contract_id
                """
            ),
            {"contract_id": contract_id},
        )
        self.db.execute(
            text(
                """
                INSERT INTO audit_logs
                (actor_user_id, target_user_id, realm, action, context_json, ip, user_agent, created_at)
                VALUES (NULL, NULL, 'admin', 'cancellation.requested', :context_json, NULL, 'fastapi-migration', NOW())
                """
            ),
            {
                "context_json": json.dumps(
                    {
                        "contract_id": contract_id,
                        "contract_uuid": contract["uuid"],
                        "reason": reason,
                        "actor_id": actor_id,
                    },
                    ensure_ascii=False,
                )
            },
        )
        self.db.commit()

        return {
            "contract_id": contract_id,
            "issuance_id": contract["uuid"],
            "status": "CANCELED",
            "reason": reason,
            "already_canceled": False,
        }