from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.services.payment_service import PaymentService


class SellerDashboardService:
    def __init__(self, db: Session):
        self.db = db

    def summary(self) -> dict:
        customers_total = self._safe_scalar("SELECT COUNT(*) FROM users WHERE realm = 'customer'", 0)
        active_plans_total = self._safe_scalar("SELECT COUNT(*) FROM plan_versions WHERE status = 'ACTIVE'", 0)
        audit_events_total = self._safe_scalar("SELECT COUNT(*) FROM audit_logs", 0)

        recent_customers = self.customers(limit=8)

        return {
            "kpis": {
                "customers_total": int(customers_total),
                "active_plans_total": int(active_plans_total),
                "audit_events_total": int(audit_events_total),
            },
            "recent_customers": recent_customers,
        }

    def customers(self, limit: int = 50) -> list[dict]:
        query = text(
            """
            SELECT id, name, email, status, created_at
            FROM users
            WHERE realm = 'customer'
            ORDER BY id DESC
            LIMIT :limit
            """
        )

        rows = self._safe_rows(query, {"limit": max(1, min(limit, 200))})

        return [
            {
                "id": int(row.get("id") or 0),
                "name": str(row.get("name") or ""),
                "email": str(row.get("email") or ""),
                "status": str(row.get("status") or ""),
                "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
            }
            for row in rows
        ]

    def sales(self, limit: int = 80) -> list[dict]:
        service = PaymentService(self.db)
        rows = service.list_payments(limit=max(1, min(limit, 300)))

        return [
            {
                "id": row.get("id"),
                "reference": row.get("reference"),
                "customer_name": row.get("customer_name"),
                "coverage_month": row.get("coverage_month"),
                "amount": row.get("amount"),
                "status": row.get("status"),
            }
            for row in rows
        ]

    def _safe_scalar(self, sql: str, fallback: int) -> int:
        try:
            value = self.db.execute(text(sql)).scalar_one()
            return int(value or 0)
        except (SQLAlchemyError, ValueError, TypeError):
            return fallback

    def _safe_rows(self, query, params: dict | None = None) -> list[dict]:
        try:
            return [dict(item) for item in self.db.execute(query, params or {}).mappings().all()]
        except SQLAlchemyError:
            return []
