from __future__ import annotations

import json
from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session


PAYMENT_ACTIONS = {
    "PROCESSING": {"payment.checkout.started", "payment.retry.started", "payment.subscribe.started"},
    "PAID": {"payment.webhook.processed", "payment.webhook.succeeded", "payment.checkout.succeeded"},
    "FAILED": {"payment.webhook.failed", "payment.checkout.failed"},
}


class PaymentService:
    def __init__(self, db: Session):
        self.db = db

    def list_payments(self, limit: int = 100) -> list[dict]:
        rows = self.db.execute(
            text(
                """
                SELECT mr.id, mr.full_name, mr.coverage_month, mr.price_final, c.uuid AS contract_uuid
                FROM capitados_monthly_records mr
                LEFT JOIN capitados_contracts c ON c.id = mr.contract_id
                ORDER BY mr.coverage_month DESC, mr.id DESC
                LIMIT :limit
                """
            ),
            {"limit": max(1, min(limit, 300))},
        ).mappings().all()

        if not rows:
            return []

        record_ids = [int(row["id"]) for row in rows]
        events_map = self._load_payment_events(record_ids)

        result: list[dict] = []
        today_month = date.today().replace(day=1)

        for row in rows:
            record_id = int(row["id"])
            events = events_map.get(record_id, [])
            last_event = events[0] if events else None
            status = self._resolve_status(row, last_event, today_month)

            result.append(
                {
                    "id": record_id,
                    "reference": f"PMR-{record_id}",
                    "contract_reference": row["contract_uuid"],
                    "customer_name": row["full_name"],
                    "coverage_month": row["coverage_month"].isoformat() if row["coverage_month"] else None,
                    "amount": float(row["price_final"] or 0),
                    "status": status,
                    "method": self._resolve_method(last_event),
                    "sync_state": "pending_webhook" if status == "PROCESSING" else "synchronized",
                    "last_event_at": last_event.get("created_at") if last_event else None,
                    "events": events[:5],
                }
            )

        return result

    def register_payment_event(self, monthly_record_id: int, action: str, context: dict | None = None) -> dict:
        context = context or {}
        event_context = {
            **context,
            "monthly_record_id": monthly_record_id,
            "payment_reference": f"PMR-{monthly_record_id}",
        }

        self.db.execute(
            text(
                """
                INSERT INTO audit_logs
                (actor_user_id, target_user_id, realm, action, context_json, ip, user_agent, created_at)
                VALUES (NULL, NULL, 'admin', :action, :context_json, NULL, 'fastapi-migration', NOW())
                """
            ),
            {
                "action": action,
                "context_json": json.dumps(event_context, ensure_ascii=False, separators=(",", ":")),
            },
        )
        self.db.commit()

        status = self._status_from_action(action)
        return {
            "payment_reference": f"PMR-{monthly_record_id}",
            "status": status,
            "sync_state": "synchronized" if action in {"payment.webhook.processed", "payment.webhook.succeeded", "payment.webhook.failed"} else "pending_webhook",
        }

    def register_webhook_event(self, monthly_record_id: int, outcome: str, event_id: str = "") -> dict:
        event_id = (event_id or "").strip()

        if event_id and self._is_duplicate_webhook_event(monthly_record_id, event_id):
            current = next((row for row in self.list_payments(limit=300) if row["id"] == monthly_record_id), None)
            fallback_status = self._status_from_webhook_event_id(event_id)
            return {
                "payment_reference": f"PMR-{monthly_record_id}",
                "status": current["status"] if current else fallback_status,
                "sync_state": current["sync_state"] if current else "synchronized",
                "event_id": event_id,
                "idempotent": True,
            }

        action = "payment.webhook.succeeded" if outcome == "success" else "payment.webhook.failed"
        result = self.register_payment_event(monthly_record_id, action, {"channel": "stripe", "event_id": event_id})
        return {**result, "event_id": event_id, "idempotent": False}

    def customer_history(self) -> list[dict]:
        rows = self.list_payments(limit=80)
        return [
            {
                "payment_reference": row["reference"],
                "method": row["method"],
                "date": row["coverage_month"],
                "status": row["status"],
                "amount": row["amount"],
            }
            for row in rows
        ]

    def customer_status(self) -> dict:
        rows = self.list_payments(limit=20)
        if not rows:
            return {
                "paymentStatus": "NO_RECONOCIDO",
                "syncState": "unknown",
                "paymentReference": "",
                "lastEventAt": None,
            }

        latest = rows[0]
        return {
            "paymentStatus": latest.get("status", "NO_RECONOCIDO"),
            "syncState": latest.get("sync_state", "unknown"),
            "paymentReference": latest.get("reference", ""),
            "lastEventAt": latest.get("last_event_at"),
        }

    def _load_payment_events(self, record_ids: list[int]) -> dict[int, list[dict]]:
        if not record_ids:
            return {}

        events = self.db.execute(
            text(
                """
                SELECT id, action, context_json, created_at
                FROM audit_logs
                WHERE action IN (
                    'payment.checkout.started',
                    'payment.subscribe.started',
                    'payment.retry.started',
                    'payment.webhook.processed',
                    'payment.webhook.succeeded',
                    'payment.webhook.failed',
                    'payment.checkout.succeeded',
                    'payment.checkout.failed'
                )
                ORDER BY id DESC
                LIMIT 5000
                """
            )
        ).mappings().all()

        grouped: dict[int, list[dict]] = {}
        target_ids = set(record_ids)

        for event in events:
            try:
                ctx = event["context_json"]
                context = json.loads(ctx) if isinstance(ctx, str) else (ctx or {})
            except (json.JSONDecodeError, TypeError):
                context = {}

            record_id = int(context.get("monthly_record_id") or 0)
            if record_id not in target_ids:
                continue

            grouped.setdefault(record_id, []).append(
                {
                    "id": int(event["id"]),
                    "action": event["action"],
                    "context": context,
                    "created_at": event["created_at"].isoformat() if event["created_at"] else None,
                }
            )

        return grouped

    def _status_from_action(self, action: str) -> str:
        if action in PAYMENT_ACTIONS["PROCESSING"]:
            return "PROCESSING"
        if action in PAYMENT_ACTIONS["PAID"]:
            return "PAID"
        if action in PAYMENT_ACTIONS["FAILED"]:
            return "FAILED"
        return "NO_RECONOCIDO"

    def _resolve_status(self, row: dict, last_event: dict | None, today_month: date) -> str:
        if last_event:
            return self._status_from_action(str(last_event.get("action") or ""))

        coverage_month = row["coverage_month"]
        if not coverage_month:
            return "NO_RECONOCIDO"

        month_norm = coverage_month.replace(day=1)
        if month_norm < today_month:
            return "PAST_DUE"

        return "PROCESSING"

    def _resolve_method(self, last_event: dict | None) -> str:
        if not last_event:
            return "Pendiente"

        channel = str(last_event.get("context", {}).get("channel") or "").lower().strip()
        if channel == "stripe":
            return "Stripe"
        if channel == "manual":
            return "Cobro manual"
        return "Pendiente"

    def _is_duplicate_webhook_event(self, monthly_record_id: int, event_id: str) -> bool:
        if not event_id:
            return False

        event_pattern = f"%{event_id}%"

        row = self.db.execute(
            text(
                """
                SELECT 1
                FROM audit_logs
                WHERE action IN ('payment.webhook.succeeded', 'payment.webhook.failed', 'payment.webhook.processed')
                  AND context_json LIKE :event_pattern
                LIMIT 1
                """
            ),
            {"event_pattern": event_pattern},
        ).mappings().first()

        return bool(row)

    def _status_from_webhook_event_id(self, event_id: str) -> str:
        row = self.db.execute(
            text(
                """
                SELECT action
                FROM audit_logs
                WHERE action IN ('payment.webhook.succeeded', 'payment.webhook.failed', 'payment.webhook.processed')
                  AND context_json LIKE :event_pattern
                ORDER BY id DESC
                LIMIT 1
                """
            ),
            {"event_pattern": f"%{event_id}%"},
        ).mappings().first()

        if not row:
            return "NO_RECONOCIDO"

        action = str(row.get("action") or "")
        if action == "payment.webhook.succeeded":
            return "PAID"
        if action == "payment.webhook.failed":
            return "FAILED"
        return "PROCESSING"
