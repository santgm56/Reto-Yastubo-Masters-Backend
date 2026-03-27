from sqlalchemy import text
from sqlalchemy.orm import Session


class AuditService:
    def __init__(self, db: Session):
        self.db = db

    def list_events(
        self,
        page: int = 1,
        per_page: int = 10,
        action: str | None = None,
        realm: str | None = None,
    ) -> dict:
        page = max(1, page)
        per_page = max(1, min(per_page, 100))
        offset = (page - 1) * per_page

        where_parts = []
        params: dict[str, object] = {}

        if action:
            where_parts.append("action LIKE :action")
            params["action"] = f"%{action.strip()}%"

        if realm in {"admin", "customer"}:
            where_parts.append("realm = :realm")
            params["realm"] = realm

        where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

        total_query = text(f"SELECT COUNT(*) AS c FROM audit_logs {where_sql}")
        total_row = self.db.execute(total_query, params).mappings().first()
        total = int(total_row["c"] if total_row else 0)

        rows_query = text(
            f"""
            SELECT id, action, realm, actor_user_id, target_user_id, ip, context_json, created_at
            FROM audit_logs
            {where_sql}
            ORDER BY id DESC
            LIMIT :limit OFFSET :offset
            """
        )
        rows_params = {**params, "limit": per_page, "offset": offset}
        rows = self.db.execute(rows_query, rows_params).mappings().all()

        last_page = max(1, (total + per_page - 1) // per_page)

        return {
            "rows": [
                {
                    "id": int(row["id"]),
                    "action": row["action"],
                    "realm": row["realm"],
                    "actor_user_id": row["actor_user_id"],
                    "target_user_id": row["target_user_id"],
                    "ip": row["ip"],
                    "context_json": row["context_json"],
                    "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                }
                for row in rows
            ],
            "pagination": {
                "current_page": page,
                "last_page": last_page,
                "per_page": per_page,
                "total": total,
            },
        }
