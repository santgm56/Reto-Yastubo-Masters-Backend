from app.services.seller_dashboard_service import SellerDashboardService


def test_seller_dashboard_summary_contract_shape(client, monkeypatch):
    monkeypatch.setattr(
        SellerDashboardService,
        "summary",
        lambda self: {
            "kpis": {
                "customers_total": 3,
                "active_plans_total": 2,
                "audit_events_total": 8,
            },
            "recent_customers": [{"id": 10, "name": "Ana", "email": "ana@test.com", "status": "ACTIVE", "created_at": None}],
        },
    )

    response = client.get("/api/v1/seller/dashboard-summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload.get("ok") is True
    assert payload.get("message") == "Dashboard seller obtenido"
    assert payload.get("data", {}).get("kpis", {}).get("customers_total") == 3
    assert isinstance(payload.get("request_id"), str)
    assert payload["request_id"].startswith("req_")


def test_seller_customers_contract_shape(client, monkeypatch):
    rows = [{"id": 11, "name": "Cliente", "email": "c@test.com", "status": "ACTIVE", "created_at": None}]
    monkeypatch.setattr(SellerDashboardService, "customers", lambda self, limit=50: rows)

    response = client.get("/api/v1/seller/customers")

    assert response.status_code == 200
    payload = response.json()
    assert payload.get("ok") is True
    assert payload.get("message") == "Clientes seller obtenidos"
    assert payload.get("data", {}).get("rows") == rows
    assert payload.get("data", {}).get("total") == 1


def test_seller_sales_contract_shape(client, monkeypatch):
    rows = [{"id": 12, "reference": "PMR-12", "customer_name": "Cliente", "coverage_month": "2026-03-01", "amount": 25.0, "status": "PAID"}]
    monkeypatch.setattr(SellerDashboardService, "sales", lambda self, limit=80: rows)

    response = client.get("/api/v1/seller/sales")

    assert response.status_code == 200
    payload = response.json()
    assert payload.get("ok") is True
    assert payload.get("message") == "Ventas seller obtenidas"
    assert payload.get("data", {}).get("rows") == rows
    assert payload.get("data", {}).get("total") == 1
