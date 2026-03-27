from app.services.payment_service import PaymentService


def test_payments_index_contract_shape(client, monkeypatch):
    rows = [
        {
            'id': 1,
            'reference': 'PMR-1',
            'contract_reference': 'uuid-1',
            'customer_name': 'Ana Lopez',
            'coverage_month': '2026-03-01',
            'amount': 13.2,
            'status': 'PROCESSING',
            'method': 'Stripe',
            'sync_state': 'pending_webhook',
            'last_event_at': None,
            'events': [],
        }
    ]

    monkeypatch.setattr(PaymentService, 'list_payments', lambda self, limit=100: rows)

    response = client.get('/api/v1/payments')

    assert response.status_code == 200
    payload = response.json()
    assert payload.get('ok') is True
    assert payload.get('message') == 'Pagos obtenidos'
    assert payload.get('data', {}).get('rows') == rows
    assert payload.get('data', {}).get('total') == 1
    assert isinstance(payload.get('request_id'), str)
    assert payload['request_id'].startswith('req_')
