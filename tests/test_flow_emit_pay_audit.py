from app.services.audit_service import AuditService
from app.services.issuance_service import IssuanceService
from app.services.payment_service import PaymentService


def test_flow_emit_pay_audit_contract_evidence(client, monkeypatch):
    monkeypatch.setattr(
        IssuanceService,
        'quote',
        lambda self, payload: {
            'quote_id': 'signed_token_flow',
            'eligible': True,
            'pricing': {
                'base_price': 12.0,
                'surcharge_percent': 10.0,
                'surcharge_amount': 1.2,
                'total_price': 13.2,
            },
            'reasons': [],
        },
    )

    monkeypatch.setattr(
        IssuanceService,
        'store',
        lambda self, payload: {
            'issuance_id': 'uuid-flow-1',
            'contract_id': 1001,
            'monthly_record_id': 3201,
            'status': 'PENDING_PAYMENT',
            'amount': 13.2,
        },
    )

    monkeypatch.setattr(
        PaymentService,
        'register_payment_event',
        lambda self, monthly_record_id, action, context=None: {
            'payment_reference': f'PMR-{monthly_record_id}',
            'status': 'PROCESSING',
            'sync_state': 'pending_webhook',
        },
    )

    monkeypatch.setattr(
        AuditService,
        'list_events',
        lambda self, page, per_page, action, realm: {
            'rows': [
                {
                    'id': 1,
                    'action': 'issuance.completed',
                    'realm': 'admin',
                    'actor_user_id': None,
                    'target_user_id': None,
                    'ip': None,
                    'context_json': '{"contract_id":1001}',
                    'created_at': '2026-03-26T14:00:00',
                },
                {
                    'id': 2,
                    'action': 'payment.checkout.started',
                    'realm': 'admin',
                    'actor_user_id': None,
                    'target_user_id': None,
                    'ip': None,
                    'context_json': '{"monthly_record_id":3201}',
                    'created_at': '2026-03-26T14:01:00',
                },
            ],
            'pagination': {
                'current_page': 1,
                'last_page': 1,
                'per_page': 10,
                'total': 2,
            },
        },
    )

    quote_response = client.post('/api/v1/issuances/quote', json={
        'plan_version_id': 1,
        'customer': {
            'document_number': '12345678',
            'full_name': 'Ana Lopez',
            'age': 37,
            'sex': 'F',
            'residence_country_id': 57,
            'repatriation_country_id': 170,
        },
    })
    assert quote_response.status_code == 200

    issuance_response = client.post('/api/v1/issuances', json={
        'quote_id': 'signed_token_flow',
        'start_date': '2026-03-26',
    })
    assert issuance_response.status_code == 200

    payment_response = client.post('/api/v1/payments/3201/checkout')
    assert payment_response.status_code == 200

    audit_response = client.get('/api/v1/admin/audit')
    assert audit_response.status_code == 200

    quote_payload = quote_response.json()
    issuance_payload = issuance_response.json()
    payment_payload = payment_response.json()
    audit_payload = audit_response.json()

    assert quote_payload.get('data', {}).get('eligible') is True
    assert issuance_payload.get('data', {}).get('status') == 'PENDING_PAYMENT'
    assert payment_payload.get('data', {}).get('status') == 'PROCESSING'
    assert audit_payload.get('data', {}).get('pagination', {}).get('total') == 2
