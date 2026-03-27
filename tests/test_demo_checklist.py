from app.services.payment_service import PaymentService
from app.services.cancellation_service import CancellationService


def test_swagger_docs_available(client):
    response = client.get('/docs')

    assert response.status_code == 200
    assert 'Swagger UI' in response.text


def test_openapi_contains_core_paths(client):
    response = client.get('/openapi.json')

    assert response.status_code == 200
    payload = response.json()
    paths = payload.get('paths', {})

    required_paths = {
        '/health',
        '/api/v1/issuances/quote',
        '/api/v1/payments',
        '/api/v1/payments/webhooks/stripe',
        '/api/v1/cancellations',
        '/api/v1/admin/audit',
        '/api/customer/portal/modules',
    }

    assert required_paths.issubset(set(paths.keys()))


def test_webhook_idempotent_contract_shape(client, monkeypatch):
    def fake_register(self, monthly_record_id, outcome, event_id=''):
        return {
            'payment_reference': f'PMR-{monthly_record_id}',
            'status': 'PAID',
            'sync_state': 'synchronized',
            'event_id': event_id,
            'idempotent': True,
        }

    monkeypatch.setattr(PaymentService, 'register_webhook_event', fake_register)

    response = client.post(
        '/api/v1/payments/webhooks/stripe',
        json={'monthly_record_id': 3201, 'outcome': 'success', 'event_id': 'evt_dup_1'},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload.get('ok') is True
    assert payload.get('message') == 'Webhook duplicado ignorado'
    assert payload.get('data', {}).get('idempotent') is True
    assert payload.get('data', {}).get('event_id') == 'evt_dup_1'
    assert isinstance(payload.get('request_id'), str)
    assert payload['request_id'].startswith('req_')


def test_cancellation_registered_and_visible(client, monkeypatch):
    shared_rows = []

    def fake_request(self, contract_id, reason, actor_id=''):
        row = {
            'contract_id': contract_id,
            'issuance_id': f'iss-{contract_id}',
            'customer_name': 'Cliente Demo',
            'entry_date': '2026-03-26',
            'status': 'CANCELED',
        }
        shared_rows.insert(0, row)
        return {
            'contract_id': contract_id,
            'issuance_id': row['issuance_id'],
            'status': 'CANCELED',
            'reason': reason,
            'already_canceled': False,
        }

    def fake_list(self, limit=120):
        return shared_rows[:limit]

    monkeypatch.setattr(CancellationService, 'request_cancellation', fake_request)
    monkeypatch.setattr(CancellationService, 'list_rows', fake_list)

    response_store = client.post(
        '/api/v1/cancellations',
        json={'contract_id': 1001, 'reason': 'Solicitud de demo'},
        headers={'x-frontend-user-id': 'admin-demo'},
    )
    assert response_store.status_code == 200
    payload_store = response_store.json()
    assert payload_store.get('ok') is True
    assert payload_store.get('message') == 'Anulacion solicitada'
    assert payload_store.get('data', {}).get('contract_id') == 1001
    assert payload_store.get('data', {}).get('status') == 'CANCELED'

    response_index = client.get('/api/v1/cancellations')
    assert response_index.status_code == 200
    payload_index = response_index.json()
    assert payload_index.get('ok') is True
    assert payload_index.get('data', {}).get('total') == 1
    assert payload_index.get('data', {}).get('rows', [])[0]['status'] == 'CANCELED'


def test_customer_portal_real_api_flow(client, monkeypatch):
    user_header = {'x-frontend-user-id': 'customer-e2e'}

    modules = client.get('/api/customer/portal/modules', headers=user_header)
    assert modules.status_code == 200
    assert modules.json().get('ok') is True

    beneficiaries_before = client.get('/api/customer/beneficiaries', headers=user_header)
    assert beneficiaries_before.status_code == 200
    before_total = beneficiaries_before.json().get('data', {}).get('total', 0)

    create_beneficiary = client.post(
        '/api/customer/beneficiaries',
        headers=user_header,
        json={
            'nombre': 'Beneficiario Demo',
            'documento': 'DOC-DEMO-9001',
            'parentesco': 'Hermano',
            'estado': 'activo',
        },
    )
    assert create_beneficiary.status_code == 200
    assert create_beneficiary.json().get('ok') is True

    beneficiaries_after = client.get('/api/customer/beneficiaries', headers=user_header)
    assert beneficiaries_after.status_code == 200
    after_total = beneficiaries_after.json().get('data', {}).get('total', 0)
    assert after_total == before_total + 1

    death_report = client.post(
        '/api/customer/death-report',
        headers=user_header,
        json={
            'nombreReportante': 'Juan Demo',
            'documentoReportante': '12345678',
            'nombreFallecido': 'Carlos Demo',
            'documentoFallecido': '87654321',
            'fechaFallecimiento': '2026-03-20',
            'observacion': 'Reporte tecnico de prueba para validacion funcional.',
            'canalContacto': 'email',
        },
    )
    assert death_report.status_code == 200
    assert death_report.json().get('data', {}).get('confirmation', {}).get('estadoCaso') == 'RECIBIDO'

    payment_method = client.post(
        '/api/customer/payment-method',
        headers=user_header,
        json={'reference': 'CARD-5555444433332222', 'brand': 'visa'},
    )
    assert payment_method.status_code == 200
    assert payment_method.json().get('data', {}).get('payment_method', {}).get('status') == 'ACTIVE'

    monkeypatch.setattr(
        PaymentService,
        'customer_history',
        lambda self: [
            {
                'payment_reference': 'PMR-3201',
                'method': 'Stripe',
                'date': '2026-03-01',
                'status': 'PAID',
                'amount': 13.2,
            }
        ],
    )
    monkeypatch.setattr(
        PaymentService,
        'customer_status',
        lambda self: {
            'paymentStatus': 'PAID',
            'syncState': 'synchronized',
            'paymentReference': 'PMR-3201',
            'lastEventAt': None,
        },
    )

    history = client.get('/api/customer/payment-history')
    assert history.status_code == 200
    assert history.json().get('data', {}).get('total') == 1

    status = client.get('/api/customer/payments/status')
    assert status.status_code == 200
    assert status.json().get('data', {}).get('paymentStatus') == 'PAID'
