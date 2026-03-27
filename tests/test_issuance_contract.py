from app.services.issuance_service import IssuanceService


def test_issuance_quote_contract_shape(client, monkeypatch):
    quote_data = {
        'quote_id': 'signed_token',
        'eligible': True,
        'pricing': {
            'base_price': 12.0,
            'surcharge_percent': 10.0,
            'surcharge_amount': 1.2,
            'total_price': 13.2,
        },
        'reasons': [],
    }

    monkeypatch.setattr(IssuanceService, 'quote', lambda self, payload: quote_data)

    response = client.post('/api/v1/issuances/quote', json={
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

    assert response.status_code == 200
    payload = response.json()
    assert payload.get('ok') is True
    assert payload.get('message') == 'Cotizacion generada'
    assert payload.get('data') == quote_data
    assert isinstance(payload.get('request_id'), str)
    assert payload['request_id'].startswith('req_')


def test_issuance_send_email_contract_shape(client, monkeypatch):
    monkeypatch.setattr(IssuanceService, 'show', lambda self, contract_id: {'issuance_id': 'uuid-123'})

    response = client.post('/api/v1/issuances/101/send-email', json={'email': 'demo@example.com'})

    assert response.status_code == 200
    payload = response.json()
    assert payload.get('ok') is True
    assert payload.get('message') == 'Email encolado'
    assert payload.get('data', {}).get('status') == 'EMAIL_QUEUED'
    assert payload.get('data', {}).get('recipient') == 'demo@example.com'
    assert isinstance(payload.get('request_id'), str)
    assert payload['request_id'].startswith('req_')
