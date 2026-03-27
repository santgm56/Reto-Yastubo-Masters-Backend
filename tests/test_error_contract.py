from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_validation_error_contract_shape() -> None:
    # Missing required field `monthly_record_id` for webhook endpoint.
    response = client.post('/api/v1/payments/webhook', params={'outcome': 'success'})

    assert response.status_code == 422
    payload = response.json()

    assert payload.get('code') == 'API_VALIDATION_ERROR'
    assert isinstance(payload.get('message'), str)
    assert isinstance(payload.get('errors'), dict)
    assert isinstance(payload.get('details'), dict)
    assert isinstance(payload.get('request_id'), str)
    assert payload.get('request_id').startswith('req_')
