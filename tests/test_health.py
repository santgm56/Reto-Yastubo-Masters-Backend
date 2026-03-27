from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_health_endpoint_ok() -> None:
    response = client.get('/health')

    assert response.status_code == 200
    payload = response.json()
    assert payload.get('ok') == 'true'
    assert isinstance(payload.get('service'), str)
    assert payload.get('service')
