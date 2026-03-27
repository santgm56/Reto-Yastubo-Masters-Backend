def test_frontend_bootstrap_contract_without_auth(client) -> None:
    response = client.get('/api/v1/frontend/bootstrap')

    assert response.status_code == 200
    payload = response.json()

    assert payload.get('ok') is True
    assert isinstance(payload.get('request_id'), str)
    assert payload.get('request_id', '').startswith('req_')

    data = payload.get('data') or {}
    runtime = data.get('runtimeConfig') or {}
    app_config = data.get('appConfig') or {}
    context = data.get('frontendContext') or {}

    assert runtime.get('apiCutoverEnabled') is True
    assert isinstance(runtime.get('abilities'), dict)
    assert app_config.get('locale') == 'es'
    assert context.get('role') == 'GUEST'
    assert context.get('channel') == 'web'


def test_frontend_bootstrap_ignores_invalid_bearer_token(client) -> None:
    response = client.get(
        '/api/v1/frontend/bootstrap',
        headers={'Authorization': 'Bearer invalid-token'},
    )

    assert response.status_code == 200
    payload = response.json()
    data = payload.get('data') or {}
    context = data.get('frontendContext') or {}
    runtime = data.get('runtimeConfig') or {}

    assert context.get('role') == 'GUEST'
    assert context.get('channel') == 'web'
    assert runtime.get('abilities') == {}
