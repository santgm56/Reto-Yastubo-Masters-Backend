from app.services.audit_service import AuditService


def test_audit_response_contract_has_request_id(client, monkeypatch):
    expected_data = {
        'rows': [
            {
                'id': 10,
                'action': 'issuance.completed',
                'realm': 'admin',
                'actor_user_id': None,
                'target_user_id': None,
                'ip': None,
                'context_json': '{}',
                'created_at': '2026-03-26T10:00:00',
            }
        ],
        'pagination': {
            'current_page': 1,
            'last_page': 1,
            'per_page': 10,
            'total': 1,
        },
    }

    monkeypatch.setattr(AuditService, 'list_events', lambda self, page, per_page, action, realm: expected_data)

    response = client.get('/api/v1/admin/audit')

    assert response.status_code == 200
    payload = response.json()
    assert payload.get('ok') is True
    assert payload.get('message') == 'Auditoria obtenida'
    assert payload.get('data') == expected_data
    assert isinstance(payload.get('request_id'), str)
    assert payload['request_id'].startswith('req_')
