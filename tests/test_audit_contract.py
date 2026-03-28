from app.services.audit_service import AuditService


def test_audit_response_contract_has_request_id(client, monkeypatch):
    expected_data = {
        'rows': [
            {
                'id': 10,
                'action': 'issuance.completed',
                'realm': 'admin',
                'actor_user_id': None,
                'actor_name': None,
                'actor_email': None,
                'target_user_id': None,
                'ip': None,
                'context_json': '{}',
                'created_at': '2026-03-26 10:00:00',
            }
        ],
        'pagination': {
            'current_page': 1,
            'last_page': 1,
            'per_page': 10,
            'total': 1,
        },
    }

    monkeypatch.setattr(
        AuditService,
        'list_events',
        lambda self, page, per_page, action, realm, actor_user_id, from_date, to_date: expected_data,
    )

    response = client.get('/api/v1/admin/audit')

    assert response.status_code == 200
    payload = response.json()
    assert payload.get('data') == expected_data
    assert isinstance(payload.get('request_id'), str)


def test_audit_forwards_laravel_equivalent_filters(client, monkeypatch):
    calls = {}

    def fake_list_events(self, page, per_page, action, realm, actor_user_id, from_date, to_date):
        calls['page'] = page
        calls['per_page'] = per_page
        calls['action'] = action
        calls['realm'] = realm
        calls['actor_user_id'] = actor_user_id
        calls['from_date'] = from_date
        calls['to_date'] = to_date
        return {
            'rows': [],
            'pagination': {
                'current_page': page,
                'last_page': 1,
                'per_page': per_page,
                'total': 0,
            },
        }

    monkeypatch.setattr(AuditService, 'list_events', fake_list_events)

    response = client.get(
        '/api/v1/admin/audit',
        params={
            'page': 2,
            'per_page': 25,
            'action': 'issuance',
            'realm': 'admin',
            'actor_user_id': 77,
            'from': '2026-03-01',
            'to': '2026-03-27',
        },
    )

    assert response.status_code == 200
    assert calls == {
        'page': 2,
        'per_page': 25,
        'action': 'issuance',
        'realm': 'admin',
        'actor_user_id': 77,
        'from_date': '2026-03-01',
        'to_date': '2026-03-27',
    }
