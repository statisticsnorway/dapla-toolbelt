import mock
import responses

from dapla.guardian import GuardianClient

target_endpoint_url = 'https://mock-target.no/get-data'
guardian_endpoint_url = 'https://mock-guardian.no/access-token'
fake_auth_token = '123456789'


@mock.patch('dapla.guardian.AuthClient')
@responses.activate
def test_call_api(auth_client_mock):
    auth_client_mock.fetch_personal_token.return_value = fake_auth_token
    fake_maskinporten_token = 'maskinporten_access_token'
    guardian_response = {
        'accessToken': fake_maskinporten_token
    }
    responses.add(responses.POST, guardian_endpoint_url, json=guardian_response, status=200)
    api_response = {
        'data': 'some interesting data'
    }
    responses.add(responses.GET, target_endpoint_url, json=api_response, status=200)

    client = GuardianClient()
    response = client.call_api(target_endpoint_url, 'fake_client_id', 'dummy:scope', guardian_endpoint_url)

    assert response['data'] == 'some interesting data'
    assert len(responses.calls) == 2


@responses.activate
def test_get_guardian_token():
    fake_maskinporten_token = 'maskinporten_access_token'
    guardian_response = {
        'accessToken': fake_maskinporten_token
    }
    responses.add(responses.POST, guardian_endpoint_url, json=guardian_response, status=200)

    client = GuardianClient()
    body = {
        'maskinportenClientId': 'fake_client_id',
        'scopes': 'dummy:scope'
    }
    response = client.get_guardian_token(guardian_endpoint_url, 'fake_auth_token', body=body)

    assert response == fake_maskinporten_token
    assert len(responses.calls) == 1
