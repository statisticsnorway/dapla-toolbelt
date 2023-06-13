import mock
import responses
from datetime import datetime, timedelta, timezone

from dapla.auth import AuthClient

auth_endpoint_url = 'https://mock-auth.no/user'


@mock.patch.dict('dapla.auth.os.environ', {'LOCAL_USER_PATH': auth_endpoint_url}, clear=True)
def test_is_ready():
    client = AuthClient()
    assert client.is_ready()


@mock.patch.dict('dapla.auth.os.environ', {'LOCAL_USER_PATH': auth_endpoint_url}, clear=True)
@responses.activate
def test_fetch_personal_token():
    mock_response = {
        'access_token': 'fake_access_token',
    }
    responses.add(responses.GET, auth_endpoint_url, json=mock_response, status=200)

    client = AuthClient()
    response = client.fetch_personal_token()

    assert response == 'fake_access_token'
    assert len(responses.calls) == 1


@mock.patch.dict('dapla.auth.os.environ', {'LOCAL_USER_PATH': auth_endpoint_url}, clear=True)
@mock.patch('dapla.auth.display')
@responses.activate
def test_fetch_personal_token_error(mock_display):
    mock_response = {
        'message': 'There was an error',
    }
    responses.add(responses.GET, auth_endpoint_url, json=mock_response, status=404)

    client = AuthClient()
    client.fetch_personal_token()
    # Assert that an error was displayed
    mock_display.assert_called_once()


@mock.patch.dict('dapla.auth.os.environ', {'LOCAL_USER_PATH': auth_endpoint_url}, clear=True)
@mock.patch('dapla.auth.display')
@responses.activate
def test_fetch_google_token_error(mock_display):
    mock_display
    mock_response = {
        'message': 'There was an error',
    }
    responses.add(responses.GET, auth_endpoint_url, json=mock_response, status=404)

    client = AuthClient()
    client.fetch_google_token()
    # Assert that an error was displayed
    mock_display.assert_called_once()


@mock.patch.dict('dapla.auth.os.environ', {'LOCAL_USER_PATH': auth_endpoint_url}, clear=True)
@responses.activate
def test_fetch_google_token():
    mock_response = {
        'access_token': 'fake_access_token',
        'exchanged_tokens': {
            'google': {
                'access_token': 'google_token',
                'exp': round((datetime.now() + timedelta(hours=1)).timestamp())
            }
        }
    }
    responses.add(responses.GET, auth_endpoint_url, json=mock_response, status=200)

    client = AuthClient()
    response = client.fetch_google_token()

    assert response == 'google_token'
    assert len(responses.calls) == 1


@mock.patch.dict('dapla.auth.os.environ', {'LOCAL_USER_PATH': auth_endpoint_url}, clear=True)
@responses.activate
def test_fetch_google_credentials():
    mock_response = {
        'access_token': 'fake_access_token',
        'exchanged_tokens': {
            'google': {
                'access_token': 'google_token',
                'exp': round((datetime.now() + timedelta(hours=1)).timestamp())
            }
        }
    }
    responses.add(responses.GET, auth_endpoint_url, json=mock_response, status=200)

    client = AuthClient()
    response = client.fetch_google_credentials()
    response.refresh(None)

    assert response.token == 'google_token'
    assert len(responses.calls) == 2
    assert not response.expired

@mock.patch.dict('dapla.auth.os.environ', {'LOCAL_USER_PATH': auth_endpoint_url}, clear=True)
@responses.activate
def test_fetch_google_credentials_expired():
    mock_response = {
        'access_token': 'fake_access_token',
        'exchanged_tokens': {
            'google': {
                'access_token': 'google_token',
                'exp': round((datetime.now() - timedelta(hours=1)).timestamp())
            }
        }
    }
    responses.add(responses.GET, auth_endpoint_url, json=mock_response, status=200)

    # Add 2 hours to expiry timestamp for refresh response
    mock_response['exchanged_tokens']['google']['exp'] += 7200
    responses.add(responses.GET, auth_endpoint_url, json=mock_response, status=200)

    client = AuthClient()
    response = client.fetch_google_credentials()

    assert response.expired

    response.refresh(None)
    assert not response.expired

