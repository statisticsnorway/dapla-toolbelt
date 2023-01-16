import mock
import responses
from datetime import datetime, timedelta

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
                'exp': round((datetime.today() + timedelta(hours=1)).timestamp())
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
                'exp': round((datetime.today() + timedelta(hours=1)).timestamp())
            }
        }
    }
    responses.add(responses.GET, auth_endpoint_url, json=mock_response, status=200)

    client = AuthClient()
    response = client.fetch_google_credentials()
    response.refresh(None)

    assert response.token == 'google_token'
    assert not response.expired()
    assert len(responses.calls) == 2
