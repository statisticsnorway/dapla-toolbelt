import json
from datetime import datetime
from datetime import timedelta
from unittest import mock
from unittest.mock import Mock

import pytest
import responses
from google.oauth2.credentials import Credentials

import dapla
from dapla.auth import AuthClient
from dapla.auth import AuthError

auth_endpoint_url = "https://mock-auth.no/user"


@mock.patch.dict(
    "dapla.auth.os.environ", {"LOCAL_USER_PATH": auth_endpoint_url}, clear=True
)
@responses.activate
def test_fetch_personal_token() -> None:
    mock_response = {
        "access_token": "fake_access_token",
    }
    responses.add(responses.GET, auth_endpoint_url, json=mock_response, status=200)

    client = AuthClient()
    token = client.fetch_personal_token()

    assert token == "fake_access_token"
    assert len(responses.calls) == 1


@mock.patch.dict(
    "dapla.auth.os.environ", {"LOCAL_USER_PATH": auth_endpoint_url}, clear=True
)
@mock.patch("dapla.auth.display")
@responses.activate
def test_fetch_personal_token_error(mock_display: Mock) -> None:
    mock_response = {
        "message": "There was an error",
    }
    responses.add(responses.GET, auth_endpoint_url, json=mock_response, status=404)
    with pytest.raises(AuthError):
        client = AuthClient()
        client.fetch_personal_token()
    # Assert that an error was displayed
    mock_display.assert_called_once()


@mock.patch.dict(
    "dapla.auth.os.environ",
    {"OIDC_TOKEN_EXCHANGE_URL": auth_endpoint_url, "OIDC_TOKEN": "dummy_token"},
    clear=True,
)
@mock.patch("dapla.auth.display")
@responses.activate
def test_fetch_google_token_exchange_error(mock_display: Mock) -> None:
    mock_response = Mock()

    mock_data = {"error_description": "Invalid token"}
    mock_json = json.dumps(mock_data)
    mock_response.data = mock_json
    mock_response.status = 404

    mock_google_request = Mock()
    mock_google_request.return_value = mock_response

    with mock.patch.object(
        dapla.auth.GoogleAuthRequest,  # type: ignore [attr-defined]
        "__call__",
        mock_response,
    ):
        with pytest.raises(AuthError):
            client = AuthClient()
            client.fetch_google_token_from_oidc_exchange(mock_google_request, [])


@mock.patch.dict(
    "dapla.auth.os.environ",
    {"LOCAL_USER_PATH": auth_endpoint_url},
    clear=True,
)
@responses.activate
def test_fetch_google_token_bip_jupyter() -> None:
    mock_response = {
        "access_token": "fake_access_token",
        "exchanged_tokens": {
            "google": {
                "access_token": "google_token",
                "exp": round((datetime.now() + timedelta(hours=1)).timestamp()),
            }
        },
    }
    responses.add(responses.GET, auth_endpoint_url, json=mock_response, status=200)

    client = AuthClient()
    token, _ = client.fetch_google_token(from_jupyterhub=True)

    assert token == "google_token"
    assert len(responses.calls) == 1


@mock.patch.dict(
    "dapla.auth.os.environ",
    {"OIDC_TOKEN_EXCHANGE_URL": auth_endpoint_url, "OIDC_TOKEN": "fake_access_token"},
    clear=True,
)
@responses.activate
def test_fetch_google_token_from_exchange_dapla_lab() -> None:
    mock_response = Mock()
    mock_response.data = json.dumps(
        {
            "access_token": "google_token",
            "expires_in": round((datetime.now() + timedelta(hours=1)).timestamp()),
        }
    )
    mock_response.status = 200
    mock_google_request = Mock()
    mock_google_request.return_value = mock_response
    with mock.patch.object(
        dapla.auth.GoogleAuthRequest,  # type: ignore [attr-defined]
        "__call__",
        mock_response,
    ):
        client = AuthClient()
        token, _expiry = client.fetch_google_token_from_oidc_exchange(
            mock_google_request, []
        )

        assert token == "google_token"


@mock.patch("dapla.auth.AuthClient.fetch_google_token_from_oidc_exchange")
@mock.patch.dict(
    "dapla.auth.os.environ",
    {"OIDC_TOKEN": "fake-token"},
    clear=True,
)
@mock.patch.dict(
    "dapla.auth.os.environ",
    {"OIDC_TOKEN_EXCHANGE_URL": "fake-endpoint"},
    clear=True,
)
@responses.activate
def test_fetch_google_credentials_from_oidc_exchange(
    fetch_google_token_from_oidc_exchange_mock: Mock,
) -> None:
    fetch_google_token_from_oidc_exchange_mock.return_value = (
        "google_token",
        datetime.now() + timedelta(hours=1),
    )

    client = AuthClient()
    credentials = client.fetch_google_credentials(force_token_exchange=True)
    credentials.refresh(None)

    assert credentials.token == "google_token"
    assert not credentials.expired


@mock.patch("dapla.auth.AuthClient.fetch_google_token_from_oidc_exchange")
@mock.patch.dict("dapla.auth.os.environ", {"OIDC_TOKEN": "fake-token"}, clear=True)
@mock.patch.dict(
    "dapla.auth.os.environ",
    {"OIDC_TOKEN_EXCHANGE_URL": "fake-endpoint"},
    clear=True,
)
@responses.activate
def test_fetch_google_credentials_expired(
    fetch_google_token_from_oidc_exchange_mock: Mock,
) -> None:
    fetch_google_token_from_oidc_exchange_mock.return_value = (
        "google_token",
        datetime.now() - timedelta(hours=1),
    )

    client = AuthClient()
    credentials = client.fetch_google_credentials(force_token_exchange=True)

    fetch_google_token_from_oidc_exchange_mock.return_value = (
        "google_token",
        datetime.now() + timedelta(hours=1),
    )

    credentials.refresh(None)
    assert not credentials.expired


def test_credentials_object_refresh_exists() -> None:
    # We test whether the "refresh" method exists,
    # since it might be removed in a future release and we are overriding the method.
    credentials = Credentials("fake-token")
    assert hasattr(credentials, "refresh")
