from unittest import mock
from unittest.mock import Mock
import pytest

import responses

from dapla.const import DaplaEnvironment, GUARDIAN_URLS
from dapla.guardian import GuardianClient

target_endpoint_url = "https://mock-target.no/get-data"
guardian_endpoint_url = "https://mock-guardian.no/access-token"
fake_auth_token = "123456789"


@responses.activate
def test_call_api() -> None:
    fake_keycloak_token = "keycloak_access_token"
    fake_maskinporten_token = "maskinporten_access_token"
    guardian_response = {"accessToken": fake_maskinporten_token}
    responses.add(
        responses.POST, guardian_endpoint_url, json=guardian_response, status=200
    )
    api_response = {"data": "some interesting data"}
    responses.add(responses.GET, target_endpoint_url, json=api_response, status=200)

    client = GuardianClient()
    response = client.call_api(
        target_endpoint_url, "fake_client_id", "dummy:scope", guardian_endpoint_url, fake_keycloak_token
    )

    assert response["data"] == "some interesting data"
    assert len(responses.calls) == 2


@responses.activate
def test_get_guardian_token() -> None:
    fake_maskinporten_token = "maskinporten_access_token"
    guardian_response = {"accessToken": fake_maskinporten_token}
    responses.add(
        responses.POST, guardian_endpoint_url, json=guardian_response, status=200
    )

    client = GuardianClient()
    body = {"maskinportenClientId": "fake_client_id", "scopes": "dummy:scope"}
    response = client.get_guardian_token(
        guardian_endpoint_url, "fake_auth_token", body=body
    )

    assert response == fake_maskinporten_token
    assert len(responses.calls) == 1


def test_get_guardian_url_empty_environment(monkeypatch):
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "")
    with pytest.raises(ValueError) as exc_info:
        GuardianClient.get_guardian_url()
    assert str(exc_info.value) == "'' is not a valid DaplaEnvironment"


def test_get_guardian_url_valid_environment(monkeypatch):
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "TEST")
    url = GuardianClient.get_guardian_url()
    assert url == GUARDIAN_URLS[DaplaEnvironment.TEST]


def test_get_guardian_url_missing_url_mapping(monkeypatch):
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "LOCAL")
    original_urls = GUARDIAN_URLS.copy()
    GUARDIAN_URLS.clear()
    try:
        with pytest.raises(ValueError) as exc_info:
            GuardianClient.get_guardian_url()
        assert str(exc_info.value) == "'LOCAL' is not a valid DaplaEnvironment"
    finally:
        GUARDIAN_URLS.update(original_urls)


def test_get_guardian_url_unknown_environment(monkeypatch):
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "UNKNOWN")
    with pytest.raises(ValueError) as exc_info:
        GuardianClient.get_guardian_url()
    assert str(exc_info.value) == "'UNKNOWN' is not a valid DaplaEnvironment"


def test_get_guardian_url_none_environment(monkeypatch):
    monkeypatch.delenv("DAPLA_ENVIRONMENT", raising=False)
    with pytest.raises(ValueError) as exc_info:
        GuardianClient.get_guardian_url()
    assert str(exc_info.value) == 'None is not a valid DaplaEnvironment'

def test_get_guardian_url_case_sensitive(monkeypatch):
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "test")
    with pytest.raises(ValueError) as exc_info:
        GuardianClient.get_guardian_url()
    assert str(exc_info.value) == "'test' is not a valid DaplaEnvironment"

def test_get_guardian_url_whitespace_environment(monkeypatch):
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "  TEST  ")
    with pytest.raises(ValueError) as exc_info:
        GuardianClient.get_guardian_url()
    assert str(exc_info.value) == "'  TEST  ' is not a valid DaplaEnvironment"

