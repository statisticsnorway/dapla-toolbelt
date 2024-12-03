import pytest
import requests
import responses

from dapla import GuardianClient
from dapla.const import GUARDIAN_URLS
from dapla.const import DaplaEnvironment


@responses.activate
def test_call_api_with_failed_api_response(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "TEST")
    fake_keycloak_token = "keycloak_access_token"
    fake_maskinporten_token = "maskinporten_access_token"
    guardian_response = {"accessToken": fake_maskinporten_token}
    responses.add(
        responses.POST,
        "https://guardian.intern.test.ssb.no/maskinporten/access-token",
        json=guardian_response,
        status=200,
    )
    responses.add(
        responses.GET,
        "https://target-endpoint-url.com",
        json={"error": "Internal Server Error"},
        status=500,
    )

    client = GuardianClient()
    with pytest.raises(RuntimeError) as exc_info:
        client.call_api(
            "https://target-endpoint-url.com",
            "fake_client_id",
            "dummy:scope",
            fake_keycloak_token,
        )
    assert "Error calling target API" in str(exc_info.value)
    assert "500" in str(exc_info.value)


@responses.activate
def test_get_guardian_token_failed_response() -> None:
    responses.add(
        responses.POST,
        "https://guardian-endpoint-url.com",
        json={"error": "Invalid token"},
        status=401,
    )

    client = GuardianClient()
    body = {"maskinportenClientId": "fake_client_id", "scopes": "dummy:scope"}
    with pytest.raises(RuntimeError) as exc_info:
        client.get_guardian_token(
            "https://guardian-endpoint-url.com", "invalid_token", body=body
        )
    assert "Error getting guardian token" in str(exc_info.value)
    assert "401" in str(exc_info.value)


@responses.activate
def test_call_api_without_keycloak_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "TEST")
    fake_auto_token = "auto_fetched_token"
    fake_maskinporten_token = "maskinporten_access_token"

    def mock_fetch_personal_token():
        return fake_auto_token

    monkeypatch.setattr(
        "dapla.AuthClient.fetch_personal_token", mock_fetch_personal_token
    )

    guardian_response = {"accessToken": fake_maskinporten_token}
    responses.add(
        responses.POST,
        "https://guardian.intern.test.ssb.no/maskinporten/access-token",
        json=guardian_response,
        status=200,
    )
    api_response = {"data": "auto token response"}
    responses.add(
        responses.GET, "https://target-endpoint-url.com", json=api_response, status=200
    )

    client = GuardianClient()
    response = client.call_api(
        "https://target-endpoint-url.com",
        "fake_client_id",
        "dummy:scope",
    )
    assert response["data"] == "auto token response"
    assert len(responses.calls) == 2


@responses.activate
def test_get_guardian_token_empty_response() -> None:
    responses.add(
        responses.POST,
        "https://guardian-endpoint-url.com",
        json={},
        status=200,
    )

    client = GuardianClient()
    body = {"maskinportenClientId": "fake_client_id", "scopes": "dummy:scope"}
    with pytest.raises(KeyError):
        client.get_guardian_token(
            "https://guardian-endpoint-url.com", "fake_token", body=body
        )


@responses.activate
def test_call_api_with_network_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "TEST")
    fake_maskinporten_token = "maskinporten_access_token"
    guardian_response = {"accessToken": fake_maskinporten_token}
    responses.add(
        responses.POST,
        "https://guardian.intern.test.ssb.no/maskinporten/access-token",
        json=guardian_response,
        status=200,
    )
    responses.add(
        responses.GET,
        "https://target-endpoint-url.com",
        body=requests.exceptions.ConnectionError(),
    )

    client = GuardianClient()
    with pytest.raises(requests.exceptions.ConnectionError):
        client.call_api(
            "https://target-endpoint-url.com",
            "fake_client_id",
            "dummy:scope",
            "fake_token",
        )


def test_get_guardian_url_invalid_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that an error is raised when the DAPLA_ENVIRONMENT environment variable is not set."""
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "")
    with pytest.raises(ValueError) as exc_info:
        GuardianClient.get_guardian_url()
    assert str(exc_info.value) == "'' is not a valid DaplaEnvironment"


def test_get_guardian_url_valid_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "TEST")
    url = GuardianClient.get_guardian_url()
    assert url == GUARDIAN_URLS[DaplaEnvironment.TEST]


def test_get_guardian_url_missing_url_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "LOCAL")
    original_urls = GUARDIAN_URLS.copy()
    GUARDIAN_URLS.clear()
    try:
        with pytest.raises(ValueError) as exc_info:
            GuardianClient.get_guardian_url()
        assert str(exc_info.value) == "'LOCAL' is not a valid DaplaEnvironment"
    finally:
        GUARDIAN_URLS.update(original_urls)


def test_get_guardian_url_unknown_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "UNKNOWN")
    with pytest.raises(ValueError) as exc_info:
        GuardianClient.get_guardian_url()
    assert str(exc_info.value) == "'UNKNOWN' is not a valid DaplaEnvironment"


def test_get_guardian_url_none_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DAPLA_ENVIRONMENT", raising=False)
    with pytest.raises(ValueError) as exc_info:
        GuardianClient.get_guardian_url()
    assert str(exc_info.value) == "None is not a valid DaplaEnvironment"


def test_get_guardian_url_case_sensitive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "test")
    with pytest.raises(ValueError) as exc_info:
        GuardianClient.get_guardian_url()
    assert str(exc_info.value) == "'test' is not a valid DaplaEnvironment"


def test_get_guardian_url_whitespace_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "  TEST  ")
    with pytest.raises(ValueError) as exc_info:
        GuardianClient.get_guardian_url()
    assert str(exc_info.value) == "'  TEST  ' is not a valid DaplaEnvironment"


def test_get_guardian_url_with_special_chars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "TEST\n")
    with pytest.raises(ValueError) as exc_info:
        GuardianClient.get_guardian_url()
    assert str(exc_info.value) == "'TEST\\n' is not a valid DaplaEnvironment"


def test_get_guardian_url_numeric_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "123")
    with pytest.raises(ValueError) as exc_info:
        GuardianClient.get_guardian_url()
    assert str(exc_info.value) == "'123' is not a valid DaplaEnvironment"


def test_get_guardian_url_mixed_case(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "TeSt")
    with pytest.raises(ValueError) as exc_info:
        GuardianClient.get_guardian_url()
    assert str(exc_info.value) == "'TeSt' is not a valid DaplaEnvironment"


def test_get_guardian_url_unicode_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "TÉST")
    with pytest.raises(ValueError) as exc_info:
        GuardianClient.get_guardian_url()
    assert str(exc_info.value) == "'TÉST' is not a valid DaplaEnvironment"


def test_get_guardian_url_empty_dict(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "TEST")
    original_urls = GUARDIAN_URLS.copy()
    GUARDIAN_URLS.clear()
    try:
        with pytest.raises(ValueError) as exc_info:
            GuardianClient.get_guardian_url()
        assert str(exc_info.value) == "Unknown environment: DaplaEnvironment.TEST"
    finally:
        GUARDIAN_URLS.update(original_urls)
    assert str(exc_info.value) == "Unknown environment: DaplaEnvironment.TEST"
