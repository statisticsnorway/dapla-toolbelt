import typing as t
from typing import Any
from typing import Optional

import requests

from .auth import AuthClient


class GuardianClient:
    """Client for interacting with the Maskinporten Guardian."""

    @staticmethod
    def call_api(
        api_endpoint_url: str,
        maskinporten_client_id: str,
        scopes: str,
        guardian_endpoint_url: str = "http://maskinporten-guardian.dapla.svc.cluster.local/maskinporten/access-token",
        keycloak_token: Optional[str] = None,
    ) -> Any:
        """Call an external API using Maskinporten Guardian.

        Args:
            api_endpoint_url: URL to the target API
            maskinporten_client_id: the Maskinporten client id
            scopes: the Maskinporten scopes
            guardian_endpoint_url: URL to the Maskinporten Guardian
            keycloak_token: the user's personal Keycloak token. Automatic fetch attempt will be made if left empty.

        Raises:
            RuntimeError: If the API call fails

        Returns:
            The endpoint json response
        """
        if keycloak_token is None:
            keycloak_token = AuthClient.fetch_personal_token()
        body = {"maskinportenClientId": maskinporten_client_id, "scopes": scopes}
        maskinporten_token = GuardianClient.get_guardian_token(
            guardian_endpoint_url, keycloak_token, body=body
        )
        api_response = requests.get(
            api_endpoint_url,
            headers={
                "Authorization": "Bearer %s" % maskinporten_token,
                "Accept": "application/json",
            },
        )
        if api_response.status_code == 200:
            return api_response.json()
        else:
            raise RuntimeError(
                f"Error calling target API ({api_endpoint_url}): Status code <{api_response.status_code}"
                f" - {api_response.text or api_response.reason}>"
            )

    @staticmethod
    def get_guardian_token(
        guardian_endpoint: str, keycloak_token: str, body: dict[str, str]
    ) -> str:
        """Retrieve access token from Maskinporten Guardian.

        Args:
            guardian_endpoint: URL to the maskinporten guardian
            keycloak_token: the user's Keycloak token
            body: maskinporten request body

        Raises:
            RuntimeError: If the Guardian token request fails

        Returns:
            The maskinporten access token
        """
        guardian_response = requests.post(
            guardian_endpoint,
            headers={
                "Authorization": "Bearer %s" % keycloak_token,
                "Content-type": "application/json",
            },
            data=body,
        )
        if guardian_response.status_code == 200:
            return t.cast(str, guardian_response.json()["accessToken"])
        else:
            raise RuntimeError(
                f"Error getting guardian token: <{guardian_response.status_code}: "
                f"{guardian_response.text or guardian_response.reason}>"
            )
