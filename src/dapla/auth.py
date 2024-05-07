import json
import os
import typing as t
from collections.abc import Sequence
from datetime import datetime
from datetime import timedelta
from functools import lru_cache
from typing import Any
from typing import Optional

import google.auth
import requests
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2.credentials import Credentials
from IPython.display import HTML
from IPython.display import display
from jupyterhub.services.auth import HubAuth

# Refresh window was modified in: https://github.com/googleapis/google-auth-library-python/commit/c6af1d692b43833baca978948376739547cf685a
# The change was directed towards high latency environments, and should not apply to us.
# Since we can't force a refresh, the threshold is lowered to keep us from waiting ~4 minutes for a new token.
# A permanent fix would be to supply credentials with a refresh endpoint
# that allways returns a token that is valid for more than 3m 45s.
google.auth._helpers.REFRESH_THRESHOLD = timedelta(seconds=20)


class AuthClient:
    """Client for retrieving authentication information."""

    @staticmethod
    def fetch_google_token_from_oidc_exchange(
        request: GoogleAuthRequest, _scopes: Sequence[str]
    ) -> tuple[str, datetime]:
        """Fetches the Google token by exchanging an OIDC token.

        Args:
            request: The GoogleAuthRequest object.
            _scopes: The scopes to request.

        Raises:
            AuthError: If the request to the OIDC token exchange endpoint fails.

        Returns:
            A tuple of (google-token, expiry).
        """
        response = request.__call__(
            url=os.environ["OIDC_TOKEN_EXCHANGE_URL"],
            method="POST",
            body={
                "subject_token": os.environ["OIDC_TOKEN"],
                "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "requested_issuer": "google",
                "client_id": "onyxia",
            },
        )
        if response.status == 200:
            auth_data = json.loads(response.data)
            expiry = datetime.utcnow() + timedelta(seconds=auth_data["expires_in"])
            return auth_data["access_token"], expiry
        else:
            raise AuthError

    @staticmethod
    def fetch_google_token_from_jupyter() -> str:
        """Fetches the personal access token for the current user.

        Raises:
            AuthError: If the token exchange request to JupyterHub fails.

        Returns:
            The personal access token.
        """
        try:
            google_token = AuthClient.fetch_local_user_from_jupyter()[
                "exchanged_tokens"
            ]["google"]["access_token"]
            return t.cast(str, google_token)
        except AuthError as err:
            err._print_warning()
            raise err

    @staticmethod
    def fetch_local_user_from_jupyter() -> dict[str, Any]:
        """Retrieves user information, most notably access tokens for use in authentication.

        Raises:
            AuthError: If the request to the user endpoint fails.

        Returns:
            The user data from the token .
        """
        # Helps getting the correct ssl configs
        hub = HubAuth()
        response = requests.get(
            os.environ["LOCAL_USER_PATH"],
            headers={"Authorization": "token %s" % hub.api_token},
            cert=(hub.certfile, hub.keyfile),
            verify=hub.client_ca,
            allow_redirects=False,
        )
        if response.status_code == 200:
            user_data = response.json()
            return t.cast(dict[str, Any], user_data)
        else:
            raise AuthError

    @staticmethod
    def fetch_google_credentials() -> Credentials:
        """Fetches the Google credentials for the current user.

        Returns:
            The Google "Credentials" object.
        """
        if AuthClient.is_ready():
            try:

                def _refresh_handler(
                    request: google.auth.transport.Request, scopes: Sequence[str]
                ) -> tuple[str, datetime]:
                    # We manually override the refresh_handler method with our custom logic for fetching tokens.
                    # Previously, we directly overrode the `refresh` method. However, this
                    # approach led to deadlock issues in gcsfs/credentials.py's maybe_refresh method.
                    return AuthClient.fetch_google_token()

                token, expiry = AuthClient.fetch_google_token()
                credentials = Credentials(
                    token=token,
                    expiry=expiry,
                    token_uri="https://oauth2.googleapis.com/token",
                    refresh_handler=_refresh_handler,
                )
            except AuthError as err:
                err._print_warning()

            return credentials
        else:
            # Fetch credentials from Google Cloud SDK
            credentials, _ = google.auth.default()
            return credentials

    @staticmethod
    def fetch_personal_token() -> str:
        """Fetches the personal access token for the current user."""
        try:
            personal_token = AuthClient.fetch_local_user_from_jupyter()["access_token"]
            return t.cast(str, personal_token)
        except AuthError as err:
            err._print_warning()
            raise err

    @staticmethod
    @lru_cache(maxsize=1)
    def fetch_email_from_credentials() -> Optional[str]:
        """Retrieves an e-mail based on current Google Credentials. Potentially makes a Google API call."""
        if AuthClient.is_ready():
            credentials = AuthClient.fetch_google_credentials()
            response = requests.get(
                url=f"https://oauth2.googleapis.com/tokeninfo?access_token={credentials.token}"
            )

            return response.json().get("email") if response.status_code == 200 else None

        else:
            user_info = AuthClient.fetch_local_user_from_jupyter()
            return user_info.get("username")

    @staticmethod
    def fetch_google_token(
        request: Optional[GoogleAuthRequest] = None,
        scopes: Optional[Sequence[str]] = None,
    ) -> tuple[str, datetime]:
        """Fetches the Google token for the current user.

        Scopes in the argument is ignored, but are kept for compatibility
        with the Credentials refresh handler method signature.

        Args:
            request: The GoogleAuthRequest object.
            scopes: The scopes to request.

        Raises:
            AuthError: If the token exchange request to JupyterHub fails.

        Returns:
            The Google token.
        """
        try:
            if AuthClient._is_oidc_token():
                if request is None:
                    request = GoogleAuthRequest()
                if scopes is None:
                    scopes = []

                google_token, expiry = AuthClient.fetch_google_token_from_oidc_exchange(
                    request, scopes
                )
            else:
                user_info = AuthClient.fetch_local_user_from_jupyter()
                google_token = user_info["exchanged_tokens"]["google"]["access_token"]
                expiry = datetime.utcfromtimestamp(
                    user_info["exchanged_tokens"]["google"]["exp"]
                )

        except AuthError as err:
            err._print_warning()
            raise err

        return google_token, expiry

    @staticmethod
    def is_ready() -> bool:
        """Checks whether the authentication handler can be used."""
        return "LOCAL_USER_PATH" in os.environ or "OIDC_TOKEN" in os.environ

    @staticmethod
    def _is_oidc_token() -> bool:
        return "OIDC_TOKEN" in os.environ


class AuthError(Exception):
    """This exception class is used when the communication with the custom auth handler fails.

    This is normally due to stale auth session.
    """

    def _print_warning(self) -> None:
        (
            display(
                HTML(
                    'Your session has timed out. Please <a href="/hub/login">log in</a> to continue.'
                )  # type: ignore [no-untyped-call]
            )
        )
