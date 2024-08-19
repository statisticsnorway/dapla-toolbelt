import json
import logging
import os
import typing as t
from collections.abc import Sequence
from datetime import datetime
from datetime import timedelta
from enum import Enum
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

logger = logging.getLogger(__name__)

# Refresh window was modified in: https://github.com/googleapis/google-auth-library-python/commit/c6af1d692b43833baca978948376739547cf685a
# The change was directed towards high latency environments, and should not apply to us.
# Since we can't force a refresh, the threshold is lowered to keep us from waiting ~4 minutes for a new token.
# A permanent fix would be to supply credentials with a refresh endpoint
# that allways returns a token that is valid for more than 3m 45s.
google.auth._helpers.REFRESH_THRESHOLD = timedelta(seconds=20)


class DaplaEnvironment(Enum):
    """Represents the 'DAPLA_ENVIRONMENT' environment variable."""

    DEV = "DEV"
    STAGING = "STAGING"
    TEST = "TEST"
    PROD = "PROD"


class DaplaService(Enum):
    """Represents the 'DAPLA_SERVICE' environment variable."""

    JUPYTERLAB = "JUPYTERLAB"
    VS_CODE = "VS_CODE"
    R_STUDIO = "R_STUDIO"
    CLOUD_RUN = "CLOUD_RUN"


class DaplaRegion(Enum):
    """Represents the 'DAPLA_REGION' environment variable."""

    ON_PREM = "ON_PREM"
    DAPLA_LAB = "DAPLA_LAB"
    BIP = "BIP"
    CLOUD_RUN = "CLOUD_RUN"


class AuthClient:
    """Client for retrieving authentication information."""

    @staticmethod
    def _get_current_dapla_metadata() -> (
        tuple[Optional[DaplaEnvironment], Optional[DaplaService], Optional[DaplaRegion]]
    ):
        try:
            env = DaplaEnvironment(os.getenv("DAPLA_ENVIRONMENT"))
        except ValueError:
            env = None

        try:
            service = DaplaService(os.getenv("DAPLA_SERVICE"))
        except ValueError:
            service = None

        try:
            region = DaplaRegion(os.getenv("DAPLA_REGION"))
        except ValueError:
            region = None

        return env, service, region

    @staticmethod
    def _refresh_handler(
        request: google.auth.transport.Request, scopes: Sequence[str]
    ) -> tuple[str, datetime]:
        # We manually override the refresh_handler method with our custom logic for fetching tokens.
        # Previously, we directly overrode the `refresh` method. However, this
        # approach led to deadlock issues in gcsfs/credentials.py's maybe_refresh method.
        return AuthClient.fetch_google_token()

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
        if os.getenv("OIDC_TOKEN_EXCHANGE_URL") is None:
            raise AuthError(
                "env variable  'OIDC_TOKEN_EXCHANGE_URL' was not found when"
                "attempting token exchange with OIDC endpoint"
            )
        response = request.__call__(
            url=os.environ["OIDC_TOKEN_EXCHANGE_URL"],
            method="POST",
            body={
                "subject_token": os.environ["OIDC_TOKEN"],
                "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "requested_issuer": "google",
                "client_id": "onyxia-api",
            },
        )
        if response.status == 200:
            auth_data = json.loads(response.data)
            expiry = datetime.utcnow() + timedelta(seconds=auth_data["expires_in"])
            return auth_data["access_token"], expiry
        else:
            error = json.loads(response.data)
            print("Error: ", error["error_description"])
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
        if os.getenv("LOCAL_USER_PATH") is None:
            raise AuthError(
                "env variable LOCAL_USER_PATH was not found when"
                "attempting token exchange with JupyterLab"
            )

        # Helps getting the correct ssl configs
        hub = HubAuth()
        response = requests.get(
            os.environ["LOCAL_USER_PATH"],
            headers={"Authorization": f"token {hub.api_token}"},
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
    def fetch_google_token(
        request: Optional[GoogleAuthRequest] = None,
        scopes: Optional[Sequence[str]] = None,
        from_jupyterhub: bool = False,
    ) -> tuple[str, datetime]:
        """Fetches the Google token for the current user.

        Scopes in the argument is ignored, but are kept for compatibility
        with the Credentials refresh handler method signature.

        Args:
            request: The GoogleAuthRequest object.
            scopes: The scopes to request.
            from_jupyterhub: Whether the google token should be exchanged from JupyterHub.
                if false, exchange from an OIDC endpoint decided by OIDC_TOKEN_EXCHANGE_URL.

        Raises:
            AuthError: If the token exchange.

        Returns:
            The Google token.
        """
        try:
            if from_jupyterhub is True:
                user_info = AuthClient.fetch_local_user_from_jupyter()
                google_token = user_info["exchanged_tokens"]["google"]["access_token"]
                expiry = datetime.utcfromtimestamp(
                    user_info["exchanged_tokens"]["google"]["exp"]
                )

            else:
                if request is None:
                    request = GoogleAuthRequest()
                if scopes is None:
                    scopes = []

                google_token, expiry = AuthClient.fetch_google_token_from_oidc_exchange(
                    request, scopes
                )

        except AuthError as err:
            err._print_warning()
            raise err

        return google_token, expiry

    @staticmethod
    def fetch_google_credentials(force_token_exchange: bool = False) -> Credentials:
        """Fetches the Google credentials for the current user.

        Args:
            force_token_exchange: Forces authentication by token exchange.

        Raises:
            AuthError: If fails to fetch credentials.

        Returns:
            The Google "Credentials" object.
        """
        env, service, region = AuthClient._get_current_dapla_metadata()
        force_token_exchange = (
            os.getenv("DAPLA_TOOLBELT_FORCE_TOKEN_EXCHANGE") == "1"
            or force_token_exchange
        )
        try:
            match (env, service, region):
                case (_, _, _) if force_token_exchange is True:
                    logger.debug("Auth - Forced token exchange")
                    token, expiry = AuthClient.fetch_google_token()
                    credentials = Credentials(
                        token=token,
                        expiry=expiry,
                        token_uri="https://oauth2.googleapis.com/token",
                        refresh_handler=AuthClient._refresh_handler,
                    )
                case (_, DaplaService.CLOUD_RUN, _):
                    logger.debug("Auth - Cloud Run detected, using ADC")
                    credentials, _ = google.auth.default()
                case (
                    _,
                    DaplaService.JUPYTERLAB,
                    DaplaRegion.ON_PREM | DaplaRegion.BIP,
                ):
                    logger.debug("Auth - JupyterLab detected, using token exchange")
                    token, expiry = AuthClient.fetch_google_token(from_jupyterhub=True)
                    credentials = Credentials(
                        token=token,
                        expiry=expiry,
                        token_uri="https://oauth2.googleapis.com/token",
                        refresh_handler=AuthClient._refresh_handler,
                    )
                case (_, _, DaplaRegion.DAPLA_LAB):
                    logger.debug("Auth - Dapla Lab detected, attempting to use ADC")
                    adc_env = os.getenv("DAPLA_GROUP_CONTEXT")
                    if adc_env is None:
                        raise AuthError(
                            "Dapla Group selection feature is not enabled. "
                            "This is necessary in order to access buckets in Dapla Lab. "
                            "The feature needs to be enabled *before* starting the service, "
                            "and can be done in the 'Buckets' configuration tab"
                        )
                    logger.debug(
                        "Auth - 'DAPLA_GROUP_CONTEXT' env variable is set, "
                        f"using ADC as group {adc_env}"
                    )
                    credentials, _ = google.auth.default()
                case (_, _, _):
                    logger.debug("Auth - Default authentication used (ADC)")
                    credentials, _ = google.auth.default()
        except AuthError as err:
            err._print_warning()
            raise err

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
        credentials = AuthClient.fetch_google_credentials()
        response = requests.get(
            url=f"https://oauth2.googleapis.com/tokeninfo?access_token={credentials.token}"
        )

        return response.json().get("email") if response.status_code == 200 else None


class AuthError(Exception):
    """This exception class is used when the communication with the custom auth handler fails.

    This is normally due to stale auth session.
    """

    def _print_warning(self) -> None:
        (
            display(
                HTML(
                    'Your session has timed out. Please <a href="/hub/login">log in</a> to continue.'
                )
            )
        )
