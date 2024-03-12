import datetime
import logging
import os

import google.cloud.storage as storage
import jwt
import requests
from gcsfs.retry import HttpError

from dapla.auth import AuthClient

logger = logging.getLogger(__name__)


class Doctor:
    """Class of functions that perform checks on Dapla.

    Checks whether user is authenticated, if the keycloak-token is valid and if user has access to GCS.
    Each method can be run individually or collectively with the 'health' method.
    """

    @staticmethod
    def jupyterhub_auth_valid() -> bool:
        """Checks wheter user is logged in and authenticated to Jupyterhub."""
        print("Checking authentication to JupyterHub...")
        try:
            # Attempt fetching the Jupyterhub user
            AuthClient.fetch_local_user_from_jupyter()
        except Exception:
            return False
        return True

    @staticmethod
    def keycloak_token_valid() -> bool:
        """Checks whether the keycloak token is valid by attempting to access a keycloak-token protected service."""
        print("Checking that your keycloak token is valid...")
        algorithms = ["RS256"]

        keycloak_token = AuthClient.fetch_personal_token()

        claims = jwt.decode(
            keycloak_token, algorithms=algorithms, options={"verify_signature": False}
        )

        if not Doctor._is_token_expired(claims):
            return True
        else:
            return False

    @staticmethod
    def _is_token_expired(token: dict[str, str]) -> bool:
        try:
            # get the expiry time from the payload
            expiry_time = token["exp"]

            # convert the expiry time to a datetime object
            expiry_timestamp = datetime.datetime.fromtimestamp(float(expiry_time))

            # if the current time is greater than the expiry time, the token is expired
            return datetime.datetime.now() > expiry_timestamp
        except KeyError:
            # if the token does not have an expiry time (exp claim), consider it not expired
            return False

    @staticmethod
    def gcs_credentials_valid() -> bool:
        """Checks whether the users google cloud storage token is valid by accessing a GCS service."""
        print("Checking your Google Cloud Storage credentials...")

        # Fetch the google token
        google_token, _ = AuthClient.fetch_google_credentials().token

        try:
            requests.get(
                "https://oauth2.googleapis.com/tokeninfo?access_token=%s" % google_token
            )
        except HttpError as ex:
            if str(ex) == "Invalid Credentials, 401":
                return False
            else:
                logger.exception(ex)

        return True

    @staticmethod
    def bucket_access() -> bool:
        """Checks whether user has access to a common google bucket."""
        print(
            "Checking that you have access to a common Google Cloud Storage bucket..."
        )

        # Fetch google credentials and create client object
        client = storage.Client(credentials=AuthClient.fetch_google_credentials())

        # Set the bucket that is to be accessed
        if os.environ["CLUSTER_ID"] == "staging-bip-app":
            bucket = "ssb-staging-dapla-felles-data-delt"
        else:
            bucket = "ssb-prod-dapla-felles-data-delt"

        try:
            # Attempt to access the bucket
            client.get_bucket(bucket)
        except Exception:
            return False
        else:
            return True

    @classmethod
    def health(cls) -> None:
        """Runs a series of checks to determine the health of Dapla setup."""
        print("Performing checks...")
        if not cls.jupyterhub_auth_valid():
            print("You are either not logged in or not authenticated to JupyterHub.")
            exit(1)
        if not cls.keycloak_token_valid():
            print("Your keycloak token seems to be expired.")
            exit(1)
        if not cls.gcs_credentials_valid():
            print(
                "Your GCS credentials seem to be invalid. Dapla Doctor could not fetch GCS bucket."
            )
            exit(1)
        if not cls.bucket_access():
            print("You do not seem to have access to a common GCS bucket.")
            exit(1)
        print("Everything seems to be in order.")
