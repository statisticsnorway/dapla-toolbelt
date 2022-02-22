import json
import os
import requests
import pandas as pd
from jupyterhub.services.auth import HubAuth
from google.oauth2.credentials import Credentials


def call_endpoint(api_endpoint, maskinporten_client_id, scopes, guardian_endpoint="https://guardian.dapla.ssb.no/maskinporten/access-token", keycloak_token=""):
    """
    Call an external API using the maskinporten guardian
    :param api_endpoint: URL to the target API
    :param maskinporten_client_id: the Maskinporten client id
    :param scopes: the Maskinporten scopes
    :param guardian_endpoint: URL to the Maskinporten Guardian
    :param keycloak_token: the user's personal Keycloak token. Automatic fetch attempt will be made if left empty.
    :return: the endpoint json response
    """
    if keycloak_token == "":
        keycloak_token = AuthClient.fetch_personal_token()
    body = {
        "maskinportenClientId": maskinporten_client_id,
        "scopes": scopes  # Array
    }
    body = json.dumps(body)
    maskinporten_token = get_guardian_token(guardian_endpoint, keycloak_token, body)
    api_response = requests.get(api_endpoint,
                                headers={
                                    'Authorization': 'Bearer %s' % maskinporten_token,
                                    'Accept': 'application/json'
                                })
    if api_response.status_code == 200:
        return api_response.json()
    else:
        raise RuntimeError(f'Error calling target endpoint: <{api_response.status_code}: {api_response.text or api_response.reason}>')


def get_guardian_token(guardian_endpoint, keycloak_token, body):
    """
    Retrieve access token from makinporten guardian
    :param guardian_endpoint: URL to the maskinporten guardian
    :param keycloak_token: the user's Keycloak token
    :param body: maskinporten request body
    :return: the maskinporten access token
    """
    guardian_response = requests.post(guardian_endpoint,
                                      headers={
                                          'Authorization': 'Bearer %s' % keycloak_token,
                                          'Content-type': 'application/json'
                                      }, data=body)
    if guardian_response.status_code == 200:
        return guardian_response.json()['accessToken']
    else:
        raise RuntimeError(f'Error getting guardian token: <{guardian_response.status_code}: {guardian_response.text or guardian_response.reason}>')


def get_gcs_file_system():
    """
    Return a pythonic file-system for Google Cloud Storage - initialized with a personal Google Identity token.
    See https://gcsfs.readthedocs.io/en/latest for usage
    """
    import gcsfs
    token = get_gcs_credentials()
    return gcsfs.GCSFileSystem(token=token)


def get_gcs_credentials():
    return Credentials(
        token=AuthClient.fetch_google_token(),
        token_uri="https://oauth2.googleapis.com/token",
    )


def gcs_ls(bucket_name):
    fs = get_gcs_file_system()
    return fs.ls(bucket_name)


def pandas_read_csv(path):
    df = pd.read_csv(f"gcs://{path}", storage_options={"token": get_gcs_credentials()})
    return df


def pandas_read_json(path):
    df = pd.read_json(f"gcs://{path}", storage_options={"token": get_gcs_credentials()})
    return df


def pandas_read_xml(path):
    df = pd.read_xml(f"gcs://{path}", storage_options={"token": get_gcs_credentials()})
    return df


class AuthClient:
    """
    A client class that connects to the AuthHandler to retrieve user and auth state info
    """
    @staticmethod
    def fetch_local_user():
        # Helps getting the correct ssl configs
        hub = HubAuth()
        response = requests.get(os.environ['LOCAL_USER_PATH'],
                                headers={
                                    'Authorization': 'token %s' % hub.api_token
                                }, cert=(hub.certfile, hub.keyfile), verify=hub.client_ca, allow_redirects=False)
        if response.status_code == 200:
            return response.json()
        else:
            raise AuthError

    @staticmethod
    def fetch_personal_token():
        try:
            return AuthClient.fetch_local_user()['access_token']
        except AuthError as err:
            err.print_warning()

    @staticmethod
    def fetch_google_token():
        try:
            return AuthClient.fetch_local_user()['exchanged_tokens']['google']['access_token']
        except AuthError as err:
            err.print_warning()

    @staticmethod
    def is_ready():
        return 'LOCAL_USER_PATH' in os.environ


class AuthError(Exception):
    """This exception class is used when the communication with the custom auth handler fails.
    This is normally due to stale auth session."""

    def print_warning(self):
        from IPython.core.display import display, HTML
        display(HTML('Your session has timed out. Please <a href="/hub/login">log in</a> to continue.'))

