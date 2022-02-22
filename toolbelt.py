import json
import requests
import pandas as pd
from google.oauth2.credentials import Credentials
from auth import AuthClient


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
