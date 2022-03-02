import json
import requests

from .auth import AuthClient


class GuardianClient:
    @staticmethod
    def call_api(api_endpoint_url, maskinporten_client_id, scopes, guardian_endpoint_url="https://guardian.dapla.ssb.no/maskinporten/access-token", keycloak_token=""):
        """
        Call an external API using the maskinporten guardian
        :param api_endpoint_url: URL to the target API
        :param maskinporten_client_id: the Maskinporten client id
        :param scopes: the Maskinporten scopes
        :param guardian_endpoint_url: URL to the Maskinporten Guardian
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
        maskinporten_token = GuardianClient.get_guardian_token(guardian_endpoint_url, keycloak_token, body)
        api_response = requests.get(api_endpoint_url,
                                    headers={
                                        'Authorization': 'Bearer %s' % maskinporten_token,
                                        'Accept': 'application/json'
                                    })
        if api_response.status_code == 200:
            return api_response.json()
        else:
            raise RuntimeError(f'Error calling target endpoint: <{api_response.status_code}: {api_response.text or api_response.reason}>')

    @staticmethod
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

