from jupyterhub.services.auth import HubAuth
import google.auth
from google.oauth2.credentials import Credentials
import os
import requests
from functools import partial
from IPython.display import display, HTML
from datetime import datetime


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
    def fetch_google_credentials():
        if AuthClient.is_ready():
            try:
                local_user = AuthClient.fetch_local_user()
                credentials = Credentials(
                    token=local_user['exchanged_tokens']['google']['access_token'],
                    expiry=datetime.fromtimestamp(local_user['exchanged_tokens']['google']['exp']),
                    token_uri="https://oauth2.googleapis.com/token",
                )
            except AuthError as err:
                err.print_warning()

            def _refresh(self, _request):
                try:
                    user = AuthClient.fetch_local_user()
                    self.token = user['exchanged_tokens']['google']['access_token']
                    self.expiry = datetime.fromtimestamp(user['exchanged_tokens']['google']['exp'])
                except AuthError as err:
                    err.print_warning()

            credentials.refresh = partial(_refresh, credentials)
            return credentials
        else:
            # Fetch credentials from Google Cloud SDK
            credentials, _ = google.auth.default()
            return credentials

    @staticmethod
    def is_ready():
        return 'LOCAL_USER_PATH' in os.environ


class AuthError(Exception):
    """This exception class is used when the communication with the custom auth handler fails.
    This is normally due to stale auth session."""

    def print_warning(self):
        display(HTML('Your session has timed out. Please <a href="/hub/login">log in</a> to continue.'))
