from google.cloud import storage
from gcsfs.retry import HttpError
from dapla.auth import AuthClient
from dapla.gcs import GCSFileSystem
from google.oauth2.credentials import Credentials
import logging
import jwt


logger = logging.getLogger(__name__)


class Doctor:
    """Class of functions that perform checks on Dapla. Checks whether user is authenticated,
    if the keycloak-token is valid and if user has access to GCS. Each method can be run 
    individually or collectively with the 'health' method.
    """
    @staticmethod
    def jupyterhub_auth_valid():
        """Checks wheter user is logged in and authenticated to Jupyterhub.
        """
        print("Checking authentication to JupyterHub...")
        try:
            # Attempt fetching the Jupyterhub user
            AuthClient.fetch_local_user()
        except: 
            return False
        return True


    @staticmethod
    def keycloak_token_valid():
        """Checks whether the keycloak token is valid by attempting to access a keycloak-token protected service
        """
        algorithms = [
            "HS256",
            "HS384",
            "HS512",
            "RS256",
            "RS384",
            "RS512",
            "ES256",
            "ES384",
            "ES512",
            "none",
        ]

        keycloak_token = AuthClient.fetch_personal_token()

        claims = jwt.decode(keycloak_token, algorithms=algorithms verify=False)

        
        


    @staticmethod
    def gcs_credentials_valid():
        """Checks whether the users google cloud storage token is valid by accessing a GCS service.
        """
        # Fetch the google token
        google_token = AuthClient.fetch_google_token()

        # Create the credentials object
        credentials = Credentials(token=google_token,token_uri="https://oauth2.googleapis.com/token")

        try:
            # Attempt to access a google-token protected service
            file = GCSFileSystem(token=credentials)
            file.ls("stat-poc-2-source-data") # Should be changed to a common storage bucket
                
        except HttpError as ex:
            if str(ex) == "Invalid Credentials, 401":
                return False
            else:
                logger.exception(ex)
        
        return True

    
    @staticmethod
    def bucket_access():
        """Checks whether user has access to a common google bucket.
        """
        # Fetch google credentials and create client object
        client = storage.Client(credentials=AuthClient.fetch_google_credentials())

        # The bucket to be accessed
        bucket_name = 'stat-poc-2-source-data'

        try:
            # Attempt to access the bucket
            client.get_bucket(bucket_name)
        except:
            return False
        else:
            return True



    @staticmethod
    def health():
        
        print("Performing checks...")
        if not jupyterhub_auth_valid():
            print("You are not logged in or authenticated to JupyterHub")
            exit(1)
        if not check_keycloak_valid():
            print()


    if __name__ == "__main__":
        health()