from google.cloud import storage
from gcsfs.retry import HttpError
from dapla.auth import AuthClient
from dapla.gcs import GCSFileSystem
from google.oauth2.credentials import Credentials
import logging
import jwt
import datetime


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
    def keycloak_token_valid(cls):
        """Checks whether the keycloak token is valid by attempting to access a keycloak-token protected service
        """
        algorithms = ["RS256"]

        keycloak_token = AuthClient.fetch_personal_token()

        claims = jwt.decode(keycloak_token, algorithms=algorithms, options={"verify_signature": False})

        if not cls._is_token_expired(claims):
            return True
        else:
            return False
  

    @staticmethod
    def _is_token_expired(token):
        try:
            # get the expiry time from the payload
            expiry_time = token['exp']

            # convert the expiry time to a datetime object
            expiry_time = datetime.datetime.fromtimestamp(expiry_time)

            # if the current time is greater than the expiry time, the token is expired
            return datetime.datetime.now() > expiry_time
        except KeyError:
            # if the token does not have an expiry time (exp claim), consider it not expired
            return False
        

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
    def health(cls):
        
        print("Performing checks...")
        if not cls.jupyterhub_auth_valid():
            print("You are not logged in or authenticated to JupyterHub")
            exit(1)
        if not cls.check_keycloak_valid():
            print("abc")
            exit(1)


    if __name__ == "__main__":
        health()