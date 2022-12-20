
import mock
from dapla.doctor import bucket_access, gcs_credentials_valid
from dapla.auth import AuthClient


from unittest.mock import Mock
from unittest.mock import patch

import pytest


@mock.patch('my_module.storage')
@mock.patch('my_module.AuthClient')
def test_bucket_access(mock_AuthClient, mock_storage):
    # Set up the mock storage.Client object
    mock_client = mock.MagicMock()
    mock_storage.Client.return_value = mock_client

    mock_credentials = mock.MagicMock()
    mock_AuthClient.fetch_google_credentials.return_value = mock_credentials

    # Test accessing a bucket that exists
    mock_client.get_bucket.return_value = True
    assert bucket_access() == True

    # Test accessing a bucket that does not exist
    mock_client.get_bucket.side_effect = Exception
    assert bucket_access() == False


@mock.patch('my_module.Credentials')
@mock.patch('my_module.AuthClient')
@mock.patch('my_module.GCSFileSystem')
def test_gcs_credentials_valid(mock_GCSFileSystem, mock_AuthClient, mock_Credentials):
    # Set up the mock google token
    mock_google_token = mock.MagicMock()
    mock_AuthClient.fetch_google_token.return_value = mock_google_token

    # Set up the mock credentials object
    mock_credentials = mock.MagicMock()
    mock_Credentials.return_value = mock_credentials

    # Set up the mock GCSFileSystem object
    mock_file = mock.MagicMock()
    mock_GCSFileSystem.return_value = mock_file

    # Test accessing a GCS service with valid credentials
    mock_file.ls.return_value = True
    assert gcs_credentials_valid() == True

    # Test accessing a GCS service with invalid credentials
    mock_file.ls.side_effect = HttpError("Invalid Credentials, 401")
    assert gcs_credentials_valid() == False


@patch('AuthClient.fetch_local_user')
def test_jupyterhub_auth_valid(self, mock_fetch_local_user):
    # Test successful authentication
    mock_fetch_local_user.return_value = "user"
    result = AuthClient.jupyterhub_auth_valid()
    self.assertTrue(result)

    # Test failed authentication
    mock_fetch_local_user.side_effect = Exception("Error fetching user")
    result = AuthClient.jupyterhub_auth_valid()
    self.assertFalse(result)