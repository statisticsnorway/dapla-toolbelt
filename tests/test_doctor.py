
from dapla.doctor import Doctor

import pytest
import os
from unittest.mock import patch



@patch('dapla.doctor.AuthClient.fetch_local_user')
def test_jupyterhub_auth_valid(mock_fetch_local_user):
    # Test authenticated user
    mock_fetch_local_user.return_value = "test_user"
    result = Doctor.jupyterhub_auth_valid()
    assert result == True

    # Test unauthenticated user
    mock_fetch_local_user.side_effect = Exception("Test exception")
    result = Doctor.jupyterhub_auth_valid()
    assert result == False


@patch('dapla.doctor.AuthClient.fetch_personal_token')
@patch('dapla.doctor.Doctor._is_token_expired')
def test_keycloak_token_valid(mock_is_token_expired, mock_fetch_personal_token):
    # Test valid token
    mock_fetch_personal_token.return_value = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
    mock_is_token_expired.return_value = False
    result = Doctor.keycloak_token_valid()
    assert result == True

    # Test invalid token
    mock_is_token_expired.return_value = True
    result = Doctor.keycloak_token_valid()
    assert result == False


@patch('dapla.doctor.AuthClient.fetch_google_credentials')
@patch('dapla.doctor.storage.Client')
@patch.dict(os.environ, {'CLUSTER_ID': 'staging-bip-app'})
def test_bucket_access(mock_client, mock_fetch_google_credentials):
    # Test successful bucket access
    mock_fetch_google_credentials.return_value = "test_credentials"
    mock_client.return_value.get_bucket.return_value = "test_bucket"
    result = Doctor.bucket_access()
    assert result == True

    # Test unsuccessful bucket access
    mock_fetch_google_credentials.return_value = "test_credentials"
    mock_client.return_value.get_bucket.side_effect = Exception("Test exception")
    result = Doctor.bucket_access()
    assert result == False


@patch('dapla.doctor.AuthClient.fetch_google_token')
@patch('dapla.doctor.GCSFileSystem')
@patch.dict(os.environ, {'CLUSTER_ID': 'staging-bip-app'})
def test_gcs_credentials_valid(mock_GCSFileSystem, mock_fetch_google_token):
    # Test valid credentials
    mock_fetch_google_token.return_value = "test_token"
    mock_GCSFileSystem.return_value.ls.return_value = "test_listing"
    result = Doctor.gcs_credentials_valid()
    assert result == True


    
