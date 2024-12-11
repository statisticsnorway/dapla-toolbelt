import os
from unittest.mock import patch

from dapla.gcs import GCSFileSystem


@patch('google.auth.default', return_value=(None, None))
def test_instance(mock_auth) -> None:
    client = GCSFileSystem()
    assert client is not None


@patch('google.auth.default', return_value=(None, None))
def test_init_with_dapla_lab(mock_auth) -> None:
    os.environ["DAPLA_REGION"] = "dapla-lab"
    client = GCSFileSystem(project="test-project")
    assert client is not None


@patch('google.auth.default', return_value=(None, None))
def test_init_with_cloud_run(mock_auth) -> None:
    os.environ["DAPLA_REGION"] = "cloud-run"
    client = GCSFileSystem(project="test-project")
    assert client is not None


@patch('google.auth.default', return_value=(None, None))
def test_init_with_custom_region(mock_auth) -> None:
    os.environ["DAPLA_REGION"] = "custom-region"
    client = GCSFileSystem(project="test-project")
    assert client is not None


@patch('google.auth.default', return_value=(None, None))
def test_init_with_additional_kwargs(mock_auth) -> None:
    client = GCSFileSystem(project="test-project", timeout=100)
    assert client is not None
