import os
from typing import Any
from unittest import mock
from unittest.mock import Mock

import pytest

from dapla.backports import show
from dapla.gcs import GCSFileSystem


@pytest.fixture(scope="function")
def mock_dapla_region(scope: str = "class") -> Any:
    with mock.patch.dict(os.environ, {"DAPLA_REGION": "DAPLA_LAB"}):
        yield


@mock.patch("dapla.backports.FileClient")
def test_show_all_subfolders(file_client_mock: Mock, mock_dapla_region: Mock) -> None:
    assert os.getenv("DAPLA_REGION") == "DAPLA_LAB"
    file_client_mock.get_gcs_file_system.return_value = GCSFileSystem()
    result = show("gs://anaconda-public-data/nyc-taxi/")
    assert result == [
        "/nyc-taxi/2015.parquet",
        "/nyc-taxi/csv",
        "/nyc-taxi/csv/2014",
        "/nyc-taxi/csv/2015",
        "/nyc-taxi/csv/2016",
        "/nyc-taxi/nyc.parquet",
        "/nyc-taxi/taxi.parquet",
    ]


@mock.patch("dapla.backports.FileClient")
def test_show_leaf_folder(file_client_mock: Mock, mock_dapla_region: Mock) -> None:
    assert os.getenv("DAPLA_REGION") == "DAPLA_LAB"
    file_client_mock.get_gcs_file_system.return_value = GCSFileSystem()
    result = show("gs://anaconda-public-data/nyc-taxi/csv/2014")
    assert result == ["/nyc-taxi/csv/2014"]


@mock.patch("dapla.backports.FileClient")
def test_show_invalid_folder(file_client_mock: Mock, mock_dapla_region: Mock) -> None:
    assert os.getenv("DAPLA_REGION") == "DAPLA_LAB"
    file_client_mock.get_gcs_file_system.return_value = GCSFileSystem()
    result = show("gs://anaconda-public-data/nyc-taxi/unknown")
    assert result == []
