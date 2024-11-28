import os
from unittest import mock
from unittest.mock import Mock

from dapla.backports import show
from dapla.gcs import GCSFileSystem


@mock.patch("dapla.backports.FileClient")
def test_show_all_subfolders(file_client_mock: Mock) -> None:
    os.environ.update({"DAPLA_REGION": "DAPLA_LAB"})
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
def test_show_leaf_folder(file_client_mock: Mock) -> None:
    os.environ.update({"DAPLA_REGION": "DAPLA_LAB"})
    file_client_mock.get_gcs_file_system.return_value = GCSFileSystem()
    result = show("gs://anaconda-public-data/nyc-taxi/csv/2014")
    assert result == ["/nyc-taxi/csv/2014"]


@mock.patch("dapla.backports.FileClient")
def test_show_invalid_folder(file_client_mock: Mock) -> None:
    os.environ.update({"DAPLA_REGION": "DAPLA_LAB"})
    file_client_mock.get_gcs_file_system.return_value = GCSFileSystem()
    result = show("gs://anaconda-public-data/nyc-taxi/unknown")
    assert result == []
