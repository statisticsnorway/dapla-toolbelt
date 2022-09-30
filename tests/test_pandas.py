import pytest
from unittest import mock
from dapla.pandas import read_pandas
from dapla.gcs import GCSFileSystem

@pytest.mark.skip(reason="Slow test")
@mock.patch('dapla.backports.FileClient')
def test_read_default_format(file_client_mock):
    file_client_mock.get_gcs_file_system.return_value = GCSFileSystem()
    result = read_pandas('gs://anaconda-public-data/nyc-taxi/2015.parquet/part.0.parquet')
    print(result.head(5))


@pytest.mark.skip(reason="Slow test")
@mock.patch('dapla.backports.AuthClient')
@mock.patch('dapla.backports.FileClient')
def test_read_default_format(file_client_mock, auth_client_mock):
    file_client_mock.get_gcs_file_system.return_value = GCSFileSystem()
    auth_client_mock.fetch_google_credentials.return_value = None
    result = read_pandas('gs://anaconda-public-data/nyc-taxi/csv/2014/green_tripdata_2014-01.csv', file_format="csv")
    print(result.head(5))
