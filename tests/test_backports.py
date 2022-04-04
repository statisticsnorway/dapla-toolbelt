import pytest
import mock
from dapla.backports import show, read_pandas
from dapla.gcs import GCSFileSystem


@mock.patch('dapla.backports.FileClient')
def test_show_all_subfolders(file_client_mock):
    file_client_mock.get_gcs_file_system.return_value = GCSFileSystem()
    result = show('gs://anaconda-public-data/nyc-taxi/')
    assert result == ['/nyc-taxi/2015.parquet',
                      '/nyc-taxi/csv',
                      '/nyc-taxi/csv/2014',
                      '/nyc-taxi/csv/2015',
                      '/nyc-taxi/csv/2016',
                      '/nyc-taxi/nyc.parquet',
                      '/nyc-taxi/taxi.parquet']


@mock.patch('dapla.backports.FileClient')
def test_show_leaf_folder(file_client_mock):
    file_client_mock.get_gcs_file_system.return_value = GCSFileSystem()
    result = show('gs://anaconda-public-data/nyc-taxi/csv/2014')
    assert result == ['/nyc-taxi/csv/2014']


@mock.patch('dapla.backports.FileClient')
def test_show_invalid_folder(file_client_mock):
    file_client_mock.get_gcs_file_system.return_value = GCSFileSystem()
    result = show('gs://anaconda-public-data/nyc-taxi/unknown')
    assert result == []


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
