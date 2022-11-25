import mock
import pandas as pd
# These import enables mock patching
from dapla.pandas import (
    AuthClient,
    FileClient,
    read_pandas,
    write_pandas,
    get_storage_options
)
from fsspec.implementations.local import LocalFileSystem
from google.oauth2.credentials import Credentials


@mock.patch('dapla.pandas.FileClient')
def test_read_default_format(file_client_mock: FileClient):
    file_client_mock.get_gcs_file_system.return_value = LocalFileSystem()
    result = read_pandas('tests/data/fruits.parquet')
    print(result.head(5))
    # Should be able to use column name (oranges) and index name (Lily)
    assert result.get('oranges')['Lily'] == 7


@mock.patch('dapla.pandas.FileClient')
@mock.patch('dapla.pandas.AuthClient')
def test_read_csv_format(auth_client_mock: AuthClient, file_client_mock: FileClient):
    auth_client_mock.fetch_google_credentials.return_value = None
    file_client_mock.get_gcs_file_system.return_value = LocalFileSystem()
    result = read_pandas('tests/data/fruits.csv', file_format="csv")
    print(result.head(5))
    assert result.get('oranges')[2] == 7


@mock.patch('dapla.pandas.FileClient')
@mock.patch('dapla.pandas.AuthClient')
def test_write_csv_format(auth_client_mock: AuthClient, file_client_mock: FileClient):
    auth_client_mock.fetch_google_credentials.return_value = None
    file_client_mock.get_gcs_file_system.return_value = LocalFileSystem()
    # Create pandas dataframe
    data = {
        'apples': [3, 2, 0, 1],
        'oranges': [0, 3, 7, 2]
    }
    df = pd.DataFrame(data, index=['June', 'Robert', 'Lily', 'David'])

    write_pandas(df, 'tests/output/test.csv', file_format="csv")


@mock.patch('dapla.pandas.AuthClient')
def test_get_storage_options(auth_client_mock: AuthClient) -> Credentials:
    auth_client_mock.fetch_google_credentials.return_value = Credentials(
        token='dummy_tokan',
        token_uri='https://oauth2.googleapis.com/token',
    )
    result = get_storage_options()
    assert result is not None
    assert result['token'].token == 'dummy_tokan'
