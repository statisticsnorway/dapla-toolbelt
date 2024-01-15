from unittest import mock
from unittest.mock import Mock

import pandas as pd
import pyarrow.compute as pc
from fsspec.implementations.local import LocalFileSystem
from google.oauth2.credentials import Credentials

# These import enables mock patching
from dapla.pandas import _get_storage_options
from dapla.pandas import read_pandas
from dapla.pandas import write_pandas


@mock.patch("dapla.pandas.FileClient")
def test_read_default_format(file_client_mock: Mock) -> None:
    file_client_mock.get_gcs_file_system.return_value = LocalFileSystem()
    file_client_mock._remove_gcs_uri_prefix.return_value = "tests/data/fruits.parquet"
    result = read_pandas("tests/data/fruits.parquet")
    print(result.head(5))
    # Should be able to use column name (oranges) and index name (Lily)
    assert result["oranges"]["Lily"] == 7


@mock.patch("dapla.pandas.FileClient")
def test_read_parquet_format_with_filterings(file_client_mock: Mock) -> None:
    file_client_mock.get_gcs_file_system.return_value = LocalFileSystem()
    file_client_mock._remove_gcs_uri_prefix.return_value = "tests/data/fruits.parquet"
    col_filter = "oranges"
    row_filter = pc.field(col_filter) == pc.scalar(7)
    result = (
        read_pandas(
            gcs_path="tests/data/fruits.parquet",
            columns=[col_filter],
            filters=row_filter,
        )
        .reset_index()
        .to_dict("records")
    )
    print(result)
    # Should be able to use column name (oranges) and index name (Lily)
    assert len(result) == 1
    assert len(result[0].keys()) == 2
    assert result[0][col_filter] == 7


@mock.patch("dapla.pandas.FileClient")
@mock.patch("dapla.pandas.AuthClient")
def test_read_csv_format(auth_client_mock: Mock, file_client_mock: Mock) -> None:
    mock_google_creds = Mock()
    mock_google_creds.token = None
    auth_client_mock.fetch_google_credentials.return_value = mock_google_creds
    file_client_mock.get_gcs_file_system.return_value = LocalFileSystem()
    result = read_pandas("tests/data/fruits.csv", file_format="csv")
    print(result.head(5))
    assert result["oranges"][2] == 7


@mock.patch("dapla.pandas.FileClient")
@mock.patch("dapla.pandas.AuthClient")
def test_write_csv_format(auth_client_mock: Mock, file_client_mock: Mock) -> None:
    mock_google_creds = Mock()
    mock_google_creds.token = None
    file_client_mock.get_gcs_file_system.return_value = LocalFileSystem()
    auth_client_mock.fetch_google_credentials.return_value = mock_google_creds
    # Create pandas dataframe
    data = {"apples": [3, 2, 0, 1], "oranges": [0, 3, 7, 2]}
    df = pd.DataFrame(data, index=["June", "Robert", "Lily", "David"])

    write_pandas(df, "tests/output/test.csv", file_format="csv")


@mock.patch("dapla.pandas.FileClient")
@mock.patch("dapla.pandas.AuthClient")
def test_read_xml_format(auth_client_mock: Mock, file_client_mock: Mock) -> None:
    mock_google_creds = Mock()
    mock_google_creds.token = None
    file_client_mock.get_gcs_file_system.return_value = LocalFileSystem()
    auth_client_mock.fetch_google_credentials.return_value = mock_google_creds
    result = read_pandas("tests/data/students.xml", file_format="xml")
    assert result["email"][3] == "skrue@mail.com"


@mock.patch("dapla.pandas.FileClient")
def test_read_partitioned_parquet(file_client_mock: Mock) -> None:
    file_client_mock.get_gcs_file_system.return_value = LocalFileSystem()
    file_client_mock._remove_gcs_uri_prefix.return_value = "tests/data/partition"
    result = read_pandas("tests/data/partition")
    print(result.head(5))
    assert result["innskudd"][1] == 2000


@mock.patch("dapla.pandas.FileClient")
@mock.patch("dapla.pandas.AuthClient")
def test_write_xml_format(auth_client_mock: Mock, file_client_mock: Mock) -> None:
    mock_google_creds = Mock()
    mock_google_creds.token = None
    file_client_mock.get_gcs_file_system.return_value = LocalFileSystem()
    auth_client_mock.fetch_google_credentials.return_value = mock_google_creds
    # Create pandas dataframe
    data = {"apples": [3, 2, 0, 1], "oranges": [0, 3, 7, 2]}
    df = pd.DataFrame(data, index=["June", "Robert", "Lily", "David"])

    write_pandas(df, "tests/output/test.xml", file_format="xml")


@mock.patch("dapla.pandas.AuthClient")
def test_get_storage_options(auth_client_mock: Mock) -> None:
    auth_client_mock.fetch_google_credentials.return_value = Credentials(
        token="dummy_tokan",
        token_uri="https://oauth2.googleapis.com/token",
    )
    auth_client_mock._is_oidc_token.return_value = True

    result = _get_storage_options()
    assert result is not None
    assert result["token"] == "dummy_tokan"
