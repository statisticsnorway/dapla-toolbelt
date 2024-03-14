from unittest import mock
from unittest.mock import Mock

import pandas as pd
import pyarrow.compute as pc
from fsspec.implementations.local import LocalFileSystem
from google.oauth2.credentials import Credentials
from pandas import read_csv
from pandas import read_excel
from pandas import read_xml

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


@mock.patch("dapla.pandas.read_csv")
@mock.patch("dapla.pandas.FileClient")
@mock.patch("dapla.pandas.AuthClient")
def test_read_csv_format(
    auth_client_mock: Mock, file_client_mock: Mock, read_csv_mock: Mock
) -> None:
    mock_google_creds = Mock(spec=Credentials)
    mock_google_creds.token = None
    auth_client_mock.fetch_google_credentials.return_value = mock_google_creds
    file_client_mock.get_gcs_file_system.return_value = LocalFileSystem()
    file_client_mock._ensure_gcs_uri_prefix.return_value = "gs://tests/data/fruits.csv"
    read_csv_mock.return_value = read_csv("tests/data/fruits.csv")
    result = read_pandas("gs://tests/data/fruits.csv", file_format="csv")
    print(result.head(5))
    assert result["oranges"][2] == 7
    read_csv_mock.assert_called_once_with(
        "gs://tests/data/fruits.csv", storage_options={"token": mock_google_creds}
    )


@mock.patch("dapla.pandas.FileClient")
def test_read_sas7bdat_format(file_client_mock: Mock) -> None:
    file_client_mock.get_gcs_file_system.return_value = LocalFileSystem()
    file_client_mock._remove_gcs_uri_prefix.return_value = "tests/data/sasdata.sas7bdat"
    file_client_mock._ensure_gcs_uri_prefix.return_value = "tests/data/sasdata.sas7bdat"
    result = read_pandas(
        "tests/data/sasdata.sas7bdat", file_format="sas7bdat", encoding="latin1"
    )
    assert result["tekst"][0] == "Dette er en tekst"


@mock.patch("dapla.pandas.read_excel")
@mock.patch("dapla.pandas.FileClient")
@mock.patch("dapla.pandas.AuthClient")
def test_read_excel_format(
    auth_client_mock: Mock, file_client_mock: Mock, read_excel_mock: Mock
) -> None:
    mock_google_creds = Mock(spec=Credentials)
    mock_google_creds.token = None
    auth_client_mock.fetch_google_credentials.return_value = mock_google_creds
    file_client_mock.get_gcs_file_system.return_value = LocalFileSystem()
    read_excel_mock.return_value = read_excel("tests/data/people.xlsx")
    file_client_mock.get_gcs_file_system.return_value = LocalFileSystem()
    file_client_mock._remove_gcs_uri_prefix.return_value = "tests/data/people.xlsx"
    file_client_mock._ensure_gcs_uri_prefix.return_value = "gs://tests/data/people.xlsx"
    result = read_pandas("gs://tests/data/people.xlsx", file_format="excel")
    print(result)
    assert sum(result["Alder"].to_list()) == 81
    read_excel_mock.assert_called_once_with(
        "gs://tests/data/people.xlsx", storage_options={"token": mock_google_creds}
    )


@mock.patch("dapla.pandas.DataFrame.to_excel")
@mock.patch("dapla.pandas.FileClient")
@mock.patch("dapla.pandas.AuthClient")
def test_write_excel_format(
    auth_client_mock: Mock, file_client_mock: Mock, to_excel_mock: Mock
) -> None:
    mock_google_creds = Mock(spec=Credentials)
    mock_google_creds.token = None
    auth_client_mock.fetch_google_credentials.return_value = mock_google_creds
    file_client_mock.get_gcs_file_system.return_value = LocalFileSystem()
    file_client_mock._ensure_gcs_uri_prefix.return_value = "gs://tests/output/test.xlsx"
    data = {"age": [23, 30, 77, 32]}
    df = pd.DataFrame(data, index=["June", "Robert", "Lily", "David"])
    to_excel_mock.return_value = None
    write_pandas(df, "gs://tests/output/test.xlsx", file_format="excel")
    to_excel_mock.assert_called_with(
        "gs://tests/output/test.xlsx", storage_options={"token": mock_google_creds}
    )


@mock.patch("dapla.pandas.DataFrame.to_csv")
@mock.patch("dapla.pandas.FileClient")
@mock.patch("dapla.pandas.AuthClient")
def test_write_csv_format(
    auth_client_mock: Mock, file_client_mock: Mock, to_csv_mock: Mock
) -> None:
    mock_google_creds = Mock(spec=Credentials)
    mock_google_creds.token = None
    file_client_mock.get_gcs_file_system.return_value = LocalFileSystem()
    auth_client_mock.fetch_google_credentials.return_value = mock_google_creds
    file_client_mock._ensure_gcs_uri_prefix.return_value = "gs://tests/output/test.csv"
    # Create pandas dataframe
    data = {"apples": [3, 2, 0, 1], "oranges": [0, 3, 7, 2]}
    df = pd.DataFrame(data, index=["June", "Robert", "Lily", "David"])
    to_csv_mock.return_value = df.to_csv("tests/output/test.csv")
    write_pandas(df, "gs://tests/output/test.csv", file_format="csv")
    to_csv_mock.assert_called_with(
        "gs://tests/output/test.csv", storage_options={"token": mock_google_creds}
    )


@mock.patch("dapla.pandas.read_xml")
@mock.patch("dapla.pandas.FileClient")
@mock.patch("dapla.pandas.AuthClient")
def test_read_xml_format(
    auth_client_mock: Mock, file_client_mock: Mock, read_xml_mock: Mock
) -> None:
    mock_google_creds = Mock(spec=Credentials)
    mock_google_creds.token = None
    file_client_mock.get_gcs_file_system.return_value = LocalFileSystem()
    auth_client_mock.fetch_google_credentials.return_value = mock_google_creds
    file_client_mock._ensure_gcs_uri_prefix.return_value = (
        "gs://tests/data/students.xml"
    )
    read_xml_mock.return_value = read_xml("tests/data/students.xml")
    result = read_pandas("gs://tests/data/students.xml", file_format="xml")
    assert result["email"][3] == "skrue@mail.com"
    read_xml_mock.assert_called_once_with(
        "gs://tests/data/students.xml", storage_options={"token": mock_google_creds}
    )


@mock.patch("dapla.pandas.FileClient")
def test_read_partitioned_parquet(file_client_mock: Mock) -> None:
    file_client_mock.get_gcs_file_system.return_value = LocalFileSystem()
    file_client_mock._remove_gcs_uri_prefix.return_value = "tests/data/partition"
    file_client_mock._ensure_gcs_uri_prefix.return_value = "gs://tests/data/partition"
    result = read_pandas("tests/data/partition")
    print(result.head(5))
    assert result["innskudd"][1] == 2000


@mock.patch("dapla.pandas.DataFrame.to_xml")
@mock.patch("dapla.pandas.FileClient")
@mock.patch("dapla.pandas.AuthClient")
def test_write_xml_format(
    auth_client_mock: Mock, file_client_mock: Mock, to_xml_mock: Mock
) -> None:
    mock_google_creds = Mock(spec=Credentials)
    mock_google_creds.token = None
    file_client_mock.get_gcs_file_system.return_value = LocalFileSystem()
    auth_client_mock.fetch_google_credentials.return_value = mock_google_creds
    file_client_mock._ensure_gcs_uri_prefix.return_value = "gs://tests/output/test.xml"
    # Create pandas dataframe
    data = {"apples": [3, 2, 0, 1], "oranges": [0, 3, 7, 2]}
    df = pd.DataFrame(data, index=["June", "Robert", "Lily", "David"])
    to_xml_mock.return_value = df.to_xml("tests/output/test.xml")

    write_pandas(df, "gs://tests/output/test.xml", file_format="xml")

    to_xml_mock.assert_called_with(
        "gs://tests/output/test.xml", storage_options={"token": mock_google_creds}
    )


@mock.patch("dapla.pandas.AuthClient")
def test_get_storage_options(auth_client_mock: Mock) -> None:
    auth_client_mock.fetch_google_credentials.return_value = Credentials(
        token="dummy_token",
        token_uri="https://oauth2.googleapis.com/token",
    )
    auth_client_mock._is_oidc_token.return_value = True

    result = _get_storage_options()
    assert result is not None
    assert result["token"] is not None
    assert result["token"].token == "dummy_token"
