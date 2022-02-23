import pandas as pd
from auth import AuthClient
import gcsfs


def ensure_gcs_uri_prefix(gcs_path):
    """
    GCS uri is the gcs file path prefixed with 'gs://'. Some operations require GCS uris,
    but we don't want the user to bother with knowing when to use the prefix,
    so we ensure its presence automatically where it is needed
    :param gcs_path:
    :return:
    """
    gs_uri_prefix = "gs://"
    if not gcs_path.startswith(gs_uri_prefix):
        gcs_path = f"{gs_uri_prefix}{gcs_path}"
    return gcs_path


def get_gcs_file_system():
    """
    Return a pythonic file-system for Google Cloud Storage - initialized with a personal Google Identity token.
    See https://gcsfs.readthedocs.io/en/latest for usage
    """
    return gcsfs.GCSFileSystem(token=AuthClient.fetch_google_credentials())


def list_content(bucket_name):
    """
    List the contents of a GCS bucket
    :param bucket_name:
    :return: Array of contents of bucket
    """
    fs = get_gcs_file_system()
    return fs.ls(bucket_name)


def get_file(gcs_path):
    """
    Copy a single remote file from gcs to local
    :param gcs_path: to the file you want to get
    :return:
    """
    fs = get_gcs_file_system()
    return fs.get_file(gcs_path)


def gcs_open(gcs_path, mode='r'):
    """
    Open a file in GCS, works like regular python open()
    :param gcs_path:
    :param mode:
    :return:
    """
    get_gcs_file_system().open(ensure_gcs_uri_prefix(gcs_path), mode)


def load_csv_to_pandas(gcs_path):
    """
    Reads a csv file from google cloud storage into a Pandas data frame
    :param gcs_path: of the file, starting with the bucket name
    :return: a Pandas data frame
    """
    df = pd.read_csv(ensure_gcs_uri_prefix(gcs_path), storage_options={"token": AuthClient.fetch_google_credentials()})
    return df


def load_json_to_pandas(gcs_path):
    """
    Reads a json file from google cloud storage into a Pandas data frame
    :param gcs_path: of the file, starting with the bucket name
    :return: a Pandas data frame
    """
    df = pd.read_json(ensure_gcs_uri_prefix(gcs_path), storage_options={"token": AuthClient.fetch_google_credentials()})
    return df


def load_xml_to_pandas(gcs_path):
    """
    Reads an xml file from google cloud storage into a Pandas data frame
    :param gcs_path: of the file, starting with the bucket name
    :return: a Pandas data frame
    """
    df = pd.read_xml(ensure_gcs_uri_prefix(gcs_path), storage_options={"token": AuthClient.fetch_google_credentials()})
    return df


def save_pandas_to_csv(df: pd.DataFrame, gcs_path):
    """
    Write the contents of a Pandas data frame to a csv file in a bucket
    :param df: the Pandas data frame to persist as csv
    :param gcs_path: target path, starting with the bucket name and ending with the file name
    :return:
    """
    df.to_csv(ensure_gcs_uri_prefix(gcs_path), storage_options={"token": AuthClient.fetch_google_credentials()})


def save_pandas_to_json(df: pd.DataFrame, gcs_path):
    """
    Write the contents of a Pandas data frame to a json file in a bucket
    :param df: the Pandas data frame to persist as json
    :param gcs_path: target path, starting with the bucket name and ending with the file name
    :return:
    """
    df.to_json(ensure_gcs_uri_prefix(gcs_path), storage_options={"token": AuthClient.fetch_google_credentials()})


def save_pandas_to_xml(df: pd.DataFrame, gcs_path):
    """
    Write the contents of a Pandas data frame to an xml file in a bucket
    :param df: the Pandas data frame to persist as xml
    :param gcs_path: target path, starting with the bucket name and ending with the file name
    :return:
    """
    df.to_xml(ensure_gcs_uri_prefix(gcs_path), storage_options={"token": AuthClient.fetch_google_credentials()})
