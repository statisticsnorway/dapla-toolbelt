import pandas as pd
from auth import AuthClient


def get_gcs_file_system():
    """
    Return a pythonic file-system for Google Cloud Storage - initialized with a personal Google Identity token.
    See https://gcsfs.readthedocs.io/en/latest for usage
    """
    import gcsfs
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
    fs = get_gcs_file_system()
    fs.get_file(gcs_path)


def load_csv_to_pandas(gcs_path):
    """
    Reads a csv file from google cloud storage into a Pandas data frame
    :param gcs_path: of the file, starting with the bucket name
    :return: a Pandas data frame
    """
    df = pd.read_csv(f"gcs://{gcs_path}", storage_options={"token": AuthClient.fetch_google_credentials()})
    return df


def load_json_to_pandas(gcs_path):
    """
    Reads a json file from google cloud storage into a Pandas data frame
    :param gcs_path: of the file, starting with the bucket name
    :return: a Pandas data frame
    """
    df = pd.read_json(f"gcs://{gcs_path}", storage_options={"token": AuthClient.fetch_google_credentials()})
    return df


def load_xml_to_pandas(gcs_path):
    """
    Reads an xml file from google cloud storage into a Pandas data frame
    :param gcs_path: of the file, starting with the bucket name
    :return: a Pandas data frame
    """
    df = pd.read_xml(f"gcs://{gcs_path}", storage_options={"token": AuthClient.fetch_google_credentials()})
    return df


def save_pandas_to_csv(df: pd.DataFrame, gcs_path):
    """
    Write the contents of a Pandas data frame to a csv file in a bucket
    :param df: the Pandas data frame to persist as csv
    :param gcs_path: target path, starting with the bucket name and ending with the file name
    :return:
    """
    df.to_csv(f"gcs://{gcs_path}", storage_options={"token": AuthClient.fetch_google_credentials()})


def save_pandas_to_json(df: pd.DataFrame, gcs_path):
    """
    Write the contents of a Pandas data frame to a json file in a bucket
    :param df: the Pandas data frame to persist as json
    :param gcs_path: target path, starting with the bucket name and ending with the file name
    :return:
    """
    df.to_json(f"gcs://{gcs_path}", storage_options={"token": AuthClient.fetch_google_credentials()})


def save_pandas_to_xml(df: pd.DataFrame, gcs_path):
    """
    Write the contents of a Pandas data frame to an xml file in a bucket
    :param df: the Pandas data frame to persist as xml
    :param gcs_path: target path, starting with the bucket name and ending with the file name
    :return:
    """
    df.to_xml(f"gcs://{gcs_path}", storage_options={"token": AuthClient.fetch_google_credentials()})
