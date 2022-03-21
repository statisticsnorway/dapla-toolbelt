import pandas as pd

from .auth import AuthClient
from .gcs import GCSFileSystem


class FileClient:
    @staticmethod
    def _ensure_gcs_uri_prefix(gcs_path):
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

    @staticmethod
    def get_gcs_file_system(**kwargs):
        """
        Return a pythonic file-system for Google Cloud Storage - initialized with a personal Google Identity token.
        See https://gcsfs.readthedocs.io/en/latest for usage
        """
        return GCSFileSystem(token=AuthClient.fetch_google_credentials(), **kwargs)

    @staticmethod
    def ls(gcs_path, detail=False, **kwargs):
        """
        List the contents of a GCS bucket path
        :param gcs_path: GCS bucket path
        :param detail: show file details
        :return: Array of contents of bucket
        """
        return FileClient.get_gcs_file_system().ls(gcs_path, detail=detail, **kwargs)

    @staticmethod
    def cat(gcs_path):
        """
        Get string content of file from gcs
        :param gcs_path: path or paths to the file(s) you want to get the contents of
        :return: utf-8 decoded string content of the given file
        """
        return FileClient.get_gcs_file_system().cat(gcs_path).decode("utf-8")

    @staticmethod
    def gcs_open(gcs_path, mode='r'):
        """
        Open a file in GCS, works like regular python open()
        :param gcs_path:
        :param mode:
        :return:
        """
        FileClient.get_gcs_file_system().open(FileClient._ensure_gcs_uri_prefix(gcs_path), mode)

    @staticmethod
    def load_csv_to_pandas(gcs_path):
        """
        Reads a csv file from google cloud storage into a Pandas data frame
        :param gcs_path: of the file, starting with the bucket name
        :return: a Pandas data frame
        """
        return pd.read_csv(FileClient._ensure_gcs_uri_prefix(gcs_path), storage_options={"token": AuthClient.fetch_google_credentials()})

    @staticmethod
    def load_json_to_pandas(gcs_path):
        """
        Reads a json file from google cloud storage into a Pandas data frame
        :param gcs_path: of the file, starting with the bucket name
        :return: a Pandas data frame
        """
        return pd.read_json(FileClient._ensure_gcs_uri_prefix(gcs_path), storage_options={"token": AuthClient.fetch_google_credentials()})

    @staticmethod
    def load_xml_to_pandas(gcs_path):
        """
        Reads an xml file from google cloud storage into a Pandas data frame
        :param gcs_path: of the file, starting with the bucket name
        :return: a Pandas data frame
        """
        return pd.read_xml(FileClient._ensure_gcs_uri_prefix(gcs_path), storage_options={"token": AuthClient.fetch_google_credentials()})

    @staticmethod
    def save_pandas_to_csv(df: pd.DataFrame, gcs_path, index=False):
        """
        Write the contents of a Pandas data frame to a csv file in a bucket
        :param df: the Pandas data frame to persist as csv
        :param gcs_path: target path, starting with the bucket name and ending with the file name
        :param index: True if you want to write the pandas index to the file
        :return:
        """
        df.to_csv(FileClient._ensure_gcs_uri_prefix(gcs_path), storage_options={"token": AuthClient.fetch_google_credentials()}, index=index)

    @staticmethod
    def save_pandas_to_json(df: pd.DataFrame, gcs_path):
        """
        Write the contents of a Pandas data frame to a json file in a bucket
        :param df: the Pandas data frame to persist as json
        :param gcs_path: target path, starting with the bucket name and ending with the file name
        :param index: True if you want to write the pandas index to the file
        :return:
        """
        df.to_json(FileClient._ensure_gcs_uri_prefix(gcs_path), storage_options={"token": AuthClient.fetch_google_credentials()})

    @staticmethod
    def save_pandas_to_xml(df: pd.DataFrame, gcs_path, index=False):
        """
        Write the contents of a Pandas data frame to an xml file in a bucket
        :param df: the Pandas data frame to persist as xml
        :param gcs_path: target path, starting with the bucket name and ending with the file name
        :param index: True if you want to write the pandas index to the file
        :return:
        """
        df.to_xml(FileClient._ensure_gcs_uri_prefix(gcs_path), storage_options={"token": AuthClient.fetch_google_credentials()}, index=index)
