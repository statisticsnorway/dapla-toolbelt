import pandas as pd
import gcsfs

from .auth import AuthClient


class FileClient:
    @staticmethod
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

    @staticmethod
    def get_gcs_file_system():
        """
        Return a pythonic file-system for Google Cloud Storage - initialized with a personal Google Identity token.
        See https://gcsfs.readthedocs.io/en/latest for usage
        """
        return gcsfs.GCSFileSystem(token=AuthClient.fetch_google_credentials())

    @staticmethod
    def ls(bucket_name):
        """
        List the contents of a GCS bucket
        :param bucket_name:
        :return: Array of contents of bucket
        """
        fs = FileClient.get_gcs_file_system()
        return fs.ls(bucket_name)

    @staticmethod
    def cat(gcs_path):
        """
        Get string content of file from gcs
        :param gcs_path: path or paths to the file(s) you want to get the contents of
        :return: utf-8 decoded string content of the given file
        """
        fs = FileClient.get_gcs_file_system()
        return fs.cat(gcs_path).decode("utf-8")

    @staticmethod
    def gcs_open(gcs_path, mode='r'):
        """
        Open a file in GCS, works like regular python open()
        :param gcs_path:
        :param mode:
        :return:
        """
        FileClient.get_gcs_file_system().open(FileClient.ensure_gcs_uri_prefix(gcs_path), mode)

    @staticmethod
    def load_csv_to_pandas(gcs_path):
        """
        Reads a csv file from google cloud storage into a Pandas data frame
        :param gcs_path: of the file, starting with the bucket name
        :return: a Pandas data frame
        """
        df = pd.read_csv(FileClient.ensure_gcs_uri_prefix(gcs_path), storage_options={"token": AuthClient.fetch_google_credentials()})
        return df

    @staticmethod
    def load_json_to_pandas(gcs_path):
        """
        Reads a json file from google cloud storage into a Pandas data frame
        :param gcs_path: of the file, starting with the bucket name
        :return: a Pandas data frame
        """
        df = pd.read_json(FileClient.ensure_gcs_uri_prefix(gcs_path), storage_options={"token": AuthClient.fetch_google_credentials()})
        return df

    @staticmethod
    def load_xml_to_pandas(gcs_path):
        """
        Reads an xml file from google cloud storage into a Pandas data frame
        :param gcs_path: of the file, starting with the bucket name
        :return: a Pandas data frame
        """
        df = pd.read_xml(FileClient.ensure_gcs_uri_prefix(gcs_path), storage_options={"token": AuthClient.fetch_google_credentials()})
        return df

    @staticmethod
    def save_pandas_to_csv(df: pd.DataFrame, gcs_path, index=False):
        """
        Write the contents of a Pandas data frame to a csv file in a bucket
        :param df: the Pandas data frame to persist as csv
        :param gcs_path: target path, starting with the bucket name and ending with the file name
        :param index: True if you want to write the pandas index to the file
        :return:
        """
        df.to_csv(FileClient.ensure_gcs_uri_prefix(gcs_path), storage_options={"token": AuthClient.fetch_google_credentials()}, index=index)

    @staticmethod
    def save_pandas_to_json(df: pd.DataFrame, gcs_path):
        """
        Write the contents of a Pandas data frame to a json file in a bucket
        :param df: the Pandas data frame to persist as json
        :param gcs_path: target path, starting with the bucket name and ending with the file name
        :param index: True if you want to write the pandas index to the file
        :return:
        """
        df.to_json(FileClient.ensure_gcs_uri_prefix(gcs_path), storage_options={"token": AuthClient.fetch_google_credentials()})

    @staticmethod
    def save_pandas_to_xml(df: pd.DataFrame, gcs_path, index=False):
        """
        Write the contents of a Pandas data frame to an xml file in a bucket
        :param df: the Pandas data frame to persist as xml
        :param gcs_path: target path, starting with the bucket name and ending with the file name
        :param index: True if you want to write the pandas index to the file
        :return:
        """
        df.to_xml(FileClient.ensure_gcs_uri_prefix(gcs_path), storage_options={"token": AuthClient.fetch_google_credentials()}, index=index)
