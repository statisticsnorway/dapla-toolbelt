import typing as t
from io import TextIOWrapper
from typing import Any

import pandas as pd
from fsspec.spec import AbstractBufferedFile
from google.cloud import storage

from .auth import AuthClient
from .gcs import GCSFileSystem

GS_URI_PREFIX = "gs://"


class FileClient:
    """Client for working with buckets and files on Google Cloud Storage.

    This class should not be instantiated, only the static methods should be used.
    """

    @staticmethod
    def _ensure_gcs_uri_prefix(gcs_path: str) -> str:
        """Ensure that a GCS uri has the 'gs://' prefix.

        Some operations require GCS uris, but we don't want the user to bother with knowing when to use the prefix,
        so we ensure its presence automatically where it is needed.
        """
        if not gcs_path.startswith(GS_URI_PREFIX):
            gcs_path = f"{GS_URI_PREFIX}{gcs_path}"
        return gcs_path

    @staticmethod
    def _remove_gcs_uri_prefix(gcs_path: str) -> str:
        """Remove the 'gs://' prefix from a GCS URI."""
        if gcs_path.startswith(GS_URI_PREFIX):
            gcs_path = gcs_path[len(GS_URI_PREFIX) :]
        return gcs_path

    @staticmethod
    def get_gcs_file_system(**kwargs: Any) -> GCSFileSystem:
        """Return a pythonic file-system for Google Cloud Storage - initialized with a personal Google Identity token.

        Args:
            kwargs: Additional arguments to pass to the underlying GCSFileSystem.

        Returns:
            A GCSFileSystem instance.

        See https://gcsfs.readthedocs.io/en/latest for advanced usage
        """
        return GCSFileSystem(token=AuthClient.fetch_google_credentials(), **kwargs)

    @staticmethod
    def ls(gcs_path: str, detail: bool = False, **kwargs: Any) -> Any:
        """List the contents of a GCS bucket path.

        Args:
            gcs_path: The GCS path to a directory.
            detail: Whether to return detailed information about the files.
            kwargs: Additional arguments to pass to the underlying 'ls()' method.

        Returns:
            List of strings if detail is False, or list of directory information dicts if detail is True.
        """
        return FileClient.get_gcs_file_system().ls(gcs_path, detail=detail, **kwargs)

    @staticmethod
    def get_versions(bucket_name: str, file_name: str) -> Any:
        """Get all versions of a file in a bucket.

        Args:
            bucket_name: Bucket name where the file is located.
            file_name: Name of the file.

        Returns:
            List of versions of the file.
        """
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        return list(bucket.list_blobs(prefix=file_name, versions=True))

    @staticmethod
    def restore_version(
        bucket_name: str,
        file_name: str,
        destination_file: str,
        generation_id: str,
        destination_generation_id: str,
    ) -> Any:
        """Restores deleted/non-current version of file to the live version.

        Args:
            bucket_name: source bucket name where the file is located.
            file_name: non-current file name.
            destination_file: name of the file to be restored .
            generation_id: generation_id of the non-current.
            destination_generation_id: Incase live version already exists, generation_id of the live version

        Returns:
            A new blob with new generation id.
        """
        storage_client = storage.Client()
        source_bucket = storage_client.bucket(bucket_name)
        source_file = source_bucket.blob(file_name)

        # Restoring file means the destination bucket will be same as source
        destination_bucket = storage_client.bucket(bucket_name)

        return source_bucket.copy_blob(
            source_file,
            destination_bucket,
            destination_file,
            source_generation=generation_id,
            if_generation_match=destination_generation_id,
        )

    @staticmethod
    def cat(gcs_path: str) -> str:
        """Get string content of a file from GCS.

        Args:
            gcs_path: The GCS path to a file.

        Returns:
            utf-8 decoded string content of the given file
        """
        return t.cast(
            str, FileClient.get_gcs_file_system().cat(gcs_path).decode("utf-8")
        )

    @staticmethod
    def gcs_open(
        gcs_path: str, mode: str = "r"
    ) -> TextIOWrapper | AbstractBufferedFile:
        """Open a file in GCS, works like regular python open().

        Args:
            gcs_path: The GCS path to a file.
            mode: File open mode. Defaults to 'r'

        Returns:
            A file-like object.
        """
        return FileClient.get_gcs_file_system().open(
            FileClient._ensure_gcs_uri_prefix(gcs_path), mode
        )

    @staticmethod
    def load_csv_to_pandas(gcs_path: str, **kwargs: Any) -> pd.DataFrame:
        """Reads a CSV file from Google Cloud Storage into a Pandas DataFrame.

        Args:
            gcs_path: The GCS path to a .csv file.
            **kwargs: Additional arguments to pass to the underlying Pandas read_csv().

        Returns:
            A Pandas DataFrame.
        """
        return t.cast(
            pd.DataFrame,
            pd.read_csv(
                FileClient._ensure_gcs_uri_prefix(gcs_path),
                storage_options={"token": AuthClient.fetch_google_credentials()},
                **kwargs,
            ),
        )

    @staticmethod
    def load_json_to_pandas(gcs_path: str, **kwargs: Any) -> pd.DataFrame:
        """Reads a JSON file from Google Cloud Storage into a Pandas DataFrame.

        Args:
            gcs_path: The GCS path to a .json file.
            **kwargs: Additional arguments to pass to the underlying Pandas read_json().

        Returns:
            A Pandas DataFrame.
        """
        return t.cast(
            pd.DataFrame,
            pd.read_json(
                FileClient._ensure_gcs_uri_prefix(gcs_path),
                storage_options={"token": AuthClient.fetch_google_credentials()},
                **kwargs,
            ),
        )

    @staticmethod
    def load_xml_to_pandas(gcs_path: str, **kwargs: Any) -> pd.DataFrame:
        """Reads an XML file from Google Cloud Storage into a Pandas DataFrame.

        Args:
            gcs_path: The GCS path to a .xml file.
            **kwargs: Additional arguments to pass to the underlying Pandas read_xml().

        Returns:
            A Pandas DataFrame.
        """
        return pd.read_xml(
            FileClient._ensure_gcs_uri_prefix(gcs_path),
            storage_options={"token": AuthClient.fetch_google_credentials()},
            **kwargs,
        )

    @staticmethod
    def save_pandas_to_csv(df: pd.DataFrame, gcs_path: str, **kwargs: Any) -> None:
        """Write the contents of a Pandas DataFrame to a CSV file in a bucket.

        Args:
            df: The Pandas DataFrame to save to file.
            gcs_path: The GCS path to the destination .csv file.
            **kwargs: Additional arguments to pass to the underlying Pandas to_csv().
        """
        df.to_csv(
            FileClient._ensure_gcs_uri_prefix(gcs_path),
            storage_options={"token": AuthClient.fetch_google_credentials()},
            **kwargs,
        )

    @staticmethod
    def save_pandas_to_json(df: pd.DataFrame, gcs_path: str, **kwargs: Any) -> None:
        """Write the contents of a Pandas DataFrame to a JSON file in a bucket.

        Args:
            df: The Pandas DataFrame to save to file.
            gcs_path: The GCS path to the destination .json file.
            **kwargs: Additional arguments to pass to the underlying Pandas to_json().
        """
        df.to_json(
            FileClient._ensure_gcs_uri_prefix(gcs_path),
            **kwargs,
        )

    @staticmethod
    def save_pandas_to_xml(df: pd.DataFrame, gcs_path: str, **kwargs: Any) -> None:
        """Write the contents of a Pandas DataFrame to an XML file in a bucket.

        Args:
            df: The Pandas DataFrame to save to file.
            gcs_path: The GCS path to the destination .xml file.
            **kwargs: Additional arguments to pass to the underlying Pandas to_xml().
        """
        df.to_xml(
            FileClient._ensure_gcs_uri_prefix(gcs_path),
            storage_options={"token": AuthClient.fetch_google_credentials()},
            **kwargs,
        )
