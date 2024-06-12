import unittest
from unittest.mock import Mock
from unittest.mock import patch

from dapla import FileClient

PATH_WITH_PREFIX = "gs://bucket/path"
PATH_WITHOUT_PREFIX = "bucket/path"


class TestGetVersionsEdgeCases(unittest.TestCase):

    def test_ensure_gcs_uri_prefix(self) -> None:
        assert FileClient._ensure_gcs_uri_prefix(PATH_WITH_PREFIX) == PATH_WITH_PREFIX
        assert (
            FileClient._ensure_gcs_uri_prefix(PATH_WITHOUT_PREFIX) == PATH_WITH_PREFIX
        )

    def test_remove_gcs_uri_prefix(self) -> None:
        assert (
            FileClient._remove_gcs_uri_prefix(PATH_WITH_PREFIX) == PATH_WITHOUT_PREFIX
        )
        assert (
            FileClient._remove_gcs_uri_prefix(PATH_WITHOUT_PREFIX)
            == PATH_WITHOUT_PREFIX
        )

    @patch("google.cloud.storage.Client")
    def test_get_versions_valid_file(self, mock_client):
        bucket_name = "test-bucket"
        file_name = "test-file.txt"

        mock_blob1 = Mock(
            name="test-file.txt",
            generation=1,
            updated="2023-04-01T00:00:00Z",
            time_deleted="2023-04-02T00:00:00Z",
        )
        mock_blob2 = Mock(
            name="test-file.txt",
            generation=2,
            updated="2023-04-03T00:00:00Z",
            time_deleted=None,
        )

        mock_client.return_value.list_blobs.return_value = [mock_blob1, mock_blob2]

        files = FileClient.get_versions(bucket_name, file_name)

        mock_client.return_value.list_blobs.assert_called_with(
            bucket_name, prefix=file_name, versions=True
        )

        assert len(files) == 2

    @patch("google.cloud.storage.Client")
    def test_get_versions_empty_bucket(self, mock_client):
        bucket_name = "test-bucket"
        file_name = "test-file.txt"
        mock_client.return_value.list_blobs.return_value = []

        files = FileClient.get_versions(bucket_name, file_name)

        mock_client.return_value.list_blobs.assert_called_with(
            bucket_name, prefix=file_name, versions=True
        )
        assert len(files) == 0

    @patch("google.cloud.storage.Client")
    def test_get_versions_non_existent_file(self, mock_client):
        bucket_name = "test-bucket"
        file_name = "non-existent-file.txt"
        mock_client.return_value.list_blobs.return_value = []

        files = FileClient.get_versions(bucket_name, file_name)

        mock_client.return_value.list_blobs.assert_called_with(
            bucket_name, prefix=file_name, versions=True
        )
        assert len(files) == 0

    @patch("google.cloud.storage.Client")
    def test_get_versions_with_invalid_bucket_name(self, mock_client):
        bucket_name = "invalid-bucket-name"
        file_name = "test-file.txt"
        mock_client.return_value.list_blobs.side_effect = Exception(
            "Invalid bucket name"
        )

        with self.assertRaises(Exception):
            FileClient.get_versions(bucket_name, file_name)

    @patch("google.cloud.storage.Client")
    def test_get_versions_with_invalid_file_name(self, mock_client):
        bucket_name = "test-bucket"
        file_name = "invalid/file/name"
        mock_client.return_value.list_blobs.side_effect = Exception("Invalid file name")

        with self.assertRaises(Exception):
            FileClient.get_versions(bucket_name, file_name)
