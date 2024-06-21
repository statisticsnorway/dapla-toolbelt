# Test for FileClient class
import unittest
from unittest.mock import Mock
from unittest.mock import patch

import google

from dapla import FileClient

PATH_WITH_PREFIX = "gs://bucket/path"
PATH_WITHOUT_PREFIX = "bucket/path"


class TestFiles(unittest.TestCase):

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

    @patch("dapla.auth.AuthClient.fetch_google_credentials", return_value="credentials")
    @patch("google.cloud.storage.Client")
    def test_get_versions_valid(
        self, mock_client: Mock, mock_auth_client: Mock
    ) -> None:
        # Arrange
        bucket_name = "test-bucket"
        file_name = "test-file.txt"
        mock_bucket = Mock()
        mock_client.return_value.get_bucket.return_value = mock_bucket
        mock_blob1 = Mock(
            name="test-file.txt",
            generation=1,
            updated="2023-04-01T00:00:00Z",
            time_deleted=None,
        )
        mock_blob2 = Mock(
            name="test-file.txt",
            generation=2,
            updated="2023-04-02T00:00:00Z",
            time_deleted=None,
        )
        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2]

        files = FileClient.get_versions(bucket_name, file_name)

        mock_client.return_value.get_bucket.assert_called_with(bucket_name)
        mock_bucket.list_blobs.assert_called_with(prefix=file_name, versions=True)

        assert len(files) == 2

        assert files[0].name == mock_blob1.name
        assert files[0].generation == mock_blob1.generation
        assert files[0].updated == mock_blob1.updated
        assert files[0].time_deleted is None

    @patch("dapla.auth.AuthClient.fetch_google_credentials", return_value="credentials")
    @patch("google.cloud.storage.Client")
    def test_get_versions_nonexistent_file(
        self, mock_client: Mock, mock_auth_client: Mock
    ) -> None:
        bucket_name = "test-bucket"
        file_name = "nonexistent-file.txt"
        mock_bucket = Mock()
        mock_client.return_value.get_bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = []

        files = FileClient.get_versions(bucket_name, file_name)

        mock_client.return_value.get_bucket.assert_called_with(bucket_name)
        mock_bucket.list_blobs.assert_called_with(prefix=file_name, versions=True)

        assert len(files) == 0
        assert files == []

    @patch("dapla.auth.AuthClient.fetch_google_credentials", return_value="credentials")
    @patch("google.cloud.storage.Client")
    def test_get_versions_empty_bucket(
        self, mock_client: Mock, mock_auth_client: Mock
    ) -> None:
        bucket_name = "test-bucket"
        file_name = "test-file.txt"
        mock_bucket = Mock()
        mock_client.return_value.get_bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = []

        files = FileClient.get_versions(bucket_name, file_name)

        mock_client.return_value.get_bucket.assert_called_with(bucket_name)
        mock_bucket.list_blobs.assert_called_with(prefix=file_name, versions=True)

        assert len(files) == 0
        assert files == []

    @patch("dapla.auth.AuthClient.fetch_google_credentials", return_value="credentials")
    @patch("google.cloud.storage.Client")
    def test_restore_version_success(
        self, mock_client: Mock, mock_auth_client: Mock
    ) -> None:
        mock_bucket = Mock()
        mock_source_blob = Mock()
        mock_client.return_value.get_bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_source_blob

        blob = FileClient.restore_version(
            source_bucket_name="test-bucket",
            source_file_name="test-file.txt",
            source_generation_id="1234567890",
            new_name="restored-file.txt",
            if_generation_match="0",
        )

        mock_client.return_value.get_bucket.assert_called_with("test-bucket")
        mock_bucket.blob.assert_called_with("test-file.txt")
        mock_bucket.copy_blob.assert_called_with(
            blob=mock_source_blob,
            destination_bucket=mock_bucket,
            source_generation="1234567890",
            new_name="restored-file.txt",
            if_generation_match="0",
        )
        assert blob == mock_bucket.copy_blob.return_value

    @patch("dapla.auth.AuthClient.fetch_google_credentials", return_value="credentials")
    @patch("google.cloud.storage.Client")
    def test_restore_version_existing_live_version(
        self, mock_client: Mock, mock_auth_client: Mock
    ) -> None:
        mock_bucket = Mock()
        mock_source_blob = Mock()
        mock_client.return_value.get_bucket.return_value = mock_bucket
        mock_bucket.versioning_enabled = True
        mock_bucket.blob.return_value = mock_source_blob

        blob = FileClient.restore_version(
            source_bucket_name="test-bucket",
            source_file_name="test-file.txt",
            source_generation_id="1234567890",
            if_generation_match="0987654321",
        )

        mock_client.return_value.get_bucket.assert_called_with("test-bucket")
        mock_bucket.blob.assert_called_with("test-file.txt")
        mock_bucket.copy_blob.assert_called_with(
            blob=mock_source_blob,
            destination_bucket=mock_bucket,
            source_generation="1234567890",
            if_generation_match="0987654321",
        )
        assert blob == mock_bucket.copy_blob.return_value

    @patch("dapla.auth.AuthClient.fetch_google_credentials", return_value="credentials")
    @patch("google.cloud.storage.Client")
    def test_get_versions_with_invalid_bucket_name(
        self, mock_client: Mock, mock_auth_client: Mock
    ) -> None:
        bucket_name = "invalid-bucket"
        file_name = "test-file.txt"
        mock_client.return_value.get_bucket.side_effect = (
            google.cloud.exceptions.NotFound("Sorry, mentioned bucket does´t exist. ")
        )

        files = FileClient.get_versions(bucket_name, file_name)

        mock_client.return_value.get_bucket.assert_called_with(bucket_name)
        assert len(files) == 0

    @patch("dapla.auth.AuthClient.fetch_google_credentials", return_value="credentials")
    @patch("google.cloud.storage.Client")
    def test_restore_version_with_invalid_bucket_name(
        self, mock_client: Mock, mock_auth_client: Mock
    ) -> None:
        source_bucket_name = "invalid-bucket"
        source_file_name = "test-file.txt"
        source_generation_id = "1234567890"
        mock_client.return_value.get_bucket.side_effect = (
            google.cloud.exceptions.NotFound("Sorry, mentioned bucket does´t exist. ")
        )

        result = FileClient.restore_version(
            source_bucket_name, source_file_name, source_generation_id
        )

        mock_client.return_value.get_bucket.assert_called_with(source_bucket_name)
        assert result is None

    @patch("dapla.auth.AuthClient.fetch_google_credentials", return_value="credentials")
    @patch("google.cloud.storage.Client")
    def test_restore_version_with_invalid_file_name(
        self, mock_client: Mock, mock_auth_client: Mock
    ) -> None:
        source_bucket_name = "test-bucket"
        source_file_name = "invalid-file.txt"
        source_generation_id = "1234567890"
        mock_bucket = Mock()
        mock_bucket.versioning_enabled = True
        mock_client.return_value.get_bucket.return_value = mock_bucket
        mock_bucket.copy_blob.side_effect = google.cloud.exceptions.NotFound(
            "No such object. Check file name or the generation number"
        )

        result = FileClient.restore_version(
            source_bucket_name, source_file_name, source_generation_id
        )

        mock_client.return_value.get_bucket.assert_called_with(source_bucket_name)
        mock_bucket.copy_blob.assert_called_with(
            blob=mock_bucket.blob(source_file_name),
            destination_bucket=mock_bucket,
            source_generation=source_generation_id,
        )
        assert result == []

    @patch("dapla.auth.AuthClient.fetch_google_credentials", return_value="credentials")
    @patch("google.cloud.storage.Client")
    def test_restore_version_with_invalid_generation_id(
        self, mock_client: Mock, mock_auth_client: Mock
    ) -> None:
        source_bucket_name = "test-bucket"
        source_file_name = "test-file.txt"
        source_generation_id = "invalid"
        mock_bucket = Mock()
        mock_bucket.versioning_enabled = True
        mock_client.return_value.get_bucket.return_value = mock_bucket
        mock_bucket.copy_blob.side_effect = ValueError("Invalid generation ID")

        with self.assertRaises(ValueError):
            FileClient.restore_version(
                source_bucket_name, source_file_name, source_generation_id
            )

        mock_client.return_value.get_bucket.assert_called_with(source_bucket_name)
        mock_bucket.copy_blob.assert_called_with(
            blob=mock_bucket.blob(source_file_name),
            destination_bucket=mock_bucket,
            source_generation=source_generation_id,
        )


if __name__ == "__main__":
    unittest.main()
