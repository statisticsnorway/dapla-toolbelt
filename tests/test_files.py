# Test for FileClient class
import unittest
from unittest.mock import Mock
from unittest.mock import patch

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
    def test_get_valid_versions_versioning_enabled(
        self, mock_client: Mock, mock_auth_client: Mock
    ) -> None:
        bucket_name = "test-bucket"
        file_name = "test-file.txt"
        mock_bucket = Mock()
        mock_bucket.versioning_enabled = True
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
            time_deleted="2023-04-03T00:00:00Z",
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

        assert files[1].name == mock_blob2.name
        assert files[1].generation == mock_blob2.generation
        assert files[1].updated == mock_blob2.updated
        assert files[1].time_deleted == "2023-04-03T00:00:00Z"

    @patch("dapla.auth.AuthClient.fetch_google_credentials", return_value="credentials")
    @patch("google.cloud.storage.Client")
    def test_get_valid_versions_versioning_disabled(
        self, mock_client: Mock, mock_auth_client: Mock
    ) -> None:
        bucket_name = "test-bucket"
        file_name = "test-file.txt"
        mock_bucket = Mock()
        mock_bucket.versioning_enabled = False
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
            time_deleted="2023-04-03T00:00:00Z",
        )
        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2]

        files = FileClient.get_versions(bucket_name, file_name)

        mock_client.return_value.get_bucket.assert_called_with(bucket_name)
        mock_bucket.list_blobs.assert_called_with(prefix=file_name, soft_deleted=True)

        assert len(files) == 2

        assert files[0].name == mock_blob1.name
        assert files[0].generation == mock_blob1.generation
        assert files[0].updated == mock_blob1.updated
        assert files[0].time_deleted is None

        assert files[1].name == mock_blob2.name
        assert files[1].generation == mock_blob2.generation
        assert files[1].updated == mock_blob2.updated
        assert files[1].time_deleted == "2023-04-03T00:00:00Z"

    @patch("dapla.auth.AuthClient.fetch_google_credentials", return_value="credentials")
    @patch("google.cloud.storage.Client")
    def test_get_versions_nonexistent_file(
        self, mock_client: Mock, mock_auth_client: Mock
    ) -> None:
        bucket_name = "test-bucket"
        file_name = "../../../invalid/file/path"
        mock_bucket = Mock()
        mock_bucket.versioning_enabled = True
        mock_client.return_value.get_bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = []

        files = FileClient.get_versions(bucket_name, file_name)

        mock_client.return_value.get_bucket.assert_called_with(bucket_name)
        mock_bucket.list_blobs.assert_called_with(prefix=file_name, versions=True)

        assert len(files) == 0
        assert files == []

    @patch("dapla.auth.AuthClient.fetch_google_credentials", return_value="credentials")
    @patch("google.cloud.storage.Client")
    def test_restore_version_versioning_enabled(
        self, mock_client: Mock, mock_auth_client: Mock
    ) -> None:
        source_bucket_name = "test-bucket"
        source_file_name = "test-file.txt"
        source_generation_id = "1234567890"
        mock_bucket = Mock()
        mock_bucket.versioning_enabled = True
        mock_client.return_value.get_bucket.return_value = mock_bucket
        mock_blob = Mock()
        mock_bucket.copy_blob.return_value = mock_blob

        result = FileClient.restore_version(
            source_bucket_name, source_file_name, source_generation_id
        )

        mock_client.return_value.get_bucket.assert_called_with(source_bucket_name)
        mock_bucket.copy_blob.assert_called_with(
            blob=mock_bucket.blob(source_file_name),
            destination_bucket=mock_bucket,
            source_generation=source_generation_id,
        )
        assert result == mock_blob

    @patch("dapla.auth.AuthClient.fetch_google_credentials", return_value="credentials")
    @patch("google.cloud.storage.Client")
    def test_restore_version_versioning_disabled(
        self, mock_client: Mock, mock_auth_client: Mock
    ) -> None:
        source_bucket_name = "test-bucket"
        source_file_name = "test-file.txt"
        source_generation_id = "1234567890"
        mock_bucket = Mock()
        mock_bucket.versioning_enabled = False
        mock_client.return_value.get_bucket.return_value = mock_bucket
        mock_blob = Mock()
        mock_bucket.restore_blob.return_value = mock_blob

        result = FileClient.restore_version(
            source_bucket_name, source_file_name, source_generation_id
        )

        mock_client.return_value.get_bucket.assert_called_with(source_bucket_name)
        mock_bucket.restore_blob.assert_called_with(
            blob_name=source_file_name, generation=source_generation_id
        )
        assert result == mock_blob

    @patch("dapla.auth.AuthClient.fetch_google_credentials", return_value="credentials")
    @patch("google.cloud.storage.Client")
    def test_restore_version_with_kwargs(
        self, mock_client: Mock, mock_auth_client: Mock
    ) -> None:
        source_bucket_name = "test-bucket"
        source_file_name = "test-file.txt"
        source_generation_id = "1234567890"
        mock_bucket = Mock()
        mock_bucket.versioning_enabled = True
        mock_client.return_value.get_bucket.return_value = mock_bucket
        mock_blob = Mock()
        mock_bucket.copy_blob.return_value = mock_blob
        kwargs = {"metadata": {"key": "value"}}

        result = FileClient.restore_version(
            source_bucket_name, source_file_name, source_generation_id, **kwargs
        )

        mock_client.return_value.get_bucket.assert_called_with(source_bucket_name)
        mock_bucket.copy_blob.assert_called_with(
            blob=mock_bucket.blob(source_file_name),
            destination_bucket=mock_bucket,
            source_generation=source_generation_id,
            **kwargs,
        )
        assert result == mock_blob

    @patch("dapla.auth.AuthClient.fetch_google_credentials", return_value="credentials")
    @patch("google.cloud.storage.Client")
    def test_restore_version_invalid_generation_id(
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
