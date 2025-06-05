import json
import unittest
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from google.api_core.exceptions import NotFound
from google.auth.exceptions import DefaultCredentialsError
from google.cloud.pubsub_v1.publisher.futures import Future

import dapla.pubsub
from dapla.pubsub import EmptyListError
from dapla.pubsub import _generate_pubsub_data
from dapla.pubsub import _get_callback
from dapla.pubsub import _get_list_of_blobs_with_prefix
from dapla.pubsub import _publish_gcs_objects_to_pubsub


class TestPubSub(unittest.TestCase):
    project_id = "prod-demo-fake-D869"
    bucket_id = "ssb-prod-demo-fake-data-kilde"
    folder_prefix = "felles/kilde1"
    topic_id = "update-kilde1"
    source_folder_name = "kilde1"
    object_id = "felles/kilde1/test.csv"

    @patch("dapla.pubsub.storage.Client")
    @patch("dapla.pubsub.AuthClient.fetch_google_credentials")
    def test_get_list_of_blobs_with_prefix(
        self, mock_fetch_google_credentials: Mock, mock_storage_client_class: Mock
    ) -> None:

        mock_fetch_google_credentials.return_value = MagicMock()

        fake_storage_client_instance = mock_storage_client_class.return_value
        fake_storage_client_instance.list_blobs.side_effect = NotFound(
            "bucket not found"
        )

        with self.assertRaises((DefaultCredentialsError, NotFound)):
            _get_list_of_blobs_with_prefix(
                self.bucket_id, self.folder_prefix, self.project_id
            )

    def test_generate_pubsub_data(self) -> None:
        byte_data = _generate_pubsub_data(self.bucket_id, self.object_id)
        json_object = json.loads(byte_data.decode("utf-8"))

        assert json_object["kind"] == "storage#object"
        assert json_object["name"] == f"{self.bucket_id}/{self.object_id}"
        assert json_object["bucket"] == self.bucket_id

    @patch("dapla.pubsub._get_list_of_blobs_with_prefix")
    def test_publish_gcs_objects_to_pubsub(self, mock_list: Mock) -> None:
        mock_list.return_value = []
        with self.assertRaises(EmptyListError):
            _publish_gcs_objects_to_pubsub(
                self.project_id, self.bucket_id, self.folder_prefix, self.topic_id
            )

    def test_get_callback(self) -> None:
        publish_future = MagicMock(side_effect=Future.result)
        callback = _get_callback(publish_future, "blob_name", timeout=1)

        callback(publish_future)
        publish_future.result.assert_called_with(timeout=1)

    @patch("dapla.pubsub._publish_gcs_objects_to_pubsub")
    def test_trigger_source_data_processing(
        self, mock_publish_gcs_objects_to_pubsub: Mock
    ) -> None:
        dapla.trigger_source_data_processing(
            self.project_id, self.source_folder_name, self.folder_prefix
        )

        self.assertTrue(mock_publish_gcs_objects_to_pubsub.called)
        mock_publish_gcs_objects_to_pubsub.assert_called_with(
            self.project_id, self.bucket_id, self.folder_prefix, topic_id=self.topic_id
        )

    @patch("dapla.pubsub._publish_gcs_objects_to_pubsub")
    def test_trigger_source_data_processing_kuben(
        self, mock_publish_gcs_objects_to_pubsub: Mock
    ) -> None:
        kuben_project_id = "dapla-kildomaten-p-zz"

        dapla.trigger_source_data_processing(
            kuben_project_id, self.source_folder_name, self.folder_prefix, True
        )

        self.assertTrue(mock_publish_gcs_objects_to_pubsub.called)
        mock_publish_gcs_objects_to_pubsub.assert_called_with(
            kuben_project_id,
            "ssb-dapla-kildomaten-data-kilde-prod",
            self.folder_prefix,
            topic_id=self.topic_id,
        )


@pytest.mark.parametrize(
    "project_id, expected_project_name",
    [
        ("my-project-123", "my-project"),
        ("another-project-789", "another-project"),
        ("one-hyphen", "one"),
        ("prod-demo-stat-b-b609d", "prod-demo-stat-b"),
    ],
)
def test_extract_project_name(project_id: str, expected_project_name: str) -> None:
    from dapla.pubsub import _extract_project_name

    assert _extract_project_name(project_id) == expected_project_name


@pytest.mark.parametrize(
    "invalid_project_id",
    [
        "invalid_project_id",
        "no_hyphen",
    ],
)
def test_invalid_project_id(invalid_project_id: str) -> None:
    from dapla.pubsub import _extract_project_name

    with pytest.raises(ValueError):
        _extract_project_name(invalid_project_id)


@pytest.mark.parametrize(
    "project_id, expected_project_id",
    [("dapla-kildomaten-p-zz", "prod"), ("dapla-t-zz", "test")],
)
def test_extract_env(project_id: str, expected_project_id: str) -> None:
    from dapla.pubsub import _extract_env

    assert _extract_env(project_id) == expected_project_id


def test_extract_env_invalid_project() -> None:
    from dapla.pubsub import _extract_env

    project_id = "dapla-kildomaten-p"
    with pytest.raises(ValueError):
        _extract_env(project_id)
