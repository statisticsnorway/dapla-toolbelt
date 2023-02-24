import concurrent
import json
import unittest
from unittest.mock import Mock

import google
import pytest
from google.cloud import pubsub_v1

import dapla.pubsub
from dapla.pubsub import (
    _extract_project_name,
    _generate_pubsub_data,
    _get_callback,
    _get_list_of_blobs_with_prefix,
    _publish_gcs_objects_to_pubsub,
)


class TestPubSub(unittest.TestCase):
    project_id = "prod-demo-fake-D869"
    bucket_id = "ssb-prod-demo-fake-data-kilde"
    folder_prefix = "felles/kilde1"
    topic_id = "kilde1-update"
    source_folder_name = "kilde1"
    object_id = "felles/kilde1/test.csv"

    def test_get_list_of_blobs_with_prefix(self) -> None:
        with self.assertRaises(
            (
                google.auth.exceptions.DefaultCredentialsError,
                google.api_core.exceptions.NotFound,
            )
        ):
            _get_list_of_blobs_with_prefix(self.bucket_id, self.folder_prefix)

    def test_generate_pubsub_data(self):
        byte_data = _generate_pubsub_data(self.bucket_id, self.object_id)
        # Decodes the object and checks that the key and values are as expected.
        json_object = json.loads(byte_data.decode("utf-8"))

        assert json_object["kind"] == "storage#object"
        assert json_object["name"] == f"{self.bucket_id}/{self.object_id}"
        assert json_object["bucket"] == self.bucket_id

    @unittest.mock.patch("dapla.pubsub._get_list_of_blobs_with_prefix")
    def test_publish_gcs_objects_to_pubsub(self, mock_list):
        # Checks if a EmptyListError is raised when no files are returned by _get_list_of_blobs_with_prefix
        mock_list.return_value = []
        with self.assertRaises(dapla.pubsub.EmptyListError):
            _publish_gcs_objects_to_pubsub(
                self.project_id, self.bucket_id, self.folder_prefix, self.topic_id
            )

    def test_get_callback(self):
        publish_future = pubsub_v1.publisher.futures.Future()
        # Create a callback function using the _get_callback helper function
        callback = _get_callback(publish_future, "blob_name", timeout=1)

        # Call the callback function with the mock future object
        with pytest.raises(concurrent.futures.TimeoutError):
            callback(publish_future)
            publish_future.result.assert_called_with(timeout=1)

    @unittest.mock.patch("dapla.pubsub._publish_gcs_objects_to_pubsub")
    def test_trigger_source_data_processing(
        self, mock_publish_gcs_objects_to_pubsub: Mock
    ):
        dapla.trigger_source_data_processing(
            self.project_id, self.source_folder_name, self.folder_prefix
        )

        self.assertTrue(mock_publish_gcs_objects_to_pubsub.called)

        # Check that _publish_gcs_objects_to_pubsub has been called with expected parameters
        mock_publish_gcs_objects_to_pubsub.assert_called_with(
            self.project_id, self.bucket_id, self.folder_prefix, self.topic_id
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
def test_extract_project_name(project_id, expected_project_name):
    assert _extract_project_name(project_id) == expected_project_name


@pytest.mark.parametrize(
    "invalid_project_id",
    [
        "invalid_project_id",
        "no_hyphen",
    ],
)
def test_invalid_project_id(invalid_project_id):
    with pytest.raises(ValueError):
        _extract_project_name(invalid_project_id)
