import concurrent
import json
import unittest

import google
import pytest
from google.cloud import pubsub_v1

import dapla.pubsub
from dapla.pubsub import (
    _generate_pubsub_data,
    _get_callback,
    _get_list_of_blobs_with_prefix,
    publish_gcs_objects_to_pubsub,
)


class TestPubSub(unittest.TestCase):
    project_id = "prod-demo-fake-D869"
    bucket_id = "ssb-prod-demo-fake-data-kilde"
    folder_prefix = "felles/kilde1"
    topic_id = "kilde1-update"
    object_id = "felles/kilde1/test.csv"

    def test_get_list_of_blobs_with_prefix(self) -> None:
        with self.assertRaises(google.auth.exceptions.DefaultCredentialsError):
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
            publish_gcs_objects_to_pubsub(
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
