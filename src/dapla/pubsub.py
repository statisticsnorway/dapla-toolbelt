import json
import re
import typing as t
from concurrent import futures
from typing import Callable

import google.cloud.storage as storage
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.publisher.futures import Future as PubSubFuture

from dapla import AuthClient


class EmptyListError(Exception):
    """Empty list error."""

    pass


def _get_list_of_blobs_with_prefix(
    bucket_name: str, folder_prefix: str, project_id: str
) -> list[storage.Blob]:
    """Helper function that gets a list of Blob objects in a Google Cloud Storage bucket that has a certain prefix.

    Args:
        bucket_name (str): The name of the Google Cloud Storage bucket to get blobs from.
        folder_prefix (str): The prefix to filter blobs by.
        project_id (str): The ID of the Google Cloud project that the Pub/Sub topic belongs to.

    Returns:
        An list over the `storage.Blob` objects representing the blobs in the specified bucket and with names starting
        with the given prefix.
    """
    google_credentials = AuthClient.fetch_google_credentials()
    storage_client = storage.Client(project=project_id, credentials=google_credentials)
    return list(storage_client.list_blobs(bucket_name, prefix=folder_prefix))


def _generate_pubsub_data(bucket_id: str, object_id: str) -> bytes:
    """Helper function that generates the message data to be published to a Google Cloud Pub/Sub topic.

    Args:
        bucket_id (str): The ID of the Google Cloud Storage bucket that contains the object.
        object_id (str): The ID of the object that has been updated.

    Returns:
        bytes: The message data, encoded as a byte string.
    """
    data = {
        "kind": "storage#object",
        "name": f"{bucket_id}/{object_id}",
        "bucket": f"{bucket_id}",
    }
    return json.dumps(data).encode("utf-8")


def _get_callback(
    _publish_future: PubSubFuture,
    blob_name: str,
    timeout: int = 60,
) -> Callable[[PubSubFuture], None]:
    """Helper function that creates a callback function for a Google Cloud Pub/Sub publish future.

    Args:
        _publish_future (PubSubFuture): The future object returned by the publish call.
        blob_name (str): The name of the Google Cloud Storage object that is being published.
        timeout (int): The number of seconds to wait for the publish call to succeed before timing out. Defaults to 60 seconds.

    Returns:
        callable: A callback function that handles success or failure of the publish operation.
    """

    def callback(publish_future: PubSubFuture) -> None:
        try:
            # Wait 60 seconds for the publish call to succeed.
            publish_future.result(timeout=timeout)
        except futures.TimeoutError:
            print(f"Publishing message for {blob_name} timed out.")

    return callback


def _publish_gcs_objects_to_pubsub(
    project_id: str, bucket_id: str, folder_prefix: str, topic_id: str
) -> None:
    """Publishes messages to a Pub/Sub topic for all objects in a Google Cloud Storage bucket with a given prefix.

    Args:
        project_id (str): The ID of the Google Cloud project that the Pub/Sub topic belongs to.
        bucket_id (str): The ID of the Google Cloud Storage bucket.
        folder_prefix (str): The prefix of the folder containing the objects to be published.
        topic_id (str): The ID of the Pub/Sub topic to publish to.

    Raises:
        EmptyListError: If there are no objects in the bucket with the given prefix.

    """
    blob_list = _get_list_of_blobs_with_prefix(bucket_id, folder_prefix, project_id)

    if len(blob_list) == 0:
        raise EmptyListError(
            f"There are no files in {bucket_id:} with the given {folder_prefix:}."
        )

    publisher = PublisherClient(credentials=AuthClient.fetch_google_credentials())
    topic_path = publisher.topic_path(project_id, topic_id)

    publish_futures = []

    for blob in blob_list:
        byte_data = _generate_pubsub_data(bucket_id, blob.name)

        # When you publish a message, the client returns a future.
        publish_future = t.cast(
            PubSubFuture,
            publisher.publish(
                topic_path,
                data=byte_data,
                payloadFormat="JSON_API_V1",
                bucketId=f"{bucket_id}",
                objectId=f"{blob.name}",
                eventType="DAPLA-REPUBLISH",
            ),
        )

        # Non-blocking. Publish failures are handled in the callback function.
        publish_future.add_done_callback(_get_callback(publish_future, blob.name))
        publish_futures.append(publish_future)

    # Wait for all the publish futures to resolve before exiting.
    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)
    print(f"Messages published to {topic_path}")


def _extract_project_name(project_id: str) -> str:
    """Extracts the project name from a project ID.

    The project ID is expected to be in the format "<project-name>-<unique-id>".
    This function extracts the project name by matching the project ID up to the
    last hyphen, and removing everything after it.

    Args:
        project_id (str): The GCP project ID to extract the name from.

    Returns:
        str: The GCP project name extracted from the project ID.

    Raises:
        ValueError: If the project ID is not in the expected format.
    """
    match = re.match(r"^(.*)-[^-]+$", project_id)
    if match:
        return match.group(1)
    else:
        raise ValueError(
            f"Invalid project ID: {project_id}, The project ID is expected to be in the format "
            f"<project-name>-<unique-id>"
        )


def trigger_source_data_processing(
    project_id: str, source_name: str, folder_prefix: str, kuben: bool = False
) -> None:
    """Triggers a source data processing service with every file that has a given prefix.

    Args:
        project_id (str): The ID of Google Cloud project containing the pubsub topic, this is normally the standard project.
        folder_prefix (str): The folder prefix of the files to be processed.
        source_name (str): The name of source that should process the files.
        kuben (bool): Whether the team is on kuben or legacy.
    """
    project_name = _extract_project_name(project_id)

    if kuben:
        bucket_id = f"ssb-{project_name.rsplit('-', 1)[0]}-data-kilde-test"
    else:
        bucket_id = f"ssb-{project_name}-data-kilde"

    _publish_gcs_objects_to_pubsub(
        project_id, bucket_id, folder_prefix, topic_id=f"update-{source_name}"
    )
