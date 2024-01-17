# Test for FileClient class


from dapla import FileClient

PATH_WITH_PREFIX = "gs://bucket/path"
PATH_WITHOUT_PREFIX = "bucket/path"


def test_ensure_gcs_uri_prefix() -> None:
    assert FileClient._ensure_gcs_uri_prefix(PATH_WITH_PREFIX) == PATH_WITH_PREFIX
    assert FileClient._ensure_gcs_uri_prefix(PATH_WITHOUT_PREFIX) == PATH_WITH_PREFIX


def test_remove_gcs_uri_prefix() -> None:
    assert FileClient._remove_gcs_uri_prefix(PATH_WITH_PREFIX) == PATH_WITHOUT_PREFIX
    assert FileClient._remove_gcs_uri_prefix(PATH_WITHOUT_PREFIX) == PATH_WITHOUT_PREFIX
