from dapla.gcs import GCSFileSystem


def test_instance() -> None:
    # Chack that instantiation works with the current version of pyarrow
    client = GCSFileSystem()
    assert client is not None
