from dapla.gcs import GCSFileSystem
from dapla.pandas import read_pandas


def test_instance():
    # Chack that instantiation works with the current version of pyarrow
    client = GCSFileSystem()
    assert client is not None


def test_read_partitioned_parquet_gcs():
    result = read_pandas(
        "gs://ssb-staging-dapla-felles-data-delt/felles/konto/partition"
    )
    print(result.head(5))
    assert result.get("innskudd")[1] == 2000
