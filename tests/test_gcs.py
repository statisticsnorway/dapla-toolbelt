from dapla.gcs import GCSFileSystem


def test_parquet_read():
    import pyarrow.parquet as pq
    fs = GCSFileSystem(token=None)
    parquet_ds = pq.ParquetDataset('gs://anaconda-public-data/nyc-taxi/nyc.parquet/part.0.parquet', filesystem=fs)
    table = parquet_ds.read_pandas().to_pandas(split_blocks=False, self_destruct=True)
    print(table.head(5))
    pass
