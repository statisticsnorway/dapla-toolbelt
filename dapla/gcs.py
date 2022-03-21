import gcsfs


class GCSFileSystem(gcsfs.GCSFileSystem):

    def __init__(self, token=None, **kwargs):
        super().__init__(token=token, **kwargs)
        # Temporary bug fix for https://issues.apache.org/jira/browse/ARROW-7867
        # Spark writes an empty file to GCS (to mimic a folder structure) before writing partitioned data
        # Resolve this by ignoring the "empty" file when reading partitioned parquet files
        from pyarrow.parquet import EXCLUDED_PARQUET_PATHS
        from pyarrow.parquet import ParquetManifest
        EXCLUDED_PARQUET_PATHS.add('')
        ParquetManifest._should_silently_exclude = GCSFileSystem._should_silently_exclude

    def isdir(self, path):
        info = super(gcsfs.GCSFileSystem, self).info(path)
        return info["type"] == "directory"

    @staticmethod
    def _should_silently_exclude(self, file_name):
        from pyarrow.parquet import EXCLUDED_PARQUET_PATHS
        return (file_name.endswith('.crc') or  # Checksums
                file_name.endswith('_$folder$') or  # HDFS directories in S3
                file_name.startswith('.') or  # Hidden files starting with .
                file_name.startswith('_') or  # Hidden files starting with _
                '.tmp' in file_name or  # Temp files
                file_name in EXCLUDED_PARQUET_PATHS)

