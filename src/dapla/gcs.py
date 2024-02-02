import typing as t
from typing import Any
from typing import Optional

import gcsfs
from google.oauth2.credentials import Credentials


class GCSFileSystem(gcsfs.GCSFileSystem):  # type: ignore [misc]
    """GCSFileSystem is a wrapper around gcsfs.GCSFileSystem."""

    def __init__(
        self, token: Optional[dict[str, str] | str | Credentials] = None, **kwargs: Any
    ) -> None:
        """Initialize GCSFileSystem."""
        super().__init__(token=token, **kwargs)
        # Temporary bug fix for https://issues.apache.org/jira/browse/ARROW-7867
        # Spark writes an empty file to GCS (to mimic a folder structure) before writing partitioned data
        # Resolve this by ignoring the "empty" file when reading partitioned parquet files
        try:
            # Constant is moved to core module in Pyarrow 10.0.0
            from pyarrow.parquet.core import (  # type: ignore [attr-defined]
                EXCLUDED_PARQUET_PATHS,
            )
        except ImportError:
            # Fallback for Pyarrow versions <10.0.0
            from pyarrow.parquet.core import (  # type: ignore [attr-defined]
                EXCLUDED_PARQUET_PATHS,
            )
        from pyarrow.parquet import ParquetManifest

        EXCLUDED_PARQUET_PATHS.add("")
        ParquetManifest._should_silently_exclude = (  # type: ignore [attr-defined]
            GCSFileSystem._should_silently_exclude
        )

    def isdir(self, path: str) -> bool:
        """Check if path is a directory."""
        info = super(gcsfs.GCSFileSystem, self).info(path)
        return t.cast(bool, info["type"] == "directory")

    @staticmethod
    # This code is from from pyarrow.parquet.core
    def _should_silently_exclude(file_name: str) -> bool:
        try:
            # Constant is moved to core module in Pyarrow 10.0.0
            from pyarrow.parquet.core import (  # type: ignore [attr-defined]
                EXCLUDED_PARQUET_PATHS,
            )
        except ImportError:
            # Fallback for Pyarrow versions <10.0.0
            from pyarrow.parquet.core import (  # type: ignore [attr-defined]
                EXCLUDED_PARQUET_PATHS,
            )

        return (
            file_name.endswith(".crc")
            or file_name.endswith("_$folder$")  # Checksums
            or file_name.startswith(".")  # HDFS directories in S3
            or file_name.startswith("_")  # Hidden files starting with .
            or ".tmp" in file_name  # Hidden files starting with _
            or file_name in EXCLUDED_PARQUET_PATHS  # Temp files
        )
