import os
import typing as t
from typing import Any

import gcsfs

from dapla import AuthClient
from dapla.const import DaplaRegion


class GCSFileSystem(gcsfs.GCSFileSystem):  # type: ignore [misc]
    """GCSFileSystem is a wrapper around gcsfs.GCSFileSystem."""

    def __init__(self, **kwargs: Any) -> None:
        """Initialize GCSFileSystem."""
        if (
            os.getenv("DAPLA_REGION") == DaplaRegion.DAPLA_LAB.value
            or os.getenv("DAPLA_REGION") == DaplaRegion.CLOUD_RUN.value
        ):
            # When using environments with ADC, return a GCSFS using auth
            # from the environment
            super().__init__(**kwargs)
        else:
            super().__init__(token=AuthClient.fetch_google_credentials(), **kwargs)

    def isdir(self, path: str) -> bool:
        """Check if path is a directory."""
        info = super(gcsfs.GCSFileSystem, self).info(path)
        return t.cast(bool, info["type"] == "directory")
