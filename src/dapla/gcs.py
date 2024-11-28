import os
import typing as t
from typing import Any
from typing import Optional

import gcsfs
from google.oauth2.credentials import Credentials

from dapla.const import DaplaRegion


class GCSFileSystem(gcsfs.GCSFileSystem):  # type: ignore [misc]
    """GCSFileSystem is a wrapper around gcsfs.GCSFileSystem."""

    def __init__(
        self, token: Optional[dict[str, str] | str | Credentials] = None, **kwargs: Any
    ) -> None:
        """Initialize GCSFileSystem."""
        if os.getenv("DAPLA_REGION") == str(DaplaRegion.DAPLA_LAB) or os.getenv(
            "DAPLA_REGION"
        ) == str(DaplaRegion.CLOUD_RUN):
            # When using environments with ADC, return a GCSFS using auth
            # from the environment
            super.__init__(**kwargs)

        super().__init__(token=token, **kwargs)

    def isdir(self, path: str) -> bool:
        """Check if path is a directory."""
        info = super(gcsfs.GCSFileSystem, self).info(path)
        return t.cast(bool, info["type"] == "directory")
