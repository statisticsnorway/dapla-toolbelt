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

    def isdir(self, path: str) -> bool:
        """Check if path is a directory."""
        info = super(gcsfs.GCSFileSystem, self).info(path)
        return t.cast(bool, info["type"] == "directory")
