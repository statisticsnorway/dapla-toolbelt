from enum import Enum
from enum import unique


@unique
class DaplaEnvironment(Enum):
    """Represents the 'DAPLA_ENVIRONMENT' environment variable."""

    DEV = "DEV"
    STAGING = "STAGING"
    TEST = "TEST"
    PROD = "PROD"


class DaplaService(Enum):
    """Represents the 'DAPLA_SERVICE' environment variable."""

    JUPYTERLAB = "JUPYTERLAB"
    VS_CODE = "VS_CODE"
    R_STUDIO = "R_STUDIO"
    CLOUD_RUN = "CLOUD_RUN"


class DaplaRegion(Enum):
    """Represents the 'DAPLA_REGION' environment variable."""

    ON_PREM = "ON_PREM"
    DAPLA_LAB = "DAPLA_LAB"
    BIP = "BIP"
    CLOUD_RUN = "CLOUD_RUN"
