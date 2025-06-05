from enum import Enum


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
    CLOUD_RUN = "CLOUD_RUN"


GUARDIAN_URLS = {
    DaplaEnvironment.DEV: "https://guardian.intern.test.ssb.no/maskinporten/access-token",
    DaplaEnvironment.STAGING: "https://guardian.intern.test.ssb.no/maskinporten/access-token",
    DaplaEnvironment.TEST: "https://guardian.intern.test.ssb.no/maskinporten/access-token",
    DaplaEnvironment.PROD: "https://guardian.intern.ssb.no/maskinporten/access-token",
}
