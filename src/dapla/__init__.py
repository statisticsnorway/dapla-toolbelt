"""Dapla Toolbelt collection."""

import importlib.metadata
import warnings

warnings.simplefilter("default", DeprecationWarning)

import tomli

# Importing Authclient here so its exposed from dapla-toolbelt
# and also is importable with `from dapla import AuthClient`
from dapla_auth_client import AuthClient

from .backports import details
from .backports import show
from .collector import CollectorClient
from .converter import ConverterClient
from .doctor import Doctor
from .files import FileClient
from .git import repo_root_dir
from .gsm import get_secret_version
from .guardian import GuardianClient
from .pandas import read_pandas
from .pandas import write_pandas
from .pubsub import trigger_source_data_processing

__all__ = [
    "AuthClient",
    "CollectorClient",
    "ConverterClient",
    "Doctor",
    "FileClient",
    "GuardianClient",
    "details",
    "get_secret_version",
    "read_pandas",
    "repo_root_dir",
    "show",
    "trigger_source_data_processing",
    "write_pandas",
]


# Register function for second try: getting from pyproject directly (when running under pytest for example)
def _try_getting_pyproject_toml(e: Exception | None = None) -> str:
    """Look for version in pyproject.toml, if not found, set to 0.0.0 ."""
    if e is None:
        passed_excep: Exception = Exception("")
    else:
        passed_excep = e
    try:
        with open("../pyproject.toml", "rb") as f:
            toml_dict = tomli.load(f)
        version: str = toml_dict["tool"]["poetry"]["version"]
        return version
    except Exception as e:
        version_missing: str = "0.0.0"
        warn_msg = f"Error from dapla-toolbelt __init__, not able to get version-number, setting it to {version_missing}: {e}, passed in from importlib: {passed_excep}"
        warnings.warn(warn_msg, category=Warning, stacklevel=2)
        return version_missing


# First try is setting __version__ attribute programatically from installed package
try:
    __version__ = importlib.metadata.version("dapla-toolbelt")
except importlib.metadata.PackageNotFoundError as e:
    __version__ = _try_getting_pyproject_toml(e)
