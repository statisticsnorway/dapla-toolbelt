"""Dapla Toolbelt collection."""

import importlib.metadata
import warnings

import tomli

from .auth import AuthClient
from .backports import details
from .backports import show
from .collector import CollectorClient
from .converter import ConverterClient
from .doctor import Doctor
from .files import FileClient
from .git import repo_root_dir
from .guardian import GuardianClient
from .jupyterhub import generate_api_token
from .pandas import read_pandas
from .pandas import write_pandas
from .pubsub import trigger_source_data_processing

__all__ = [
    "AuthClient",
    "details",
    "show",
    "CollectorClient",
    "ConverterClient",
    "Doctor",
    "FileClient",
    "repo_root_dir",
    "GuardianClient",
    "generate_api_token",
    "read_pandas",
    "write_pandas",
    "trigger_source_data_processing",
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
