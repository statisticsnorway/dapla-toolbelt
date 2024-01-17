from pathlib import Path
from typing import Optional


def repo_root_dir(directory: Optional[Path | str] = None) -> Path:
    """Find the root directory of a git repo, searching upwards from a given path.

    Args:
        directory: The path to search from, defaults to the current working directory.
            The directory can be of type string or of type pathlib.Path.

    Returns:
        Path to the git repo's root directory.

    Raises:
        RuntimeError: If no .git directory is found when searching upwards.

    Example:
    --------
    >>> import dapla as dp
    >>> import tomli
    >>>
    >>> config_file = dp.repo_root_dir() / "pyproject.toml"
    >>> with open(config_file, mode="rb") as fp:
    >>>     config = tomli.load(fp)
    """
    if directory is None:
        directory = Path.cwd()

    if isinstance(directory, str):
        directory = Path(directory)

    while directory / ".git" not in directory.iterdir():
        if directory == Path("/"):
            raise RuntimeError(f"The directory {directory} is not in a git repo.")
        else:
            directory = directory.parent
    return directory
