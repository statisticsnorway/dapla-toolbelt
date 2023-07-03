from pathlib import Path


def repo_root_dir(directory: Path | str = Path.cwd()) -> Path:
    """Find the root directory of a git repo, searching upwards from a given path.

    Args:
        directory: The path to search from, defaults to the current working directory.
            The directory can be of type string or of type pathlib.Path.

    Returns:
        Path to the git repo's root directory.

    Raises:
        RuntimeError: If no .git directory is found when searching upwards.

    Example
    --------
    >>> import dapla as dp
    >>> import tomli
    >>>
    >>> config_file = dp.repo_root_dir() / "config" / "config.toml"
    >>> with open(config_file, mode="rb") as fp:
    >>>     config = tomli.load(fp)
    """
    if isinstance(directory, str):
        directory = Path(directory)

    while directory / ".git" not in directory.iterdir():
        if directory == Path("/"):
            raise RuntimeError(f"The directory {directory} is not in a git repo.")
        else:
            directory = directory.parent
    return directory
