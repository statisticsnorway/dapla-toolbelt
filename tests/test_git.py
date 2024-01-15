from pathlib import Path

import pytest

from dapla.git import repo_root_dir


def test_repo_root_dir() -> None:
    # Directory in a git repo
    repo_root = repo_root_dir()
    assert repo_root is not None

    # Directory outside a git repo
    with pytest.raises(RuntimeError):
        repo_root_dir(Path("/"))

    # Directory as string instead of pathlib.Path
    dir_string = str(Path.cwd())
    repo_root_str = repo_root_dir(dir_string)
    assert repo_root_str is not None
