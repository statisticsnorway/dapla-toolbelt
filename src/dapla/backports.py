from .files import FileClient


def show(gcs_path: str) -> list[str]:
    """Backported dapla function to recursively show all folders below a given GCS path.

    Args:
        gcs_path: the path from which you want to list all folders

    Returns:
        A simplified list of files or folders
    """
    fs = FileClient.get_gcs_file_system()
    out = dict()
    files = None
    for _path, dirs, files in fs.walk(gcs_path, detail=True):  # noqa: B007
        out.update({_trimmed_name(info): info for name, info in dirs.items()})
    # Add the base path (if it exists) to avoid an empty list when there are no subfolders
    if len(out) == 0 and files is not None:
        # Get the bucket name from any of the files
        bucket_name = next(iter(files.values()))["bucket"]
        trimmed_name = gcs_path.replace("gs://", "").lstrip(bucket_name).rstrip("/")
        out[trimmed_name] = {}
    return sorted(out)


def details(gcs_path: str) -> list[dict[str, str]]:
    """Backported dapla function to support detailed list of files for a given GCS path.

    Args:
        gcs_path: the path from which you want to list all folders

    Returns:
        A list of dicts containing file details
    """
    fs = FileClient.get_gcs_file_system()
    return list(
        map(
            lambda o: (
                _folder_item(o) if o["storageClass"] == "DIRECTORY" else _file_item(o)
            ),
            fs.ls(gcs_path, detail=True),
        )
    )


def _trimmed_name(o: dict[str, str]) -> str:
    return o["name"].lstrip(o["bucket"])


def _folder_item(o: dict[str, str]) -> dict[str, str]:
    return {"Size": o["size"], "Name": _trimmed_name(o)}


def _file_item(o: dict[str, str]) -> dict[str, str]:
    return {
        "Size": o["size"],
        "Name": _trimmed_name(o),
        "Created": o["timeCreated"],
        "Updated": o["updated"],
    }
