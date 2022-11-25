from .files import FileClient


def show(gcs_path):
    """
    Backported dapla function to recursively show all folders below a given GCS path
    :param gcs_path: the path from which you want to list all folders
    :return: a simplified list of files or folders
    """
    fs = FileClient.get_gcs_file_system()
    out = dict()
    files = None
    for path, dirs, files in fs.walk(gcs_path, detail=True):
        out.update({_trimmed_name(info): info for name, info in dirs.items()})
    # Add the base path (if it exists) to avoid an empty list when there are no subfolders
    if len(out) == 0 and files is not None:
        # Get the bucket name from any of the files
        bucket_name = list(files.values())[0]['bucket']
        trimmed_name = gcs_path.lstrip('gs://').lstrip(bucket_name).rstrip('/')
        out[trimmed_name] = {}
    return sorted(out)


def details(gcs_path):
    """
    Backported dapla function to support detailed list of files for a given GCS path
    :param gcs_path: path to the file(s) you want to get the contents of
    :return: a detailed list of files
    """
    fs = FileClient.get_gcs_file_system()
    return list(map(lambda o: _folder_item(o) if o['storageClass'] == 'DIRECTORY' else _file_item(o),
                    fs.ls(gcs_path, detail=True)))


def _trimmed_name(o):
    return o['name'].lstrip(o['bucket'])


def _folder_item(o):
    return {'Size': o['size'], 'Name': _trimmed_name(o)}


def _file_item(o):
    return {'Size': o['size'], 'Name': _trimmed_name(o), 'Created': o['timeCreated'], 'Updated': o['updated']}
