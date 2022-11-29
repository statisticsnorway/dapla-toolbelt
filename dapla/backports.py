from .files import FileClient


def show(gcs_path):
    """
    Backported dapla function to recursively show all files and folders below a given GCS path
    :param gcs_path: the path from which you want to list all files and folders
    :return: a list of files and folders
    """
    fs = FileClient.get_gcs_file_system()
    out = dict()
    files = None
    for path, dirs, files in fs.walk(gcs_path, detail=True):
        out.update({_trimmed_name(info): info for name, info in files.items()})
    if len(out) == 0 and files is None:
        return []
    # Handle leaf nodes
    elif len(out) == 0:
        out.update({_trimmed_name(info): info for name, info in files.items()})
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
