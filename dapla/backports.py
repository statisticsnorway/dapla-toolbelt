from .files import FileClient


def show(gcs_path):
    """
    Backported dapla function to support a simplified list of files or folders for a given GCS path
    :param gcs_path: path or paths to the file(s) you want to get the contents of
    :return: a simplified list of files or folders
    """
    fs = FileClient.get_gcs_file_system()
    return list(map(lambda o:  _trimmed_name(o), fs.ls(gcs_path, detail=True)))


def details(gcs_path):
    """
    Backported dapla function to support detailed list of files or folders for a given GCS path
    :param gcs_path: path or paths to the file(s) you want to get the contents of
    :return: a detailed list of files or folders
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


