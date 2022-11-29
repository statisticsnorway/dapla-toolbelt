import mock
from dapla.backports import show, details
from dapla.gcs import GCSFileSystem


@mock.patch('dapla.backports.FileClient')
def test_show_subfolders_and_files(file_client_mock):
    file_client_mock.get_gcs_file_system.return_value = GCSFileSystem()
    result = show('gs://anaconda-public-data/projects-data')
    assert result == ['/projects-data/',
                      '/projects-data/tensorboard/',
                      '/projects-data/tensorboard/mnist_tutorial.zip']


@mock.patch('dapla.backports.FileClient')
def test_show_leaf_folder_with_file(file_client_mock):
    file_client_mock.get_gcs_file_system.return_value = GCSFileSystem()
    result = show('gs://anaconda-public-data/iris')
    assert result == ['/iris/iris.csv']


@mock.patch('dapla.backports.FileClient')
def test_show_leaf_folder_with_file_details(file_client_mock):
    file_client_mock.get_gcs_file_system.return_value = GCSFileSystem()
    result = details('gs://anaconda-public-data/iris')
    assert result == [{'Created': '2017-04-05T21:51:42.503Z',
                       'Name': '/iris/iris.csv',
                       'Size': 4636,
                       'Updated': '2017-04-05T21:51:42.503Z'}]


@mock.patch('dapla.backports.FileClient')
def test_show_invalid_folder(file_client_mock):
    file_client_mock.get_gcs_file_system.return_value = GCSFileSystem()
    result = show('gs://anaconda-public-data/nyc-taxi/unknown')
    assert result == []
