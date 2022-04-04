from .files import FileClient
from .auth import AuthClient
from pandas import read_csv, read_json
# import utils to register the pyarrow extension types
import pandas.core.arrays._arrow_utils  # noqa


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


def read_pandas(gcs_path, file_format="parquet", columns=None, **kwargs):
    """
    Backported dapla function read a dataset for a given GCS path and convert it to a Pandas dataframe.
    :param gcs_path: path to the directory or file you want to get the contents of
    :param file_format: the expected file format. All file formats other than "parquet" are delegated to Pandas'
        read_table method
    :param columns: If not None, only these columns will be read
    :return: a pandas dataframe

    Other arguments are passed through to the to_pandas method.
    """
    if file_format == "parquet":
        import pyarrow.parquet as pq
        fs = FileClient.get_gcs_file_system()
        parquet_ds = pq.ParquetDataset(gcs_path, filesystem=fs)
        return parquet_ds.read_pandas(columns=columns).to_pandas(split_blocks=False, self_destruct=True, **kwargs)
    elif file_format == "json":
        return read_json(gcs_path, storage_options=_get_storage_options(), **kwargs)
    elif file_format == "csv":
        return read_csv(gcs_path, storage_options=_get_storage_options(), **kwargs)
    else:
        raise ValueError(f"Invalid file format {file_format}")


def write_pandas(df, gcs_path, file_format="parquet", **kwargs):
    """
    Backported dapla function to write a pandas dataset to a given GCS path.
    :param df: the pandas dataset
    :param gcs_path: path to the file you want to write to - must have a suffix that corresponds to the file_format
    :param file_format: file format of file you want to write to
    Other arguments are passed through to the pyarrow write_table method.
    """
    import pyarrow.parquet
    import pandas.io.parquet
    pandas.io.parquet.BaseImpl.validate_dataframe(df)
    fs = FileClient.get_gcs_file_system()

    if file_format == "parquet":
        # Transfom and write pandas dataframe
        from_pandas_kwargs = {"schema": kwargs.pop("schema", None)}
        table = pyarrow.Table.from_pandas(df, preserve_index=True, **from_pandas_kwargs)
        if '.parquet' not in gcs_path:
            raise Exception('Path must be a parquet file')
        with fs.open(gcs_path, mode="wb") as buffer:
            pyarrow.parquet.write_table(
                table,
                buffer,
                flavor={'spark'},
                compression="snappy",
                coerce_timestamps="ms",
                **kwargs,
            )
    elif file_format == "json":
        df.to_json(gcs_path, storage_options=_get_storage_options(), **kwargs)
    elif file_format == "csv":
        df.to_csv(gcs_path, storage_options=_get_storage_options(), **kwargs)
    else:
        raise ValueError(f"Invalid file format {file_format}")


def _get_storage_options():
    token = AuthClient.fetch_google_credentials()
    return {"token": token} if token is not None else None


def _trimmed_name(o):
    return o['name'].lstrip(o['bucket'])


def _folder_item(o):
    return {'Size': o['size'], 'Name': _trimmed_name(o)}


def _file_item(o):
    return {'Size': o['size'], 'Name': _trimmed_name(o), 'Created': o['timeCreated'], 'Updated': o['updated']}
