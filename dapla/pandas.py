from .auth import AuthClient
from .files import FileClient
from pandas import read_csv, read_json, read_fwf, DataFrame
from typing import Any, Dict, List, Optional
# import this module to trigger import side-effect and register the pyarrow extension types
import pandas.core.arrays.arrow.extension_types  # noqa: F401


def read_pandas(gcs_path: str, file_format: str = "parquet", columns: List[str] = None, **kwargs) -> DataFrame:
    """
    Convenience method for reading a dataset from a given GCS path and convert it to a Pandas dataframe.
    :param gcs_path: path to the directory or file you want to get the contents of
    :param file_format: the expected file format. All file formats other than "parquet" are delegated to Pandas'
        methods like read_json, read_csv, etc.
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
        return read_json(gcs_path, storage_options=get_storage_options(), **kwargs)
    elif file_format == "csv":
        return read_csv(gcs_path, storage_options=get_storage_options(), **kwargs)
    elif file_format == "fwf":
        return read_fwf(gcs_path, storage_options=get_storage_options(), **kwargs)
    else:
        raise ValueError(f"Invalid file format {file_format}")


def write_pandas(df: DataFrame, gcs_path: str, file_format: str = "parquet", **kwargs) -> DataFrame:
    """
    Convenience method for writing a pandas dataframe to a given GCS path.
    :param df: the pandas' dataset
    :param gcs_path: path to the file you want to write to - must have a suffix that corresponds to the file_format
    :param file_format: file format of file you want to write to
    Other arguments are passed through to the pyarrow write_table method.
    """
    import pyarrow.parquet
    import pandas.io.parquet
    pandas.io.parquet.BaseImpl.validate_dataframe(df)

    if file_format == "parquet":
        # Transfom and write pandas dataframe
        from_pandas_kwargs = {"schema": kwargs.pop("schema", None)}
        table = pyarrow.Table.from_pandas(df, preserve_index=True, **from_pandas_kwargs)
        if '.parquet' not in gcs_path:
            raise Exception('Path must be a parquet file')
        fs = FileClient.get_gcs_file_system()
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
        df.to_json(gcs_path, storage_options=get_storage_options(), **kwargs)
    elif file_format == "csv":
        df.to_csv(gcs_path, storage_options=get_storage_options(), **kwargs)
    else:
        raise ValueError(f"Invalid file format {file_format}")


def get_storage_options() -> Optional[Dict[str, Any]]:
    """
    Returns the ``storage_options`` that are used in Pandas for specifying extra options for a particular storage
    connection that will be parsed by ``fsspec``. In this case when the URL starts with "gcs://".
    An error will be raised by Pandas if providing this argument with a local path or a file-like buffer.
    See the fsspec and backend storage implementation docs for the set of allowed keys and values
    """
    token = AuthClient.fetch_google_credentials()
    return {"token": token} if token is not None else None

