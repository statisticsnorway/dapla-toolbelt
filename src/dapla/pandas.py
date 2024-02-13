from __future__ import annotations

import typing as t
from enum import Enum
from typing import Any
from typing import Optional

# import this module to trigger import side-effect and register the pyarrow extension types
import pandas.core.arrays.arrow.extension_types  # type: ignore [import-untyped] # noqa: F401
import pyarrow.compute
from google.oauth2.credentials import Credentials
from pandas import DataFrame
from pandas import Series
from pandas import read_csv
from pandas import read_excel
from pandas import read_fwf
from pandas import read_json
from pandas import read_sas
from pandas import read_xml

from .auth import AuthClient
from .files import FileClient


class SupportedFileFormat(Enum):
    """A collection of supported file formats."""

    PARQUET = "parquet"
    JSON = "json"
    CSV = "csv"
    FWF = "fwf"
    XML = "xml"
    SAS7BDAT = "sas7bdat"
    EXCEL = "excel"

    @classmethod
    def _missing_(cls, value: object) -> None:
        raise ValueError(
            f"{value} is not a valid file format. Valid formats: %s"
            % (", ".join([repr(m.value) for m in cls]))
        )


def read_pandas(
    gcs_path: str | list[str],
    file_format: Optional[str] = "parquet",
    columns: Optional[list[str]] = None,
    filters: Optional[
        list[tuple[Any] | list[tuple[Any]]] | pyarrow.compute.Expression
    ] = None,
    **kwargs: Any,
) -> t.Union[DataFrame, Series]:  # type: ignore [type-arg]
    """Convenience method for reading a dataset from a given GCS path and convert it to a Pandas dataframe.

    Args:
        gcs_path: Path or paths to the directory or file you want to get the contents of.
            Support multiple paths if reading parquet format.
        file_format: The expected file format. All file formats other than "parquet" are delegated to Pandas
            methods like read_json, read_csv, etc. Defaults to "parquet".
        columns: Choose specific columsn to read. Defaults to None.
        filters: Add row filter to process when reading parquet. The filter
            should follow pyarrow methods. See examples in the docs:
            https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html#pyarrow.parquet.ParquetDataset.
            Defaults to None.
        kwargs: Additional arguments to pass to the underlying Pandas "read_*()" method.

    Raises:
        ValueError: If multiple paths are provided for non-parquet formats.

    Returns:
        A Pandas DataFrame containing the selected dataset.

    """
    if isinstance(gcs_path, list) and file_format != "parquet":
        raise ValueError("Multiple paths are only supported for parquet format")
    match SupportedFileFormat(file_format):
        case SupportedFileFormat.PARQUET:
            import pyarrow.parquet as pq

            fs = FileClient.get_gcs_file_system()

            # Workaround for https://github.com/apache/arrow/issues/30481
            if isinstance(gcs_path, list):
                gcs_path = [
                    FileClient._remove_gcs_uri_prefix(file) for file in gcs_path
                ]
            else:
                gcs_path = FileClient._remove_gcs_uri_prefix(gcs_path)

            parquet_ds = pq.ParquetDataset(
                gcs_path,
                filesystem=fs,
                filters=filters,  # type: ignore [arg-type]
            )  # Stubs show the incorrect type -
            # see https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html

            return parquet_ds.read_pandas(columns=columns).to_pandas(
                split_blocks=False, self_destruct=True, **kwargs
            )
        case SupportedFileFormat.JSON:
            assert isinstance(gcs_path, str)
            return t.cast(
                "DataFrame | Series[Any]",
                read_json(gcs_path, storage_options=_get_storage_options(), **kwargs),
            )
        case SupportedFileFormat.CSV:
            assert isinstance(gcs_path, str)
            return t.cast(
                "DataFrame | Series[Any]",
                read_csv(gcs_path, storage_options=_get_storage_options(), **kwargs),
            )
        case SupportedFileFormat.FWF:
            assert isinstance(gcs_path, str)
            return t.cast(
                "DataFrame | Series[Any]",
                read_fwf(gcs_path, storage_options=_get_storage_options(), **kwargs),
            )
        case SupportedFileFormat.XML:
            assert isinstance(gcs_path, str)
            return t.cast(
                "DataFrame | Series[Any]",
                read_xml(gcs_path, storage_options=_get_storage_options(), **kwargs),
            )
        case SupportedFileFormat.EXCEL:
            assert isinstance(gcs_path, str)

            return t.cast(
                "DataFrame | Series[Any]",
                read_excel(gcs_path, storage_options=_get_storage_options(), **kwargs),
            )
        case SupportedFileFormat.SAS7BDAT:
            assert isinstance(gcs_path, str)

            fs = FileClient.get_gcs_file_system()

            with fs.open(gcs_path) as sas:
                df = read_sas(sas, format="sas7bdat", **kwargs)

            return t.cast("DataFrame | Series[Any]", df)
        case _:
            raise ValueError(f"Invalid file format {file_format}")


def write_pandas(
    df: DataFrame, gcs_path: str, file_format: str = "parquet", **kwargs: Any
) -> None:
    """Convenience method for writing a Pandas DataFrame to a given GCS path.

    Args:
        df: The Pandas DataFrame to write to file.
        gcs_path: The GCS path to the destination file. Must have an extension that corresponds to the file_format
        file_format: The expected file format. All file formats other than "parquet" are delegated to Pandas
        **kwargs: Additional arguments to pass to the underlying Pandas "to_*()" method.

    Raises:
        ValueError: If the file format is invalid.
        ValueError: If the path does not have an extension that corresponds to the file format.
    """
    import pyarrow.parquet

    match SupportedFileFormat(file_format):
        case SupportedFileFormat.PARQUET:
            # Transfom and write pandas dataframe
            from_pandas_kwargs = {"schema": kwargs.pop("schema", None)}
            table = pyarrow.Table.from_pandas(
                df, preserve_index=True, **from_pandas_kwargs
            )
            if ".parquet" not in gcs_path:
                raise ValueError("Path must be a parquet file")
            fs = FileClient.get_gcs_file_system()
            with fs.open(gcs_path, mode="wb") as buffer:
                pyarrow.parquet.write_table(
                    table,
                    buffer,
                    flavor="spark",
                    compression="snappy",
                    coerce_timestamps="ms",
                    **kwargs,
                )
        case SupportedFileFormat.JSON:
            df.to_json(gcs_path, **kwargs)
        case SupportedFileFormat.CSV:
            df.to_csv(gcs_path, storage_options=_get_storage_options(), **kwargs)
        case SupportedFileFormat.XML:
            df.to_xml(gcs_path, storage_options=_get_storage_options(), **kwargs)
        case SupportedFileFormat.EXCEL:
            # FIXME: mypy complains about `storage_options` being an unknown argument
            df.to_excel(gcs_path, storage_options=_get_storage_options(), **kwargs)  # type: ignore [call-arg]
        case SupportedFileFormat.FWF:
            raise ValueError("Writing with fixed width format is not supported")
        case SupportedFileFormat.SAS7BDAT:
            raise ValueError(
                "Writing to SAS7BDAT is not supported since it's a proprietary format"
            )
        case _:
            raise ValueError(f"Invalid file format {file_format}")


def _get_storage_options() -> Optional[dict[str, Optional[Credentials]]]:
    """Returns the ``storage_options`` that are used in Pandas for specifying extra options for a particular storage connection that will be parsed by ``fsspec``.

    In this case when the URL starts with "gcs://".
    An error will be raised by Pandas if providing this argument with a local path or a file-like buffer.
    See the fsspec and backend storage implementation docs for the set of allowed keys and values
    """
    credentials = AuthClient.fetch_google_credentials()
    return {"token": credentials} if credentials is not None else None
