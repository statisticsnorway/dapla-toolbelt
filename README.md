# Dapla Toolbelt

[![PyPI](https://img.shields.io/pypi/v/dapla-toolbelt.svg)][pypi status]
[![Status](https://img.shields.io/pypi/status/dapla-toolbelt.svg)][pypi status]
[![Python Version](https://img.shields.io/pypi/pyversions/dapla-toolbelt)][pypi status]
[![License](https://img.shields.io/pypi/l/dapla-toolbelt)][license]

[![Documentation](https://github.com/statisticsnorway/dapla-toolbelt/actions/workflows/docs.yml/badge.svg)][documentation]
[![Tests](https://github.com/statisticsnorway/dapla-toolbelt/actions/workflows/tests.yml/badge.svg)][tests]
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=statisticsnorway_dapla-toolbelt&metric=coverage)][sonarcov]
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=statisticsnorway_dapla-toolbelt&metric=alert_status)][sonarquality]

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)][pre-commit]
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)][black]
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)][poetry]

[pypi status]: https://pypi.org/project/dapla-toolbelt/
[documentation]: https://statisticsnorway.github.io/dapla-toolbelt
[tests]: https://github.com/statisticsnorway/dapla-toolbelt/actions?workflow=Tests

[sonarcov]: https://sonarcloud.io/summary/overall?id=statisticsnorway_dapla-toolbelt
[sonarquality]: https://sonarcloud.io/summary/overall?id=statisticsnorway_dapla-toolbelt
[pre-commit]: https://github.com/pre-commit/pre-commit
[black]: https://github.com/psf/black
[poetry]: https://python-poetry.org/

Python module for use within Jupyterlab notebooks, specifically aimed for Statistics Norway's data platform called
`Dapla`. It contains support for authenticated access to Google Services such as Google Cloud Storage (GCS) and custom
Dapla services such as [Maskinporten Guardian](https://github.com/statisticsnorway/maskinporten-guardian). The
authentication process is based on the [TokenExchangeAuthenticator](https://github.com/statisticsnorway/jupyterhub-extensions/tree/main/TokenExchangeAuthenticator)
for Jupyterhub.

## Features

These operations are supported:
* List contents of a bucket
* Open a file in GCS
* Copy a file from GCS into local
* Load a file (CSV, JSON or XML) from GCS into a pandas dataframe
* Save contents of a data frame into a file (CSV, JSON, XML) in GCS

When the user gives the path to a resource, they do not need to give the GCS uri, only the path.
This just means users don't have to prefix a path with "gs://".
It is implicitly understood that all resources accessed with this tool are located in GCS,
with the first level of the path being a GCS bucket name.

## Requirements

- Python >3.8 (3.10 is preferred)
- Poetry, install via `curl -sSL https://install.python-poetry.org | python3 -`

## Installation

You can install _Dapla Toolbelt_ via [pip] from [PyPI]:

```console
pip install dapla-toolbelt
```

## Usage

``` python
from dapla import FileClient
from dapla import GuardianClient
import pandas as pd

# Load data using the Maskinporten Guardian client
response = GuardianClient.call_api("https://data.udir.no/api/kag", "88ace991-7871-4ccc-aaec-8fb6d78ed04e", "udir:datatilssb")
data_json = response.json()

raw_data_df = pd.DataFrame(data_json)  # create pandas data frame from json
raw_data_df.head()  # show first rows of data frame

FileClient.ls("bucket-name/folder")  # list contents of given folder

# Save data into different formats
path_base = "bucket-name/folder/raw_data"
FileClient.save_pandas_to_json(raw_data_df, f"{path_base}.json")  # generate json from data frame, and save to given path
FileClient.save_pandas_to_csv(raw_data_df, f"{path_base}.csv")  # generate csv from data frame, and save to given path
FileClient.save_pandas_to_xml(raw_data_df, f"{path_base}.xml")  # generate xml from data frame, and save to given path

FileClient.cat(f"{path_base}.json")  # print contents of file

# Load data from different formats
# All these data frames should contain the same data:
df = FileClient.load_json_to_pandas(f"{path_base}.json")  # read json from path and load into pandas data frame
df.head()  # show first rows of data frame
df = FileClient.load_csv_to_pandas(f"{path_base}.csv")  # read csv from path and load into pandas data frame
df.head()  # show first rows of data frame
df = FileClient.load_xml_to_pandas(f"{path_base}.xml")  # read xml from path and load into pandas data frame
df.head()  # show first rows of data frame

```

## Contributing

Contributions are very welcome.
To learn more, see the [Contributor Guide].

## License

Distributed under the terms of the [MIT license][license],
_Dapla Toolbelt_ is free and open source software.

## Issues

If you encounter any problems,
please [file an issue] along with a detailed description.

## Credits

This project was generated from [Statistics Norway]'s [SSB PyPI Template].

[statistics norway]: https://www.ssb.no/en
[pypi]: https://pypi.org/
[ssb pypi template]: https://github.com/statisticsnorway/ssb-pypitemplate
[file an issue]: https://github.com/statisticsnorway/dapla-toolbelt/issues
[pip]: https://pip.pypa.io/

<!-- github-only -->

[license]: https://github.com/statisticsnorway/dapla-toolbelt/blob/main/LICENSE
[contributor guide]: https://github.com/statisticsnorway/dapla-toolbelt/blob/main/CONTRIBUTING.md
[reference guide]: https://statisticsnorway.github.io/dapla-toolbelt/reference.html
