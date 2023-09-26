# dapla-toolbelt

Python module for use within Jupyterlab notebooks, specifically aimed for Statistics Norway's data platform called
`Dapla`. It contains support for authenticated access to Google Services such as Google Cloud Storage (GCS) and custom
Dapla services such as [Maskinporten Guardian](https://github.com/statisticsnorway/maskinporten-guardian). The
authentication process is based on the [TokenExchangeAuthenticator](https://github.com/statisticsnorway/jupyterhub-extensions/tree/main/TokenExchangeAuthenticator)
for Jupyterhub.

[![PyPI version](https://img.shields.io/pypi/v/dapla-toolbelt.svg)](https://pypi.python.org/pypi/dapla-toolbelt/)
![Unit tests](https://github.com/statisticsnorway/dapla-toolbelt/actions/workflows/unit-tests.yml/badge.svg)
![Code coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/bjornandre/73205f2f30335801fa2819c31b3ecf79/raw/pytest-coverage-badge-dapla-toolbelt.json)
![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)
[![License](https://img.shields.io/pypi/l/dapla-toolbelt.svg)](https://pypi.python.org/pypi/dapla-toolbelt/)

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

## Installation

`pip install dapla-toolbelt`

## Usage Examples

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

## Development

### Prerequisites

- Python >3.8 (3.10 is preferred)
- Poetry, install via `curl -sSL https://install.python-poetry.org | python3 -`

You can also execute `make` in the project folder to see available `make` commands.

### Dependency Management

Poetry is used for dependency management. [Poe the Poet](https://github.com/nat-n/poethepoet) is used for running poe tasks within poetry's virtualenv.
Upon cloning this project first install necessary dependencies, then run the tests to verify everything is working.

#### Install all dependencies

```shell
poetry install
```

### Add dependencies

#### Main

```shell
poetry add <python package name>
```

#### Dev

```shell
poetry add --group dev <python package name>
```

### Run tests

```shell
poetry run poe test
```

### Run project locally in Jupyter

To run the project locally in Jupyter run:

```shell
poetry run poe jupyter
```

A Jupyter instance should open in your browser. Open and run the cells in the `demo.ipynb` file.

### Bumping version

Use `make` to bump the *patch*, *minor* version or *major* version before creating a pull request to the `main` GIT branch.
Or run a poe task like this:

```shell
poetry run poe bump-version-patch
```

You can use either `bump-version-patch`, `bump-version-minor`, or `bump-version-major`.
Bumping must be done with a clean git working space, and automatically commits with the new version number.

Then just run `git push origin --tags` to push the changes and trigger the release process.

### Building and releasing

Before merging your changes into the `main` branch, make sure you have bumped the version like outlined above.

An automatic release process will build *dapla-toolbelt* upon pull request-creation, merges, and direct commits to the
`main` GIT branch. It will also release a new version of the package to **pypi.org** automatically when a commit is
tagged, for example by a GitHub release.

### Building and releasing manually

Run `make build` to build a wheel and a source distribution.

Run `make release-validate` to do all that AND validate it for release.

Run this (replacing <SEMVER> with your current version number) to check the contents of your wheel:
`tar tzf dist/dapla-toolbelt-<SEMVER>.tar.gz`

#### Test release

You have to bump the version of the package (see documentation on "Bumping version" above) before releasing,
because even test.pypi.org does not allow re-releases of a previously released version.

Run the following command in order to build, validate, and test package publication by uploading to TestPyPI:
`make release-test`

You will have to manually enter a username and password for a user registered to [test.pypi.org](https://test.pypi.org)
in order for this to work.

#### Production release

**NB: A manual production release should only be done as a last resort**, if the regular CI/CD pipeline
does not work, and it's necessary to release regardless.

You have to bump the version of the package (see documentation on "Bumping version" above) to something
different from the last release before releasing.

In order to publish a new version of the package to PyPI for real, run `make release`.
Authenticate by manually entering your [pypi.org](https://pypi.org) username and password.
