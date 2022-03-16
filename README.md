# dapla-toolbelt

Python module for use within Jupyterlab notebooks, specifically aimed for Statistics Norway's data platform called 
`Dapla`. It contains support for authenticated access to Google Services such as Google Cloud Storage (GCS) and custom
Dapla services such as [Maskinporten Guardian](https://github.com/statisticsnorway/maskinporten-guardian). The 
authentication process is based on the [TokenExchangeAuthenticator](https://github.com/statisticsnorway/jupyterhub-extensions/tree/main/TokenExchangeAuthenticator)
for Jupyterhub.

[![PyPI version](https://img.shields.io/pypi/v/dapla-toolbelt.svg)](https://pypi.python.org/pypi/dapla-toolbelt/)
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

### Dependencies

External package dependencies are specified in the `setup.py` file, at `install_requires=`.

### Bumping version

Use `make` to bump the patch, minor version or major version before creating a pull request to `main`. 


The version number is located in `bumpversion.cfg`, `dapla-toolbelt/dapla-toolbelt/__init__.py` and in `dapla-toolbelt/setup.py`.
To bump the version everywhere at once, run `make bump-version-patch`, `make bump-version-minor`, or `make bump-version-major`.
Bumping must be done with a clean git working space, and automatically commits with the new version number.

### Building and/or releasing automatically

Azure pipelines will build automatically on pull request creation, merge, and commit to `main`, 
and will release the new version of the package to pypi.org automatically upon tagging a given commit.

### Building locally for development

Run `make build` to build wheel, source distribution, and check the distribution with `twine`.

Run this to check the contents of your wheel:
`tar tzf dist/dapla-toolbelt-<SEMVER>.tar.gz` 
replacing <SEMVER> with your current version number

### Manually releasing

#### Test

Run this to test package publication by uploading to TestPyPI:
`twine upload --repository-url https://test.pypi.org/legacy/ dist/*`
You will have to manually enter a username and password for a user registered to test.pypi.org in order for this to work.

#### Prod

**NB:** This should only be done as a last resort, if the regular CI/CD pipeline (azure pipeline) does not work
and it's necessary to release regardless.

In order to publish a new version of the package to PyPI for real, run this:
`twine upload dist/*`
Authenticate with your regular pypi.org username and password. 

