# dapla-toolbelt

This package is a convenience package for authenticated and authorized GCS file access, from within a JupyterHub environment where the user is logged on with keycloak. 
It is built for the Statistics Norway data platform "Dapla", but is generally applicable in similar settings.

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

## Usage Examples

``` python
from dapla_toolbelt.files import FileClient as fcli
from dapla_toolbelt.guardian import GuardianClient as gcli
import pandas as pd

response = gcli.call_api("https://data.udir.no/api/kag", "88ace991-7871-4ccc-aaec-8fb6d78ed04e", "udir:datatilssb")
data_json = response.json()

raw_data_df = pd.DataFrame(data_json)  # create pandas data frame from json
raw_data_df.head()  # show first rows of data frame

fcli.ls("bucket-name/folder")  # list contents of given folder

# Save data into different formats
path_base = "bucket-name/folder/raw_data"
fcli.save_pandas_to_json(raw_data_df, f"{path_base}.json")  # generate json from data frame, and save to given path
fcli.save_pandas_to_csv(raw_data_df, f"{path_base}.csv")  # generate csv from data frame, and save to given path
fcli.save_pandas_to_xml(raw_data_df, f"{path_base}.xml")  # generate xml from data frame, and save to given path

fcli.cat(f"{path_base}.json")  # print contents of file

# Load data from different formats
# All these data frames should contain the same data:
df = fcli.load_json_to_pandas(f"{path_base}.json")  # read json from path and load into pandas data frame
df.head()  # show first rows of data frame
df = fcli.load_csv_to_pandas(f"{path_base}.csv")  # read csv from path and load into pandas data frame
df.head()  # show first rows of data frame
df = fcli.load_csv_to_pandas(f"{path_base}.xml")  # read xml from path and load into pandas data frame
df.head()  # show first rows of data frame

```


## Installation

`pip install dapla-toolbelt`

## Development

### Building

The version number is located in `dapla-toolbelt/dapla-toolbelt/__init__.py` and in `dapla-toolbelt/setup.py`

External package dependencies are specified in the `setup.py` file, at `install_requires=`.

In the top level where the `setup.py` is located, run
`python -m build`
to generate a source distribution and wheel.

Run this to check the contents of your wheel:
`tar tzf dist/dapla-toolbelt-<SEMVER>.tar.gz` 
replacing <SEMVER> with your current version number

Run this to check that the package description will render properly on PyPI when published:
`twine check dist/*`

### Releasing

Run this to test package publication by uploading to TestPyPI:
`twine upload --repository-url https://test.pypi.org/legacy/ dist/*`
You will have to manually enter a username and password for a user registered to test.pypi.org in order for this to work.

In order to publish a new version of the package to PyPI for real, run this:
`twine upload dist/*`
Authenticate with your regular pypi.org username and password.
