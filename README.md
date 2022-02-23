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
