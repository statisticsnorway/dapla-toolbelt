# dapla-toolbelt

This package is a convenience package for authenticated and authorized file operations against GCS from within a Jupyter environment where the user is logged on with keycloak.

These operations are supported:
* List contents of a bucket
* Open a file in GCS
* Copy a file from GCS into local
* Load a file (CSV, JSON or XML) from GCS into a pandas dataframe
* Save contents of a data frame into a file (CSV, JSON, XML) in GCS

When the user gives the path to a resource, they do not need to give the GCS uri. 
This means users never have to prefix a path with "gs://". 
It is implicitly understood that all resources are located in GCS, and the first level of the path is always a bucket name.
