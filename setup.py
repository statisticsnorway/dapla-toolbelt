import pathlib
from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="dapla-toolbelt",
    version="0.0.10",
    description="Python module for use within Jupyterlab notebooks, specifically aimed for Statistics Norway's data "
                "platform called Dapla",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/statisticsnorway/dapla-toolbelt",
    author="Statistics Norway",
    author_email="old@ssb.no",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
    ],
    packages=["dapla"],
    include_package_data=True,
    install_requires=["pandas", "jupyterhub", "gcsfs", "ipython", "lxml"],
)
