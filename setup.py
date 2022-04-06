import io
import os
import re

from setuptools import setup
from setuptools import find_packages


def read(filename):
    filename = os.path.join(os.path.dirname(__file__), filename)
    text_type = type(u"")
    with io.open(filename, mode="r", encoding='utf-8') as fd:
        return re.sub(text_type(r':[a-z]+:`~?(.*?)`'), text_type(r'``\1``'), fd.read())


DEPENDENCIES = ["pandas", "jupyterhub", "gcsfs", "ipython", "lxml", "pyarrow"]

# This call to setup() does all the work
setup(
    name="dapla-toolbelt",
    version="1.2.2",
    license='Apache Software License',
    description="Python module for use within Jupyterlab notebooks, specifically aimed for Statistics Norway's data "
                "platform called Dapla",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    url="https://github.com/statisticsnorway/dapla-toolbelt",
    author="Statistics Norway",
    author_email="old@ssb.no",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9"
    ],
    packages=find_packages(exclude=('tests', 'examples')),
    include_package_data=True,
    install_requires=DEPENDENCIES
)
