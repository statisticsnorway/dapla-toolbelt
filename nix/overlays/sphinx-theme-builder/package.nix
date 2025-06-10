{ lib
, buildPythonPackage
, fetchPypi
, flit-core
, nodeenv
, packaging
, pyproject-metadata
, pythonOlder
, setuptools
, sphinx
, sphinx-material
, rich
, tomli
}:

buildPythonPackage rec {
  pname = "sphinx-theme-builder";
  version = "0.2.0b2";
  format = "pyproject";

  disabled = pythonOlder "3.7";

  src = fetchPypi {
    inherit pname version;
    hash = "sha256-6c2Ywrs1v0FP5yFGmgQ83MEPCAjR/89gastKYoKm8og=";
  };

  build-system = [ flit-core ];

  buildInputs = [
    nodeenv
    packaging
    pyproject-metadata
    setuptools
    rich
    sphinx-material
    tomli
  ];

  propagatedBuildInputs = [
    sphinx
  ];

  doCheck = false; # no tests

  pythonImportsCheck = [
    "sphinx_material"
  ];

  meta = with lib; {
    description = "A material-based, responsive theme inspired by mkdocs-material";
    homepage = "https://bashtage.github.io/sphinx-material";
    license = licenses.mit;
    maintainers = with maintainers; [ FlorianFranzen ];
  };
}
