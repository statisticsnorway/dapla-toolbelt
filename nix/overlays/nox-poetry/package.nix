{
  buildPythonPackage,
  fetchPypi,
  lib,
  nox,
  packaging,
  tomlkit,
  poetry-core,
}:
buildPythonPackage rec {
  pname = "nox_poetry";
  version = "1.0.3";
  pyproject = true;

  src = fetchPypi {
    inherit pname version;
    hash = "sha256-3H7LvYEqMzoMC1WPV+Wzf3wSkmzdvOyvImSVf9Nzgk4=";
  };

  dependencies = [nox packaging poetry-core tomlkit];

  pythonImportsCheck = ["nox_poetry"];

  meta = {
    description = "Use Poetry inside Nox session";
    homepage = "https://github.com/cjolowicz/nox-poetry";
    license = lib.licenses.mit;
  };
}
