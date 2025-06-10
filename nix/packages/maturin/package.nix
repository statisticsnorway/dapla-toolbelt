{ stdenv, buildPythonPackage, fetchFromGitHub, lib, libiconv, rustPlatform }:
buildPythonPackage rec {
  pname = "maturin";
  version = "1.5.1";

  format = "pyproject";

  src = fetchFromGitHub {
    owner = "pyo3";
    repo = pname;
    rev = "v${version}";
    hash = "sha256-3rID2epV1pCwpofFf9Wuafs1SlBWH7e7/4HPaSUAriQ=";
  };

  cargoDeps = rustPlatform.fetchCargoTarball {
    inherit src;
    name = "${pname}-${version}";
    hash = "sha256-hPyPMQm/Oege0PPjYIrd1fEDOGqoQ1ffS2l6o8je4t4=";
  };

  buildInputs = lib.optionals stdenv.isDarwin [ libiconv ];

  nativeBuildInputs = [
    rustPlatform.cargoSetupHook
    rustPlatform.maturinBuildHook
  ];

  pythonImportsCheck = [
    "maturin"
  ];
}
