final: prev: {
  # python312Packages = prev.python312Packages.override {
  #   overrides = finalPyPkgs: prevPyPkgs: {
  #     cryptography = prevPyPkgs.cryptography.overrideAttrs(finalAttrs: prevAttrs: {
  #       version = "42.0.4";
  #       src = prev.fetchPypi {
  #         inherit (finalAttrs) pname version;
  #         hash = "sha256-gxpLN6zO8wzM00/LkWpde1vjy74nJooCgyw+RQrqOcs=";
  #       };

  #       cargoDeps = prev.rustPlatform.fetchCargoTarball {
  #         inherit (finalAttrs) src;
  #         sourceRoot = "${finalAttrs.pname}-${finalAttrs.version}/${finalAttrs.cargoRoot}";
  #         name = "${finalAttrs.pname}-${finalAttrs.version}";
  #         hash = "sha256-qaXQiF1xZvv4sNIiR2cb5TfD7oNiYdvUwcm37nh2P2M=";
  #       };

  #       nativeCheckInputs = with prev; [certifi pretend pytestCheckHook pytest-xdist finalPyPkgs.cryptography_vectors];
  #     });

  #     cryptography_vectors = prevPyPkgs.cryptography.overrideAttrs(finalAttrs: prevAttrs: {
  #       version = finalPyPkgs.cryptography.version;
  #       src = prev.fetchPypi {
  #         pname = "cryptography_vectors";
  #         inherit (finalAttrs) version;
  #         hash = "sha256-1rcH0jil4jkMPa53YbmXtowrjAcj7ST8E/gyvMhzmUU=";
  #       };
  #     });
  #   };
  # };
}
