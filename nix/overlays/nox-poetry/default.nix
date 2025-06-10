final: prev: {
  python312 = prev.python312.override {
    packageOverrides = finalPyPkgs: prevPyPkgs: {
      inherit (final.python312Packages) nox-poetry;
    };
  };
  python312Packages = prev.python312Packages.override {
    overrides = finalPyPkgs: prevPyPkgs: {
      nox-poetry = prev.python312Packages.callPackage ./package.nix {};
    };
  };
}
