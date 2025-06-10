final: prev: {
  python312Packages = prev.python312Packages.override {
    overrides = finalPyPkgs: prevPyPkgs: {
      sphinx-theme-builder = prev.python312Packages.callPackage ./package.nix {};
    };
  };
}
