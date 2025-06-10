final: prev: {
  python312Packages = prev.python312Packages.override {
    overrides = finalPyPkgs: prevPyPkgs: {
      css-html-js-minify = prevPyPkgs.css-html-js-minify.overridePythonAttrs(prevAttrs: {
        propagadatedBuildInputs = (prevAttrs.dependencies or [  ]) ++ [ prevPyPkgs.setuptools ];
        buildInputs = (prevAttrs.dependencies or [  ]) ++ [ prevPyPkgs.setuptools-scm ];
        # buildInputs = (prevAttrs.buildInputs or [ ]) ++ [ prevPyPkgs.setuptools ];
      });
    };
  };
}
