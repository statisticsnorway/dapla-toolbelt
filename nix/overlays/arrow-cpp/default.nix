final: prev: {
  arrow-cpp = prev.arrow-cpp.overrideAttrs (finalAttrs: prevAttrs: {
    version = "14.0.0";
    src = prev.fetchurl {
      url = "mirror://apache/arrow/arrow-${finalAttrs.version}/apache-arrow-${finalAttrs.version}.tar.gz";
      hash = "sha256-TrDaUOwHG68V/BY8tIBYkx4AbxyGLI3vDhgP0H1TECE=";
    };
  });
}
