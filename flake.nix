{
  description = "Development environment for dapla-toolbelt";
  inputs = {
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    poetry2nix = {
      url = "git+file:///Users/nvj/Projects/poetry2nix?ref=update-overrides";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = inputs @ {flake-parts, ...}:
    flake-parts.lib.mkFlake {inherit inputs;} {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      perSystem = {
        self',
        system,
        pkgs,
        lib,
        ...
      }: let
        utils = import ./nix/utils.nix {inherit lib;};
        poetry2nix = inputs.poetry2nix.lib.mkPoetry2Nix {inherit pkgs;};
        defaultPython = pkgs.python312;
      in {
        # Register all overlays named "default.nix" in nixpkgs
        _module.args.pkgs =
          builtins.foldl'
          (pkgSet: overlayPath: pkgSet.extend (import overlayPath))
          inputs.nixpkgs.legacyPackages.${system}
          (utils.listNixFilesRecursive "default.nix" ./nix/overlays);

        checks.fmt-check = pkgs.stdenvNoCC.mkDerivation {
          name = "fmt-check";
          dontBuild = true;
          src = ./.;
          doCheck = true;
          nativeBuildInputs = with pkgs; [alejandra];
          checkPhase = ''
            alejandra --check .
          '';
          installPhase = ''
            touch $out
          '';
        };

        devShells.default = pkgs.mkShell {
            name = "dapla-toolbelt development";
            packages = with pkgs; [
              # Nox is provided as a propagated runtime dependency to 'nox-poetry'
              (defaultPython.withPackages (p: [p.nox]))
              # Nix LSP
              nixd
              (poetry.override {python3 = defaultPython;})
              ruff-lsp
              xz
              zlib
            ];
          };

        legacyPackages = pkgs;

        packages = {
          arrow-cpp = pkgs.arrow-cpp;
          maturin = pkgs.python312Packages.callPackage ./nix/packages/maturin/package.nix {};
          dapla-toolbelt = poetry2nix.mkPoetryApplication {
            projectDir = ./.;
            python = defaultPython;
            overrides = poetry2nix.defaultPoetryOverrides.extend (final: prev: {
              deptry = prev.deptry.overridePythonAttrs(prevAttrs: {
                buildInputs = (prevAttrs.buildInputs or [ ]) ++ [ prev.poetry-core ];
              });

              frozenlist = pkgs.python312Packages.frozenlist;

              python-calamine = prev.python-calamine.overridePythonAttrs(prevAttrs: {
                buildInputs =
                  (prevAttrs.buildInputs or [ ])
                    ++ [ self'.packages.maturin ]
                    ++ lib.optionals pkgs.stdenv.isDarwin [ pkgs.iconv ];
                nativeBuildInputs =
                  (prevAttrs.nativeBuildInputs or []) ++
                  (with pkgs.rustPlatform; [ cargoSetupHook maturinBuildHook ]);

                cargoDeps = pkgs.rustPlatform.fetchCargoTarball {
                  inherit (prevAttrs) src;
                  name = "${prevAttrs.pname}-${prevAttrs.version}";
                  hash = "sha256-Xqgwn6Bi2J8kHvC5xECYQnpa6UM3Zq+dLnFAvdvmLnM=";
                };
              });

              pyxlsb = prev.pyxlsb.overridePythonAttrs(prevAttrs: {
                buildInputs = (prevAttrs.buildInputs or [ ]) ++ [ prev.setuptools ];
              });

              ruamel-yaml-clib = pkgs.python312Packages.ruamel-yaml-clib;

              sphinx-autobuild = pkgs.python312Packages.sphinx-autobuild;

              sphinx-click = prev.sphinx-click.overridePythonAttrs(prevAttrs: {
                format = "setuptools";
                buildInputs = (prevAttrs.buildInputs or [ ]) ++ [ prev.setuptools ];
              });

              yarl = pkgs.python312Packages.yarl;
            });
          };
          sphinx-theme-builder = pkgs.python312Packages.sphinx-theme-builder;
          css-html-js-minify = pkgs.python312Packages.css-html-js-minify;
        };

        formatter = pkgs.alejandra;
      };
    };
}
