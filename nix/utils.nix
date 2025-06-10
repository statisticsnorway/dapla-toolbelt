{lib}: let
  listNixFilesRecursive = filename: path:
    builtins.filter (p: lib.hasSuffix filename p) (map toString (lib.filesystem.listFilesRecursive path));
in {
  inherit listNixFilesRecursive;
}
