{
  sources ? import ./nix/sources.nix {},
  pkgs ? import sources.nixpkgs {},
  lib ? pkgs.lib,
}:
let
  inherit (pkgs) mkShellNoCC;
  inherit (pkgs.lib) makeLibraryPath;
  py = pkgs.python312;
in
{
  devShell = with pkgs; mkShellNoCC {
    # PIP_DISABLE_PIP_VERSION_CHECK = 1;
    venvDir = "./.venv";

    buildInputs = with pkgs; [
      niv
      uv
      (py.withPackages (ps: with ps; [
        pip
      ]))
      py.pkgs.venvShellHook
    ];

    LD_LIBRARY_PATH = lib.makeLibraryPath [stdenv.cc.cc.lib];
    postVenvCreation = ''
      unset SOURCE_DATE_EPOCH
      #pip install -r requirements.txt
    '';
  };
}
