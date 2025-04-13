{
  description = "A basic flake with a shell";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    systems.url = "github:nix-systems/default";
    flake-utils = {
      url = "github:numtide/flake-utils";
      inputs.systems.follows = "systems";
    };
  };

  outputs = {
    nixpkgs,
    flake-utils,
    ...
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        pkgs = nixpkgs.legacyPackages.${system};
      in {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            bashInteractive
            go
            gopls
            gotools
            gofumpt
            golangci-lint

            basedpyright
            (pkgs.python3.withPackages (python-pkgs: [
              python-pkgs.typer

              python-pkgs.ruff
              python-pkgs.black
              python-pkgs.isort
            ]))
          ];
        };
      }
    );
}
