{
  description = "aedb dev shell — Rust nightly + profiling tools";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ (import rust-overlay) ];
        };

        # Rust nightly with components needed for development + profiling.
        rust-toolchain = pkgs.rust-bin.selectLatestNightlyWith (toolchain:
          toolchain.default.override {
            extensions = [ "rust-src" "rust-analyzer" "rustfmt" "clippy" "llvm-tools-preview" ];
          });
      in {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            rust-toolchain

            # Build deps that some crates pull in via -sys
            pkg-config
            zstd

            # CPU profiling
            cargo-flamegraph
            samply
            linuxPackages.perf

            # Heap profiling
            heaptrack

            # Criterion plots
            gnuplot

            # Misc useful
            cargo-nextest
            cargo-watch
            git
            jq
          ];

          shellHook = ''
            export RUST_BACKTRACE=1
            export CARGO_TERM_COLOR=always

            # cargo-flamegraph needs perf_event_paranoid <= 1 (or root).
            # NixOS default is usually 2. Print a hint without trying to change it.
            if [ -r /proc/sys/kernel/perf_event_paranoid ]; then
              paranoid=$(cat /proc/sys/kernel/perf_event_paranoid)
              if [ "$paranoid" -gt 1 ]; then
                echo "note: kernel.perf_event_paranoid=$paranoid — flamegraph/perf may need:"
                echo "      sudo sysctl -w kernel.perf_event_paranoid=1"
              fi
            fi

            echo "aedb dev shell ready"
            echo "  rust:       $(rustc --version)"
            echo "  flamegraph: cargo flamegraph --bench perf"
            echo "  samply:     samply record cargo bench --bench perf"
            echo "  dhat:       cargo run --release --example heap_profile"
            echo "  heaptrack:  heaptrack target/release/examples/heap_profile"
          '';
        };
      });
}
