#!/bin/bash
set -e

case $1 in
  setup)
    rustup component add rustfmt --toolchain nightly
    rustup component add clippy --toolchain nightly
    cargo install cargo-release --version ~0.13.0
    cargo install cargo-udeps --version ~0.1.21
    pre-commit install
    echo "Ready to go..."
    ;;

  check)
    pre-commit run --all-files
    ;;

  release)
    cargo release --manifest-path main/Cargo.toml "${@:2}"
    ;;

  *)
    echo "invalid argument, setup/check/release expected"
    exit 1
    ;;
esac
