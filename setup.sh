#!/bin/bash
set -e

rustup component add rustfmt --toolchain nightly
rustup component add clippy --toolchain nightly
pre-commit install
echo "Ready to go..."
