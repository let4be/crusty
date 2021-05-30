#!/bin/bash
set -e

rustup component add rustfmt --toolchain nightly
rustup component add clippy --toolchain nightly
git config core.hooksPath ./infra/.git-hooks