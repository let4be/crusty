#!/bin/bash
set -e

cargo release --manifest-path main/Cargo.toml "$@"
