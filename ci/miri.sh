#!/bin/bash
set -e

rustup toolchain install nightly --component miri
rustup override set nightly
cargo miri setup

export MIRIFLAGS="-Zmiri-strict-provenance -Zmiri-disable-isolation -Zmiri-symbolic-alignment-check"

cargo miri test --tests --target x86_64-unknown-linux-gnu
cargo miri test --tests --target aarch64-unknown-linux-gnu
cargo miri test --tests --target i686-unknown-linux-gnu
cargo miri test --tests --target powerpc64-unknown-linux-gnu
