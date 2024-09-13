#!/bin/bash
set -e

rustup toolchain install nightly --component miri
rustup override set nightly
cargo miri setup

export MIRIFLAGS="-Zmiri-symbolic-alignment-check -Zmiri-disable-isolation -Zmiri-tree-borrows"

cargo miri test --tests --target x86_64-unknown-linux-gnu --test test-unsync
# cargo miri test --tests --target aarch64-unknown-linux-gnu #crossbeam_utils has problem on this platform
cargo miri test --tests --target i686-unknown-linux-gnu --test test-unsync
cargo miri test --tests --target powerpc64-unknown-linux-gnu --test test-unsync
