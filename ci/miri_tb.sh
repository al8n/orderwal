#!/bin/bash
set -euxo pipefail
IFS=$'\n\t'

# We need 'ts' for the per-line timing
sudo apt-get -y install moreutils
echo

# Check if TARGET and CONFIG_FLAGS are provided, otherwise panic
if [ -z "$1" ]; then
  echo "Error: TARGET is not provided"
  exit 1
fi

if [ -z "$2" ]; then
  echo "Error: CONFIG_FLAGS are not provided"
  exit 1
fi

TARGET=$1
CONFIG_FLAGS=$2

rustup toolchain install nightly --component miri
rustup override set nightly
cargo miri setup

# Zmiri-ignore-leaks needed because of https://github.com/crossbeam-rs/crossbeam/issues/579
export MIRIFLAGS="-Zmiri-symbolic-alignment-check -Zmiri-disable-isolation -Zmiri-tree-borrows -Zmiri-ignore-leaks"
export RUSTFLAGS="--cfg test_$CONFIG_FLAGS"

cargo miri test --tests --target $TARGET --lib -- --test-threads 1 2>&1 | ts -i '%.s  '

