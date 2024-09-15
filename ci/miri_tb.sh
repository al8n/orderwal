#!/bin/bash
set -e

# Check if TARGET and FEATURES are provided, otherwise panic
if [ -z "$1" ]; then
  echo "Error: TARGET is not provided"
  exit 1
fi

if [ -z "$2" ]; then
  echo "Error: FEATURES are not provided"
  exit 1
fi

TARGET=$1
FEATURES=$2

rustup toolchain install nightly --component miri
rustup override set nightly
cargo miri setup

export MIRIFLAGS="-Zmiri-symbolic-alignment-check -Zmiri-disable-isolation -Zmiri-tree-borrows -Zmiri-ignore-leaks"

cargo miri test --tests --target $TARGET --features $FEATURES --lib

