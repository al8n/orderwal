#!/bin/bash

set -ex

export ASAN_OPTIONS="detect_odr_violation=0 detect_leaks=0"

# Run address sanitizer
RUSTFLAGS="-Z sanitizer=address --cfg all_orderwal_tests" \
cargo test -Z build-std --all --release --tests --target x86_64-unknown-linux-gnu --all-features --exclude benchmarks -- --test-threads=1

# Run memory sanitizer
RUSTFLAGS="-Z sanitizer=memory --cfg all_orderwal_tests" \
cargo test -Z build-std --all --release --tests --target x86_64-unknown-linux-gnu --all-features --exclude benchmarks -- --test-threads=1

# Run thread sanitizer
cargo clean
TSAN_OPTIONS="suppressions=$(pwd)/ci/tsan" \
RUSTFLAGS="${RUSTFLAGS:-} -Z sanitizer=thread --cfg all_orderwal_tests" \
    cargo test -Z build-std --all --release --target x86_64-unknown-linux-gnu --all-features --tests --exclude benchmarks -- --test-threads=1