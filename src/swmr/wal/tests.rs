use tempfile::tempdir;

use crate::tests::*;

use super::*;

#[cfg(all(test, feature = "test-swmr-constructor"))]
mod constructor;

#[cfg(all(test, feature = "test-swmr-insert"))]
mod insert;

#[cfg(all(test, feature = "test-swmr-iters"))]
mod iter;

#[cfg(all(test, feature = "test-swmr-get"))]
mod get;

const MB: u32 = 1024 * 1024;
