use tempfile::tempdir;

use crate::tests::*;

use super::*;

const MB: u32 = 1024 * 1024;

common_unittests!(unsync::OrderWal);
