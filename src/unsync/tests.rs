use tempfile::tempdir;

use crate::tests::*;

use super::*;

const MB: u32 = 1024 * 1024;

common_unittests!(unsync::OrderWal);

#[test]
fn test_last_inmemory1() {
  last(&mut OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
}
