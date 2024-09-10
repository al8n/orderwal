use crate::tests::*;

use super::*;

#[test]
fn test_construct_inmemory() {
  construct_inmemory::<OrderWal<Ascend, Crc32>>();
}

#[test]
fn test_construct_map_anon() {
  construct_map_anon::<OrderWal<Ascend, Crc32>>();
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_construct_map_file() {
  construct_map_file::<OrderWal<Ascend, Crc32>>("swmr");
}

#[test]
fn test_construct_with_small_capacity_inmemory() {
  construct_with_small_capacity_inmemory::<OrderWal<Ascend, Crc32>>();
}

#[test]
fn test_construct_with_small_capacity_map_anon() {
  construct_with_small_capacity_map_anon::<OrderWal<Ascend, Crc32>>();
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_construct_with_small_capacity_map_file() {
  construct_with_small_capacity_map_file::<OrderWal<Ascend, Crc32>>("swmr");
}
