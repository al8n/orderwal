use tempfile::tempdir;

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

#[test]
fn test_insert_inmemory() {
  insert(OrderWal::new(Builder::new().with_capacity(1024 * 1024)).unwrap());
}

#[test]
fn test_insert_map_anon() {
  insert(OrderWal::map_anon(Builder::new().with_capacity(1024 * 1024)).unwrap());
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_insert_map_file() {
  let dir = tempdir().unwrap();
  insert(
    OrderWal::map_mut(
      dir.path().join("test_swmr_insert_map_file"),
      Builder::new(),
      OpenOptions::new()
        .create_new(Some(1024 * 1024))
        .write(true)
        .read(true),
    )
    .unwrap(),
  );
}

#[test]
fn test_iter_inmemory() {
  iter(OrderWal::new(Builder::new().with_capacity(1024 * 1024)).unwrap());
}

#[test]
fn test_iter_map_anon() {
  iter(OrderWal::map_anon(Builder::new().with_capacity(1024 * 1024)).unwrap());
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_iter_map_file() {
  let dir = tempdir().unwrap();
  iter(
    OrderWal::map_mut(
      dir.path().join("test_swmr_iter_map_file"),
      Builder::new(),
      OpenOptions::new()
        .create_new(Some(1024 * 1024))
        .write(true)
        .read(true),
    )
    .unwrap(),
  );
}
