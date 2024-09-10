use super::*;
use tempfile::tempdir;
use wal::ImmutableWal;

const MB: usize = 1024 * 1024;

pub(crate) fn construct_inmemory<W: Wal<Ascend, Crc32>>() {
  let mut wal = W::new(Builder::new().with_capacity(MB as u32)).unwrap();
  let wal = &mut wal;
  wal.insert(b"key1", b"value1").unwrap();
}

pub(crate) fn construct_map_anon<W: Wal<Ascend, Crc32>>() {
  let mut wal = W::map_anon(Builder::new().with_capacity(MB as u32)).unwrap();
  let wal = &mut wal;
  wal.insert(b"key1", b"value1").unwrap();
}

pub(crate) fn construct_map_file<W: Wal<Ascend, Crc32>>(prefix: &str) {
  let dir = tempdir().unwrap();
  let path = dir.path().join(format!("{prefix}_construct_map_file"));

  {
    let mut wal = W::map_mut(
      &path,
      Builder::new(),
      OpenOptions::new()
        .create_new(Some(MB as u32))
        .write(true)
        .read(true),
    )
    .unwrap();

    let wal = &mut wal;
    wal.insert(b"key1", b"value1").unwrap();
    assert_eq!(wal.get(b"key1").unwrap(), b"value1");
  }

  let wal = W::map(&path, Builder::new()).unwrap();
  assert_eq!(wal.get(b"key1").unwrap(), b"value1");
}

pub(crate) fn construct_with_small_capacity_inmemory<W: Wal<Ascend, Crc32>>() {
  let wal = W::new(Builder::new().with_capacity(1));

  assert!(wal.is_err());
  match wal {
    Err(e) => println!("error: {:?}", e),
    _ => panic!("unexpected error"),
  }
}

pub(crate) fn construct_with_small_capacity_map_anon<W: Wal<Ascend, Crc32>>() {
  let wal = W::map_anon(Builder::new().with_capacity(1));

  assert!(wal.is_err());
  match wal {
    Err(e) => println!("error: {:?}", e),
    _ => panic!("unexpected error"),
  }
}

pub(crate) fn construct_with_small_capacity_map_file<W: Wal<Ascend, Crc32>>(prefix: &str) {
  let dir = tempdir().unwrap();
  let path = dir
    .path()
    .join(format!("{prefix}_construct_with_small_capacity_map_file"));

  let wal = W::map_mut(
    &path,
    Builder::new(),
    OpenOptions::new()
      .create_new(Some(1))
      .write(true)
      .read(true),
  );

  assert!(wal.is_err());
  match wal {
    Err(e) => println!("{:?}", e),
    _ => panic!("unexpected error"),
  }
}
