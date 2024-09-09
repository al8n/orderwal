use super::*;
use tempfile::tempdir;

const MB: usize = 1024 * 1024;

pub(crate) fn construct_inmemory<W: Wal<Ascend, Crc32>>() {
  let mut wal = W::new(WalBuidler::new().with_capacity(MB as u32)).unwrap();
  let wal = &mut wal;
  wal.insert(b"key1", b"value1").unwrap();
}

pub(crate) fn construct_map_anon<W: Wal<Ascend, Crc32>>() {
  let mut wal = W::map_anon(WalBuidler::new().with_capacity(MB as u32)).unwrap();
  let wal = &mut wal;
  wal.insert(b"key1", b"value1").unwrap();
}

pub(crate) fn construct_map_file<W: Wal<Ascend, Crc32>>(prefix: &str) {
  let dir = tempdir().unwrap();
  let path = dir.path().join(format!("{prefix}_construct_map_file"));

  {
    let mut wal = W::map_mut(
      &path,
      WalBuidler::new(),
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

  let wal = W::map(&path, WalBuidler::new()).unwrap();
  assert_eq!(wal.get(b"key1").unwrap(), b"value1");
}
