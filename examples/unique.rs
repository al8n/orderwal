use orderwal::{
  dynamic::unique::{OrderWal, Reader, Writer},
  memtable::MemtableEntry,
  Builder,
};

fn main() {
  let dir = tempfile::tempdir().unwrap();
  let path = dir.path().join("not_sized.wal");

  let mut wal = unsafe {
    Builder::new()
      .with_capacity(1024 * 1024)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<OrderWal, _>(&path)
      .unwrap()
  };

  wal.insert(b"a", b"a1".as_slice()).unwrap();
  wal.insert(b"a", b"a3".as_slice()).unwrap();
  wal.insert(b"c", b"c1".as_slice()).unwrap();
  wal.insert(b"c", b"c3".as_slice()).unwrap();


  let a = wal.get(b"a").unwrap();
  let c = wal.get(b"c").unwrap();

  assert_eq!(a.value(), b"a3");
  assert_eq!(c.value(), b"c3");
}
