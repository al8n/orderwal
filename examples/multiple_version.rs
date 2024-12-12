use orderwal::{
  dynamic::multiple_version::{OrderWal, Reader, Writer}, memtable::MemtableEntry, Builder
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

  wal.insert(1, b"a", b"a1".as_slice()).unwrap();
  wal.insert(3, b"a", b"a3".as_slice()).unwrap();
  wal.insert(1, b"c", b"c1".as_slice()).unwrap();
  wal.insert(3, b"c", b"c3".as_slice()).unwrap();

  for ent in wal.iter_all_points(3) {
    println!("{:?}", ent);
  }

  let a = wal.get(2, b"a").unwrap();
  let c = wal.get(2, b"c").unwrap();

  assert_eq!(a.value(), b"a1");
  assert_eq!(c.value(), b"c1");

  let a = wal.get(3, b"a").unwrap();
  let c = wal.get(3, b"c").unwrap();

  assert_eq!(a.value(), b"a3");
  assert_eq!(c.value(), b"c3");
}
