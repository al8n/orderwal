use orderwal::{
  base::{OrderWal, Reader, Writer},
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
      .map_mut::<OrderWal<str, [u8]>, _>(&path)
      .unwrap()
  };

  wal.insert("a", b"a1".as_slice()).unwrap();
  wal.insert("c", b"c1".as_slice()).unwrap();

  let a = wal.get("a").unwrap();
  let c = wal.get("c").unwrap();

  assert_eq!(a.value(), b"a1");
  assert_eq!(c.value(), b"c1");
}
