use orderwal::{
  generic::multiple_version::{ArenaTable, OrderWal, Reader, Writer}, memtable::MemtableEntry, Builder
};

use skl::generic::{multiple_version::{sync::SkipMap, Map}, Builder as MapBuilder};

fn main() {
  let dir = tempfile::tempdir().unwrap();
  let path = dir.path().join("not_sized.wal");

  let mut wal = unsafe {
    Builder::new()
      .with_capacity(1024 * 1024)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<OrderWal<ArenaTable<str, [u8]>>, _>(&path)
      .unwrap()
  };

  wal.insert(1, b"a", b"a1".as_slice()).unwrap();
  wal.insert(3, b"a", b"a3".as_slice()).unwrap();
  wal.insert(1, b"c", b"c1".as_slice()).unwrap();
  wal.insert(3, b"c", b"c3".as_slice()).unwrap();

  for ent in wal.iter_all_points(3) {
    println!("{:?}", ent);
  }

  // let a = wal.get(2, "a").unwrap();
  // let c = wal.get(2, "c").unwrap();

  // assert_eq!(a.value(), b"a1");
  // assert_eq!(c.value(), b"c1");

  // let a = wal.get(3, "a").unwrap();
  // let c = wal.get(3, "c").unwrap();

  // assert_eq!(a.value(), b"a3");
  // assert_eq!(c.value(), b"c3");

  // let map: SkipMap<str, [u8]> = MapBuilder::new().with_capacity(1024).alloc().unwrap();
  // map.insert(1, "a", b"a1".as_slice()).unwrap();
  // map.insert(3, "a", b"a3".as_slice()).unwrap();
  // map.insert(1, "c", b"c1".as_slice()).unwrap();
  // map.insert(3, "c", b"c3".as_slice()).unwrap();

  // for ent in map.iter_all(3) {
  //   println!("{:?}", ent);
  // }
}
