use orderwal::{
  generic::{ArenaTable, OrderWal, Reader, Writer, Descend},
  memtable::MemtableEntry,
  Builder,
};

fn main() {
  let dir = tempfile::tempdir().unwrap();
  let path = dir.path().join("descend.wal");

  let mut wal = unsafe {
    Builder::<ArenaTable<u64, u64, Descend>>::new()
      .with_capacity(1024 * 1024)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<OrderWal<ArenaTable<u64, u64, Descend>>, _>(&path)
      .unwrap()
  };

  for i in 0..10u64 {
    wal.insert(0, &i, &i).unwrap();
  }

  let x = wal.iter(0).map(|ent| ent.value()).collect::<Vec<_>>();
  assert_eq!(x, (0..10).rev().collect::<Vec<_>>());
}
