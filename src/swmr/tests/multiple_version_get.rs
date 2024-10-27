use crate::{memtable::MultipleVersionMemtable, sealed::WithVersion};
use multiple_version::{GenericVersionPointer, Reader, Writer};

use super::*;

expand_unit_tests!("linked": MultipleVersionGenericOrderWalLinkedTable<str, str> {
  mvcc,
});

expand_unit_tests!("arena": MultipleVersionGenericOrderWalArenaTable<str, str> {
  mvcc,
});

#[allow(clippy::needless_borrows_for_generic_args)]
fn mvcc<M>(wal: &mut multiple_version::GenericOrderWal<str, str, M>)
where
  M: MultipleVersionMemtable<Pointer = GenericVersionPointer<str, str>> + 'static,
  M::Pointer: WithVersion,
  M::Error: std::fmt::Debug,
  for<'a> M::MultipleVersionItem<'a>: std::fmt::Debug,
{
  wal.insert(1, "a", "a1").unwrap();
  wal.insert(3, "a", "a2").unwrap();
  wal.insert(1, "c", "c1").unwrap();
  wal.insert(3, "c", "c2").unwrap();

  for ent in wal.iter_all_versions(4) {
    println!("{:?}", ent);
  }

  let ent = wal.get(1, "a").unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.get(2, "a").unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.get(3, "a").unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.get(4, "a").unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.version(), 3);
}
