use base::{Reader, Writer};

use crate::memtable::{
  alternative::{Table, TableOptions},
  Memtable, MemtableEntry,
};

use super::*;

fn zero_reserved<M>(wal: &mut OrderWal<Person, String, M>)
where
  M: Memtable<Key = Person, Value = String> + 'static,
  for<'a> M::Item<'a>: MemtableEntry<'a>,
  M::Error: std::fmt::Debug,
{
  unsafe {
    assert_eq!(wal.reserved_slice(), b"");
    assert_eq!(wal.reserved_slice_mut(), b"");

    let wal = wal.reader();
    assert_eq!(wal.reserved_slice(), b"");
  }
}

fn reserved<M>(wal: &mut OrderWal<Person, String, M>)
where
  M: Memtable<Key = Person, Value = String> + 'static,
  for<'a> M::Item<'a>: MemtableEntry<'a>,
  M::Error: std::fmt::Debug,
{
  unsafe {
    let buf = wal.reserved_slice_mut();
    buf.copy_from_slice(b"al8n");
    assert_eq!(wal.reserved_slice(), b"al8n");
    assert_eq!(wal.reserved_slice_mut(), b"al8n");

    let wal = wal.reader();
    assert_eq!(wal.reserved_slice(), b"al8n");
  }
}

expand_unit_tests!(
  "linked": OrderWalAlternativeTable<Person, String> [TableOptions::Linked]: Table<_, _> {
    zero_reserved,
  }
);

expand_unit_tests!(
  "linked": OrderWalAlternativeTable<Person, String> [TableOptions::Linked]: Table<_, _> {
    reserved({
      crate::Builder::new()
        .with_capacity(MB)
        .with_reserved(4)
    }),
  }
);

expand_unit_tests!(
  "arena": OrderWalAlternativeTable<Person, String> [TableOptions::Arena(Default::default())]: Table<_, _> {
    zero_reserved,
  }
);

expand_unit_tests!(
  "arena": OrderWalAlternativeTable<Person, String> [TableOptions::Arena(Default::default())]: Table<_, _> {
    reserved({
      crate::Builder::new()
        .with_capacity(MB)
        .with_reserved(4)
    }),
  }
);
