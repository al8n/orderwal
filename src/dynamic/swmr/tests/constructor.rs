use base::{Reader, Writer};
use skl::KeySize;

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

#[cfg(feature = "std")]
expand_unit_tests!(
  "linked": OrderWalAlternativeTable<Person, String> [TableOptions::Linked]: Table<_, _> {
    zero_reserved,
  }
);

#[cfg(feature = "std")]
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

#[test]
#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
#[cfg_attr(miri, ignore)]
fn reopen_wrong_mode() {
  use crate::Builder;

  let dir = tempfile::tempdir().unwrap();
  let path = dir.path().join("test_reopen_wrong_kind");
  let wal = unsafe {
    Builder::new()
      .with_capacity(MB)
      .with_maximum_key_size(KeySize::with(10))
      .with_maximum_value_size(10)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<OrderWal<Person, String>, _>(path.as_path())
      .unwrap()
  };

  assert!(!wal.read_only());
  assert_eq!(wal.capacity(), MB);
  assert!(wal.remaining() < MB);
  assert_eq!(wal.maximum_key_size(), 10);
  assert_eq!(wal.maximum_value_size(), 10);
  assert_eq!(wal.path().unwrap().as_path(), path.as_path());
  assert_eq!(wal.options().maximum_key_size(), 10);

  let err = unsafe {
    Builder::new()
      .with_capacity(MB)
      .with_read(true)
      .map_mut::<crate::multiple_version::OrderWal<Person, String>, _>(path.as_path())
      .unwrap_err()
  };
  assert!(matches!(err, crate::error::Error::ModeMismatch { .. }));
}
