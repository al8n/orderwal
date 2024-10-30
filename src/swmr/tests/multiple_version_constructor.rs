use multiple_version::{OrderWal, Reader, Writer};

use crate::memtable::{MultipleVersionMemtable, MultipleVersionMemtableEntry};

use super::*;

fn zero_reserved<M>(wal: &mut OrderWal<Person, String, M>)
where
  M: MultipleVersionMemtable<Key = Person, Value = String> + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Item<'a>: MultipleVersionMemtableEntry<'a> + std::fmt::Debug,
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
  M: MultipleVersionMemtable<Key = Person, Value = String> + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Item<'a>: MultipleVersionMemtableEntry<'a> + std::fmt::Debug,
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
  "linked": MultipleVersionOrderWalLinkedTable<Person, String> {
    zero_reserved,
  }
);

expand_unit_tests!(
  "linked": MultipleVersionOrderWalLinkedTable<Person, String> {
    reserved({
      crate::Builder::new()
        .with_capacity(MB)
        .with_reserved(4)
    }),
  }
);

expand_unit_tests!(
  "arena": MultipleVersionOrderWalArenaTable<Person, String> {
    zero_reserved,
  }
);

expand_unit_tests!(
  "arena": MultipleVersionOrderWalArenaTable<Person, String> {
    reserved({
      crate::Builder::new()
        .with_capacity(MB)
        .with_reserved(4)
    }),
  }
);

#[test]
#[cfg_attr(miri, ignore)]
fn reopen_wrong_kind() {
  use crate::Builder;

  let dir = tempfile::tempdir().unwrap();
  let path = dir.path().join("test_reopen_wrong_kind");
  let _ = unsafe {
    Builder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<OrderWal<Person, String>, _>(path.as_path())
      .unwrap()
  };

  let err = unsafe {
    Builder::new()
      .with_capacity(MB)
      .with_read(true)
      .map_mut::<crate::base::OrderWal<Person, String>, _>(path.as_path())
      .unwrap_err()
  };
  assert!(matches!(err, crate::error::Error::KindMismatch { .. }));
}
