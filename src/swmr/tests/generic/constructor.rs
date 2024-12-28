use crate::generic::{BoundedTable, GenericMemtable, OrderWal, Reader, UnboundedTable, Writer};

use super::{Person, MB};

fn zero_reserved<M>(wal: &mut OrderWal<M>)
where
  M: GenericMemtable<Person, String> + 'static,
  M::Error: std::fmt::Debug,
{
  unsafe {
    assert_eq!(wal.reserved_slice(), b"");
    assert_eq!(wal.reserved_slice_mut(), b"");

    let wal = wal.reader();
    assert_eq!(wal.reserved_slice(), b"");
  }
}

fn reserved<M>(wal: &mut OrderWal<M>)
where
  M: GenericMemtable<Person, String> + 'static,
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
  "unbounded": OrderWal<UnboundedTable<Person, String>> [Default::default()]: UnboundedTable<_, _> {
    zero_reserved,
  }
);

#[cfg(feature = "std")]
expand_unit_tests!(
  "unbounded": OrderWal<UnboundedTable<Person, String>> [Default::default()]: UnboundedTable<_, _> {
    reserved({
      crate::Builder::new()
        .with_capacity(MB)
        .with_reserved(4)
    }),
  }
);

expand_unit_tests!(
  "bounded": OrderWal<BoundedTable<Person, String>> [Default::default()]: BoundedTable<_, _> {
    zero_reserved,
  }
);

expand_unit_tests!(
  "bounded": OrderWal<BoundedTable<Person, String>> [Default::default()]: BoundedTable<_, _> {
    reserved({
      crate::Builder::new()
        .with_capacity(MB)
        .with_reserved(4)
    }),
  }
);
