use crate::dynamic::{BoundedTable, DynamicMemtable, OrderWal, Reader, UnboundedTable, Writer};

use super::MB;

fn zero_reserved<M>(wal: &mut OrderWal<M>)
where
  M: DynamicMemtable + 'static,
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
  M: DynamicMemtable + 'static,
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

#[cfg(feature = "unbounded")]
expand_unit_tests!(
  "unbounded": OrderWal<UnboundedTable> [Default::default()]: UnboundedTable {
    zero_reserved,
  }
);

#[cfg(feature = "unbounded")]
expand_unit_tests!(
  "unbounded": OrderWal<UnboundedTable> [Default::default()]: UnboundedTable {
    reserved({
      crate::Builder::new()
        .with_capacity(MB)
        .with_reserved(4)
    }),
  }
);

#[cfg(feature = "bounded")]
expand_unit_tests!(
  "bounded": OrderWal<BoundedTable> [Default::default()]: BoundedTable {
    zero_reserved,
  }
);

#[cfg(feature = "bounded")]
expand_unit_tests!(
  "bounded": OrderWal<BoundedTable> [Default::default()]: BoundedTable {
    reserved({
      crate::Builder::new()
        .with_capacity(MB)
        .with_reserved(4)
    }),
  }
);
