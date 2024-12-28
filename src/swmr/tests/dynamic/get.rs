use dbutils::{
  buffer::VacantBuffer,
  state::{Active, MaybeTombstone},
  types::Type,
};

use core::ops::Bound;

use crate::{
  dynamic::{BoundedTable, DynamicMemtable, OrderWal, Reader, UnboundedTable, Writer},
  memtable::{Entry, MutableMemtable, RawEntry},
  types::{KeyBuilder, ValueBuilder},
};

use super::{Person, MB};

#[cfg(feature = "std")]
expand_unit_tests!("unbounded": OrderWal<UnboundedTable> [Default::default()]: UnboundedTable  {
  mvcc,
  gt,
  ge,
  le,
  lt,
});

expand_unit_tests!("bounded": OrderWal<BoundedTable> [Default::default()]: BoundedTable  {
  mvcc,
  gt,
  ge,
  le,
  lt,
});

#[cfg(feature = "std")]
expand_unit_tests!("unbounded": OrderWal<UnboundedTable> [Default::default()]: UnboundedTable {
  insert,
  unbounded_insert_with_value_builder,
  unbounded_insert_with_key_builder,
  unbounded_insert_with_bytes,
  unbounded_insert_with_builders,
});

expand_unit_tests!("bounded": OrderWal<BoundedTable> [Default::default()]: BoundedTable {
  insert,
  bounded_insert_with_value_builder,
  bounded_insert_with_key_builder,
  bounded_insert_with_bytes,
  bounded_insert_with_builders,
});

fn mvcc<M>(wal: &mut OrderWal<M>)
where
  M: DynamicMemtable + MutableMemtable + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Entry<'a, Active>: Entry<'a, Key = &'a [u8], Value = &'a [u8]>
    + RawEntry<'a, RawValue = &'a [u8]>
    + std::fmt::Debug,
  for<'a> M::Entry<'a, MaybeTombstone>: Entry<'a, Key = &'a [u8], Value = Option<&'a [u8]>>
    + RawEntry<'a, RawValue = Option<&'a [u8]>>
    + std::fmt::Debug,
{
  wal.insert(1, b"a", b"a1").unwrap();
  wal.insert(3, b"a", b"a2").unwrap();
  wal.insert(1, b"c", b"c1").unwrap();
  wal.insert(3, b"c", b"c2").unwrap();

  let ent = wal.get(1, b"a").unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.get(2, b"a").unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.get(3, b"a").unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.get(4, b"a").unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  assert!(wal.get(0, b"b").is_none());
  assert!(wal.get(1, b"b").is_none());
  assert!(wal.get(2, b"b").is_none());
  assert!(wal.get(3, b"b").is_none());
  assert!(wal.get(4, b"b").is_none());

  let ent = wal.get(1, b"c").unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.get(2, b"c").unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.get(3, b"c").unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.get(4, b"c").unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  assert!(wal.get(5, b"d").is_none());
}

fn gt<M>(wal: &mut OrderWal<M>)
where
  M: DynamicMemtable + MutableMemtable + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Entry<'a, Active>: Entry<'a, Key = &'a [u8], Value = &'a [u8]>
    + RawEntry<'a, RawValue = &'a [u8]>
    + std::fmt::Debug,
  for<'a> M::Entry<'a, MaybeTombstone>: Entry<'a, Key = &'a [u8], Value = Option<&'a [u8]>>
    + RawEntry<'a, RawValue = Option<&'a [u8]>>
    + std::fmt::Debug,
{
  wal.insert(1, b"a", b"a1").unwrap();
  wal.insert(3, b"a", b"a2").unwrap();
  wal.insert(1, b"c", b"c1").unwrap();
  wal.insert(3, b"c", b"c2").unwrap();
  wal.insert(5, b"c", b"c3").unwrap();

  assert!(wal.lower_bound(0, Bound::Excluded(b"a")).is_none());
  assert!(wal.lower_bound(0, Bound::Excluded(b"b")).is_none());
  assert!(wal.lower_bound(0, Bound::Excluded(b"c")).is_none());

  let ent = wal.lower_bound(1, Bound::Excluded(b"")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(2, Bound::Excluded(b"")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(3, Bound::Excluded(b"")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(1, Bound::Excluded(b"a")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(2, Bound::Excluded(b"a")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(3, Bound::Excluded(b"a")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(1, Bound::Excluded(b"b")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(2, Bound::Excluded(b"b")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(3, Bound::Excluded(b"b")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(4, Bound::Excluded(b"b")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(5, Bound::Excluded(b"b")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c3");
  assert_eq!(ent.version(), 5);

  let ent = wal.lower_bound(6, Bound::Excluded(b"b")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c3");
  assert_eq!(ent.version(), 5);

  assert!(wal.lower_bound(1, Bound::Excluded(b"c")).is_none());
  assert!(wal.lower_bound(2, Bound::Excluded(b"c")).is_none());
  assert!(wal.lower_bound(3, Bound::Excluded(b"c")).is_none());
  assert!(wal.lower_bound(4, Bound::Excluded(b"c")).is_none());
  assert!(wal.lower_bound(5, Bound::Excluded(b"c")).is_none());
  assert!(wal.lower_bound(6, Bound::Excluded(b"c")).is_none());
}

fn ge<M>(wal: &mut OrderWal<M>)
where
  M: DynamicMemtable + MutableMemtable + 'static,
  M::Error: std::fmt::Debug,

  for<'a> M::Entry<'a, Active>: Entry<'a, Key = &'a [u8], Value = &'a [u8]>
    + RawEntry<'a, RawValue = &'a [u8]>
    + std::fmt::Debug,
  for<'a> M::Entry<'a, MaybeTombstone>: Entry<'a, Key = &'a [u8], Value = Option<&'a [u8]>>
    + RawEntry<'a, RawValue = Option<&'a [u8]>>
    + std::fmt::Debug,
{
  wal.insert(1, b"a", b"a1").unwrap();
  wal.insert(3, b"a", b"a2").unwrap();
  wal.insert(1, b"c", b"c1").unwrap();
  wal.insert(3, b"c", b"c2").unwrap();

  assert!(wal.lower_bound(0, Bound::Included(b"a")).is_none());
  assert!(wal.lower_bound(0, Bound::Included(b"b")).is_none());
  assert!(wal.lower_bound(0, Bound::Included(b"c")).is_none());

  let ent = wal.lower_bound(1, Bound::Included(b"a")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(2, Bound::Included(b"a")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(3, Bound::Included(b"a")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(4, Bound::Included(b"a")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(1, Bound::Included(b"b")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(2, Bound::Included(b"b")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(3, Bound::Included(b"b")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(4, Bound::Included(b"b")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(1, Bound::Included(b"b")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(2, Bound::Included(b"b")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(3, Bound::Included(b"b")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(4, Bound::Included(b"b")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  assert!(wal.lower_bound(0, Bound::Included(b"d")).is_none());
  assert!(wal.lower_bound(1, Bound::Included(b"d")).is_none());
  assert!(wal.lower_bound(2, Bound::Included(b"d")).is_none());
  assert!(wal.lower_bound(3, Bound::Included(b"d")).is_none());
  assert!(wal.lower_bound(4, Bound::Included(b"d")).is_none());
}

fn le<M>(wal: &mut OrderWal<M>)
where
  M: DynamicMemtable + MutableMemtable + 'static,
  M::Error: std::fmt::Debug,

  for<'a> M::Entry<'a, Active>: Entry<'a, Key = &'a [u8], Value = &'a [u8]>
    + RawEntry<'a, RawValue = &'a [u8]>
    + std::fmt::Debug,
  for<'a> M::Entry<'a, MaybeTombstone>: Entry<'a, Key = &'a [u8], Value = Option<&'a [u8]>>
    + RawEntry<'a, RawValue = Option<&'a [u8]>>
    + std::fmt::Debug,
{
  wal.insert(1, b"a", b"a1").unwrap();
  wal.insert(3, b"a", b"a2").unwrap();
  wal.insert(1, b"c", b"c1").unwrap();
  wal.insert(3, b"c", b"c2").unwrap();

  assert!(wal.upper_bound(0, Bound::Included(b"a")).is_none());
  assert!(wal.upper_bound(0, Bound::Included(b"b")).is_none());
  assert!(wal.upper_bound(0, Bound::Included(b"c")).is_none());

  let ent = wal.upper_bound(1, Bound::Included(b"a")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(2, Bound::Included(b"a")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(3, Bound::Included(b"a")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(4, Bound::Included(b"a")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(1, Bound::Included(b"b")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(2, Bound::Included(b"b")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(3, Bound::Included(b"b")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(4, Bound::Included(b"b")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(1, Bound::Included(b"c")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(2, Bound::Included(b"c")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(3, Bound::Included(b"c")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(4, Bound::Included(b"c")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(1, Bound::Included(b"d")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(2, Bound::Included(b"d")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(3, Bound::Included(b"d")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(4, Bound::Included(b"d")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);
}

fn lt<M>(wal: &mut OrderWal<M>)
where
  M: DynamicMemtable + MutableMemtable + 'static,
  M::Error: std::fmt::Debug,

  for<'a> M::Entry<'a, Active>: Entry<'a, Key = &'a [u8], Value = &'a [u8]>
    + RawEntry<'a, RawValue = &'a [u8]>
    + std::fmt::Debug,
  for<'a> M::Entry<'a, MaybeTombstone>: Entry<'a, Key = &'a [u8], Value = Option<&'a [u8]>>
    + RawEntry<'a, RawValue = Option<&'a [u8]>>
    + std::fmt::Debug,
{
  wal.insert(1, b"a", b"a1").unwrap();
  wal.insert(3, b"a", b"a2").unwrap();
  wal.insert(1, b"c", b"c1").unwrap();
  wal.insert(3, b"c", b"c2").unwrap();

  assert!(wal.upper_bound(0, Bound::Excluded(b"a")).is_none());
  assert!(wal.upper_bound(0, Bound::Excluded(b"b")).is_none());
  assert!(wal.upper_bound(0, Bound::Excluded(b"c")).is_none());
  assert!(wal.upper_bound(1, Bound::Excluded(b"a")).is_none());
  assert!(wal.upper_bound(2, Bound::Excluded(b"a")).is_none());

  let ent = wal.upper_bound(1, Bound::Excluded(b"b")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(2, Bound::Excluded(b"b")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(3, Bound::Excluded(b"b")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(4, Bound::Excluded(b"b")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(1, Bound::Excluded(b"c")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(2, Bound::Excluded(b"c")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(3, Bound::Excluded(b"c")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(4, Bound::Excluded(b"c")).unwrap();
  assert_eq!(ent.key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), b"a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(1, Bound::Excluded(b"d")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(2, Bound::Excluded(b"d")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(3, Bound::Excluded(b"d")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(4, Bound::Excluded(b"d")).unwrap();
  assert_eq!(ent.key(), b"c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), b"c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);
}

#[allow(clippy::needless_borrows_for_generic_args)]
fn insert<M>(wal: &mut OrderWal<M>)
where
  M: DynamicMemtable + MutableMemtable + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Entry<'a, Active>: Entry<'a, Value = &'a [u8]> + RawEntry<'a> + std::fmt::Debug,
  for<'a> M::Entry<'a, MaybeTombstone>:
    Entry<'a, Value = Option<&'a [u8]>> + RawEntry<'a> + std::fmt::Debug,
{
  let people = (0..100)
    .map(|_| {
      let p = Person::random();
      let v = std::format!("My name is {}", p.name);
      wal.insert(0, &p.to_vec(), v.as_bytes()).unwrap();
      (p, v)
    })
    .collect::<Vec<_>>();

  for (p, pv) in &people {
    assert!(wal.contains_key(0, &p.to_vec()));

    assert_eq!(wal.get(0, &p.to_vec()).unwrap().value(), pv.as_bytes());
  }
}

macro_rules! insert_with_value_builder {
  ($wal:ident) => {{
    let people = (0..100)
      .map(|_| {
        let p = Person::random();
        let v = std::format!("My name is {}", p.name);
        $wal
          .insert_with_value_builder(
            0,
            &p.to_vec(),
            ValueBuilder::once(v.len(), |buf: &mut VacantBuffer<'_>| {
              buf.put_slice(v.as_bytes()).map(|_| v.len())
            }),
          )
          .unwrap();
        (p, v)
      })
      .collect::<Vec<_>>();

    for (p, _) in &people {
      assert!($wal.contains_key(0, &p.to_vec()));
    }
  }};
}

fn bounded_insert_with_value_builder(wal: &mut OrderWal<BoundedTable>) {
  insert_with_value_builder!(wal);
}

fn unbounded_insert_with_value_builder(wal: &mut OrderWal<UnboundedTable>) {
  insert_with_value_builder!(wal);
}

macro_rules! insert_with_key_builder {
  ($wal:ident) => {{
    let people = (0..100)
      .map(|_| {
        let p = Person::random();
        let v = std::format!("My name is {}", p.name);
        $wal
          .insert_with_key_builder(
            0,
            KeyBuilder::once(p.encoded_len(), |buf| p.encode_to_buffer(buf)),
            v.as_bytes(),
          )
          .unwrap();
        (p, v)
      })
      .collect::<Vec<_>>();

    for (p, pv) in &people {
      assert!($wal.contains_key(0, &p.to_vec()));
      assert_eq!($wal.get(0, &p.to_vec()).unwrap().value(), pv.as_bytes());
    }
  }};
}

fn bounded_insert_with_key_builder(wal: &mut OrderWal<BoundedTable>) {
  insert_with_key_builder!(wal);
}

fn unbounded_insert_with_key_builder(wal: &mut OrderWal<UnboundedTable>) {
  insert_with_key_builder!(wal);
}

macro_rules! insert_with_bytes {
  ($wal:ident) => {{
    let people = (0..100)
      .map(|_| {
        let p = Person::random();
        let v = std::format!("My name is {}", p.name);
        $wal.insert(0, p.to_vec().as_slice(), v.as_bytes()).unwrap();
        (p, v)
      })
      .collect::<Vec<_>>();

    for (p, pv) in &people {
      assert!($wal.contains_key(0, &p.to_vec()));
      assert_eq!($wal.get(0, &p.to_vec()).unwrap().value(), pv.as_bytes());
    }
  }};
}

fn bounded_insert_with_bytes(wal: &mut OrderWal<BoundedTable>) {
  insert_with_bytes!(wal);
}

fn unbounded_insert_with_bytes(wal: &mut OrderWal<UnboundedTable>) {
  insert_with_bytes!(wal);
}

macro_rules! insert_with_builders {
  ($wal:ident) => {{
    let people = (0..1)
      .map(|_| {
        let p = Person::random();
        let pvec = p.to_vec();
        let v = std::format!("My name is {}", p.name);
        $wal
          .insert_with_builders(
            0,
            KeyBuilder::new(pvec.len(), |buf: &mut VacantBuffer<'_>| {
              p.encode_to_buffer(buf)
            }),
            ValueBuilder::new(v.len(), |buf: &mut VacantBuffer<'_>| {
              buf.put_slice(v.as_bytes()).map(|_| v.len())
            }),
          )
          .unwrap();
        (p, pvec, v)
      })
      .collect::<Vec<_>>();

    for (p, _, pv) in &people {
      assert!($wal.contains_key(0, &p.to_vec()));
      assert!($wal.contains_key_with_tombstone(0, &p.to_vec()));
      assert_eq!($wal.get(0, &p.to_vec()).unwrap().value(), pv.as_bytes());
      assert_eq!(
        $wal
          .get_with_tombstone(0, &p.to_vec())
          .unwrap()
          .value()
          .unwrap(),
        pv.as_bytes()
      );
    }
  }};
}

fn bounded_insert_with_builders(wal: &mut OrderWal<BoundedTable>) {
  insert_with_builders!(wal);
}

fn unbounded_insert_with_builders(wal: &mut OrderWal<UnboundedTable>) {
  insert_with_builders!(wal);
}
