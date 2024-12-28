use dbutils::{
  buffer::VacantBuffer,
  equivalentor::{TypeRefComparator, TypeRefQueryComparator},
  state::{Active, MaybeTombstone},
  types::{MaybeStructured, Str, Type},
};

use core::ops::Bound;

use crate::{
  generic::{GenericMemtable, OrderWal, Reader, Writer},
  memtable::{Entry, MutableMemtable, RawEntry},
  types::{KeyBuilder, ValueBuilder},
};

#[cfg(feature = "bounded")]
use crate::generic::BoundedTable;

#[cfg(feature = "unbounded")]
use crate::generic::UnboundedTable;

use super::*;

#[cfg(feature = "unbounded")]
expand_unit_tests!("unbounded": OrderWal<UnboundedTable<str, str>> [Default::default()]: UnboundedTable<_, _>  {
  mvcc,
  gt,
  ge,
  le,
  lt,
});

#[cfg(feature = "bounded")]
expand_unit_tests!("bounded": OrderWal<BoundedTable<str, str>> [Default::default()]: BoundedTable<_, _>  {
  mvcc,
  gt,
  ge,
  le,
  lt,
});

#[cfg(feature = "unbounded")]
expand_unit_tests!("unbounded": OrderWal<UnboundedTable<Person, String>> [Default::default()]: UnboundedTable<_, _> {
  insert,
  unbounded_insert_with_value_builder,
  unbounded_insert_with_key_builder,
  unbounded_insert_with_bytes,
  unbounded_insert_with_builders,
});

#[cfg(feature = "bounded")]
expand_unit_tests!("bounded": OrderWal<BoundedTable<Person, String>> [Default::default()]: BoundedTable<_, _> {
  insert,
  bounded_insert_with_value_builder,
  bounded_insert_with_key_builder,
  bounded_insert_with_bytes,
  bounded_insert_with_builders,
});

fn mvcc<M>(wal: &mut OrderWal<M>)
where
  M: GenericMemtable<str, str> + MutableMemtable + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Comparator: TypeRefComparator<'a, str> + TypeRefQueryComparator<'a, str, str>,
  for<'a> M::Entry<'a, Active>:
    Entry<'a, Key = Str<'a>, Value = Str<'a>> + RawEntry<'a, RawValue = &'a [u8]> + std::fmt::Debug,
  for<'a> M::Entry<'a, MaybeTombstone>: Entry<'a, Key = Str<'a>, Value = Option<Str<'a>>>
    + RawEntry<'a, RawValue = Option<&'a [u8]>>
    + std::fmt::Debug,
{
  wal.insert(1, "a", "a1").unwrap();
  wal.insert(3, "a", "a2").unwrap();
  wal.insert(1, "c", "c1").unwrap();
  wal.insert(3, "c", "c2").unwrap();

  let ent = wal.get(1, "a").unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.get(2, "a").unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.get(3, "a").unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.get(4, "a").unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  assert!(wal.get(0, "b").is_none());
  assert!(wal.get(1, "b").is_none());
  assert!(wal.get(2, "b").is_none());
  assert!(wal.get(3, "b").is_none());
  assert!(wal.get(4, "b").is_none());

  let ent = wal.get(1, "c").unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.get(2, "c").unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.get(3, "c").unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.get(4, "c").unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  assert!(wal.get(5, "d").is_none());
}

fn gt<M>(wal: &mut OrderWal<M>)
where
  M: GenericMemtable<str, str> + MutableMemtable + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Comparator: TypeRefComparator<'a, str> + TypeRefQueryComparator<'a, str, str>,
  for<'a> M::Entry<'a, Active>:
    Entry<'a, Key = Str<'a>, Value = Str<'a>> + RawEntry<'a, RawValue = &'a [u8]> + std::fmt::Debug,
  for<'a> M::Entry<'a, MaybeTombstone>: Entry<'a, Key = Str<'a>, Value = Option<Str<'a>>>
    + RawEntry<'a, RawValue = Option<&'a [u8]>>
    + std::fmt::Debug,
{
  wal.insert(1, "a", "a1").unwrap();
  wal.insert(3, "a", "a2").unwrap();
  wal.insert(1, "c", "c1").unwrap();
  wal.insert(3, "c", "c2").unwrap();
  wal.insert(5, "c", "c3").unwrap();

  assert!(wal.lower_bound(0, Bound::Excluded("a")).is_none());
  assert!(wal.lower_bound(0, Bound::Excluded("b")).is_none());
  assert!(wal.lower_bound(0, Bound::Excluded("c")).is_none());

  let ent = wal.lower_bound(1, Bound::Excluded("")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(2, Bound::Excluded("")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(3, Bound::Excluded("")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(1, Bound::Excluded("a")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(2, Bound::Excluded("a")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(3, Bound::Excluded("a")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(1, Bound::Excluded("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(2, Bound::Excluded("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(3, Bound::Excluded("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(4, Bound::Excluded("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(5, Bound::Excluded("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c3");
  assert_eq!(ent.version(), 5);

  let ent = wal.lower_bound(6, Bound::Excluded("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c3");
  assert_eq!(ent.version(), 5);

  assert!(wal.lower_bound(1, Bound::Excluded("c")).is_none());
  assert!(wal.lower_bound(2, Bound::Excluded("c")).is_none());
  assert!(wal.lower_bound(3, Bound::Excluded("c")).is_none());
  assert!(wal.lower_bound(4, Bound::Excluded("c")).is_none());
  assert!(wal.lower_bound(5, Bound::Excluded("c")).is_none());
  assert!(wal.lower_bound(6, Bound::Excluded("c")).is_none());
}

fn ge<M>(wal: &mut OrderWal<M>)
where
  M: GenericMemtable<str, str> + MutableMemtable + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Comparator: TypeRefComparator<'a, str> + TypeRefQueryComparator<'a, str, str>,
  for<'a> M::Entry<'a, Active>:
    Entry<'a, Key = Str<'a>, Value = Str<'a>> + RawEntry<'a, RawValue = &'a [u8]> + std::fmt::Debug,
  for<'a> M::Entry<'a, MaybeTombstone>: Entry<'a, Key = Str<'a>, Value = Option<Str<'a>>>
    + RawEntry<'a, RawValue = Option<&'a [u8]>>
    + std::fmt::Debug,
{
  wal.insert(1, "a", "a1").unwrap();
  wal.insert(3, "a", "a2").unwrap();
  wal.insert(1, "c", "c1").unwrap();
  wal.insert(3, "c", "c2").unwrap();

  assert!(wal.lower_bound(0, Bound::Included("a")).is_none());
  assert!(wal.lower_bound(0, Bound::Included("b")).is_none());
  assert!(wal.lower_bound(0, Bound::Included("c")).is_none());

  let ent = wal.lower_bound(1, Bound::Included("a")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(2, Bound::Included("a")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(3, Bound::Included("a")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(4, Bound::Included("a")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(1, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(2, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(3, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(4, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(1, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(2, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(3, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(4, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  assert!(wal.lower_bound(0, Bound::Included("d")).is_none());
  assert!(wal.lower_bound(1, Bound::Included("d")).is_none());
  assert!(wal.lower_bound(2, Bound::Included("d")).is_none());
  assert!(wal.lower_bound(3, Bound::Included("d")).is_none());
  assert!(wal.lower_bound(4, Bound::Included("d")).is_none());
}

fn le<M>(wal: &mut OrderWal<M>)
where
  M: GenericMemtable<str, str> + MutableMemtable + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Comparator: TypeRefComparator<'a, str> + TypeRefQueryComparator<'a, str, str>,
  for<'a> M::Entry<'a, Active>:
    Entry<'a, Key = Str<'a>, Value = Str<'a>> + RawEntry<'a, RawValue = &'a [u8]> + std::fmt::Debug,
  for<'a> M::Entry<'a, MaybeTombstone>: Entry<'a, Key = Str<'a>, Value = Option<Str<'a>>>
    + RawEntry<'a, RawValue = Option<&'a [u8]>>
    + std::fmt::Debug,
{
  wal.insert(1, "a", "a1").unwrap();
  wal.insert(3, "a", "a2").unwrap();
  wal.insert(1, "c", "c1").unwrap();
  wal.insert(3, "c", "c2").unwrap();

  assert!(wal.upper_bound(0, Bound::Included("a")).is_none());
  assert!(wal.upper_bound(0, Bound::Included("b")).is_none());
  assert!(wal.upper_bound(0, Bound::Included("c")).is_none());

  let ent = wal.upper_bound(1, Bound::Included("a")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(2, Bound::Included("a")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(3, Bound::Included("a")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(4, Bound::Included("a")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(1, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(2, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(3, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(4, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(1, Bound::Included("c")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(2, Bound::Included("c")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(3, Bound::Included("c")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(4, Bound::Included("c")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(1, Bound::Included("d")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(2, Bound::Included("d")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(3, Bound::Included("d")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(4, Bound::Included("d")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);
}

fn lt<M>(wal: &mut OrderWal<M>)
where
  M: GenericMemtable<str, str> + MutableMemtable + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Comparator: TypeRefComparator<'a, str> + TypeRefQueryComparator<'a, str, str>,
  for<'a> M::Entry<'a, Active>:
    Entry<'a, Key = Str<'a>, Value = Str<'a>> + RawEntry<'a, RawValue = &'a [u8]> + std::fmt::Debug,
  for<'a> M::Entry<'a, MaybeTombstone>: Entry<'a, Key = Str<'a>, Value = Option<Str<'a>>>
    + RawEntry<'a, RawValue = Option<&'a [u8]>>
    + std::fmt::Debug,
{
  wal.insert(1, "a", "a1").unwrap();
  wal.insert(3, "a", "a2").unwrap();
  wal.insert(1, "c", "c1").unwrap();
  wal.insert(3, "c", "c2").unwrap();

  assert!(wal.upper_bound(0, Bound::Excluded("a")).is_none());
  assert!(wal.upper_bound(0, Bound::Excluded("b")).is_none());
  assert!(wal.upper_bound(0, Bound::Excluded("c")).is_none());
  assert!(wal.upper_bound(1, Bound::Excluded("a")).is_none());
  assert!(wal.upper_bound(2, Bound::Excluded("a")).is_none());

  let ent = wal.upper_bound(1, Bound::Excluded("b")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(2, Bound::Excluded("b")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(3, Bound::Excluded("b")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(4, Bound::Excluded("b")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(1, Bound::Excluded("c")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(2, Bound::Excluded("c")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.raw_value(), b"a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(3, Bound::Excluded("c")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(4, Bound::Excluded("c")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.raw_key(), b"a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.raw_value(), b"a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(1, Bound::Excluded("d")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(2, Bound::Excluded("d")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.raw_value(), b"c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(3, Bound::Excluded("d")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(4, Bound::Excluded("d")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.raw_key(), b"c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.raw_value(), b"c2");
  assert_eq!(ent.version(), 3);
}

#[allow(clippy::needless_borrows_for_generic_args)]
fn insert<M>(wal: &mut OrderWal<M>)
where
  M: GenericMemtable<Person, String> + MutableMemtable + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Comparator: TypeRefComparator<'a, Person> + TypeRefQueryComparator<'a, Person, Person>,
  for<'a> M::Entry<'a, Active>: Entry<'a, Value = Str<'a>> + RawEntry<'a> + std::fmt::Debug,
  for<'a> M::Entry<'a, MaybeTombstone>:
    Entry<'a, Value = Option<Str<'a>>> + RawEntry<'a> + std::fmt::Debug,
{
  let people = (0..100)
    .map(|_| {
      let p = Person::random();
      let v = std::format!("My name is {}", p.name);
      wal.insert(0, &p, &v).unwrap();
      (p, v)
    })
    .collect::<Vec<_>>();

  for (p, pv) in &people {
    assert!(wal.contains_key(0, p));

    assert_eq!(wal.get(0, p).unwrap().value(), pv);
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
            &p,
            ValueBuilder::once(v.len(), |buf: &mut VacantBuffer<'_>| {
              buf.put_slice(v.as_bytes()).map(|_| v.len())
            }),
          )
          .unwrap();
        (p, v)
      })
      .collect::<Vec<_>>();

    for (p, _) in &people {
      assert!($wal.contains_key(0, p));
      assert!($wal.contains_key(0, &p.as_ref()));
    }
  }};
}

#[cfg(feature = "bounded")]
fn bounded_insert_with_value_builder(wal: &mut OrderWal<BoundedTable<Person, String>>) {
  insert_with_value_builder!(wal);
}

#[cfg(feature = "unbounded")]
fn unbounded_insert_with_value_builder(wal: &mut OrderWal<UnboundedTable<Person, String>>) {
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
            &v,
          )
          .unwrap();
        (p, v)
      })
      .collect::<Vec<_>>();

    for (p, pv) in &people {
      assert!($wal.contains_key(0, p));
      assert_eq!($wal.get(0, p).unwrap().value(), pv);
    }
  }};
}

#[cfg(feature = "bounded")]
fn bounded_insert_with_key_builder(wal: &mut OrderWal<BoundedTable<Person, String>>) {
  insert_with_key_builder!(wal);
}

#[cfg(feature = "unbounded")]
fn unbounded_insert_with_key_builder(wal: &mut OrderWal<UnboundedTable<Person, String>>) {
  insert_with_key_builder!(wal);
}

macro_rules! insert_with_bytes {
  ($wal:ident) => {{
    let people = (0..100)
      .map(|_| {
        let p = Person::random();
        let v = std::format!("My name is {}", p.name);
        unsafe {
          $wal
            .insert(
              0,
              MaybeStructured::from_slice(p.to_vec().as_slice()),
              MaybeStructured::from_slice(v.as_bytes()),
            )
            .unwrap();
        }
        (p, v)
      })
      .collect::<Vec<_>>();

    for (p, pv) in &people {
      assert!($wal.contains_key(0, p));
      assert!($wal.contains_key(0, &p.as_ref()));
      assert_eq!($wal.get(0, p).unwrap().value(), pv);
    }
  }};
}

#[cfg(feature = "bounded")]
fn bounded_insert_with_bytes(wal: &mut OrderWal<BoundedTable<Person, String>>) {
  insert_with_bytes!(wal);
}

#[cfg(feature = "unbounded")]
fn unbounded_insert_with_bytes(wal: &mut OrderWal<UnboundedTable<Person, String>>) {
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
      assert!($wal.contains_key(0, p));
      assert!($wal.contains_key_with_tombstone(0, p));
      assert_eq!($wal.get(0, p).unwrap().value(), pv);
      assert_eq!($wal.get_with_tombstone(0, p).unwrap().value().unwrap(), pv);
    }
  }};
}

#[cfg(feature = "bounded")]
fn bounded_insert_with_builders(wal: &mut OrderWal<BoundedTable<Person, String>>) {
  insert_with_builders!(wal);
}

#[cfg(feature = "unbounded")]
fn unbounded_insert_with_builders(wal: &mut OrderWal<UnboundedTable<Person, String>>) {
  insert_with_builders!(wal);
}
