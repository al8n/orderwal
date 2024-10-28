use core::ops::Bound;

use crate::memtable::{MultipleVersionMemtable, MultipleVersionMemtableEntry};
use multiple_version::{Reader, Writer};

use super::*;

expand_unit_tests!("linked": MultipleVersionGenericOrderWalLinkedTable<str, str> {
  mvcc,
  gt,
  ge,
  le,
  lt,
});

expand_unit_tests!("arena": MultipleVersionGenericOrderWalArenaTable<str, str> {
  mvcc,
  gt,
  ge,
  le,
  lt,
});

fn mvcc<M>(wal: &mut multiple_version::GenericOrderWal<str, str, M>)
where
  M: MultipleVersionMemtable<Key = str, Value = str> + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Item<'a>: MultipleVersionMemtableEntry<'a> + std::fmt::Debug,
{
  wal.insert(1, "a", "a1").unwrap();
  wal.insert(3, "a", "a2").unwrap();
  wal.insert(1, "c", "c1").unwrap();
  wal.insert(3, "c", "c2").unwrap();

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

  assert!(wal.get(0, "b").is_none());
  assert!(wal.get(1, "b").is_none());
  assert!(wal.get(2, "b").is_none());
  assert!(wal.get(3, "b").is_none());
  assert!(wal.get(4, "b").is_none());

  let ent = wal.get(1, "c").unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.get(2, "c").unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.get(3, "c").unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.get(4, "c").unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.version(), 3);

  assert!(wal.get(5, "d").is_none());
}

fn gt<M>(wal: &mut multiple_version::GenericOrderWal<str, str, M>)
where
  M: MultipleVersionMemtable<Key = str, Value = str> + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Item<'a>: MultipleVersionMemtableEntry<'a> + std::fmt::Debug,
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
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(2, Bound::Excluded("")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(3, Bound::Excluded("")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(1, Bound::Excluded("a")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(2, Bound::Excluded("a")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(3, Bound::Excluded("a")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(1, Bound::Excluded("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(2, Bound::Excluded("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(3, Bound::Excluded("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(4, Bound::Excluded("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(5, Bound::Excluded("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c3");
  assert_eq!(ent.version(), 5);

  let ent = wal.lower_bound(6, Bound::Excluded("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c3");
  assert_eq!(ent.version(), 5);

  assert!(wal.lower_bound(1, Bound::Excluded("c")).is_none());
  assert!(wal.lower_bound(2, Bound::Excluded("c")).is_none());
  assert!(wal.lower_bound(3, Bound::Excluded("c")).is_none());
  assert!(wal.lower_bound(4, Bound::Excluded("c")).is_none());
  assert!(wal.lower_bound(5, Bound::Excluded("c")).is_none());
  assert!(wal.lower_bound(6, Bound::Excluded("c")).is_none());
}

fn ge<M>(wal: &mut multiple_version::GenericOrderWal<str, str, M>)
where
  M: MultipleVersionMemtable<Key = str, Value = str> + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Item<'a>: MultipleVersionMemtableEntry<'a> + std::fmt::Debug,
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
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(2, Bound::Included("a")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(3, Bound::Included("a")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(4, Bound::Included("a")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(1, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(2, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(3, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(4, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(1, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(2, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.lower_bound(3, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.lower_bound(4, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.version(), 3);

  assert!(wal.lower_bound(0, Bound::Included("d")).is_none());
  assert!(wal.lower_bound(1, Bound::Included("d")).is_none());
  assert!(wal.lower_bound(2, Bound::Included("d")).is_none());
  assert!(wal.lower_bound(3, Bound::Included("d")).is_none());
  assert!(wal.lower_bound(4, Bound::Included("d")).is_none());
}

fn le<M>(wal: &mut multiple_version::GenericOrderWal<str, str, M>)
where
  M: MultipleVersionMemtable<Key = str, Value = str> + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Item<'a>: MultipleVersionMemtableEntry<'a> + std::fmt::Debug,
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
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(2, Bound::Included("a")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(3, Bound::Included("a")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(4, Bound::Included("a")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(1, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(2, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(3, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(4, Bound::Included("b")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(1, Bound::Included("c")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(2, Bound::Included("c")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(3, Bound::Included("c")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(4, Bound::Included("c")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(1, Bound::Included("d")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(2, Bound::Included("d")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(3, Bound::Included("d")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(4, Bound::Included("d")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.version(), 3);
}

fn lt<M>(wal: &mut multiple_version::GenericOrderWal<str, str, M>)
where
  M: MultipleVersionMemtable<Key = str, Value = str> + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Item<'a>: MultipleVersionMemtableEntry<'a> + std::fmt::Debug,
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
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(2, Bound::Excluded("b")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(3, Bound::Excluded("b")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(4, Bound::Excluded("b")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(1, Bound::Excluded("c")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(2, Bound::Excluded("c")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(3, Bound::Excluded("c")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(4, Bound::Excluded("c")).unwrap();
  assert_eq!(ent.key(), "a");
  assert_eq!(ent.value(), "a2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(1, Bound::Excluded("d")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(2, Bound::Excluded("d")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c1");
  assert_eq!(ent.version(), 1);

  let ent = wal.upper_bound(3, Bound::Excluded("d")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.version(), 3);

  let ent = wal.upper_bound(4, Bound::Excluded("d")).unwrap();
  assert_eq!(ent.key(), "c");
  assert_eq!(ent.value(), "c2");
  assert_eq!(ent.version(), 3);
}
