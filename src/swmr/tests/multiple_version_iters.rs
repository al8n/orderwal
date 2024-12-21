use core::ops::Bound;

use crate::memtable::{
  alternative::{MultipleVersionTable, TableOptions},
  MultipleVersionMemtable, MultipleVersionMemtableEntry,
};
use multiple_version::{Reader, Writer};

use super::*;

#[cfg(feature = "std")]
expand_unit_tests!("linked": MultipleVersionOrderWalAlternativeTable<str, str> [TableOptions::Linked]: MultipleVersionTable<_, _> {
  iter_with_tombstone_mvcc,
});

expand_unit_tests!("arena": MultipleVersionOrderWalAlternativeTable<str, str> [TableOptions::Arena(Default::default())]: MultipleVersionTable<_, _> {
  iter_with_tombstone_mvcc,
});

#[cfg(feature = "std")]
expand_unit_tests!("linked": MultipleVersionOrderWalAlternativeTable<String, String> [TableOptions::Linked]: MultipleVersionTable<_, _> {
  iter_next,
  iter_with_tombstone_next_by_entry,
  iter_with_tombstone_next_by_with_tombstone_entry,
  range_next,
  iter_prev,
  range_prev,
  iter_with_tombstone_prev_by_entry,
  iter_with_tombstone_prev_by_with_tombstone_entry,
});

macro_rules! arena_builder {
  () => {{
    crate::Builder::new()
      .with_memtable_options(
        crate::memtable::arena::TableOptions::new()
          .with_capacity(1024 * 1024)
          .into(),
      )
      .with_capacity(8 * 1024)
  }};
}

expand_unit_tests!("arena": MultipleVersionOrderWalAlternativeTable<String, String> [TableOptions::Arena(Default::default())]: MultipleVersionTable<_, _> {
  iter_next(arena_builder!()),
  iter_with_tombstone_next_by_entry(arena_builder!()),
  iter_with_tombstone_next_by_with_tombstone_entry(arena_builder!()),
  range_next(arena_builder!()),
  iter_prev(arena_builder!()),
  range_prev(arena_builder!()),
  iter_with_tombstone_prev_by_entry(arena_builder!()),
  iter_with_tombstone_prev_by_with_tombstone_entry(arena_builder!()),
});

fn make_int_key(i: usize) -> String {
  ::std::format!("{:05}", i)
}

fn make_value(i: usize) -> String {
  ::std::format!("v{:05}", i)
}

fn iter_with_tombstone_mvcc<M>(wal: &mut multiple_version::OrderWal<str, str, M>)
where
  M: MultipleVersionMemtable<Key = str, Value = str> + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Item<'a>: MultipleVersionMemtableEntry<'a> + std::fmt::Debug,
{
  wal.insert(1, "a", "a1").unwrap();
  wal.insert(3, "a", "a2").unwrap();
  wal.insert(1, "c", "c1").unwrap();
  wal.insert(3, "c", "c2").unwrap();

  let mut iter = wal.iter_with_tombstone(0);
  let mut num = 0;
  while iter.next().is_some() {
    num += 1;
  }
  assert_eq!(num, 0);

  let mut iter = wal.iter_with_tombstone(1);
  let mut num = 0;
  while iter.next().is_some() {
    num += 1;
  }
  assert_eq!(num, 2);

  let mut iter = wal.iter_with_tombstone(2);
  let mut num = 0;
  while iter.next().is_some() {
    num += 1;
  }
  assert_eq!(num, 2);

  let mut iter = wal.iter_with_tombstone(3);
  let mut num = 0;
  while iter.next().is_some() {
    num += 1;
  }
  assert_eq!(num, 4);

  let upper_bound = wal.upper_bound(1, Bound::Included("b")).unwrap();
  assert_eq!(upper_bound.value(), "a1");

  let upper_bound = wal
    .upper_bound_with_tombstone(1, Bound::Included("b"))
    .unwrap();
  assert_eq!(upper_bound.value().unwrap(), "a1");

  let upper_bound = unsafe { wal.upper_bound_by_bytes(1, Bound::Included(b"b")).unwrap() };
  assert_eq!(upper_bound.value(), "a1");

  let upper_bound = unsafe {
    wal
      .upper_bound_with_tombstone_by_bytes(1, Bound::Included(b"b"))
      .unwrap()
  };
  assert_eq!(upper_bound.value().unwrap(), "a1");

  let lower_bound = wal.lower_bound(1, Bound::Included("b")).unwrap();
  assert_eq!(lower_bound.value(), "c1");

  let lower_bound = wal
    .lower_bound_with_tombstone(1, Bound::Included("b"))
    .unwrap();
  assert_eq!(lower_bound.value().unwrap(), "c1");

  let lower_bound = unsafe { wal.lower_bound_by_bytes(1, Bound::Included(b"b")).unwrap() };
  assert_eq!(lower_bound.value(), "c1");

  let lower_bound = unsafe {
    wal
      .lower_bound_with_tombstone_by_bytes(1, Bound::Included(b"b"))
      .unwrap()
  };
  assert_eq!(lower_bound.value().unwrap(), "c1");
}

fn iter_next<M>(wal: &mut multiple_version::OrderWal<String, String, M>)
where
  M: MultipleVersionMemtable<Key = String, Value = String> + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Item<'a>: MultipleVersionMemtableEntry<'a> + std::fmt::Debug,
{
  const N: usize = 100;

  for i in (0..N).rev() {
    wal.insert(0, &make_int_key(i), &make_value(i)).unwrap();
  }

  let iter = wal.iter_with_tombstone(0);

  let mut i = 0;
  for ent in iter {
    assert_eq!(ent.key(), make_int_key(i).as_str());
    assert_eq!(ent.raw_key(), make_int_key(i).as_bytes());
    assert_eq!(ent.value().unwrap(), make_value(i).as_str());
    assert_eq!(ent.raw_value().unwrap(), make_value(i).as_bytes());
    i += 1;
  }

  assert_eq!(i, N);

  let iter = wal.iter(0);
  let mut i = 0;
  for ent in iter {
    assert_eq!(ent.key(), make_int_key(i).as_str());
    assert_eq!(ent.raw_key(), make_int_key(i).as_bytes());
    assert_eq!(ent.value(), make_value(i).as_str());
    assert_eq!(ent.raw_value(), make_value(i).as_bytes());
    i += 1;
  }

  assert_eq!(i, N);

  let iter = wal.values(0);

  let mut i = 0;
  for ent in iter {
    assert_eq!(ent.value(), make_value(i).as_str());
    assert_eq!(ent.raw_value(), make_value(i).as_bytes());
    i += 1;
  }

  assert_eq!(i, N);

  let iter = wal.keys(0);
  let mut i = 0;
  for ent in iter {
    assert_eq!(ent.key(), make_int_key(i).as_str());
    assert_eq!(ent.raw_key(), make_int_key(i).as_bytes());
    i += 1;
  }

  assert_eq!(i, N);
}

fn iter_with_tombstone_next_by_entry<M>(wal: &mut multiple_version::OrderWal<String, String, M>)
where
  M: MultipleVersionMemtable<Key = String, Value = String> + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Item<'a>: MultipleVersionMemtableEntry<'a> + std::fmt::Debug,
{
  const N: usize = 100;

  for i in (0..N).rev() {
    wal.insert(0, &make_int_key(i), &make_value(i)).unwrap();
  }

  let mut ent = wal.first(0).clone();
  #[cfg(feature = "std")]
  std::println!("{ent:?}");
  let mut i = 0;
  while let Some(ref mut entry) = ent {
    assert_eq!(entry.key(), make_int_key(i).as_str());
    assert_eq!(entry.value(), make_value(i).as_str());
    ent = entry.next();
    i += 1;
  }
  assert_eq!(i, N);

  let mut ent = wal.keys(0).next().clone();
  #[cfg(feature = "std")]
  std::println!("{ent:?}");

  let mut i = 0;
  while let Some(ref mut entry) = ent {
    assert_eq!(entry.key(), make_int_key(i).as_str());
    ent = entry.next();
    i += 1;
  }
  assert_eq!(i, N);

  let mut ent = wal.values(0).next().clone();
  #[cfg(feature = "std")]
  std::println!("{ent:?}");

  let mut i = 0;
  while let Some(ref mut entry) = ent {
    assert_eq!(entry.value(), make_value(i).as_str());
    ent = entry.next();
    i += 1;
  }
  assert_eq!(i, N);
}

fn iter_with_tombstone_next_by_with_tombstone_entry<M>(
  wal: &mut multiple_version::OrderWal<String, String, M>,
) where
  M: MultipleVersionMemtable<Key = String, Value = String> + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Item<'a>: MultipleVersionMemtableEntry<'a> + std::fmt::Debug,
  for<'a> M::MultipleVersionEntry<'a>: std::fmt::Debug,
{
  const N: usize = 100;

  for i in 0..N {
    let k = make_int_key(i);
    let v = make_value(i);
    wal.insert(0, &k, &v).unwrap();
    wal.remove(1, &k).unwrap();
  }

  let mut ent = wal.first(0).clone();
  let mut i = 0;
  while let Some(ref mut entry) = ent {
    assert_eq!(entry.key(), make_int_key(i).as_str());
    assert_eq!(entry.value(), make_value(i).as_str());
    ent = entry.next();
    i += 1;
  }
  assert_eq!(i, N);

  let mut ent = wal.first_with_tombstone(1).clone();
  #[cfg(feature = "std")]
  std::println!("{ent:?}");
  let mut i = 0;
  while let Some(ref mut entry) = ent {
    if i % 2 == 1 {
      assert_eq!(entry.version(), 0);
      assert_eq!(entry.key(), make_int_key(i / 2).as_str());
      assert_eq!(entry.value().unwrap(), make_value(i / 2).as_str());
    } else {
      assert_eq!(entry.version(), 1);
      assert_eq!(entry.key(), make_int_key(i / 2).as_str());
      assert!(entry.value().is_none());
    }

    ent = entry.next();
    i += 1;
  }
  assert_eq!(i, N * 2);
  let ent = wal.first(1);
  assert!(ent.is_none());
}

fn range_next<M>(wal: &mut multiple_version::OrderWal<String, String, M>)
where
  M: MultipleVersionMemtable<Key = String, Value = String> + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Item<'a>: MultipleVersionMemtableEntry<'a> + std::fmt::Debug,
{
  const N: usize = 100;

  for i in (0..N).rev() {
    wal.insert(0, &make_int_key(i), &make_value(i)).unwrap();
  }

  let upper = make_int_key(50);
  let mut i = 0;
  let mut iter = wal.range(0, ..=upper.as_str());
  for ent in &mut iter {
    assert_eq!(ent.key(), make_int_key(i).as_str());
    assert_eq!(ent.raw_key(), make_int_key(i).as_bytes());
    assert_eq!(ent.value(), make_value(i).as_str());
    assert_eq!(ent.raw_value(), make_value(i).as_bytes());
    i += 1;
  }

  assert_eq!(i, 51);

  let mut i = 0;
  let mut iter = wal.range_with_tombstone(0, ..=upper.as_str());
  for ent in &mut iter {
    assert_eq!(ent.key(), make_int_key(i).as_str());
    assert_eq!(ent.raw_key(), make_int_key(i).as_bytes());
    assert_eq!(ent.value().unwrap(), make_value(i).as_str());
    assert_eq!(ent.raw_value().unwrap(), make_value(i).as_bytes());
    i += 1;
  }

  assert_eq!(i, 51);

  let mut i = 0;
  let mut iter = wal.range_keys(0, ..=upper.as_str());
  for ent in &mut iter {
    assert_eq!(ent.key(), make_int_key(i).as_str());
    assert_eq!(ent.raw_key(), make_int_key(i).as_bytes());
    i += 1;
  }

  assert_eq!(i, 51);

  let mut i = 0;
  let mut iter = wal.range_values(0, ..=upper.as_str());
  for ent in &mut iter {
    assert_eq!(ent.value(), make_value(i).as_str());
    assert_eq!(ent.raw_value(), make_value(i).as_bytes());
    i += 1;
  }
  assert_eq!(i, 51);
}

fn iter_prev<M>(wal: &mut multiple_version::OrderWal<String, String, M>)
where
  M: MultipleVersionMemtable<Key = String, Value = String> + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Item<'a>: MultipleVersionMemtableEntry<'a> + std::fmt::Debug,
{
  const N: usize = 100;

  for i in 0..N {
    wal.insert(0, &make_int_key(i), &make_value(i)).unwrap();
  }

  let iter = wal.iter_with_tombstone(0).rev();
  let mut i = N;
  for ent in iter {
    assert_eq!(ent.key(), make_int_key(i - 1).as_str());
    assert_eq!(ent.value().unwrap(), make_value(i - 1).as_str());
    i -= 1;
  }

  assert_eq!(i, 0);

  let iter = wal.iter(0).rev();
  let mut i = N;
  for ent in iter {
    assert_eq!(ent.key(), make_int_key(i - 1).as_str());
    assert_eq!(ent.value(), make_value(i - 1).as_str());
    i -= 1;
  }

  assert_eq!(i, 0);

  let iter = wal.values(0).rev();
  let mut i = N;
  for ent in iter {
    assert_eq!(ent.value(), make_value(i - 1).as_str());
    i -= 1;
  }

  assert_eq!(i, 0);

  let iter = wal.keys(0).rev();
  let mut i = N;
  for ent in iter {
    assert_eq!(ent.key(), make_int_key(i - 1).as_str());
    i -= 1;
  }

  assert_eq!(i, 0);
}

fn iter_with_tombstone_prev_by_entry<M>(wal: &mut multiple_version::OrderWal<String, String, M>)
where
  M: MultipleVersionMemtable<Key = String, Value = String> + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Item<'a>: MultipleVersionMemtableEntry<'a> + std::fmt::Debug,
{
  const N: usize = 100;

  for i in 0..N {
    wal.insert(0, &make_int_key(i), &make_value(i)).unwrap();
  }

  let mut ent = wal.last(0);

  let mut i = 0;
  while let Some(ref mut entry) = ent {
    i += 1;
    assert_eq!(entry.key(), make_int_key(N - i).as_str());
    assert_eq!(entry.value(), make_value(N - i).as_str());
    ent = entry.prev();
  }
  assert_eq!(i, N);

  let mut ent = wal.values(0).next_back();

  let mut i = 0;
  while let Some(ref mut entry) = ent {
    i += 1;
    assert_eq!(entry.value(), make_value(N - i).as_str());
    ent = entry.prev();
  }

  assert_eq!(i, N);

  let mut ent = wal.keys(0).next_back();

  let mut i = 0;
  while let Some(ref mut entry) = ent {
    i += 1;
    assert_eq!(entry.key(), make_int_key(N - i).as_str());
    ent = entry.prev();
  }

  assert_eq!(i, N);
}

fn iter_with_tombstone_prev_by_with_tombstone_entry<M>(
  wal: &mut multiple_version::OrderWal<String, String, M>,
) where
  M: MultipleVersionMemtable<Key = String, Value = String> + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Item<'a>: MultipleVersionMemtableEntry<'a> + std::fmt::Debug,
  for<'a> M::MultipleVersionEntry<'a>: MultipleVersionMemtableEntry<'a> + std::fmt::Debug,
{
  const N: usize = 100;

  for i in 0..N {
    let k = make_int_key(i);
    let v = make_value(i);
    wal.insert(0, &k, &v).unwrap();
    wal.remove(1, &k).unwrap();
  }

  let mut ent = wal.last(0);
  let mut i = 0;
  while let Some(ref mut entry) = ent {
    i += 1;
    assert_eq!(entry.key(), make_int_key(N - i).as_str());
    assert_eq!(entry.value(), make_value(N - i).as_str());
    ent = entry.prev();
  }
  assert_eq!(i, N);

  let mut ent = wal.last_with_tombstone(1);
  let mut i = 0;
  while let Some(ref mut entry) = ent {
    if i % 2 == 0 {
      assert_eq!(entry.version(), 0);
      assert_eq!(entry.key(), make_int_key(N - 1 - i / 2).as_str());
      assert_eq!(entry.value().unwrap(), make_value(N - 1 - i / 2).as_str());
    } else {
      assert_eq!(entry.version(), 1);
      assert_eq!(entry.key(), make_int_key(N - 1 - i / 2).as_str());
      assert!(entry.value().is_none());
    }

    ent = entry.prev();
    i += 1;
  }

  assert_eq!(i, N * 2);
  let ent = wal.last(1);
  assert!(ent.is_none());
}

fn range_prev<M>(wal: &mut multiple_version::OrderWal<String, String, M>)
where
  M: MultipleVersionMemtable<Key = String, Value = String> + 'static,
  M::Error: std::fmt::Debug,
  for<'a> M::Item<'a>: MultipleVersionMemtableEntry<'a> + std::fmt::Debug,
{
  const N: usize = 100;

  for i in 0..N {
    wal.insert(0, &make_int_key(i), &make_value(i)).unwrap();
  }

  let lower = make_int_key(50);
  let it = wal.range(0, lower.as_str()..).rev();
  let mut i = N - 1;

  for ent in it {
    assert_eq!(ent.key(), make_int_key(i).as_str());
    assert_eq!(ent.raw_key(), make_int_key(i).as_bytes());
    assert_eq!(ent.value(), make_value(i).as_str());
    assert_eq!(ent.raw_value(), make_value(i).as_bytes());
    assert_eq!(ent.version(), 0);
    i -= 1;
  }

  assert_eq!(i, 49);

  let it = wal.range_with_tombstone(0, lower.as_str()..).rev();
  let mut i = N - 1;

  for ent in it {
    assert_eq!(ent.key(), make_int_key(i).as_str());
    assert_eq!(ent.raw_key(), make_int_key(i).as_bytes());
    assert_eq!(ent.value().unwrap(), make_value(i).as_str());
    assert_eq!(ent.raw_value().unwrap(), make_value(i).as_bytes());
    assert_eq!(ent.version(), 0);
    i -= 1;
  }

  assert_eq!(i, 49);

  let mut i = N - 1;
  let mut iter = wal.range_keys(0, lower.as_str()..).rev();
  for ent in &mut iter {
    assert_eq!(ent.key(), make_int_key(i).as_str());
    assert_eq!(ent.raw_key(), make_int_key(i).as_bytes());
    assert_eq!(ent.version(), 0);
    i -= 1;
  }
  assert_eq!(i, 49);

  let mut i = N - 1;
  let mut iter = wal.range_values(0, lower.as_str()..).rev();
  for ent in &mut iter {
    assert_eq!(ent.value(), make_value(i).as_str());
    assert_eq!(ent.raw_value(), make_value(i).as_bytes());
    assert_eq!(ent.version(), 0);
    i -= 1;
  }
  assert_eq!(i, 49);
}
