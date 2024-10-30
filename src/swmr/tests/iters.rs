use core::ops::Bound;
use std::collections::BTreeMap;

use base::{OrderWal, Reader, Writer};

use crate::memtable::{
  alternative::{Table, TableOptions},
  Memtable, MemtableEntry,
};

use super::*;

fn iter<M>(wal: &mut OrderWal<Person, String, M>)
where
  M: Memtable<Key = Person, Value = String> + 'static,
  for<'a> M::Item<'a>: MemtableEntry<'a>,
  M::Error: std::fmt::Debug,
{
  let mut people = (0..100)
    .map(|_| {
      let p = Person::random();
      let v = format!("My name is {}", p.name);
      wal.insert(&p, &v).unwrap();
      (p, v)
    })
    .collect::<Vec<_>>();

  people.sort_by(|a, b| a.0.cmp(&b.0));

  let mut iter = wal.iter();

  for (pwal, pvec) in people.iter().zip(iter.by_ref()) {
    assert!(pwal.0.equivalent(pvec.key()));
    assert_eq!(&pwal.1, pvec.value());
  }

  let mut rev_iter = wal.iter().rev();

  for (pwal, pvec) in people.iter().rev().zip(rev_iter.by_ref()) {
    assert!(pwal.0.equivalent(pvec.key()));
    assert_eq!(&pwal.1, pvec.value());
  }

  let wal = wal.reader();
  let mut iter = wal.iter();

  for (pwal, pvec) in people.iter().zip(iter.by_ref()) {
    assert!(pwal.0.equivalent(pvec.key()));
    assert_eq!(&pwal.1, pvec.value());
  }
}

fn bounds<M>(wal: &mut OrderWal<u32, u32, M>)
where
  M: Memtable<Key = u32, Value = u32> + 'static,
  for<'a> M::Item<'a>: MemtableEntry<'a>,
  M::Error: std::fmt::Debug,
{
  for i in 0..100u32 {
    wal.insert(&i, &i).unwrap();
  }

  let upper50 = wal.upper_bound(Bound::Included(&50u32)).unwrap();
  assert_eq!(upper50.value(), &50u32);
  let upper51 = wal.upper_bound(Bound::Excluded(&51u32)).unwrap();
  assert_eq!(upper51.value(), &50u32);

  let upper50 = unsafe {
    wal
      .upper_bound_by_bytes(Bound::Included(50u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(upper50.value(), &50u32);
  let upper51 = unsafe {
    wal
      .upper_bound_by_bytes(Bound::Excluded(51u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(upper51.value(), &50u32);

  let upper101 = wal.upper_bound(Bound::Included(&101u32)).unwrap();
  assert_eq!(upper101.value(), &99u32);
  let upper101 = unsafe {
    wal
      .upper_bound_by_bytes(Bound::Included(101u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(upper101.value(), &99u32);

  let upper_unbounded = wal.upper_bound::<u32>(Bound::Unbounded).unwrap();
  assert_eq!(upper_unbounded.value(), &99u32);
  let upper_unbounded = unsafe { wal.upper_bound_by_bytes(Bound::Unbounded).unwrap() };
  assert_eq!(upper_unbounded.value(), &99u32);

  let lower50 = wal.lower_bound(Bound::Included(&50u32)).unwrap();
  assert_eq!(lower50.value(), &50u32);
  let lower50 = unsafe {
    wal
      .lower_bound_by_bytes(Bound::Included(50u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(lower50.value(), &50u32);

  let lower51 = wal.lower_bound(Bound::Excluded(&51u32)).unwrap();
  assert_eq!(lower51.value(), &52u32);
  let lower51 = unsafe {
    wal
      .lower_bound_by_bytes(Bound::Excluded(51u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(lower51.value(), &52u32);

  let lower0 = wal.lower_bound(Bound::Excluded(&0u32)).unwrap();
  assert_eq!(lower0.value(), &1u32);
  let lower0 = unsafe {
    wal
      .lower_bound_by_bytes(Bound::Excluded(0u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(lower0.value(), &1u32);

  let lower_unbounded = wal.lower_bound::<u32>(Bound::Unbounded).unwrap();
  assert_eq!(lower_unbounded.value(), &0u32);
  let lower_unbounded = unsafe { wal.lower_bound_by_bytes(Bound::Unbounded).unwrap() };
  assert_eq!(lower_unbounded.value(), &0u32);

  let wal = wal.reader();
  let upper50 = wal.upper_bound(Bound::Included(&50u32)).unwrap();
  assert_eq!(upper50.value(), &50u32);
  let upper50 = unsafe {
    wal
      .upper_bound_by_bytes(Bound::Included(50u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(upper50.value(), &50u32);

  let upper51 = wal.upper_bound(Bound::Excluded(&51u32)).unwrap();
  assert_eq!(upper51.value(), &50u32);
  let upper51 = unsafe {
    wal
      .upper_bound_by_bytes(Bound::Excluded(51u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(upper51.value(), &50u32);

  let upper101 = wal.upper_bound(Bound::Included(&101u32)).unwrap();
  assert_eq!(upper101.value(), &99u32);
  let upper101 = unsafe {
    wal
      .upper_bound_by_bytes(Bound::Included(101u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(upper101.value(), &99u32);

  let upper_unbounded = wal.upper_bound::<u32>(Bound::Unbounded).unwrap();
  assert_eq!(upper_unbounded.value(), &99u32);
  let upper_unbounded = unsafe { wal.upper_bound_by_bytes(Bound::Unbounded).unwrap() };
  assert_eq!(upper_unbounded.value(), &99u32);

  let lower50 = wal.lower_bound(Bound::Included(&50u32)).unwrap();
  assert_eq!(lower50.value(), &50u32);
  let lower50 = unsafe {
    wal
      .lower_bound_by_bytes(Bound::Included(50u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(lower50.value(), &50u32);

  let lower51 = wal.lower_bound(Bound::Excluded(&51u32)).unwrap();
  assert_eq!(lower51.value(), &52u32);
  let lower51 = unsafe {
    wal
      .lower_bound_by_bytes(Bound::Excluded(51u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(lower51.value(), &52u32);

  let lower0 = wal.lower_bound(Bound::Excluded(&0u32)).unwrap();
  assert_eq!(lower0.value(), &1u32);
  let lower0 = unsafe {
    wal
      .lower_bound_by_bytes(Bound::Excluded(0u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(lower0.value(), &1u32);

  let lower_unbounded = wal.lower_bound::<u32>(Bound::Unbounded).unwrap();
  assert_eq!(lower_unbounded.value(), &0u32);
  let lower_unbounded = unsafe { wal.lower_bound_by_bytes(Bound::Unbounded).unwrap() };
  assert_eq!(lower_unbounded.value(), &0u32);
}

fn range<M>(wal: &mut OrderWal<Person, String, M>)
where
  M: Memtable<Key = Person, Value = String> + 'static,
  for<'a> M::Item<'a>: MemtableEntry<'a>,
  M::Error: std::fmt::Debug,
{
  let mut mid = Person::random();
  let people = (0..100)
    .map(|idx| {
      let p = Person::random();
      let v = format!("My name is {}", p.name);
      wal.insert(&p, &v).unwrap();

      if idx == 500 {
        mid = p.clone();
      }
      (p, v)
    })
    .collect::<BTreeMap<_, _>>();

  let mut iter = wal.range::<Person, _>(&mid..);

  for (pwal, pvec) in people.range(&mid..).zip(iter.by_ref()) {
    assert!(pwal.0.equivalent(pvec.key()));
    assert_eq!(&pwal.1, pvec.value());
  }

  assert!(iter.next().is_none());

  let wal = wal.reader();
  let mut iter = wal.range::<Person, _>(&mid..);

  for (pwal, pvec) in people.range(&mid..).zip(iter.by_ref()) {
    assert!(pwal.0.equivalent(pvec.key()));
    assert_eq!(&pwal.1, pvec.value());
  }

  let mut rev_iter = wal.range::<Person, _>(&mid..).rev();

  for (pwal, pvec) in people.range(&mid..).rev().zip(rev_iter.by_ref()) {
    assert!(pwal.0.equivalent(pvec.key()));
    assert_eq!(&pwal.1, pvec.value());
  }
}

fn entry_iter<M>(wal: &mut OrderWal<u32, u32, M>)
where
  M: Memtable<Key = u32, Value = u32> + 'static,
  for<'a> M::Item<'a>: MemtableEntry<'a>,
  M::Error: std::fmt::Debug,
{
  for i in 0..100u32 {
    wal.insert(&i, &i).unwrap();
  }

  let mut curr = wal.first();
  let mut cursor = 0;
  while let Some(mut ent) = curr {
    assert_eq!(ent.key(), &cursor);
    assert_eq!(ent.value(), &cursor);
    cursor += 1;
    curr = ent.next();
  }

  let curr = wal.last();

  let mut curr = curr.clone();
  let mut cursor = 100;
  while let Some(mut ent) = curr {
    cursor -= 1;
    assert_eq!(ent.key(), &cursor);
    assert_eq!(ent.value(), &cursor);
    curr = ent.prev();
  }
}

expand_unit_tests!("linked": OrderWalAlternativeTable<u32, u32> [TableOptions::Linked]: Table<_, _> {
  bounds,
  entry_iter,
});

expand_unit_tests!("arena": OrderWalAlternativeTable<u32, u32> [TableOptions::Arena(Default::default())]: Table<_, _> {
  bounds,
  entry_iter,
});

expand_unit_tests!("linked": OrderWalAlternativeTable<Person, String> [TableOptions::Linked]: Table<_, _> {
  range,
  iter,
});

expand_unit_tests!("arena": OrderWalAlternativeTable<Person, String> [TableOptions::Arena(Default::default())]: Table<_, _> {
  range,
  iter,
});
