use base::OrderWal;

use dbutils::{buffer::VacantBuffer, types::MaybeStructured};

use std::collections::BTreeMap;

use crate::{
  memtable::{alternative::TableOptions, Memtable, MemtableEntry},
  swmr::base::{Reader, Writer},
  types::{KeyBuilder, ValueBuilder},
};

use super::*;

fn first<M>(wal: &mut OrderWal<Person, String, M>)
where
  M: Memtable<Key = Person, Value = String> + 'static,
  for<'a> M::Item<'a>: MemtableEntry<'a>,
  M::Error: std::fmt::Debug,
{
  let people = (0..10)
    .map(|_| {
      let p = Person::random();
      let v = std::format!("My name is {}", p.name);
      wal.insert(&p, &v).unwrap();

      (p, v)
    })
    .collect::<BTreeMap<_, _>>();

  let ent = wal.first().unwrap();
  let (p, v) = people.first_key_value().unwrap();
  assert!(ent.key().equivalent(p));
  assert_eq!(ent.value(), v);

  let wal = wal.reader();
  let ent = wal.first().unwrap();
  let (p, v) = people.first_key_value().unwrap();
  assert!(ent.key().equivalent(p));
  assert_eq!(ent.value(), v);
}

fn last<M>(wal: &mut OrderWal<Person, String, M>)
where
  M: Memtable<Key = Person, Value = String> + 'static,
  for<'a> M::Item<'a>: MemtableEntry<'a>,
  M::Error: std::fmt::Debug,
{
  let people = (0..10)
    .map(|_| {
      let p = Person::random();
      let v = std::format!("My name is {}", p.name);
      wal.insert(&p, &v).unwrap();

      (p, v)
    })
    .collect::<BTreeMap<_, _>>();

  let ent = wal.last().unwrap();
  let (p, v) = people.last_key_value().unwrap();
  assert!(ent.key().equivalent(p));
  assert_eq!(ent.value(), v);

  let wal = wal.reader();
  let ent = wal.last().unwrap();
  assert!(ent.key().equivalent(p));
  assert_eq!(ent.value(), v);
}

#[allow(clippy::needless_borrows_for_generic_args)]
fn insert<M>(wal: &mut OrderWal<Person, String, M>)
where
  M: Memtable<Key = Person, Value = String> + 'static,
  for<'a> M::Item<'a>: MemtableEntry<'a>,
  M::Error: std::fmt::Debug,
{
  let people = (0..100)
    .map(|_| {
      let p = Person::random();
      let v = std::format!("My name is {}", p.name);
      wal.insert(&p, &v).unwrap();
      (p, v)
    })
    .collect::<Vec<_>>();

  assert_eq!(wal.len(), 100);

  for (p, pv) in &people {
    assert!(wal.contains_key(p));

    assert_eq!(wal.get(p).unwrap().value(), pv);
  }

  for (p, _) in &people {
    assert!(wal.contains_key(p));
  }
}

fn insert_with_value_builder<M>(wal: &mut OrderWal<Person, String, M>)
where
  M: Memtable<Key = Person, Value = String> + 'static,
  for<'a> M::Item<'a>: MemtableEntry<'a>,
  M::Error: std::fmt::Debug,
{
  let people = (0..100)
    .map(|_| {
      let p = Person::random();
      let v = std::format!("My name is {}", p.name);
      wal
        .insert_with_value_builder(
          &p,
          ValueBuilder::new(v.len(), |buf: &mut VacantBuffer<'_>| {
            buf.put_slice(v.as_bytes()).map(|_| v.len())
          }),
        )
        .unwrap();
      (p, v)
    })
    .collect::<Vec<_>>();

  assert_eq!(wal.len(), 100);

  for (p, _) in &people {
    assert!(wal.contains_key(p));
    assert!(wal.contains_key(&p.as_ref()));
  }
}

#[allow(clippy::needless_borrows_for_generic_args)]
fn insert_with_key_builder<M>(wal: &mut OrderWal<Person, String, M>)
where
  M: Memtable<Key = Person, Value = String> + 'static,
  for<'a> M::Item<'a>: MemtableEntry<'a>,
  M::Error: std::fmt::Debug,
{
  let people = (0..100)
    .map(|_| {
      let p = Person::random();
      let pvec = p.to_vec();
      let v = std::format!("My name is {}", p.name);
      unsafe {
        wal
          .insert_with_key_builder(
            KeyBuilder::once(p.encoded_len(), |buf| p.encode_to_buffer(buf)),
            &v,
          )
          .unwrap();
      }
      (p, v)
    })
    .collect::<Vec<_>>();

  assert_eq!(wal.len(), 100);

  for (p, pv) in &people {
    assert!(wal.contains_key(p));
    assert_eq!(wal.get(p).unwrap().value(), pv);
  }

  for (p, _) in &people {
    assert!(wal.contains_key(p));
  }
}

fn insert_with_bytes<M>(wal: &mut OrderWal<Person, String, M>)
where
  M: Memtable<Key = Person, Value = String> + 'static,
  for<'a> M::Item<'a>: MemtableEntry<'a>,
  M::Error: std::fmt::Debug,
{
  let people = (0..100)
    .map(|_| {
      let p = Person::random();
      let v = std::format!("My name is {}", p.name);
      unsafe {
        wal
          .insert(
            MaybeStructured::from_slice(p.to_vec().as_slice()),
            MaybeStructured::from_slice(v.as_bytes()),
          )
          .unwrap();
      }
      (p, v)
    })
    .collect::<Vec<_>>();

  assert_eq!(wal.len(), 100);

  for (p, pv) in &people {
    assert!(wal.contains_key(p));
    assert!(wal.contains_key(&p.as_ref()));
    assert_eq!(wal.get(p).unwrap().value(), pv);
  }
}

fn insert_with_builders<M>(wal: &mut OrderWal<Person, String, M>)
where
  M: Memtable<Key = Person, Value = String> + 'static,
  for<'a> M::Item<'a>: MemtableEntry<'a> + std::fmt::Debug,
  M::Error: std::fmt::Debug,
{
  let people = (0..1)
    .map(|_| {
      let p = Person::random();
      let pvec = p.to_vec();
      let v = std::format!("My name is {}", p.name);
      wal
        .insert_with_builders(
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

  assert_eq!(wal.len(), 1);

  for (p, pvec, pv) in &people {
    assert!(wal.contains_key(p));
    unsafe {
      assert_eq!(wal.get_by_bytes(pvec.as_ref()).unwrap().value(), pv);
    }
  }

  for (p, _, _) in &people {
    assert!(wal.contains_key(p));
  }
}

#[cfg(feature = "std")]
expand_unit_tests!("linked": OrderWalAlternativeTable<Person, String> [TableOptions::Linked]: crate::memtable::alternative::Table<_, _> {
  first,
  last,
  insert,
  insert_with_value_builder,
  insert_with_key_builder,
  insert_with_bytes,
  insert_with_builders,
});

expand_unit_tests!("arena": OrderWalAlternativeTable<Person, String> [TableOptions::Arena(Default::default())]: crate::memtable::alternative::Table<_, _> {
  first,
  last,
  insert,
  insert_with_value_builder,
  insert_with_key_builder,
  insert_with_bytes,
  insert_with_builders,
});
