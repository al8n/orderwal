use dbutils::{buffer::VacantBuffer, state::Active, types::Type};

#[cfg(feature = "std")]
use std::thread::spawn;

use crate::{
  batch::BatchEntry,
  dynamic::{DynamicMemtable, OrderWal, OrderWalReader, Reader, Writer},
  memtable::{Entry, MutableMemtable},
  types::{KeyBuilder, ValueBuilder},
  Builder,
};

#[cfg(feature = "bounded")]
use crate::dynamic::BoundedTable;

#[cfg(feature = "unbounded")]
use crate::dynamic::UnboundedTable;

use super::*;

#[cfg(feature = "std")]
fn concurrent_basic<M>(mut w: OrderWal<M>)
where
  M: DynamicMemtable + MutableMemtable + Send + 'static,
  M::Error: core::fmt::Debug,
  for<'a> M::Entry<'a, Active>: Entry<'a, Key = &'a [u8], Value = &'a [u8]>,
{
  let readers = (0..100u32).map(|i| (i, w.reader())).collect::<Vec<_>>();

  let handles = readers.into_iter().map(|(i, reader)| {
    spawn(move || loop {
      if let Some(p) = reader.get(1, &i.to_le_bytes()) {
        assert_eq!(p.key(), i.to_le_bytes());
        assert_eq!(p.value(), i.to_le_bytes());
        break;
      }
    })
  });

  spawn(move || {
    for i in 0..100u32 {
      #[allow(clippy::needless_borrows_for_generic_args)]
      w.insert(1, &i.to_le_bytes(), &i.to_le_bytes()).unwrap();
    }
  });

  for handle in handles {
    handle.join().unwrap();
  }
}

#[cfg(feature = "std")]
fn concurrent_one_key<M>(mut w: OrderWal<M>)
where
  M: DynamicMemtable + MutableMemtable + Send + 'static,
  M::Error: core::fmt::Debug,
  for<'a> M::Entry<'a, Active>: Entry<'a, Key = &'a [u8], Value = &'a [u8]>,
{
  let readers = (0..100u32).map(|i| (i, w.reader())).collect::<Vec<_>>();
  let handles = readers.into_iter().map(|(_, reader)| {
    spawn(move || loop {
      if let Some(p) = reader.get(1, &1u32.to_le_bytes()) {
        assert_eq!(p.key(), 1u32.to_le_bytes());
        assert_eq!(p.value(), 1u32.to_le_bytes());
        break;
      }
    })
  });

  w.insert(1, &1u32.to_le_bytes(), &1u32.to_le_bytes())
    .unwrap();

  for handle in handles {
    handle.join().unwrap();
  }
}

fn apply<M>(mut wal: OrderWal<M>) -> (Person, Vec<(Person, String)>, Person)
where
  M: DynamicMemtable + MutableMemtable + Send + 'static,
  M::Error: core::fmt::Debug,
  for<'a> M::Entry<'a, Active>: Entry<'a, Value = &'a [u8]>,
{
  const N: u32 = 5;

  let mut batch = vec![];
  let output = (0..N)
    .map(|i| {
      (
        {
          let mut p = Person::random();
          p.id = i as u64;
          p
        },
        std::format!("My id is {i}"),
      )
        .clone()
    })
    .collect::<Vec<_>>();

  for (person, val) in output.iter() {
    batch.push(BatchEntry::insert(1, person.to_vec(), val.as_bytes()));
  }

  let rp1 = Person::random();
  wal.insert(1, &rp1.to_vec(), "rp1".as_bytes()).unwrap();
  wal.apply(&mut batch).unwrap();
  let rp2 = Person::random();
  wal.insert(1, &rp2.to_vec(), "rp2".as_bytes()).unwrap();

  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, &p.to_vec()).unwrap().value(), val.as_bytes());
  }

  assert_eq!(wal.get(1, &rp1.to_vec()).unwrap().value(), b"rp1");
  assert_eq!(wal.get(1, &rp2.to_vec()).unwrap().value(), b"rp2");

  let wal = wal.reader();
  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, &p.to_vec()).unwrap().value(), val.as_bytes());
  }

  assert_eq!(wal.get(1, &rp1.to_vec()).unwrap().value(), b"rp1");
  assert_eq!(wal.get(1, &rp2.to_vec()).unwrap().value(), b"rp2");

  (rp1, output, rp2)
}

fn apply_with_key_builder<M>(mut wal: OrderWal<M>) -> (Person, Vec<(Person, String)>, Person)
where
  M: DynamicMemtable + MutableMemtable + Send + 'static,
  M::Error: core::fmt::Debug,
  for<'a> M::Entry<'a, Active>: Entry<'a, Value = &'a [u8]>,
{
  const N: u32 = 5;

  let mut batch = vec![];
  let output = (0..N)
    .map(|i| {
      (
        {
          let mut p = Person::random();
          p.id = i as u64;
          p
        },
        std::format!("My id is {i}"),
      )
        .clone()
    })
    .collect::<Vec<_>>();

  for (person, val) in output.iter() {
    batch.push(BatchEntry::insert(
      1,
      KeyBuilder::new(person.encoded_len(), |buf: &mut VacantBuffer<'_>| {
        buf.set_len(person.encoded_len());
        person.encode(buf)
      }),
      val.as_bytes(),
    ));
  }

  let rp1 = Person::random();
  wal.insert(1, &rp1.to_vec(), b"rp1").unwrap();
  wal.apply_with_key_builder(&mut batch).unwrap();
  let rp2 = Person::random();
  wal.insert(1, &rp2.to_vec(), b"rp2").unwrap();

  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, &p.to_vec()).unwrap().value(), val.as_bytes());
  }

  assert_eq!(wal.get(1, &rp1.to_vec()).unwrap().value(), b"rp1");
  assert_eq!(wal.get(1, &rp2.to_vec()).unwrap().value(), b"rp2");

  let wal = wal.reader();
  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, &p.to_vec()).unwrap().value(), val.as_bytes());
  }

  assert_eq!(wal.get(1, &rp1.to_vec()).unwrap().value(), b"rp1");
  assert_eq!(wal.get(1, &rp2.to_vec()).unwrap().value(), b"rp2");

  (rp1, output, rp2)
}

fn apply_with_value_builder<M>(mut wal: OrderWal<M>) -> (Person, Vec<(Person, String)>, Person)
where
  M: DynamicMemtable + MutableMemtable + Send + 'static,
  M::Error: core::fmt::Debug,
  for<'a> M::Entry<'a, Active>: Entry<'a, Value = &'a [u8]>,
{
  const N: u32 = 5;

  let mut batch = vec![];
  let output = (0..N)
    .map(|i| {
      (
        {
          let mut p = Person::random();
          p.id = i as u64;
          p
        },
        std::format!("My id is {i}"),
      )
        .clone()
    })
    .collect::<Vec<_>>();

  for (person, val) in output.iter() {
    batch.push(BatchEntry::insert(
      1,
      person.to_vec(),
      ValueBuilder::new(val.len(), |buf: &mut VacantBuffer<'_>| {
        buf.put_slice(val.as_bytes()).map(|_| val.len())
      }),
    ));
  }

  let rp1 = Person::random();
  wal.insert(1, &rp1.to_vec(), b"rp1").unwrap();
  wal.apply_with_value_builder(&mut batch).unwrap();
  let rp2 = Person::random();
  wal.insert(1, &rp2.to_vec(), b"rp2").unwrap();

  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, &p.to_vec()).unwrap().value(), val.as_bytes());
  }

  assert_eq!(wal.get(1, &rp1.to_vec()).unwrap().value(), b"rp1");
  assert_eq!(wal.get(1, &rp2.to_vec()).unwrap().value(), b"rp2");

  let wal = wal.reader();
  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, &p.to_vec()).unwrap().value(), val.as_bytes());
  }

  assert_eq!(wal.get(1, &rp1.to_vec()).unwrap().value(), b"rp1");
  assert_eq!(wal.get(1, &rp2.to_vec()).unwrap().value(), b"rp2");

  (rp1, output, rp2)
}

fn apply_with_builders<M>(mut wal: OrderWal<M>) -> (Person, Vec<(Person, String)>, Person)
where
  M: DynamicMemtable + MutableMemtable + Send + 'static,
  M::Error: core::fmt::Debug,
  for<'a> M::Entry<'a, Active>: Entry<'a, Value = &'a [u8]>,
{
  const N: u32 = 1;

  let mut batch = vec![];
  let output = (0..N)
    .map(|i| {
      (
        {
          let mut p = Person::random();
          p.id = i as u64;
          p
        },
        std::format!("My id is {i}"),
      )
        .clone()
    })
    .collect::<Vec<_>>();

  for (person, val) in output.iter() {
    batch.push(BatchEntry::insert(
      1,
      KeyBuilder::new(person.encoded_len(), |buf: &mut VacantBuffer<'_>| {
        buf.set_len(person.encoded_len());
        person.encode(buf)
      }),
      ValueBuilder::new(val.len(), |buf: &mut VacantBuffer<'_>| {
        buf.put_slice(val.as_bytes()).map(|_| val.len())
      }),
    ));
  }

  let rp1 = Person::random();
  wal.insert(1, &rp1.to_vec(), b"rp1").unwrap();
  wal.apply_with_builders(&mut batch).unwrap();
  let rp2 = Person::random();
  wal.insert(1, &rp2.to_vec(), b"rp2").unwrap();

  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, &p.to_vec()).unwrap().value(), val.as_bytes());
  }

  assert_eq!(wal.get(1, &rp1.to_vec()).unwrap().value(), b"rp1");
  assert_eq!(wal.get(1, &rp2.to_vec()).unwrap().value(), b"rp2");

  let wal = wal.reader();
  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, &p.to_vec()).unwrap().value(), val.as_bytes());
  }

  assert_eq!(wal.get(1, &rp1.to_vec()).unwrap().value(), b"rp1");
  assert_eq!(wal.get(1, &rp2.to_vec()).unwrap().value(), b"rp2");

  (rp1, output, rp2)
}

#[cfg(feature = "unbounded")]
expand_unit_tests!(
  move "unbounded": OrderWal<UnboundedTable> [Default::default()]: UnboundedTable {
    concurrent_basic |p, _res| {
      let wal = unsafe { Builder::new().map::<OrderWalReader<UnboundedTable>, _>(p).unwrap() };

      for i in 0..100u32 {
        assert!(wal.contains_key(1, &i.to_le_bytes()));
      }
    },
    concurrent_one_key |p, _res| {
      let wal = unsafe { Builder::new().map::<OrderWalReader<UnboundedTable>, _>(p).unwrap() };
      assert!(wal.contains_key(1, &1u32.to_le_bytes()));
    },
  }
);

#[cfg(feature = "unbounded")]
expand_unit_tests!(
  move "unbounded": OrderWal<UnboundedTable> [Default::default()]: UnboundedTable {
    apply |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<OrderWalReader<UnboundedTable>, _>(&p)
          .unwrap()
      };

      let data: Vec<(Person, String)> = data;
      for (p, val) in data {
        assert_eq!(map.get(1, &p.to_vec()).unwrap().value(), val.as_bytes());
      }
      let rp1: Person = rp1;
      let rp2: Person = rp2;
      assert_eq!(map.get(1, &rp1.to_vec()).unwrap().value(), b"rp1");
      assert_eq!(map.get(1, &rp2.to_vec()).unwrap().value(), b"rp2");
    },
    apply_with_key_builder |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<OrderWalReader<UnboundedTable>, _>(&p)
          .unwrap()
      };

      let data: Vec<(Person, String)> = data;
      for (p, val) in data {
        assert_eq!(map.get(1, &p.to_vec()).unwrap().value(), val.as_bytes());
      }
      let rp1: Person = rp1;
      let rp2: Person = rp2;
      assert_eq!(map.get(1, &rp1.to_vec()).unwrap().value(), b"rp1");
      assert_eq!(map.get(1, &rp2.to_vec()).unwrap().value(), b"rp2");
    },
    apply_with_value_builder |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<OrderWalReader<UnboundedTable>, _>(&p)
          .unwrap()
      };

      let data: Vec<(Person, String)> = data;
      for (p, val) in data {
        assert_eq!(map.get(1, &p.to_vec()).unwrap().value(), val.as_bytes());
      }
      let rp1: Person = rp1;
      let rp2: Person = rp2;
      assert_eq!(map.get(1, &rp1.to_vec()).unwrap().value(), b"rp1");
      assert_eq!(map.get(1, &rp2.to_vec()).unwrap().value(), b"rp2");
    },
    apply_with_builders |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<OrderWalReader<UnboundedTable>, _>(&p)
          .unwrap()
      };

      let data: Vec<(Person, String)> = data;

      for (p, val) in data {
        assert_eq!(map.get(1, &p.to_vec()).unwrap().value(), val.as_bytes());
      }
      let rp1: Person = rp1;
      let rp2: Person = rp2;
      assert_eq!(map.get(1, &rp1.to_vec()).unwrap().value(), b"rp1");
      assert_eq!(map.get(1, &rp2.to_vec()).unwrap().value(), b"rp2");
    }
  }
);

#[cfg(all(feature = "bounded", feature = "std"))]
expand_unit_tests!(
  move "bounded": OrderWal<BoundedTable> [Default::default()]: BoundedTable {
    concurrent_basic |p, _res| {
      let wal = unsafe { Builder::new().map::<OrderWalReader<BoundedTable>, _>(p).unwrap() };

      for i in 0..100u32 {
        assert!(wal.contains_key(1, &i.to_le_bytes()));
      }
    },
    concurrent_one_key |p, _res| {
      let wal = unsafe { Builder::new().map::<OrderWalReader<BoundedTable>, _>(p).unwrap() };
      assert!(wal.contains_key(1, &1u32.to_le_bytes()));
    },
  }
);

#[cfg(feature = "bounded")]
expand_unit_tests!(
  move "bounded": OrderWal<BoundedTable> [Default::default()]: BoundedTable {
    apply |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<OrderWalReader<BoundedTable>, _>(&p)
          .unwrap()
      };

      let data: Vec<(Person, String)> = data;
      for (p, val) in data {
        assert_eq!(map.get(1, &p.to_vec()).unwrap().value(), val.as_bytes());
      }
      let rp1: Person = rp1;
      let rp2: Person = rp2;
      assert_eq!(map.get(1, &rp1.to_vec()).unwrap().value(), b"rp1");
      assert_eq!(map.get(1, &rp2.to_vec()).unwrap().value(), b"rp2");
    },
    apply_with_key_builder |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<OrderWalReader<BoundedTable>, _>(&p)
          .unwrap()
      };

      let data: Vec<(Person, String)> = data;
      for (p, val) in data {
        assert_eq!(map.get(1, &p.to_vec()).unwrap().value(), val.as_bytes());
      }
      let rp1: Person = rp1;
      let rp2: Person = rp2;
      assert_eq!(map.get(1, &rp1.to_vec()).unwrap().value(), b"rp1");
      assert_eq!(map.get(1, &rp2.to_vec()).unwrap().value(), b"rp2");
    },
    apply_with_value_builder |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<OrderWalReader<BoundedTable>, _>(&p)
          .unwrap()
      };

      let data: Vec<(Person, String)> = data;
      for (p, val) in data {
        assert_eq!(map.get(1, &p.to_vec()).unwrap().value(), val.as_bytes());
      }
      let rp1: Person = rp1;
      let rp2: Person = rp2;
      assert_eq!(map.get(1, &rp1.to_vec()).unwrap().value(), b"rp1");
      assert_eq!(map.get(1, &rp2.to_vec()).unwrap().value(), b"rp2");
    },
    apply_with_builders |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<OrderWalReader<BoundedTable>, _>(&p)
          .unwrap()
      };

      let data: Vec<(Person, String)> = data;
      for (p, val) in data {
        assert_eq!(map.get(1, &p.to_vec()).unwrap().value(), val.as_bytes());
      }
      let rp1: Person = rp1;
      let rp2: Person = rp2;
      assert_eq!(map.get(1, &rp1.to_vec()).unwrap().value(), b"rp1");
      assert_eq!(map.get(1, &rp2.to_vec()).unwrap().value(), b"rp2");
    }
  }
);
