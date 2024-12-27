use dbutils::{
  buffer::VacantBuffer,
  equivalentor::{TypeRefComparator, TypeRefQueryComparator},
  state::Active,
  types::{MaybeStructured, Type},
};

use std::thread::spawn;

use crate::{
  batch::BatchEntry,
  generic::{
    BoundedTable, GenericMemtable, OrderWal, OrderWalReader, Reader, UnboundedTable, Writer,
  },
  memtable::{Entry, MutableMemtable},
  types::{KeyBuilder, ValueBuilder},
  Builder,
};

use super::{Person, MB};

#[cfg(feature = "std")]
fn concurrent_basic<M>(mut w: OrderWal<M>)
where
  M: GenericMemtable<u32, [u8; 4]> + MutableMemtable + Send + 'static,
  M::Error: core::fmt::Debug,
  for<'a> M::Entry<'a, Active>: Entry<'a, Key = u32, Value = [u8; 4]>,
  for<'a> M::Comparator: TypeRefComparator<'a, u32> + TypeRefQueryComparator<'a, u32, u32>,
{
  let readers = (0..100u32).map(|i| (i, w.reader())).collect::<Vec<_>>();

  let handles = readers.into_iter().map(|(i, reader)| {
    spawn(move || loop {
      if let Some(p) = reader.get(1, &i) {
        assert_eq!(p.key(), i);
        assert_eq!(p.value(), i.to_le_bytes());
        break;
      }
    })
  });

  spawn(move || {
    for i in 0..100u32 {
      #[allow(clippy::needless_borrows_for_generic_args)]
      w.insert(1, &i, &i.to_le_bytes()).unwrap();
    }
  });

  for handle in handles {
    handle.join().unwrap();
  }
}

#[cfg(feature = "std")]
fn concurrent_one_key<M>(mut w: OrderWal<M>)
where
  M: GenericMemtable<u32, [u8; 4]> + MutableMemtable + Send + 'static,
  M::Error: core::fmt::Debug,
  for<'a> M::Entry<'a, Active>: Entry<'a, Key = u32, Value = [u8; 4]>,
  for<'a> M::Comparator: TypeRefComparator<'a, u32> + TypeRefQueryComparator<'a, u32, u32>,
{
  let readers = (0..100u32).map(|i| (i, w.reader())).collect::<Vec<_>>();
  let handles = readers.into_iter().map(|(_, reader)| {
    spawn(move || loop {
      if let Some(p) = reader.get(1, &1) {
        assert_eq!(p.key(), 1);
        assert_eq!(p.value(), 1u32.to_le_bytes());
        break;
      }
    })
  });

  w.insert(1, &1, &1u32.to_le_bytes()).unwrap();

  for handle in handles {
    handle.join().unwrap();
  }
}

fn apply<M>(mut wal: OrderWal<M>) -> (Person, Vec<(Person, String)>, Person)
where
  M: GenericMemtable<Person, String> + MutableMemtable + Send + 'static,
  M::Error: core::fmt::Debug,
  for<'a> M::Entry<'a, Active>: Entry<'a, Value = <String as Type>::Ref<'a>>,
  for<'a> M::Comparator: TypeRefComparator<'a, Person> + TypeRefQueryComparator<'a, Person, Person>,
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
      MaybeStructured::from(person),
      MaybeStructured::from(val),
    ));
  }

  let rp1 = Person::random();
  wal.insert(1, &rp1, &"rp1".to_string()).unwrap();
  wal.apply(&mut batch).unwrap();
  let rp2 = Person::random();
  wal.insert(1, &rp2, &"rp2".to_string()).unwrap();

  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, p).unwrap().value(), val);
  }

  assert_eq!(wal.get(1, &rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(1, &rp2).unwrap().value(), "rp2");

  let wal = wal.reader();
  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, p).unwrap().value(), val);
  }

  assert_eq!(wal.get(1, &rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(1, &rp2).unwrap().value(), "rp2");

  (rp1, output, rp2)
}

fn apply_with_key_builder<M>(mut wal: OrderWal<M>) -> (Person, Vec<(Person, String)>, Person)
where
  M: GenericMemtable<Person, String> + MutableMemtable + Send + 'static,
  M::Error: core::fmt::Debug,
  for<'a> M::Entry<'a, Active>: Entry<'a, Value = <String as Type>::Ref<'a>>,
  for<'a> M::Comparator: TypeRefComparator<'a, Person> + TypeRefQueryComparator<'a, Person, Person>,
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
      MaybeStructured::from(val),
    ));
  }

  let rp1 = Person::random();
  wal.insert(1, &rp1, &"rp1".to_string()).unwrap();
  wal.apply(&mut batch).unwrap();
  let rp2 = Person::random();
  wal.insert(1, &rp2, &"rp2".to_string()).unwrap();

  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, p).unwrap().value(), val);
  }

  assert_eq!(wal.get(1, &rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(1, &rp2).unwrap().value(), "rp2");

  let wal = wal.reader();
  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, p).unwrap().value(), val);
  }

  assert_eq!(wal.get(1, &rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(1, &rp2).unwrap().value(), "rp2");

  (rp1, output, rp2)
}

fn apply_with_value_builder<M>(mut wal: OrderWal<M>) -> (Person, Vec<(Person, String)>, Person)
where
  M: GenericMemtable<Person, String> + MutableMemtable + Send + 'static,
  M::Error: core::fmt::Debug,
  for<'a> M::Entry<'a, Active>: Entry<'a, Value = <String as Type>::Ref<'a>>,
  for<'a> M::Comparator: TypeRefComparator<'a, Person> + TypeRefQueryComparator<'a, Person, Person>,
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
      MaybeStructured::from(person),
      ValueBuilder::new(val.len(), |buf: &mut VacantBuffer<'_>| {
        buf.put_slice(val.as_bytes()).map(|_| val.len())
      }),
    ));
  }

  let rp1 = Person::random();
  wal.insert(1, &rp1, &"rp1".to_string()).unwrap();
  wal.apply(&mut batch).unwrap();
  let rp2 = Person::random();
  wal.insert(1, &rp2, &"rp2".to_string()).unwrap();

  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, p).unwrap().value(), val);
  }

  assert_eq!(wal.get(1, &rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(1, &rp2).unwrap().value(), "rp2");

  let wal = wal.reader();
  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, p).unwrap().value(), val);
  }

  assert_eq!(wal.get(1, &rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(1, &rp2).unwrap().value(), "rp2");

  (rp1, output, rp2)
}

fn apply_with_builders<M>(mut wal: OrderWal<M>) -> (Person, Vec<(Person, String)>, Person)
where
  M: GenericMemtable<Person, String> + MutableMemtable + Send + 'static,
  M::Error: core::fmt::Debug,
  for<'a> M::Entry<'a, Active>: Entry<'a, Value = <String as Type>::Ref<'a>>,
  for<'a> M::Comparator: TypeRefComparator<'a, Person> + TypeRefQueryComparator<'a, Person, Person>,
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
  wal.insert(1, &rp1, &"rp1".to_string()).unwrap();
  wal.apply(&mut batch).unwrap();
  let rp2 = Person::random();
  wal.insert(1, &rp2, &"rp2".to_string()).unwrap();

  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, p).unwrap().value(), val);
  }

  assert_eq!(wal.get(1, &rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(1, &rp2).unwrap().value(), "rp2");

  let wal = wal.reader();
  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, p).unwrap().value(), val);
  }

  assert_eq!(wal.get(1, &rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(1, &rp2).unwrap().value(), "rp2");

  (rp1, output, rp2)
}

#[cfg(feature = "std")]
expand_unit_tests!(
  move "unbounded": OrderWal<UnboundedTable<u32, [u8; 4]>> [Default::default()]: UnboundedTable<_, _> {
    concurrent_basic |p, _res| {
      let wal = unsafe { Builder::new().map::<OrderWalReader<UnboundedTable<u32, [u8; 4]>>, _>(p).unwrap() };

      for i in 0..100u32 {
        assert!(wal.contains_key(1, &i));
      }
    },
    concurrent_one_key |p, _res| {
      let wal = unsafe { Builder::new().map::<OrderWalReader<UnboundedTable<u32, [u8; 4]>>, _>(p).unwrap() };
      assert!(wal.contains_key(1, &1));
    },
  }
);

expand_unit_tests!(
  move "unbounded": OrderWal<UnboundedTable<Person, String>> [Default::default()]: UnboundedTable<_, _> {
    apply |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<OrderWalReader<UnboundedTable<Person, String>>, _>(&p)
          .unwrap()
      };

      for (p, val) in data {
        assert_eq!(map.get(1, &p).unwrap().value(), &val);
      }
      assert_eq!(map.get(1, &rp1).unwrap().value(), "rp1");
      assert_eq!(map.get(1, &rp2).unwrap().value(), "rp2");
    },
    apply_with_key_builder |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<OrderWalReader<UnboundedTable<Person, String>>, _>(&p)
          .unwrap()
      };

      for (p, val) in data {
        assert_eq!(map.get(1, &p).unwrap().value(), &val);
      }
      assert_eq!(map.get(1, &rp1).unwrap().value(), "rp1");
      assert_eq!(map.get(1, &rp2).unwrap().value(), "rp2");
    },
    apply_with_value_builder |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<OrderWalReader<UnboundedTable<Person, String>>, _>(&p)
          .unwrap()
      };

      for (p, val) in data {
        assert_eq!(map.get(1, &p).unwrap().value(), &val);
      }
      assert_eq!(map.get(1, &rp1).unwrap().value(), "rp1");
      assert_eq!(map.get(1, &rp2).unwrap().value(), "rp2");
    },
    apply_with_builders |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<OrderWalReader<UnboundedTable<Person, String>>, _>(&p)
          .unwrap()
      };

      for (p, val) in data {
        assert_eq!(map.get(1, &p).unwrap().value(), &val);
      }
      assert_eq!(map.get(1, &rp1).unwrap().value(), "rp1");
      assert_eq!(map.get(1, &rp2).unwrap().value(), "rp2");
    }
  }
);

#[cfg(feature = "std")]
expand_unit_tests!(
  move "bounded": OrderWal<BoundedTable<u32, [u8; 4]>> [Default::default()]: BoundedTable<_, _> {
    concurrent_basic |p, _res| {
      let wal = unsafe { Builder::new().map::<OrderWalReader<BoundedTable<u32, [u8; 4]>>, _>(p).unwrap() };

      for i in 0..100u32 {
        assert!(wal.contains_key(1, &i));
      }
    },
    concurrent_one_key |p, _res| {
      let wal = unsafe { Builder::new().map::<OrderWalReader<BoundedTable<u32, [u8; 4]>>, _>(p).unwrap() };
      assert!(wal.contains_key(1, &1));
    },
  }
);

expand_unit_tests!(
  move "bounded": OrderWal<BoundedTable<Person, String>> [Default::default()]: BoundedTable<_, _> {
    apply |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<OrderWalReader<BoundedTable<Person, String>>, _>(&p)
          .unwrap()
      };

      for (p, val) in data {
        assert_eq!(map.get(1, &p).unwrap().value(), &val);
      }
      assert_eq!(map.get(1, &rp1).unwrap().value(), "rp1");
      assert_eq!(map.get(1, &rp2).unwrap().value(), "rp2");
    },
    apply_with_key_builder |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<OrderWalReader<BoundedTable<Person, String>>, _>(&p)
          .unwrap()
      };

      for (p, val) in data {
        assert_eq!(map.get(1, &p).unwrap().value(), &val);
      }
      assert_eq!(map.get(1, &rp1).unwrap().value(), "rp1");
      assert_eq!(map.get(1, &rp2).unwrap().value(), "rp2");
    },
    apply_with_value_builder |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<OrderWalReader<BoundedTable<Person, String>>, _>(&p)
          .unwrap()
      };

      for (p, val) in data {
        assert_eq!(map.get(1, &p).unwrap().value(), &val);
      }
      assert_eq!(map.get(1, &rp1).unwrap().value(), "rp1");
      assert_eq!(map.get(1, &rp2).unwrap().value(), "rp2");
    },
    apply_with_builders |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<OrderWalReader<BoundedTable<Person, String>>, _>(&p)
          .unwrap()
      };

      for (p, val) in data {
        assert_eq!(map.get(1, &p).unwrap().value(), &val);
      }
      assert_eq!(map.get(1, &rp1).unwrap().value(), "rp1");
      assert_eq!(map.get(1, &rp2).unwrap().value(), "rp2");
    }
  }
);
