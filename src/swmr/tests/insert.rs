use dbutils::{buffer::VacantBuffer, traits::MaybeStructured};
use generic::{GenericPointer, Reader, Writer};

use crate::{
  batch::BatchEntry,
  memtable::Memtable,
  sealed::WithoutVersion,
  types::{KeyBuilder, ValueBuilder},
  Builder,
};

use super::*;

fn concurrent_basic<M>(mut w: GenericOrderWal<u32, [u8; 4], M>)
where
  M: Memtable<Pointer = GenericPointer<u32, [u8; 4]>> + Send + 'static,
  M::Pointer: WithoutVersion,
  M::Error: std::fmt::Debug,
{
  let readers = (0..100u32).map(|i| (i, w.reader())).collect::<Vec<_>>();

  let handles = readers.into_iter().map(|(i, reader)| {
    spawn(move || loop {
      if let Some(p) = reader.get(&i) {
        assert_eq!(p.key(), &i);
        assert_eq!(p.value(), &i.to_le_bytes());
        break;
      }
    })
  });

  spawn(move || {
    for i in 0..100u32 {
      #[allow(clippy::needless_borrows_for_generic_args)]
      w.insert(&i, &i.to_le_bytes()).unwrap();
    }
  });

  for handle in handles {
    handle.join().unwrap();
  }
}

fn concurrent_one_key<M>(mut w: GenericOrderWal<u32, [u8; 4], M>)
where
  M: Memtable<Pointer = GenericPointer<u32, [u8; 4]>> + Send + 'static,
  M::Pointer: WithoutVersion,
  M::Error: std::fmt::Debug,
{
  let readers = (0..100u32).map(|i| (i, w.reader())).collect::<Vec<_>>();
  let handles = readers.into_iter().map(|(_, reader)| {
    spawn(move || loop {
      if let Some(p) = reader.get(&1) {
        assert_eq!(p.key(), &1);
        assert_eq!(p.value(), &1u32.to_le_bytes());
        break;
      }
    })
  });

  w.insert(&1, &1u32.to_le_bytes()).unwrap();

  for handle in handles {
    handle.join().unwrap();
  }
}

fn insert_batch<M>(
  mut wal: GenericOrderWal<Person, String, M>,
) -> (Person, Vec<(Person, String)>, Person)
where
  M: Memtable<Pointer = GenericPointer<Person, String>> + Send + 'static,
  M::Pointer: WithoutVersion,
  M::Error: std::fmt::Debug,
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
        format!("My id is {i}"),
      )
        .clone()
    })
    .collect::<Vec<_>>();

  for (person, val) in output.iter() {
    batch.push(BatchEntry::new(
      MaybeStructured::from(person),
      MaybeStructured::from(val),
    ));
  }

  let rp1 = Person::random();
  wal.insert(&rp1, &"rp1".to_string()).unwrap();
  wal.insert_batch(&mut batch).unwrap();
  let rp2 = Person::random();
  wal.insert(&rp2, &"rp2".to_string()).unwrap();

  for (p, val) in output.iter() {
    assert_eq!(wal.get(p).unwrap().value(), val);
  }

  assert_eq!(wal.get(&rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(&rp2).unwrap().value(), "rp2");

  let wal = wal.reader();
  for (p, val) in output.iter() {
    assert_eq!(wal.get(p).unwrap().value(), val);
  }

  assert_eq!(wal.get(&rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(&rp2).unwrap().value(), "rp2");

  (rp1, output, rp2)
}

fn insert_batch_with_key_builder<M>(
  mut wal: GenericOrderWal<Person, String, M>,
) -> (Person, Vec<(Person, String)>, Person)
where
  M: Memtable<Pointer = GenericPointer<Person, String>> + Send + 'static,
  M::Pointer: WithoutVersion,
  M::Error: std::fmt::Debug,
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
        format!("My id is {i}"),
      )
        .clone()
    })
    .collect::<Vec<_>>();

  for (person, val) in output.iter() {
    batch.push(BatchEntry::new(
      KeyBuilder::new(person.encoded_len(), |buf: &mut VacantBuffer<'_>| {
        buf.set_len(person.encoded_len());
        person.encode(buf)
      }),
      MaybeStructured::from(val),
    ));
  }

  let rp1 = Person::random();
  wal.insert(&rp1, &"rp1".to_string()).unwrap();
  wal.insert_batch_with_key_builder(&mut batch).unwrap();
  let rp2 = Person::random();
  wal.insert(&rp2, &"rp2".to_string()).unwrap();

  for (p, val) in output.iter() {
    assert_eq!(wal.get(p).unwrap().value(), val);
  }

  assert_eq!(wal.get(&rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(&rp2).unwrap().value(), "rp2");

  let wal = wal.reader();
  for (p, val) in output.iter() {
    assert_eq!(wal.get(p).unwrap().value(), val);
  }

  assert_eq!(wal.get(&rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(&rp2).unwrap().value(), "rp2");

  (rp1, output, rp2)
}

fn insert_batch_with_value_builder<M>(
  mut wal: GenericOrderWal<Person, String, M>,
) -> (Person, Vec<(Person, String)>, Person)
where
  M: Memtable<Pointer = GenericPointer<Person, String>> + Send + 'static,
  M::Pointer: WithoutVersion,
  M::Error: std::fmt::Debug,
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
        format!("My id is {i}"),
      )
        .clone()
    })
    .collect::<Vec<_>>();

  for (person, val) in output.iter() {
    batch.push(BatchEntry::new(
      person.into(),
      ValueBuilder::new(val.len(), |buf: &mut VacantBuffer<'_>| {
        buf.put_slice(val.as_bytes()).map(|_| val.len())
      }),
    ));
  }

  let rp1 = Person::random();
  wal.insert(&rp1, &"rp1".to_string()).unwrap();
  wal.insert_batch_with_value_builder(&mut batch).unwrap();
  let rp2 = Person::random();
  wal.insert(&rp2, &"rp2".to_string()).unwrap();

  for (p, val) in output.iter() {
    assert_eq!(wal.get(p).unwrap().value(), val);
  }

  assert_eq!(wal.get(&rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(&rp2).unwrap().value(), "rp2");

  let wal = wal.reader();
  for (p, val) in output.iter() {
    assert_eq!(wal.get(p).unwrap().value(), val);
  }

  assert_eq!(wal.get(&rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(&rp2).unwrap().value(), "rp2");

  (rp1, output, rp2)
}

fn insert_batch_with_builders<M>(
  mut wal: GenericOrderWal<Person, String, M>,
) -> (Person, Vec<(Person, String)>, Person)
where
  M: Memtable<Pointer = GenericPointer<Person, String>> + Send + 'static,
  M::Pointer: WithoutVersion,
  M::Error: std::fmt::Debug,
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
        format!("My id is {i}"),
      )
        .clone()
    })
    .collect::<Vec<_>>();

  for (person, val) in output.iter() {
    batch.push(BatchEntry::new(
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
  wal.insert(&rp1, &"rp1".to_string()).unwrap();
  wal.insert_batch_with_builders(&mut batch).unwrap();
  let rp2 = Person::random();
  wal.insert(&rp2, &"rp2".to_string()).unwrap();

  for (p, val) in output.iter() {
    assert_eq!(wal.get(p).unwrap().value(), val);
  }

  assert_eq!(wal.get(&rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(&rp2).unwrap().value(), "rp2");

  let wal = wal.reader();
  for (p, val) in output.iter() {
    assert_eq!(wal.get(p).unwrap().value(), val);
  }

  assert_eq!(wal.get(&rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(&rp2).unwrap().value(), "rp2");

  (rp1, output, rp2)
}

expand_unit_tests!(
  move "linked": GenericOrderWalLinkedTable<u32, [u8; 4]> {
    concurrent_basic |p, _res| {
      let wal = unsafe { Builder::new().map::<u32, [u8; 4], GenericOrderWalReaderLinkedTable<_, _>, _>(p).unwrap() };

      for i in 0..100u32 {
        assert!(wal.contains_key(&i));
      }
    },
    concurrent_one_key |p, _res| {
      let wal = unsafe { Builder::new().map::<u32, [u8; 4], GenericOrderWalReaderLinkedTable<_, _>, _>(p).unwrap() };
      assert!(wal.contains_key(&1));
    },
  }
);

expand_unit_tests!(
  move "linked": GenericOrderWalLinkedTable<Person, String> {
    insert_batch |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<Person, String, GenericOrderWalReaderLinkedTable<_, _>, _>(&p)
          .unwrap()
      };

      for (p, val) in data {
        assert_eq!(map.get(&p).unwrap().value(), &val);
      }
      assert_eq!(map.get(&rp1).unwrap().value(), "rp1");
      assert_eq!(map.get(&rp2).unwrap().value(), "rp2");
    },
    insert_batch_with_key_builder |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<Person, String, GenericOrderWalReaderLinkedTable<_, _>, _>(&p)
          .unwrap()
      };

      for (p, val) in data {
        assert_eq!(map.get(&p).unwrap().value(), &val);
      }
      assert_eq!(map.get(&rp1).unwrap().value(), "rp1");
      assert_eq!(map.get(&rp2).unwrap().value(), "rp2");
    },
    insert_batch_with_value_builder |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<Person, String, GenericOrderWalReaderLinkedTable<_, _>, _>(&p)
          .unwrap()
      };

      for (p, val) in data {
        assert_eq!(map.get(&p).unwrap().value(), &val);
      }
      assert_eq!(map.get(&rp1).unwrap().value(), "rp1");
      assert_eq!(map.get(&rp2).unwrap().value(), "rp2");
    },
    insert_batch_with_builders |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<Person, String, GenericOrderWalReaderLinkedTable<_, _>, _>(&p)
          .unwrap()
      };

      for (p, val) in data {
        assert_eq!(map.get(&p).unwrap().value(), &val);
      }
      assert_eq!(map.get(&rp1).unwrap().value(), "rp1");
      assert_eq!(map.get(&rp2).unwrap().value(), "rp2");
    }
  }
);

expand_unit_tests!(
  move "arena": GenericOrderWalArenaTable<u32, [u8; 4]> {
    concurrent_basic |p, _res| {
      let wal = unsafe { Builder::new().map::<u32, [u8; 4], GenericOrderWalReaderArenaTable<_, _>, _>(p).unwrap() };

      for i in 0..100u32 {
        assert!(wal.contains_key(&i));
      }
    },
    concurrent_one_key |p, _res| {
      let wal = unsafe { Builder::new().map::<u32, [u8; 4], GenericOrderWalReaderArenaTable<_, _>, _>(p).unwrap() };
      assert!(wal.contains_key(&1));
    },
  }
);

expand_unit_tests!(
  move "arena": GenericOrderWalArenaTable<Person, String> {
    insert_batch |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<Person, String, GenericOrderWalReaderArenaTable<_, _>, _>(&p)
          .unwrap()
      };

      for (p, val) in data {
        assert_eq!(map.get(&p).unwrap().value(), &val);
      }
      assert_eq!(map.get(&rp1).unwrap().value(), "rp1");
      assert_eq!(map.get(&rp2).unwrap().value(), "rp2");
    },
    insert_batch_with_key_builder |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<Person, String, GenericOrderWalReaderArenaTable<_, _>, _>(&p)
          .unwrap()
      };

      for (p, val) in data {
        assert_eq!(map.get(&p).unwrap().value(), &val);
      }
      assert_eq!(map.get(&rp1).unwrap().value(), "rp1");
      assert_eq!(map.get(&rp2).unwrap().value(), "rp2");
    },
    insert_batch_with_value_builder |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<Person, String, GenericOrderWalReaderArenaTable<_, _>, _>(&p)
          .unwrap()
      };

      for (p, val) in data {
        assert_eq!(map.get(&p).unwrap().value(), &val);
      }
      assert_eq!(map.get(&rp1).unwrap().value(), "rp1");
      assert_eq!(map.get(&rp2).unwrap().value(), "rp2");
    },
    insert_batch_with_builders |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<Person, String, GenericOrderWalReaderArenaTable<_, _>, _>(&p)
          .unwrap()
      };

      for (p, val) in data {
        assert_eq!(map.get(&p).unwrap().value(), &val);
      }
      assert_eq!(map.get(&rp1).unwrap().value(), "rp1");
      assert_eq!(map.get(&rp2).unwrap().value(), "rp2");
    }
  }
);
