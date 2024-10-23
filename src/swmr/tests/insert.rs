use dbutils::traits::MaybeStructured;
use generic::{GenericPointer, Writer, Reader};
use tempfile::tempdir;

use crate::{batch::BatchEntry, memtable::Memtable, sealed::WithoutVersion, types::Generic, Builder};

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

#[test]
fn concurrent_basic_linked_inmemory() {
  let wal = Builder::new().with_capacity(MB).alloc::<u32, [u8; 4], GenericOrderWalLinkedTable<_, _>>().unwrap();
  concurrent_basic(wal);
}

#[test]
fn concurrent_basic_linked_map_anon() {
  let wal = Builder::new().with_capacity(MB).map_anon::<u32, [u8; 4], GenericOrderWalLinkedTable<_, _>>().unwrap();
  concurrent_basic(wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn concurrent_basic_linked_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_concurrent_basic_linked_map_file");

  let wal = unsafe {
    Builder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<u32, [u8; 4], GenericOrderWalArenaTable<_, _>, _>(&path)
      .unwrap()
  };

  concurrent_basic(wal);

  let wal = unsafe { Builder::new().map::<u32, [u8; 4], GenericOrderWalReaderLinkedTable<_, _>, _>(path).unwrap() };

  for i in 0..100u32 {
    assert!(wal.contains_key(&i));
  }
}

#[test]
fn concurrent_basic_arena_inmemory() {
  let wal = Builder::new().with_capacity(MB).alloc::<u32, [u8; 4], GenericOrderWalArenaTable<_, _>>().unwrap();
  concurrent_basic(wal);
}

#[test]
fn concurrent_basic_arena_map_anon() {
  let wal = Builder::new().with_capacity(MB).map_anon::<u32, [u8; 4], GenericOrderWalArenaTable<_, _>>().unwrap();
  concurrent_basic(wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn concurrent_basic_arena_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_concurrent_basic_arena_map_file");

  let wal = unsafe {
    Builder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<u32, [u8; 4], GenericOrderWalArenaTable<_, _>, _>(&path)
      .unwrap()
  };

  concurrent_basic(wal);

  let wal = unsafe { Builder::new().map::<u32, [u8; 4], GenericOrderWalReaderArenaTable<_, _>, _>(path).unwrap() };

  for i in 0..100u32 {
    assert!(wal.contains_key(&i));
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

#[test]
fn concurrent_one_key_linked_inmemory() {
  let wal = Builder::new().with_capacity(MB).alloc::<u32, [u8; 4], GenericOrderWalLinkedTable<_, _>>().unwrap();
  concurrent_one_key(wal);
}

#[test]
fn concurrent_one_key_linked_map_anon() {
  let wal = Builder::new().with_capacity(MB).map_anon::<u32, [u8; 4], GenericOrderWalLinkedTable<_, _>>().unwrap();
  concurrent_one_key(wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn concurrent_one_key_linked_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_concurrent_basic_linked_map_file");

  let wal = unsafe {
    Builder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<u32, [u8; 4], GenericOrderWalLinkedTable<_, _>, _>(&path)
      .unwrap()
  };

  concurrent_one_key(wal);

  let wal = unsafe { Builder::new().map::<u32, [u8; 4], GenericOrderWalReaderLinkedTable<_, _>, _>(path).unwrap() };

  assert!(wal.contains_key(&1));
}

#[test]
fn concurrent_one_key_arena_inmemory() {
  let wal = Builder::new().with_capacity(MB).alloc::<u32, [u8; 4], GenericOrderWalArenaTable<_, _>>().unwrap();
  concurrent_one_key(wal);
}

#[test]
fn concurrent_one_key_arena_map_anon() {
  let wal = Builder::new().with_capacity(MB).map_anon::<u32, [u8; 4], GenericOrderWalArenaTable<_, _>>().unwrap();
  concurrent_one_key(wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn concurrent_one_key_arena_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_concurrent_basic_arena_map_file");

  let wal = unsafe {
    Builder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<u32, [u8; 4], GenericOrderWalArenaTable<_, _>, _>(&path)
      .unwrap()
  };

  concurrent_one_key(wal);

  let wal = unsafe { Builder::new().map::<u32, [u8; 4], GenericOrderWalReaderArenaTable<_, _>, _>(path).unwrap() };

  assert!(wal.contains_key(&1));
}

fn insert_batch<M>(
  wal: &mut GenericOrderWal<Person, String, M>,
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
    batch.push(BatchEntry::new(Generic::from(person), Generic::from(val)));
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

#[test]
fn test_insert_batch_inmemory() {
  insert_batch(&mut Builder::new().with_capacity(MB).alloc().unwrap());
}

#[test]
fn test_insert_batch_map_anon() {
  insert_batch(&mut Builder::new().with_capacity(MB).map_anon().unwrap());
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_insert_batch_map_file() {
  let dir = ::tempfile::tempdir().unwrap();
  let path = dir.path().join(concat!(
    "test_",
    stringify!($prefix),
    "_insert_batch_map_file"
  ));
  let mut map = unsafe {
    Builder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut(&path)
      .unwrap()
  };

  let (rp1, data, rp2) = insert_batch(&mut map);

  let map = unsafe {
    Builder::new()
      .map::<Person, String, _>(&path)
      .unwrap()
  };

  for (p, val) in data {
    assert_eq!(map.get(&p).unwrap().value(), &val);
  }
  assert_eq!(map.get(&rp1).unwrap().value(), "rp1");
  assert_eq!(map.get(&rp2).unwrap().value(), "rp2");
}

fn insert_batch_with_key_builder(
  wal: &mut GenericOrderWal<Person, String>,
) -> (Person, Vec<(Person, String)>, Person) {
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
    batch.push(EntryWithKeyBuilder::new(
      KeyBuilder::new(person.encoded_len() as u32, |buf: &mut VacantBuffer<'_>| {
        buf.set_len(person.encoded_len());
        person.encode(buf).map(|_| ())
      }),
      Generic::from(val),
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

#[test]
fn test_insert_batch_with_key_builder_inmemory() {
  insert_batch_with_key_builder(&mut Builder::new().with_capacity(MB).alloc().unwrap());
}

#[test]
fn test_insert_batch_with_key_builder_map_anon() {
  insert_batch_with_key_builder(&mut Builder::new().with_capacity(MB).map_anon().unwrap());
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_insert_batch_with_key_builder_map_file() {
  let dir = ::tempfile::tempdir().unwrap();
  let path = dir.path().join(concat!(
    "test_",
    stringify!($prefix),
    "_insert_batch_with_key_builder_map_file"
  ));
  let mut map = unsafe {
    Builder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut(&path)
      .unwrap()
  };

  let (rp1, data, rp2) = insert_batch_with_key_builder(&mut map);

  let map = unsafe {
    Builder::new()
      .map::<Person, String, _>(&path)
      .unwrap()
  };

  for (p, val) in data {
    assert_eq!(map.get(&p).unwrap().value(), &val);
  }
  assert_eq!(map.get(&rp1).unwrap().value(), "rp1");
  assert_eq!(map.get(&rp2).unwrap().value(), "rp2");
}

fn insert_batch_with_value_builder(
  wal: &mut GenericOrderWal<Person, String>,
) -> (Person, Vec<(Person, String)>, Person) {
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
    batch.push(EntryWithValueBuilder::new(
      person.into(),
      ValueBuilder::new(val.len() as u32, |buf: &mut VacantBuffer<'_>| {
        buf.put_slice(val.as_bytes())
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

#[test]
fn test_insert_batch_with_value_builder_inmemory() {
  insert_batch_with_value_builder(&mut Builder::new().with_capacity(MB).alloc().unwrap());
}

#[test]
fn test_insert_batch_with_value_builder_map_anon() {
  insert_batch_with_value_builder(&mut Builder::new().with_capacity(MB).map_anon().unwrap());
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_insert_batch_with_value_builder_map_file() {
  let dir = ::tempfile::tempdir().unwrap();
  let path = dir.path().join(concat!(
    "test_",
    stringify!($prefix),
    "_insert_batch_with_value_builder_map_file"
  ));
  let mut map = unsafe {
    Builder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut(&path)
      .unwrap()
  };

  let (rp1, data, rp2) = insert_batch_with_value_builder(&mut map);

  let map = unsafe {
    Builder::new()
      .map::<Person, String, _>(&path)
      .unwrap()
  };

  for (p, val) in data {
    assert_eq!(map.get(&p).unwrap().value(), &val);
  }
  assert_eq!(map.get(&rp1).unwrap().value(), "rp1");
  assert_eq!(map.get(&rp2).unwrap().value(), "rp2");
}

fn insert_batch_with_builders(
  wal: &mut GenericOrderWal<Person, String>,
) -> (Person, Vec<(Person, String)>, Person) {
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
    batch.push(EntryWithBuilders::new(
      KeyBuilder::new(person.encoded_len() as u32, |buf: &mut VacantBuffer<'_>| {
        buf.set_len(person.encoded_len());
        person.encode(buf).map(|_| ())
      }),
      ValueBuilder::new(val.len() as u32, |buf: &mut VacantBuffer<'_>| {
        buf.put_slice(val.as_bytes())
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

#[test]
fn test_insert_batch_with_builders_inmemory() {
  insert_batch_with_builders(&mut Builder::new().with_capacity(MB).alloc().unwrap());
}

#[test]
fn test_insert_batch_with_builders_map_anon() {
  insert_batch_with_builders(&mut Builder::new().with_capacity(MB).map_anon().unwrap());
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_insert_batch_with_builders_map_file() {
  let dir = ::tempfile::tempdir().unwrap();
  let path = dir.path().join(concat!(
    "test_",
    stringify!($prefix),
    "_insert_batch_with_builders_map_file"
  ));
  let mut map = unsafe {
    Builder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut(&path)
      .unwrap()
  };

  let (rp1, data, rp2) = insert_batch_with_builders(&mut map);

  let map = unsafe {
    Builder::new()
      .map::<Person, String, _>(&path)
      .unwrap()
  };

  for (p, val) in data {
    assert_eq!(map.get(&p).unwrap().value(), &val);
  }
  assert_eq!(map.get(&rp1).unwrap().value(), "rp1");
  assert_eq!(map.get(&rp2).unwrap().value(), "rp2");
}
