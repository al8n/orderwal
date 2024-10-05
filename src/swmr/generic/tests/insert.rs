use super::*;

fn insert_to_full(wal: &mut GenericOrderWal<Person, String>) {
  let mut full = false;
  for _ in 0u32.. {
    let p = Person::random();
    #[allow(clippy::needless_borrows_for_generic_args)]
    match wal.insert(&p, &format!("My name is {}", p.name)) {
      Ok(_) => {}
      Err(e) => match e {
        Among::Right(Error::InsufficientSpace { .. }) => {
          full = true;
          break;
        }
        _ => panic!("unexpected error"),
      },
    }
  }
  assert!(full);
}

#[test]
fn insert_to_full_inmemory() {
  let mut wal = GenericBuilder::new().with_capacity(100).alloc().unwrap();
  insert_to_full(&mut wal);
}

#[test]
fn insert_to_full_map_anon() {
  let mut wal = GenericBuilder::new().with_capacity(100).map_anon().unwrap();
  insert_to_full(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn insert_to_full_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_insert_to_full_map_file");

  unsafe {
    let mut wal = GenericBuilder::new()
      .with_capacity(100)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut(&path)
      .unwrap();
    insert_to_full(&mut wal);
  }
}

fn insert(wal: &mut GenericOrderWal<Person, String>) -> Vec<Person> {
  let people = (0..100)
    .map(|_| {
      let p = Person::random();
      #[allow(clippy::needless_borrows_for_generic_args)]
      wal.insert(&p, &format!("My name is {}", p.name)).unwrap();
      p
    })
    .collect::<Vec<_>>();

  assert_eq!(wal.len(), 100);

  for p in &people {
    assert!(wal.contains_key(p));
    assert_eq!(
      wal.get(p).unwrap().value(),
      &format!("My name is {}", p.name)
    );
  }

  people
}

#[test]
fn insert_inmemory() {
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .alloc::<Person, String>()
    .unwrap();
  insert(&mut wal);
}

#[test]
fn insert_map_anon() {
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .map_anon::<Person, String>()
    .unwrap();
  insert(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn insert_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_insert_map_file");

  let people = unsafe {
    let mut wal = GenericBuilder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut(&path)
      .unwrap();
    insert(&mut wal)
  };

  let wal = unsafe {
    GenericBuilder::new()
      .map::<Person, String, _>(&path)
      .unwrap()
  };

  for p in people {
    assert!(wal.contains_key(&p));
    assert_eq!(
      wal.get(&p).unwrap().value(),
      &format!("My name is {}", p.name)
    );
  }
}

fn insert_with_key_builder(wal: &mut GenericOrderWal<Person, String>) -> Vec<Person> {
  let people = (0..100)
    .map(|_| unsafe {
      let p = Person::random();
      wal
        .insert_with_key_builder(
          KeyBuilder::new(p.encoded_len() as u32, |buf: &mut VacantBuffer<'_>| {
            p.encode_to_buffer(buf).map(|_| ())
          }),
          &format!("My name is {}", p.name),
        )
        .unwrap();
      p
    })
    .collect::<Vec<_>>();

  assert_eq!(wal.len(), 100);

  for p in &people {
    assert!(wal.contains_key(p));
    assert_eq!(
      wal.get(p).unwrap().value(),
      &format!("My name is {}", p.name)
    );
  }

  people
}

#[test]
fn insert_with_key_builder_inmemory() {
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .alloc::<Person, String>()
    .unwrap();
  insert_with_key_builder(&mut wal);
}

#[test]
fn insert_with_key_builder_map_anon() {
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .map_anon::<Person, String>()
    .unwrap();
  insert_with_key_builder(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn insert_with_key_builder_map_file() {
  let dir = tempdir().unwrap();
  let path = dir
    .path()
    .join("generic_wal_insert_with_key_builder_map_file");

  let people = unsafe {
    let mut wal = GenericBuilder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut(&path)
      .unwrap();
    insert_with_key_builder(&mut wal)
  };

  let wal = unsafe {
    GenericBuilder::new()
      .map::<Person, String, _>(&path)
      .unwrap()
  };

  for p in people {
    assert!(wal.contains_key(&p));
    assert_eq!(
      wal.get(&p).unwrap().value(),
      &format!("My name is {}", p.name)
    );
  }
}

fn insert_with_value_builder(wal: &mut GenericOrderWal<Person, String>) -> Vec<Person> {
  let people = (0..100)
    .map(|_| unsafe {
      let p = Person::random();
      let v = format!("My name is {}", p.name);

      wal
        .insert_with_value_builder(
          &p,
          ValueBuilder::new(v.len() as u32, |buf: &mut VacantBuffer<'_>| {
            buf.put_slice(v.as_bytes())
          }),
        )
        .unwrap();
      p
    })
    .collect::<Vec<_>>();

  assert_eq!(wal.len(), 100);

  for p in &people {
    assert!(wal.contains_key(p));
    assert_eq!(
      wal.get(p).unwrap().value(),
      &format!("My name is {}", p.name)
    );
  }

  people
}

#[test]
fn insert_with_value_builder_inmemory() {
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .alloc::<Person, String>()
    .unwrap();
  insert_with_value_builder(&mut wal);
}

#[test]
fn insert_with_value_builder_map_anon() {
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .map_anon::<Person, String>()
    .unwrap();
  insert_with_value_builder(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn insert_with_value_builder_map_file() {
  let dir = tempdir().unwrap();
  let path = dir
    .path()
    .join("generic_wal_insert_with_value_builder_map_file");

  let people = unsafe {
    let mut wal = GenericBuilder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut(&path)
      .unwrap();
    insert_with_value_builder(&mut wal)
  };

  let wal = unsafe {
    GenericBuilder::new()
      .map::<Person, String, _>(&path)
      .unwrap()
  };

  for p in people {
    assert!(wal.contains_key(&p));
    assert_eq!(
      wal.get(&p).unwrap().value(),
      &format!("My name is {}", p.name)
    );
  }
}

fn insert_with_builders(wal: &mut GenericOrderWal<Person, String>) -> Vec<Person> {
  let people = (0..100)
    .map(|_| unsafe {
      let p = Person::random();
      let v = format!("My name is {}", p.name);
      wal
        .insert_with_builders(
          KeyBuilder::new(p.encoded_len() as u32, |buf: &mut VacantBuffer<'_>| {
            buf.set_len(p.encoded_len());
            p.encode(buf).map(|_| ())
          }),
          ValueBuilder::new(v.len() as u32, |buf: &mut VacantBuffer<'_>| {
            buf.put_slice(v.as_bytes())
          }),
        )
        .unwrap();
      p
    })
    .collect::<Vec<_>>();

  assert_eq!(wal.len(), 100);

  for p in &people {
    assert!(wal.contains_key(p));
    assert_eq!(
      wal.get(p).unwrap().value(),
      &format!("My name is {}", p.name)
    );
  }

  people
}

#[test]
fn insert_with_builders_inmemory() {
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .alloc::<Person, String>()
    .unwrap();
  insert_with_builders(&mut wal);
}

#[test]
fn insert_with_builders_map_anon() {
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .map_anon::<Person, String>()
    .unwrap();
  insert_with_builders(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn insert_with_builders_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_insert_with_builders_map_file");

  let people = unsafe {
    let mut wal = GenericBuilder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut(&path)
      .unwrap();
    insert_with_builders(&mut wal)
  };

  let wal = unsafe {
    GenericBuilder::new()
      .map::<Person, String, _>(&path)
      .unwrap()
  };

  for p in people {
    assert!(wal.contains_key(&p));
    assert_eq!(
      wal.get(&p).unwrap().value(),
      &format!("My name is {}", p.name)
    );
  }
}

fn insert_key_bytes_with_value(
  wal: &mut GenericOrderWal<Person, String>,
) -> Vec<(Vec<u8>, Person)> {
  let people = (0..100)
    .map(|_| {
      let p = Person::random();
      let pbytes = p.to_vec();
      unsafe {
        wal
          .insert(
            Generic::from_slice(&pbytes),
            &format!("My name is {}", p.name),
          )
          .unwrap();
      }
      (pbytes, p)
    })
    .collect::<Vec<_>>();

  assert_eq!(wal.len(), 100);

  for (pbytes, p) in &people {
    assert!(wal.contains_key(p));
    unsafe {
      assert!(wal.contains_key_by_bytes(pbytes));
    }
    assert_eq!(
      wal.get(p).unwrap().value(),
      &format!("My name is {}", p.name)
    );

    assert_eq!(
      unsafe { wal.get_by_bytes(pbytes).unwrap().value() },
      &format!("My name is {}", p.name)
    );
  }

  people
}

#[test]
fn insert_key_bytes_with_value_inmemory() {
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .alloc::<Person, String>()
    .unwrap();
  insert_key_bytes_with_value(&mut wal);
}

#[test]
fn insert_key_bytes_with_value_map_anon() {
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .map_anon::<Person, String>()
    .unwrap();
  insert_key_bytes_with_value(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn insert_key_bytes_with_value_map_file() {
  let dir = tempdir().unwrap();
  let path = dir
    .path()
    .join("generic_wal_insert_key_bytes_with_value_map_file");

  let mut wal = unsafe {
    GenericBuilder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut(&path)
      .unwrap()
  };
  let people = insert_key_bytes_with_value(&mut wal);

  let wal = wal.reader();

  for (pbytes, p) in &people {
    assert!(wal.contains_key(p));
    unsafe {
      assert!(wal.contains_key_by_bytes(pbytes));
    }
    assert_eq!(
      wal.get(p).unwrap().value(),
      &format!("My name is {}", p.name)
    );
    assert_eq!(
      unsafe { wal.get_by_bytes(pbytes).unwrap().value() },
      &format!("My name is {}", p.name)
    );
  }

  let wal = unsafe {
    GenericBuilder::new()
      .map::<Person, String, _>(&path)
      .unwrap()
  };

  for (pbytes, p) in people {
    assert!(wal.contains_key(&p));
    unsafe {
      assert!(wal.contains_key_by_bytes(&pbytes));
    }
    assert_eq!(
      wal.get(&p).unwrap().value(),
      &format!("My name is {}", p.name)
    );
    assert_eq!(
      unsafe { wal.get_by_bytes(&pbytes).unwrap().value() },
      &format!("My name is {}", p.name)
    );
  }
}

fn insert_key_with_value_bytes(wal: &mut GenericOrderWal<Person, String>) -> Vec<Person> {
  let people = (0..100)
    .map(|_| {
      let p = Person::random();
      unsafe {
        wal
          .insert(
            &p,
            Generic::from_slice(format!("My name is {}", p.name).as_bytes()),
          )
          .unwrap();
      }
      p
    })
    .collect::<Vec<_>>();

  assert_eq!(wal.len(), 100);

  for p in &people {
    assert!(wal.contains_key(p));
    assert_eq!(
      wal.get(p).unwrap().value(),
      &format!("My name is {}", p.name)
    );
  }

  people
}

#[test]
fn insert_key_with_value_bytes_inmemory() {
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .alloc::<Person, String>()
    .unwrap();
  insert_key_with_value_bytes(&mut wal);
}

#[test]
fn insert_key_with_value_bytes_map_anon() {
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .map_anon::<Person, String>()
    .unwrap();
  insert_key_with_value_bytes(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn insert_key_with_value_bytes_map_file() {
  let dir = tempdir().unwrap();
  let path = dir
    .path()
    .join("generic_wal_insert_key_with_value_bytes_map_file");

  let mut wal = unsafe {
    GenericBuilder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut(&path)
      .unwrap()
  };

  let people = insert_key_with_value_bytes(&mut wal);
  let wal = wal.reader();

  for p in &people {
    assert!(wal.contains_key(p));
    assert_eq!(
      wal.get(p).unwrap().value(),
      &format!("My name is {}", p.name)
    );
  }
}

fn insert_bytes(wal: &mut GenericOrderWal<Person, String>) -> Vec<Person> {
  let people = (0..100)
    .map(|_| {
      let p = Person::random();
      let pbytes = p.to_vec();
      unsafe {
        wal
          .insert(
            Generic::from_slice(&pbytes),
            Generic::from_slice(format!("My name is {}", p.name).as_bytes()),
          )
          .unwrap();
      }
      p
    })
    .collect::<Vec<_>>();

  assert_eq!(wal.len(), 100);

  for p in &people {
    assert!(wal.contains_key(p));
    unsafe {
      assert!(wal.contains_key_by_bytes(&p.to_vec()));
    }
    assert_eq!(
      wal.get(p).unwrap().value(),
      &format!("My name is {}", p.name)
    );
  }

  people
}

#[test]
fn insert_bytes_inmemory() {
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .alloc::<Person, String>()
    .unwrap();
  insert_bytes(&mut wal);
}

#[test]
fn insert_bytes_map_anon() {
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .map_anon::<Person, String>()
    .unwrap();
  insert_bytes(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn insert_bytes_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_insert_bytes_map_file");

  let mut wal = unsafe {
    GenericBuilder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut(&path)
      .unwrap()
  };

  let people = insert_bytes(&mut wal);

  let wal = wal.reader();

  for p in &people {
    assert!(wal.contains_key(p));
    unsafe {
      assert!(wal.contains_key_by_bytes(&p.to_vec()));
    }
    assert_eq!(
      wal.get(p).unwrap().value(),
      &format!("My name is {}", p.name)
    );
  }
}

fn concurrent_basic(mut w: GenericOrderWal<u32, [u8; 4]>) {
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
fn concurrent_basic_inmemory() {
  let wal = GenericBuilder::new().with_capacity(MB).alloc().unwrap();
  concurrent_basic(wal);
}

#[test]
fn concurrent_basic_map_anon() {
  let wal = GenericBuilder::new().with_capacity(MB).map_anon().unwrap();
  concurrent_basic(wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn concurrent_basic_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_concurrent_basic_map_file");

  let wal = unsafe {
    GenericBuilder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut(&path)
      .unwrap()
  };

  concurrent_basic(wal);

  let wal = unsafe { GenericBuilder::new().map::<u32, [u8; 4], _>(path).unwrap() };

  for i in 0..100u32 {
    assert!(wal.contains_key(&i));
  }
}

fn concurrent_one_key(mut w: GenericOrderWal<u32, [u8; 4]>) {
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
fn concurrent_one_key_inmemory() {
  let wal = GenericBuilder::new().with_capacity(MB).alloc().unwrap();
  concurrent_one_key(wal);
}

#[test]
fn concurrent_one_key_map_anon() {
  let wal = GenericBuilder::new().with_capacity(MB).map_anon().unwrap();
  concurrent_one_key(wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn concurrent_one_key_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_concurrent_basic_map_file");

  let wal = unsafe {
    GenericBuilder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut(&path)
      .unwrap()
  };

  concurrent_one_key(wal);

  let wal = unsafe { GenericBuilder::new().map::<u32, [u8; 4], _>(path).unwrap() };

  assert!(wal.contains_key(&1));
}

fn insert_batch(
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
    if person.id % 2 == 0 {
      batch.push(GenericEntry::new(person, val));
    } else {
      unsafe {
        batch.push(GenericEntry::new(
          person,
          Generic::from_slice(val.as_bytes()),
        ));
      }
    }
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
  insert_batch(&mut GenericBuilder::new().with_capacity(MB).alloc().unwrap());
}

#[test]
fn test_insert_batch_map_anon() {
  insert_batch(&mut GenericBuilder::new().with_capacity(MB).map_anon().unwrap());
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
    GenericBuilder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut(&path)
      .unwrap()
  };

  let (rp1, data, rp2) = insert_batch(&mut map);

  let map = unsafe {
    GenericBuilder::new()
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
  insert_batch_with_key_builder(&mut GenericBuilder::new().with_capacity(MB).alloc().unwrap());
}

#[test]
fn test_insert_batch_with_key_builder_map_anon() {
  insert_batch_with_key_builder(&mut GenericBuilder::new().with_capacity(MB).map_anon().unwrap());
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
    GenericBuilder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut(&path)
      .unwrap()
  };

  let (rp1, data, rp2) = insert_batch_with_key_builder(&mut map);

  let map = unsafe {
    GenericBuilder::new()
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
  insert_batch_with_value_builder(&mut GenericBuilder::new().with_capacity(MB).alloc().unwrap());
}

#[test]
fn test_insert_batch_with_value_builder_map_anon() {
  insert_batch_with_value_builder(&mut GenericBuilder::new().with_capacity(MB).map_anon().unwrap());
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
    GenericBuilder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut(&path)
      .unwrap()
  };

  let (rp1, data, rp2) = insert_batch_with_value_builder(&mut map);

  let map = unsafe {
    GenericBuilder::new()
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
  insert_batch_with_builders(&mut GenericBuilder::new().with_capacity(MB).alloc().unwrap());
}

#[test]
fn test_insert_batch_with_builders_map_anon() {
  insert_batch_with_builders(&mut GenericBuilder::new().with_capacity(MB).map_anon().unwrap());
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
    GenericBuilder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut(&path)
      .unwrap()
  };

  let (rp1, data, rp2) = insert_batch_with_builders(&mut map);

  let map = unsafe {
    GenericBuilder::new()
      .map::<Person, String, _>(&path)
      .unwrap()
  };

  for (p, val) in data {
    assert_eq!(map.get(&p).unwrap().value(), &val);
  }
  assert_eq!(map.get(&rp1).unwrap().value(), "rp1");
  assert_eq!(map.get(&rp2).unwrap().value(), "rp2");
}
