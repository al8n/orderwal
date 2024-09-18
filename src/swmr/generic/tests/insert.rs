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
  let mut wal =
    GenericBuilder::new().with_capacity(100).map_anon().unwrap();
  insert_to_full(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn insert_to_full_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_insert_to_full_map_file");

  unsafe {
    let mut wal = GenericBuilder::new().map_mut(
      &path,
      OpenOptions::new()
        .create_new(Some(100))
        .write(true)
        .read(true),
    )
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
    assert!(wal.contains_key_by_ref(&p.as_ref()));
    assert_eq!(
      wal.get(p).unwrap().value(),
      format!("My name is {}", p.name)
    );
  }

  people
}

#[test]
fn insert_inmemory() {
  let mut wal = GenericBuilder::new().with_capacity(MB).alloc::<Person, String>().unwrap();
  insert(&mut wal);
}

#[test]
fn insert_map_anon() {
  let mut wal =
    GenericBuilder::new().with_capacity(MB).map_anon::<Person, String>().unwrap();
  insert(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn insert_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_insert_map_file");

  let people = unsafe {
    let mut wal = GenericBuilder::new().map_mut(
      &path,
      OpenOptions::new()
        .create_new(Some(MB))
        .write(true)
        .read(true),
    )
    .unwrap();
    insert(&mut wal)
  };

  let wal = unsafe { GenericBuilder::new().map::<Person, String, _>(&path).unwrap() };

  for p in people {
    assert!(wal.contains_key(&p));
    assert!(wal.contains_key_by_ref(&p.as_ref()));
    assert_eq!(
      wal.get(&p).unwrap().value(),
      format!("My name is {}", p.name)
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
          .insert_key_bytes_with_value(&pbytes, &format!("My name is {}", p.name))
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
      format!("My name is {}", p.name)
    );

    assert_eq!(
      unsafe { wal.get_by_bytes(pbytes).unwrap().value() },
      format!("My name is {}", p.name)
    );
  }

  people
}

#[test]
fn insert_key_bytes_with_value_inmemory() {
  let mut wal = GenericBuilder::new().with_capacity(MB).alloc::<Person, String>().unwrap();
  insert_key_bytes_with_value(&mut wal);
}

#[test]
fn insert_key_bytes_with_value_map_anon() {
  let mut wal =
    GenericBuilder::new().with_capacity(MB).map_anon::<Person, String>().unwrap();
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
    GenericBuilder::new().map_mut(
      &path,
      OpenOptions::new()
        .create_new(Some(MB))
        .write(true)
        .read(true),
    )
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
      format!("My name is {}", p.name)
    );
    assert_eq!(
      unsafe { wal.get_by_bytes(pbytes).unwrap().value() },
      format!("My name is {}", p.name)
    );
  }

  let wal = unsafe { GenericBuilder::new().map::<Person, String, _>(&path,).unwrap() };

  for (pbytes, p) in people {
    assert!(wal.contains_key(&p));
    unsafe {
      assert!(wal.contains_key_by_bytes(&pbytes));
    }
    assert_eq!(
      wal.get(&p).unwrap().value(),
      format!("My name is {}", p.name)
    );
    assert_eq!(
      unsafe { wal.get_by_bytes(&pbytes).unwrap().value() },
      format!("My name is {}", p.name)
    );
  }
}

fn insert_key_with_value_bytes(wal: &mut GenericOrderWal<Person, String>) -> Vec<Person> {
  let people = (0..100)
    .map(|_| {
      let p = Person::random();
      unsafe {
        wal
          .insert(&p, Generic::from_slice(format!("My name is {}", p.name).as_bytes()))
          .unwrap();
      }
      p
    })
    .collect::<Vec<_>>();

  assert_eq!(wal.len(), 100);

  for p in &people {
    assert!(wal.contains_key(p));
    assert!(wal.contains_key_by_ref(&p.as_ref()));
    assert_eq!(
      wal.get_by_ref(p).unwrap().value(),
      format!("My name is {}", p.name)
    );
  }

  people
}

#[test]
fn insert_key_with_value_bytes_inmemory() {
  let mut wal = GenericBuilder::new().with_capacity(MB).alloc::<Person, String>().unwrap();
  insert_key_with_value_bytes(&mut wal);
}

#[test]
fn insert_key_with_value_bytes_map_anon() {
  let mut wal =
    GenericBuilder::new().with_capacity(MB).map_anon::<Person, String>().unwrap();
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
    GenericBuilder::new().map_mut(
      &path,
     
      OpenOptions::new()
        .create_new(Some(MB))
        .write(true)
        .read(true),
    )
    .unwrap()
  };

  let people = insert_key_with_value_bytes(&mut wal);
  let wal = wal.reader();

  for p in &people {
    assert!(wal.contains_key(p));
    assert!(wal.contains_key_by_ref(&p.as_ref()));
    assert_eq!(
      wal.get_by_ref(p).unwrap().value(),
      format!("My name is {}", p.name)
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
          .insert(Generic::from_slice(&pbytes), Generic::from_slice(format!("My name is {}", p.name).as_bytes()))
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
      format!("My name is {}", p.name)
    );
  }

  people
}

#[test]
fn insert_bytes_inmemory() {
  let mut wal = GenericBuilder::new().with_capacity(MB).alloc::<Person, String>().unwrap();
  insert_bytes(&mut wal);
}

#[test]
fn insert_bytes_map_anon() {
  let mut wal =
    GenericBuilder::new().with_capacity(MB).map_anon::<Person, String>().unwrap();
  insert_bytes(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn insert_bytes_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_insert_bytes_map_file");

  let mut wal = unsafe {
    GenericBuilder::new().map_mut(
      &path,
     
      OpenOptions::new()
        .create_new(Some(MB))
        .write(true)
        .read(true),
    )
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
      format!("My name is {}", p.name)
    );
  }
}

fn concurrent_basic(mut w: GenericOrderWal<u32, [u8; 4]>) {
  let readers = (0..100u32).map(|i| (i, w.reader())).collect::<Vec<_>>();

  let handles = readers.into_iter().map(|(i, reader)| {
    spawn(move || loop {
      if let Some(p) = reader.get(&i) {
        assert_eq!(p.key(), i);
        assert_eq!(p.value(), i.to_le_bytes());
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
  let wal = GenericBuilder::new().with_capacity(MB).alloc()
    .unwrap();
  concurrent_basic(wal);
}

#[test]
fn concurrent_basic_map_anon() {
  let wal =
    GenericBuilder::new().with_capacity(MB).map_anon()
      .unwrap();
  concurrent_basic(wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn concurrent_basic_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_concurrent_basic_map_file");

  let wal = unsafe {
    GenericBuilder::new().map_mut(
      &path,
      OpenOptions::new()
        .create_new(Some(MB))
        .write(true)
        .read(true),
    )
    .unwrap()
  };

  concurrent_basic(wal);

  let wal =
    unsafe { GenericBuilder::new().map::<u32, [u8; 4], _>(path).unwrap() };

  for i in 0..100u32 {
    assert!(wal.contains_key(&i));
  }
}

fn concurrent_one_key(mut w: GenericOrderWal<u32, [u8; 4]>) {
  let readers = (0..100u32).map(|i| (i, w.reader())).collect::<Vec<_>>();
  let handles = readers.into_iter().map(|(_, reader)| {
    spawn(move || loop {
      if let Some(p) = reader.get(&1) {
        assert_eq!(p.key(), 1);
        assert_eq!(p.value(), 1u32.to_le_bytes());
        break;
      }
    })
  });

  w.insert(1, 1u32.to_le_bytes()).unwrap();

  for handle in handles {
    handle.join().unwrap();
  }
}

#[test]
fn concurrent_one_key_inmemory() {
  let wal = GenericBuilder::new().alloc()
    .unwrap();
  concurrent_one_key(wal);
}

#[test]
fn concurrent_one_key_map_anon() {
  let wal =
    GenericBuilder::new().map_anon()
      .unwrap();
  concurrent_one_key(wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn concurrent_one_key_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_concurrent_basic_map_file");

  let wal = unsafe {
    GenericBuilder::new().map_mut(
      &path,
      OpenOptions::new()
        .create_new(Some(MB))
        .write(true)
        .read(true),
    )
    .unwrap()
  };

  concurrent_one_key(wal);

  let wal =
    unsafe { GenericBuilder::new().map::<u32, [u8; 4], _>(path).unwrap() };

  assert!(wal.contains_key(&1));
}
