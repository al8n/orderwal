use super::*;

fn first(wal: &mut GenericOrderWal<Person, String>) {
  let people = (0..10)
    .map(|_| {
      let p = Person::random();
      let v = format!("My name is {}", p.name);
      wal.insert(&p, &v).unwrap();

      (p, v)
    })
    .collect::<BTreeMap<_, _>>();

  let ent = wal.first().unwrap();
  let (p, v) = people.first_key_value().unwrap();
  assert!(ent.key().equivalent(p));
  assert_eq!(ent.value(), v);

  let wal = wal.reader().clone();
  let ent = wal.first().unwrap();
  let (p, v) = people.first_key_value().unwrap();
  assert!(ent.key().equivalent(p));
  assert_eq!(ent.value(), v);
}

#[test]
fn first_inmemory() {
  let mut wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(MB)).unwrap();
  first(&mut wal);
}

#[test]
fn first_map_anon() {
  let mut wal =
    GenericOrderWal::<Person, String>::map_anon(Options::new().with_capacity(MB)).unwrap();
  first(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn first_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_first_map_file");

  let mut wal = unsafe {
    GenericOrderWal::<Person, String>::map_mut(
      &path,
      Options::new(),
      OpenOptions::new()
        .create_new(Some(MB))
        .write(true)
        .read(true),
    )
    .unwrap()
  };

  first(&mut wal);
}

fn last(wal: &mut GenericOrderWal<Person, String>) {
  let people = (0..10)
    .map(|_| {
      let p = Person::random();
      let v = format!("My name is {}", p.name);
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

#[test]
fn last_inmemory() {
  let mut wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(MB)).unwrap();
  last(&mut wal);
}

#[test]
fn last_map_anon() {
  let mut wal =
    GenericOrderWal::<Person, String>::map_anon(Options::new().with_capacity(MB)).unwrap();
  last(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn last_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_last_map_file");

  let mut wal = unsafe {
    GenericOrderWal::<Person, String>::map_mut(
      &path,
      Options::new(),
      OpenOptions::new()
        .create_new(Some(MB))
        .write(true)
        .read(true),
    )
    .unwrap()
  };

  last(&mut wal);
}

fn get_or_insert(wal: &mut GenericOrderWal<Person, String>) {
  let people = (0..100)
    .map(|_| {
      let p = Person::random();
      let v = format!("My name is {}", p.name);
      wal.get_or_insert(&p, &v).unwrap_right().unwrap();
      (p, v)
    })
    .collect::<Vec<_>>();

  assert_eq!(wal.len(), 100);

  for (p, pv) in &people {
    assert!(wal.contains_key(p));
    assert!(wal.contains_key_by_ref(&p.as_ref()));
    assert_eq!(
      wal
        .get_or_insert(p, &format!("Hello! {}!", p.name))
        .unwrap_left()
        .value(),
      pv
    );
  }

  for (p, _) in &people {
    assert!(wal.contains_key(p));
    assert!(wal.contains_key_by_ref(&p.as_ref()));
  }
}

#[test]
fn get_or_insert_inmemory() {
  let mut wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(MB)).unwrap();
  get_or_insert(&mut wal);
}

#[test]
fn get_or_insert_map_anon() {
  let mut wal =
    GenericOrderWal::<Person, String>::map_anon(Options::new().with_capacity(MB)).unwrap();
  get_or_insert(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn get_or_insert_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_get_or_insert_map_file");

  let mut wal = unsafe {
    GenericOrderWal::<Person, String>::map_mut(
      &path,
      Options::new(),
      OpenOptions::new()
        .create_new(Some(MB))
        .write(true)
        .read(true),
    )
    .unwrap()
  };

  get_or_insert(&mut wal);
}

fn get_or_insert_with(wal: &mut GenericOrderWal<Person, String>) {
  let people = (0..100)
    .map(|_| {
      let p = Person::random();
      let v = format!("My name is {}", p.name);
      wal
        .get_or_insert_with(&p, || v.clone())
        .unwrap_right()
        .unwrap();
      (p, v)
    })
    .collect::<Vec<_>>();

  assert_eq!(wal.len(), 100);

  for (p, pv) in &people {
    assert!(wal.contains_key(p));
    assert!(wal.contains_key_by_ref(&p.as_ref()));
    assert_eq!(
      wal
        .get_or_insert_with(p, || format!("Hello! {}!", p.name))
        .unwrap_left()
        .value(),
      pv
    );
  }

  for (p, _) in &people {
    assert!(wal.contains_key(p));
    assert!(wal.contains_key_by_ref(&p.as_ref()));
  }
}

#[test]
fn get_or_insert_with_inmemory() {
  let mut wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(MB)).unwrap();
  get_or_insert_with(&mut wal);
}

#[test]
fn get_or_insert_with_map_anon() {
  let mut wal =
    GenericOrderWal::<Person, String>::map_anon(Options::new().with_capacity(MB)).unwrap();
  get_or_insert_with(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn get_or_insert_with_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_get_or_insert_with_map_file");

  let mut wal = unsafe {
    GenericOrderWal::<Person, String>::map_mut(
      &path,
      Options::new(),
      OpenOptions::new()
        .create_new(Some(MB))
        .write(true)
        .read(true),
    )
    .unwrap()
  };

  get_or_insert_with(&mut wal);
}

fn get_or_insert_key_with_value_bytes(wal: &mut GenericOrderWal<Person, String>) {
  let people = (0..100)
    .map(|_| {
      let p = Person::random();
      let pvec = p.to_vec();
      let v = format!("My name is {}", p.name);
      unsafe {
        wal
          .get_by_bytes_or_insert(pvec.as_ref(), &v)
          .unwrap_right()
          .unwrap();
      }
      (p, v)
    })
    .collect::<Vec<_>>();

  assert_eq!(wal.len(), 100);

  for (p, pv) in &people {
    assert!(wal.contains_key(p));
    assert!(wal.contains_key_by_ref(&p.as_ref()));
    assert_eq!(
      wal
        .get_or_insert(p, &format!("Hello! {}!", p.name))
        .unwrap_left()
        .value(),
      pv
    );
  }

  for (p, _) in &people {
    assert!(wal.contains_key(p));
    assert!(wal.contains_key_by_ref(&p.as_ref()));
  }
}

#[test]
fn get_or_insert_key_with_value_bytes_inmemory() {
  let mut wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(MB)).unwrap();
  get_or_insert_key_with_value_bytes(&mut wal);
}

#[test]
fn get_or_insert_key_with_value_bytes_map_anon() {
  let mut wal =
    GenericOrderWal::<Person, String>::map_anon(Options::new().with_capacity(MB)).unwrap();
  get_or_insert_key_with_value_bytes(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn get_or_insert_key_with_value_bytes_map_file() {
  let dir = tempdir().unwrap();
  let path = dir
    .path()
    .join("generic_wal_get_or_insert_key_with_value_bytes_map_file");

  let mut wal = unsafe {
    GenericOrderWal::<Person, String>::map_mut(
      &path,
      Options::new(),
      OpenOptions::new()
        .create_new(Some(MB))
        .write(true)
        .read(true),
    )
    .unwrap()
  };

  get_or_insert_key_with_value_bytes(&mut wal);
}

fn get_or_insert_value_bytes(wal: &mut GenericOrderWal<Person, String>) {
  let people = (0..100)
    .map(|_| {
      let p = Person::random();
      let v = format!("My name is {}", p.name);
      unsafe {
        wal
          .get_or_insert_bytes(&p, v.as_bytes())
          .unwrap_right()
          .unwrap();
      }
      (p, v)
    })
    .collect::<Vec<_>>();

  assert_eq!(wal.len(), 100);

  for (p, pv) in &people {
    assert!(wal.contains_key(p));
    assert!(wal.contains_key_by_ref(&p.as_ref()));
    unsafe {
      assert_eq!(
        wal
          .get_or_insert_bytes(p, pv.as_bytes())
          .unwrap_left()
          .value(),
        pv
      );
    }
  }

  for (p, _) in &people {
    assert!(wal.contains_key(p));
    assert!(wal.contains_key_by_ref(&p.as_ref()));
  }
}

#[test]
fn get_or_insert_value_bytes_inmemory() {
  let mut wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(MB)).unwrap();
  get_or_insert_value_bytes(&mut wal);
}

#[test]
fn get_or_insert_value_bytes_map_anon() {
  let mut wal =
    GenericOrderWal::<Person, String>::map_anon(Options::new().with_capacity(MB)).unwrap();
  get_or_insert_value_bytes(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn get_or_insert_value_bytes_map_file() {
  let dir = tempdir().unwrap();
  let path = dir
    .path()
    .join("generic_wal_get_or_insert_value_bytes_map_file");

  let mut wal = unsafe {
    GenericOrderWal::<Person, String>::map_mut(
      &path,
      Options::new(),
      OpenOptions::new()
        .create_new(Some(MB))
        .write(true)
        .read(true),
    )
    .unwrap()
  };

  get_or_insert_value_bytes(&mut wal);
}

fn get_by_bytes_or_insert_with(wal: &mut GenericOrderWal<Person, String>) {
  let people = (0..100)
    .map(|_| {
      let p = Person::random();
      let pvec = p.to_vec();
      let v = format!("My name is {}", p.name);
      unsafe {
        wal
          .get_by_bytes_or_insert_with(pvec.as_ref(), || v.clone())
          .unwrap_right()
          .unwrap();
      }
      (p, pvec, v)
    })
    .collect::<Vec<_>>();

  assert_eq!(wal.len(), 100);

  for (p, pvec, pv) in &people {
    assert!(wal.contains_key(p));
    assert!(wal.contains_key_by_ref(&p.as_ref()));
    unsafe {
      assert_eq!(
        wal
          .get_by_bytes_or_insert_with(pvec, || format!("Hello! {}!", p.name))
          .unwrap_left()
          .value(),
        pv
      );
    }
  }

  for (p, _, _) in &people {
    assert!(wal.contains_key(p));
    assert!(wal.contains_key_by_ref(&p.as_ref()));
  }
}

#[test]
fn get_by_bytes_or_insert_with_inmemory() {
  let mut wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(MB)).unwrap();
  get_by_bytes_or_insert_with(&mut wal);
}

#[test]
fn get_by_bytes_or_insert_with_map_anon() {
  let mut wal =
    GenericOrderWal::<Person, String>::map_anon(Options::new().with_capacity(MB)).unwrap();
  get_by_bytes_or_insert_with(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn get_by_bytes_or_insert_with_map_file() {
  let dir = tempdir().unwrap();
  let path = dir
    .path()
    .join("generic_wal_get_by_bytes_or_insert_with_map_file");

  let mut wal = unsafe {
    GenericOrderWal::<Person, String>::map_mut(
      &path,
      Options::new(),
      OpenOptions::new()
        .create_new(Some(MB))
        .write(true)
        .read(true),
    )
    .unwrap()
  };

  get_by_bytes_or_insert_with(&mut wal);
}

fn get_by_bytes_or_insert_bytes(wal: &mut GenericOrderWal<Person, String>) {
  let people = (0..100)
    .map(|_| {
      let p = Person::random();
      let pvec = p.to_vec();
      let v = format!("My name is {}", p.name);
      unsafe {
        wal
          .get_by_bytes_or_insert_bytes(pvec.as_ref(), v.as_bytes())
          .unwrap_right()
          .unwrap();
      }
      (p, pvec, v)
    })
    .collect::<Vec<_>>();

  assert_eq!(wal.len(), 100);

  for (p, pvec, pv) in &people {
    assert!(wal.contains_key(p));
    assert!(wal.contains_key_by_ref(&p.as_ref()));
    unsafe {
      assert_eq!(
        wal
          .get_by_bytes_or_insert_bytes(pvec, pv.as_bytes())
          .unwrap_left()
          .value(),
        pv
      );
    }
  }

  for (p, _, _) in &people {
    assert!(wal.contains_key(p));
    assert!(wal.contains_key_by_ref(&p.as_ref()));
  }
}

#[test]
fn get_by_bytes_or_insert_bytes_inmemory() {
  let mut wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(MB)).unwrap();
  get_by_bytes_or_insert_bytes(&mut wal);
}

#[test]
fn get_by_bytes_or_insert_bytes_map_anon() {
  let mut wal =
    GenericOrderWal::<Person, String>::map_anon(Options::new().with_capacity(MB)).unwrap();
  get_by_bytes_or_insert_bytes(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn get_by_bytes_or_insert_bytes_map_file() {
  let dir = tempdir().unwrap();
  let path = dir
    .path()
    .join("generic_wal_get_by_bytes_or_insert_bytes_map_file");

  let mut wal = unsafe {
    GenericOrderWal::<Person, String>::map_mut(
      &path,
      Options::new(),
      OpenOptions::new()
        .create_new(Some(MB))
        .write(true)
        .read(true),
    )
    .unwrap()
  };

  get_by_bytes_or_insert_bytes(&mut wal);
}