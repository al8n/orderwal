use super::*;

#[test]
fn owned_comparable() {
  let p1 = Person {
    id: 3127022870678870148,
    name: "enthusiastic-magic".into(),
  };
  let p2 = Person {
    id: 9872687799307360216,
    name: "damaged-friend".into(),
  };

  let p1bytes = p1.to_vec();
  let p2bytes = p2.to_vec();

  let ptr1 = Pointer::<Person, String>::new(p1bytes.len(), 0, p1bytes.as_ptr());
  let ptr2 = Pointer::<Person, String>::new(p2bytes.len(), 0, p2bytes.as_ptr());

  let map = SkipSet::new();
  map.insert(ptr1);
  map.insert(ptr2);

  assert!(map.contains(&Owned::new(&p1)));
  assert!(map.get(&Owned::new(&p1)).is_some());

  assert!(map.contains(&Owned::new(&p2)));
  assert!(map.get(&Owned::new(&p2)).is_some());

  let mut wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(MB)).unwrap();
  wal.insert(&p1, &"My name is Alice!".to_string()).unwrap();
  wal.insert(&p2, &"My name is Bob!".to_string()).unwrap();

  assert!(wal.contains_key(&p1));
  assert_eq!(wal.get(&p1).unwrap().value(), "My name is Alice!");

  assert!(wal.contains_key(&p2));
  assert_eq!(wal.get(&p2).unwrap().value(), "My name is Bob!");
}

#[test]
fn ref_comparable() {
  let p1 = PersonRef {
    id: 3127022870678870148,
    name: "enthusiastic-magic",
  };
  let p2 = PersonRef {
    id: 9872687799307360216,
    name: "damaged-friend",
  };

  let p1bytes = p1.to_vec();
  let p2bytes = p2.to_vec();

  let ptr1 = Pointer::<Person, String>::new(p1bytes.len(), 0, p1bytes.as_ptr());
  let ptr2 = Pointer::<Person, String>::new(p2bytes.len(), 0, p2bytes.as_ptr());

  let map = SkipSet::new();
  map.insert(ptr1);
  map.insert(ptr2);

  assert!(map.contains(&Owned::new(&p1)));
  assert!(map.get(&Owned::new(&p1)).is_some());

  assert!(map.contains(&Owned::new(&p2)));
  assert!(map.get(&Owned::new(&p2)).is_some());

  let mut wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(MB)).unwrap();

  unsafe {
    wal
      .insert_key_bytes_with_value(&p1bytes, &"My name is Alice!".to_string())
      .unwrap();
    wal
      .insert_key_bytes_with_value(&p2bytes, &"My name is Bob!".to_string())
      .unwrap();
  }

  assert!(wal.contains_key(&p1));
  assert_eq!(wal.get(&p1).unwrap().value(), "My name is Alice!");

  assert!(wal.contains_key(&p2));
  assert_eq!(wal.get(&p2).unwrap().value(), "My name is Bob!");

  assert!(wal.contains_key_by_ref(&p1));
  assert_eq!(wal.get(&p1).unwrap().value(), "My name is Alice!");

  assert!(wal.contains_key_by_ref(&p2));
  assert_eq!(wal.get(&p2).unwrap().value(), "My name is Bob!");

  unsafe {
    assert!(wal.contains_key_by_bytes(&p1bytes));
    assert_eq!(wal.get(&p1).unwrap().value(), "My name is Alice!");

    assert!(wal.contains_key_by_bytes(&p2bytes));
    assert_eq!(wal.get(&p2).unwrap().value(), "My name is Bob!");
  }
}

#[test]
fn construct_inmemory() {
  let mut wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(MB)).unwrap();

  let person = Person {
    id: 1,
    name: "Alice".to_string(),
  };

  assert!(wal.is_empty());

  wal
    .insert(&person, &"My name is Alice!".to_string())
    .unwrap();

  let wal = wal.reader();

  assert_eq!(wal.len(), 1);
  assert!(!wal.is_empty());
}

#[test]
fn construct_map_anon() {
  let mut wal =
    GenericOrderWal::<Person, String>::map_anon(Options::new().with_capacity(MB)).unwrap();

  let person = Person {
    id: 1,
    name: "Alice".to_string(),
  };

  wal
    .insert(&person, &"My name is Alice!".to_string())
    .unwrap();
}

#[test]
#[cfg_attr(miri, ignore)]
fn construct_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_construct_map_file");

  unsafe {
    let mut wal = GenericOrderWal::<Person, String>::map_mut(
      &path,
      Options::new(),
      OpenOptions::new()
        .create_new(Some(MB))
        .write(true)
        .read(true),
    )
    .unwrap();
    let person = Person {
      id: 1,
      name: "Alice".to_string(),
    };

    wal
      .insert(&person, &"My name is Alice!".to_string())
      .unwrap();
    assert_eq!(wal.get(&person).unwrap().value(), "My name is Alice!");

    assert_eq!(*wal.path().unwrap().as_ref(), path);
  }

  let pr = PersonRef {
    id: 1,
    name: "Alice",
  };

  unsafe {
    let wal = GenericOrderWal::<Person, String>::map_mut(
      &path,
      Options::new(),
      OpenOptions::new().create(Some(MB)).write(true).read(true),
    )
    .unwrap();
    assert_eq!(wal.get(&pr).unwrap().value(), "My name is Alice!");
  }

  let wal = unsafe { GenericOrderWal::<Person, String>::map(&path, Options::new()).unwrap() };
  assert_eq!(wal.get(&pr).unwrap().value(), "My name is Alice!");
}

#[test]
fn construct_with_small_capacity_inmemory() {
  let wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(1));

  assert!(wal.is_err());
  match wal {
    Err(e) => println!("error: {:?}", e),
    _ => panic!("unexpected error"),
  }
}

#[test]
fn construct_with_small_capacity_map_anon() {
  let wal = GenericOrderWal::<Person, String>::map_anon(Options::new().with_capacity(1));

  assert!(wal.is_err());
  match wal {
    Err(e) => println!("error: {:?}", e),
    _ => panic!("unexpected error"),
  }
}

#[test]
fn construct_with_small_capacity_map_file() {
  let dir = tempdir().unwrap();
  let path = dir
    .path()
    .join("generic_wal_construct_with_small_capacity_map_file");

  let wal = unsafe {
    GenericOrderWal::<Person, String>::map_mut(
      &path,
      Options::new(),
      OpenOptions::new()
        .create_new(Some(1))
        .write(true)
        .read(true),
    )
  };

  assert!(wal.is_err());
  match wal {
    Err(e) => println!("{:?}", e),
    _ => panic!("unexpected error"),
  }
}

fn zero_reserved(wal: &mut GenericOrderWal<Person, String>) {
  unsafe {
    assert_eq!(wal.reserved_slice(), &[]);
    assert_eq!(wal.reserved_slice_mut(), &mut []);

    let wal = wal.reader();
    assert_eq!(wal.reserved_slice(), &[]);
  }
}

#[test]
fn zero_reserved_inmemory() {
  let mut wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(MB)).unwrap();
  zero_reserved(&mut wal);
}

#[test]
fn zero_reserved_map_anon() {
  let mut wal =
    GenericOrderWal::<Person, String>::map_anon(Options::new().with_capacity(MB)).unwrap();
  zero_reserved(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn zero_reserved_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_zero_reserved_map_file");

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

  zero_reserved(&mut wal);
}

fn reserved(wal: &mut GenericOrderWal<Person, String>) {
  unsafe {
    let buf = wal.reserved_slice_mut();
    buf.copy_from_slice(b"al8n");
    assert_eq!(wal.reserved_slice(), b"al8n");
    assert_eq!(wal.reserved_slice_mut(), b"al8n");

    let wal = wal.reader();
    assert_eq!(wal.reserved_slice(), b"al8n");
  }
}

#[test]
fn reserved_inmemory() {
  let mut wal =
    GenericOrderWal::<Person, String>::new(Options::new().with_capacity(MB).with_reserved(4))
      .unwrap();
  reserved(&mut wal);
}

#[test]
fn reserved_map_anon() {
  let mut wal =
    GenericOrderWal::<Person, String>::map_anon(Options::new().with_capacity(MB).with_reserved(4))
      .unwrap();
  reserved(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn reserved_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_reserved_map_file");

  let mut wal = unsafe {
    GenericOrderWal::<Person, String>::map_mut(
      &path,
      Options::new().with_reserved(4),
      OpenOptions::new()
        .create_new(Some(MB))
        .write(true)
        .read(true),
    )
    .unwrap()
  };

  reserved(&mut wal);
}