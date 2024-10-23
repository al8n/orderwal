use super::*;

#[test]
#[allow(clippy::needless_borrows_for_generic_args)]
fn query_comparable() {
  let p1 = Person {
    id: 3127022870678870148,
    name: "enthusiastic-magic".into(),
  };
  let p2 = Person {
    id: 9872687799307360216,
    name: "damaged-friend".into(),
  };

  let p1bytes = p1.encode_into_vec().unwrap();
  let p2bytes = p2.encode_into_vec().unwrap();

  let ptr1 = GenericPointer::<Person, String>::new(p1bytes.len(), 0, p1bytes.as_ptr());
  let ptr2 = GenericPointer::<Person, String>::new(p2bytes.len(), 0, p2bytes.as_ptr());

  let map = SkipSet::new();
  map.insert(ptr1);
  map.insert(ptr2);

  assert!(map.contains(&Query::new(&p1)));
  assert!(map.get(&Query::new(&p1)).is_some());

  assert!(map.contains(&Query::new(&p2)));
  assert!(map.get(&Query::new(&p2)).is_some());

  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .alloc::<Person, String>()
    .unwrap();
  wal.insert(&p1, &"My name is Alice!".to_string()).unwrap();
  wal.insert(&p2, &"My name is Bob!".to_string()).unwrap();

  assert!(wal.contains_key(&p1));
  assert_eq!(wal.get(&p1).unwrap().value(), "My name is Alice!");

  assert!(wal.contains_key(&p2));
  assert_eq!(wal.get(&p2).unwrap().value(), "My name is Bob!");
}

#[test]
#[allow(clippy::needless_borrows_for_generic_args)]
fn construct_inmemory() {
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .alloc::<Person, String>()
    .unwrap();

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
#[allow(clippy::needless_borrows_for_generic_args)]
fn construct_map_anon() {
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .map_anon::<Person, String>()
    .unwrap();
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
#[allow(clippy::needless_borrows_for_generic_args)]
fn construct_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_construct_map_file");

  unsafe {
    let mut wal = GenericBuilder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<Person, String, _>(&path)
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
    let wal = GenericBuilder::new()
      .with_capacity(MB)
      .with_create(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<Person, String, _>(&path)
      .unwrap();
    assert_eq!(wal.get(&pr).unwrap().value(), "My name is Alice!");
  }

  let wal = unsafe {
    GenericBuilder::new()
      .map::<Person, String, _>(&path)
      .unwrap()
  };
  assert_eq!(wal.get(&pr).unwrap().value(), "My name is Alice!");
}

#[test]
fn construct_with_small_capacity_inmemory() {
  let wal = GenericBuilder::new()
    .with_capacity(1)
    .alloc::<Person, String>();

  assert!(wal.is_err());
  match wal {
    Err(e) => println!("error: {:?}", e),
    _ => panic!("unexpected error"),
  }
}

#[test]
fn construct_with_small_capacity_map_anon() {
  let wal = GenericBuilder::new()
    .with_capacity(1)
    .map_anon::<Person, String>();

  assert!(wal.is_err());
  match wal {
    Err(e) => println!("error: {:?}", e),
    _ => panic!("unexpected error"),
  }
}

#[test]
#[cfg_attr(miri, ignore)]
fn construct_with_small_capacity_map_file() {
  let dir = tempdir().unwrap();
  let path = dir
    .path()
    .join("generic_wal_construct_with_small_capacity_map_file");

  let wal = unsafe {
    GenericBuilder::new()
      .with_capacity(1)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<Person, String, _>(&path)
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
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .alloc::<Person, String>()
    .unwrap();
  zero_reserved(&mut wal);
}

#[test]
fn zero_reserved_map_anon() {
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .map_anon::<Person, String>()
    .unwrap();
  zero_reserved(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn zero_reserved_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_zero_reserved_map_file");

  let mut wal = unsafe {
    GenericBuilder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<Person, String, _>(&path)
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
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .with_reserved(4)
    .alloc()
    .unwrap();
  reserved(&mut wal);
}

#[test]
fn reserved_map_anon() {
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .with_reserved(4)
    .map_anon()
    .unwrap();
  reserved(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn reserved_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_reserved_map_file");

  let mut wal = unsafe {
    GenericBuilder::new()
      .with_reserved(4)
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<Person, String, _>(&path)
      .unwrap()
  };

  reserved(&mut wal);
}
