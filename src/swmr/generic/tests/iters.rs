use super::*;

fn iter(wal: &mut GenericOrderWal<Person, String>) -> Vec<(Person, String)> {
  let mut people = (0..100)
    .map(|_| {
      let p = Person::random();
      let v = format!("My name is {}", p.name);
      wal.insert(&p, &v).unwrap();
      (p, v)
    })
    .collect::<Vec<_>>();

  people.sort_by(|a, b| a.0.cmp(&b.0));

  let mut iter = wal.iter();

  for (pwal, pvec) in people.iter().zip(iter.by_ref()) {
    assert!(pwal.0.equivalent(pvec.key()));
    assert_eq!(&pwal.1, pvec.value());
  }

  let mut rev_iter = wal.iter().rev();

  for (pwal, pvec) in people.iter().rev().zip(rev_iter.by_ref()) {
    assert!(pwal.0.equivalent(pvec.key()));
    assert_eq!(&pwal.1, pvec.value());
  }

  people
}

#[test]
fn iter_inmemory() {
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .alloc::<Person, String>()
    .unwrap();
  iter(&mut wal);
}

#[test]
fn iter_map_anon() {
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .map_anon::<Person, String>()
    .unwrap();
  iter(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn iter_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_iter_map_file");

  let mut wal = unsafe {
    GenericBuilder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<Person, String, _>(&path)
      .unwrap()
  };

  let people = iter(&mut wal);

  let wal = wal.reader();
  let mut iter = wal.iter();

  for (pwal, pvec) in people.iter().zip(iter.by_ref()) {
    assert!(pwal.0.equivalent(pvec.key()));
    assert_eq!(&pwal.1, pvec.value());
  }
}

fn bounds(wal: &mut GenericOrderWal<u32, u32>) {
  for i in 0..100u32 {
    wal.insert(&i, &i).unwrap();
  }

  let upper50 = wal.upper_bound(Bound::Included(&50u32)).unwrap();
  assert_eq!(upper50.value(), &50u32);
  let upper51 = wal.upper_bound(Bound::Excluded(&51u32)).unwrap();
  assert_eq!(upper51.value(), &50u32);

  let upper50 = unsafe {
    wal
      .upper_bound_by_bytes(Bound::Included(50u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(upper50.value(), &50u32);
  let upper51 = unsafe {
    wal
      .upper_bound_by_bytes(Bound::Excluded(51u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(upper51.value(), &50u32);

  let upper101 = wal.upper_bound(Bound::Included(&101u32)).unwrap();
  assert_eq!(upper101.value(), &99u32);
  let upper101 = unsafe {
    wal
      .upper_bound_by_bytes(Bound::Included(101u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(upper101.value(), &99u32);

  let upper_unbounded = wal.upper_bound::<u32>(Bound::Unbounded).unwrap();
  assert_eq!(upper_unbounded.value(), &99u32);
  let upper_unbounded = unsafe { wal.upper_bound_by_bytes(Bound::Unbounded).unwrap() };
  assert_eq!(upper_unbounded.value(), &99u32);

  let lower50 = wal.lower_bound(Bound::Included(&50u32)).unwrap();
  assert_eq!(lower50.value(), &50u32);
  let lower50 = unsafe {
    wal
      .lower_bound_by_bytes(Bound::Included(50u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(lower50.value(), &50u32);

  let lower51 = wal.lower_bound(Bound::Excluded(&51u32)).unwrap();
  assert_eq!(lower51.value(), &52u32);
  let lower51 = unsafe {
    wal
      .lower_bound_by_bytes(Bound::Excluded(51u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(lower51.value(), &52u32);

  let lower0 = wal.lower_bound(Bound::Excluded(&0u32)).unwrap();
  assert_eq!(lower0.value(), &1u32);
  let lower0 = unsafe {
    wal
      .lower_bound_by_bytes(Bound::Excluded(0u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(lower0.value(), &1u32);

  let lower_unbounded = wal.lower_bound::<u32>(Bound::Unbounded).unwrap();
  assert_eq!(lower_unbounded.value(), &0u32);
  let lower_unbounded = unsafe { wal.lower_bound_by_bytes(Bound::Unbounded).unwrap() };
  assert_eq!(lower_unbounded.value(), &0u32);

  let wal = wal.reader();
  let upper50 = wal.upper_bound(Bound::Included(&50u32)).unwrap();
  assert_eq!(upper50.value(), &50u32);
  let upper50 = unsafe {
    wal
      .upper_bound_by_bytes(Bound::Included(50u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(upper50.value(), &50u32);

  let upper51 = wal.upper_bound(Bound::Excluded(&51u32)).unwrap();
  assert_eq!(upper51.value(), &50u32);
  let upper51 = unsafe {
    wal
      .upper_bound_by_bytes(Bound::Excluded(51u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(upper51.value(), &50u32);

  let upper101 = wal.upper_bound(Bound::Included(&101u32)).unwrap();
  assert_eq!(upper101.value(), &99u32);
  let upper101 = unsafe {
    wal
      .upper_bound_by_bytes(Bound::Included(101u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(upper101.value(), &99u32);

  let upper_unbounded = wal.upper_bound::<u32>(Bound::Unbounded).unwrap();
  assert_eq!(upper_unbounded.value(), &99u32);
  let upper_unbounded = unsafe { wal.upper_bound_by_bytes(Bound::Unbounded).unwrap() };
  assert_eq!(upper_unbounded.value(), &99u32);

  let lower50 = wal.lower_bound(Bound::Included(&50u32)).unwrap();
  assert_eq!(lower50.value(), &50u32);
  let lower50 = unsafe {
    wal
      .lower_bound_by_bytes(Bound::Included(50u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(lower50.value(), &50u32);

  let lower51 = wal.lower_bound(Bound::Excluded(&51u32)).unwrap();
  assert_eq!(lower51.value(), &52u32);
  let lower51 = unsafe {
    wal
      .lower_bound_by_bytes(Bound::Excluded(51u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(lower51.value(), &52u32);

  let lower0 = wal.lower_bound(Bound::Excluded(&0u32)).unwrap();
  assert_eq!(lower0.value(), &1u32);
  let lower0 = unsafe {
    wal
      .lower_bound_by_bytes(Bound::Excluded(0u32.to_le_bytes().as_ref()))
      .unwrap()
  };
  assert_eq!(lower0.value(), &1u32);

  let lower_unbounded = wal.lower_bound::<u32>(Bound::Unbounded).unwrap();
  assert_eq!(lower_unbounded.value(), &0u32);
  let lower_unbounded = unsafe { wal.lower_bound_by_bytes(Bound::Unbounded).unwrap() };
  assert_eq!(lower_unbounded.value(), &0u32);
}

#[test]
fn bounds_inmemory() {
  let mut wal = GenericBuilder::new().with_capacity(MB).alloc().unwrap();
  bounds(&mut wal);
}

#[test]
fn bounds_map_anon() {
  let mut wal = GenericBuilder::new().with_capacity(MB).map_anon().unwrap();
  bounds(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn bounds_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_bounds_map_file");

  let mut wal = unsafe {
    GenericBuilder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<u32, u32, _>(&path)
      .unwrap()
  };

  bounds(&mut wal);
}

fn range(wal: &mut GenericOrderWal<Person, String>) {
  let mut mid = Person::random();
  let people = (0..100)
    .map(|idx| {
      let p = Person::random();
      let v = format!("My name is {}", p.name);
      wal.insert(&p, &v).unwrap();

      if idx == 500 {
        mid = p.clone();
      }
      (p, v)
    })
    .collect::<BTreeMap<_, _>>();

  let mut iter = wal.range(Bound::Included(&mid), Bound::Unbounded);

  for (pwal, pvec) in people.range(&mid..).zip(iter.by_ref()) {
    assert!(pwal.0.equivalent(pvec.key()));
    assert_eq!(&pwal.1, pvec.value());
  }

  assert!(iter.next().is_none());

  let wal = wal.reader();
  let mut iter = wal.range(Bound::Included(&mid), Bound::Unbounded);

  for (pwal, pvec) in people.range(&mid..).zip(iter.by_ref()) {
    assert!(pwal.0.equivalent(pvec.key()));
    assert_eq!(&pwal.1, pvec.value());
  }

  let mut rev_iter = wal.range(Bound::Included(&mid), Bound::Unbounded).rev();

  for (pwal, pvec) in people.range(&mid..).rev().zip(rev_iter.by_ref()) {
    assert!(pwal.0.equivalent(pvec.key()));
    assert_eq!(&pwal.1, pvec.value());
  }
}

#[test]
fn range_inmemory() {
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .alloc::<Person, String>()
    .unwrap();
  range(&mut wal);
}

#[test]
fn range_map_anon() {
  let mut wal = GenericBuilder::new()
    .with_capacity(MB)
    .map_anon::<Person, String>()
    .unwrap();
  range(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn range_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_range_map_file");

  let mut wal = unsafe {
    GenericBuilder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<Person, String, _>(&path)
      .unwrap()
  };

  range(&mut wal);
}

fn entry_iter(wal: &mut GenericOrderWal<u32, u32>) {
  for i in 0..100u32 {
    wal.insert(&i, &i).unwrap();
  }

  let mut curr = wal.first();
  let mut cursor = 0;
  while let Some(ent) = curr {
    assert_eq!(ent.key(), &cursor);
    assert_eq!(ent.value(), &cursor);
    cursor += 1;
    curr = ent.next();
  }

  let curr = wal.last();
  std::println!("{:?}", curr);

  let mut curr = curr.clone();
  let mut cursor = 100;
  while let Some(ent) = curr {
    cursor -= 1;
    assert_eq!(ent.key(), &cursor);
    assert_eq!(ent.value(), &cursor);
    curr = ent.prev();
  }
}

#[test]
fn entry_iter_inmemory() {
  let mut wal = GenericBuilder::new().with_capacity(MB).alloc().unwrap();
  entry_iter(&mut wal);
}

#[test]
fn entry_iter_map_anon() {
  let mut wal = GenericBuilder::new().with_capacity(MB).map_anon().unwrap();
  entry_iter(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn entry_iter_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_entry_iter_map_file");

  let mut wal = unsafe {
    GenericBuilder::new()
      .with_capacity(MB)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut(&path)
      .unwrap()
  };

  entry_iter(&mut wal);
}
