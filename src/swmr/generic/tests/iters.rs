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
    assert!(pwal.0.equivalent(&pvec.key()));
    assert_eq!(pwal.1, pvec.value());
  }

  let mut rev_iter = wal.iter().rev();

  for (pwal, pvec) in people.iter().rev().zip(rev_iter.by_ref()) {
    assert!(pwal.0.equivalent(&pvec.key()));
    assert_eq!(pwal.1, pvec.value());
  }

  people
}

#[test]
fn iter_inmemory() {
  let mut wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(MB)).unwrap();
  iter(&mut wal);
}

#[test]
fn iter_map_anon() {
  let mut wal =
    GenericOrderWal::<Person, String>::map_anon(Options::new().with_capacity(MB)).unwrap();
  iter(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn iter_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_iter_map_file");

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

  let people = iter(&mut wal);

  let wal = wal.reader();
  let mut iter = wal.iter();

  for (pwal, pvec) in people.iter().zip(iter.by_ref()) {
    assert!(pwal.0.equivalent(&pvec.key()));
    assert_eq!(pwal.1, pvec.value());
  }
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
    assert!(pwal.0.equivalent(&pvec.key()));
    assert_eq!(pwal.1, pvec.value());
  }

  assert!(iter.next().is_none());

  let wal = wal.reader();
  let mut iter = wal.range(Bound::Included(&mid), Bound::Unbounded);

  for (pwal, pvec) in people.range(&mid..).zip(iter.by_ref()) {
    assert!(pwal.0.equivalent(&pvec.key()));
    assert_eq!(pwal.1, pvec.value());
  }

  let mut rev_iter = wal.range(Bound::Included(&mid), Bound::Unbounded).rev();

  for (pwal, pvec) in people.range(&mid..).rev().zip(rev_iter.by_ref()) {
    assert!(pwal.0.equivalent(&pvec.key()));
    assert_eq!(pwal.1, pvec.value());
  }
}

#[test]
fn range_inmemory() {
  let mut wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(MB)).unwrap();
  range(&mut wal);
}

#[test]
fn range_map_anon() {
  let mut wal =
    GenericOrderWal::<Person, String>::map_anon(Options::new().with_capacity(MB)).unwrap();
  range(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn range_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_range_map_file");

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

  range(&mut wal);
}

fn range_ref(wal: &mut GenericOrderWal<Person, String>) {
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

  let mid_ref = mid.as_ref();
  let mut iter = wal.range_by_ref(Bound::Included(&mid_ref), Bound::Unbounded);

  for (pwal, pvec) in people.range(&mid..).zip(iter.by_ref()) {
    assert!(pwal.0.equivalent(&pvec.key()));
    assert_eq!(pwal.1, pvec.value());
  }

  assert!(iter.next().is_none());

  let wal = wal.reader();
  let mut iter = wal.range_by_ref(Bound::Included(&mid), Bound::Unbounded);

  for (pwal, pvec) in people.range(&mid..).zip(iter.by_ref()) {
    assert!(pwal.0.equivalent(&pvec.key()));
    assert_eq!(pwal.1, pvec.value());
  }

  let mut rev_iter = wal
    .range_by_ref(Bound::Included(&mid), Bound::Unbounded)
    .rev();

  for (pwal, pvec) in people.range(&mid..).rev().zip(rev_iter.by_ref()) {
    assert!(pwal.0.equivalent(&pvec.key()));
    assert_eq!(pwal.1, pvec.value());
  }
}

#[test]
fn range_ref_inmemory() {
  let mut wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(MB)).unwrap();
  range(&mut wal);
}

#[test]
fn range_ref_map_anon() {
  let mut wal =
    GenericOrderWal::<Person, String>::map_anon(Options::new().with_capacity(MB)).unwrap();
  range_ref(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn range_ref_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_range_map_file");

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

  range_ref(&mut wal);
}