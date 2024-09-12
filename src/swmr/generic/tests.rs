use std::collections::BTreeMap;

use arbitrary::Arbitrary;
use dbutils::leb128::{decode_u64_varint, encode_u64_varint, encoded_u64_varint_len};
use tempfile::tempdir;

use super::*;

const MB: u32 = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Arbitrary)]
struct Person {
  id: u64,
  name: String,
}

impl Person {
  fn random() -> Self {
    Self {
      id: rand::random(),
      name: names::Generator::default().next().unwrap(),
    }
  }

  fn as_ref(&self) -> PersonRef<'_> {
    PersonRef {
      id: self.id,
      name: &self.name,
    }
  }

  fn to_vec(&self) -> Vec<u8> {
    let mut buf = vec![0; self.encoded_len()];
    self.encode(&mut buf).unwrap();
    buf
  }
}

#[derive(Debug)]
struct PersonRef<'a> {
  id: u64,
  name: &'a str,
}

impl<'a> PersonRef<'a> {
  fn encoded_len(&self) -> usize {
    encoded_u64_varint_len(self.id) + self.name.len()
  }

  fn encode(&self, buf: &mut [u8]) -> Result<(), dbutils::leb128::EncodeVarintError> {
    let id_size = encode_u64_varint(self.id, buf)?;
    buf[id_size..].copy_from_slice(self.name.as_bytes());
    Ok(())
  }

  fn to_vec(&self) -> Vec<u8> {
    let mut buf = vec![0; self.encoded_len()];
    self.encode(&mut buf).unwrap();
    buf
  }
}

impl<'a> PartialEq for PersonRef<'a> {
  fn eq(&self, other: &Self) -> bool {
    self.id == other.id && self.name == other.name
  }
}

impl<'a> Eq for PersonRef<'a> {}

impl<'a> PartialOrd for PersonRef<'a> {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<'a> Ord for PersonRef<'a> {
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    self
      .id
      .cmp(&other.id)
      .then_with(|| self.name.cmp(other.name))
  }
}

impl Equivalent<Person> for PersonRef<'_> {
  fn equivalent(&self, key: &Person) -> bool {
    self.id == key.id && self.name == key.name
  }
}

impl Comparable<Person> for PersonRef<'_> {
  fn compare(&self, key: &Person) -> std::cmp::Ordering {
    self.id.cmp(&key.id).then_with(|| self.name.cmp(&key.name))
  }
}

impl Equivalent<PersonRef<'_>> for Person {
  fn equivalent(&self, key: &PersonRef<'_>) -> bool {
    self.id == key.id && self.name == key.name
  }
}

impl Comparable<PersonRef<'_>> for Person {
  fn compare(&self, key: &PersonRef<'_>) -> std::cmp::Ordering {
    self
      .id
      .cmp(&key.id)
      .then_with(|| self.name.as_str().cmp(key.name))
  }
}

impl<'a> KeyRef<'a, Person> for PersonRef<'a> {
  fn compare<Q>(&self, a: &Q) -> cmp::Ordering
  where
    Q: ?Sized + Ord + Comparable<Self>,
  {
    Comparable::compare(a, self)
  }

  fn compare_binary(this: &[u8], other: &[u8]) -> cmp::Ordering {
    let (this_id_size, this_id) = decode_u64_varint(this).unwrap();
    let (other_id_size, other_id) = decode_u64_varint(other).unwrap();

    PersonRef {
      id: this_id,
      name: std::str::from_utf8(&this[this_id_size..]).unwrap(),
    }
    .cmp(&PersonRef {
      id: other_id,
      name: std::str::from_utf8(&other[other_id_size..]).unwrap(),
    })
  }
}

impl Type for Person {
  type Ref<'a> = PersonRef<'a>;
  type Error = dbutils::leb128::EncodeVarintError;

  fn encoded_len(&self) -> usize {
    encoded_u64_varint_len(self.id) + self.name.len()
  }

  fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
    let id_size = encode_u64_varint(self.id, buf)?;
    buf[id_size..].copy_from_slice(self.name.as_bytes());
    Ok(())
  }
}

impl<'a> TypeRef<'a> for PersonRef<'a> {
  fn from_slice(src: &'a [u8]) -> Self {
    let (id_size, id) = decode_u64_varint(src).unwrap();
    let name = std::str::from_utf8(&src[id_size..]).unwrap();
    PersonRef { id, name }
  }
}

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

  wal
    .insert(&person, &"My name is Alice!".to_string())
    .unwrap();
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
  }

  let wal = unsafe { GenericOrderWal::<Person, String>::map(&path, Options::new()).unwrap() };

  let pr = PersonRef {
    id: 1,
    name: "Alice",
  };
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

fn insert_to_full(wal: &mut GenericOrderWal<Person, String>) {
  let mut full = false;
  for _ in 0u32.. {
    let p = Person::random();
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
  let mut wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(MB)).unwrap();
  insert_to_full(&mut wal);
}

#[test]
fn insert_to_full_map_anon() {
  let mut wal =
    GenericOrderWal::<Person, String>::map_anon(Options::new().with_capacity(MB)).unwrap();
  insert_to_full(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn insert_to_full_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_insert_to_full_map_file");

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
    insert_to_full(&mut wal);
  }
}

fn insert(wal: &mut GenericOrderWal<Person, String>) -> Vec<Person> {
  let people = (0..1000)
    .map(|_| {
      let p = Person::random();
      wal.insert(&p, &format!("My name is {}", p.name)).unwrap();
      p
    })
    .collect::<Vec<_>>();

  assert_eq!(wal.len(), 1000);

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
  let mut wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(MB)).unwrap();
  insert(&mut wal);
}

#[test]
fn insert_map_anon() {
  let mut wal =
    GenericOrderWal::<Person, String>::map_anon(Options::new().with_capacity(MB)).unwrap();
  insert(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn insert_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_insert_map_file");

  let people = unsafe {
    let mut wal = GenericOrderWal::<Person, String>::map_mut(
      &path,
      Options::new(),
      OpenOptions::new()
        .create_new(Some(MB))
        .write(true)
        .read(true),
    )
    .unwrap();
    insert(&mut wal)
  };

  let wal = unsafe { GenericOrderWal::<Person, String>::map(&path, Options::new()).unwrap() };

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
  let people = (0..1000)
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

  assert_eq!(wal.len(), 1000);

  for (pbytes, p) in &people {
    assert!(wal.contains_key(p));
    unsafe {
      assert!(wal.contains_key_by_bytes(pbytes));
    }
    assert_eq!(
      wal.get(p).unwrap().value(),
      format!("My name is {}", p.name)
    );
  }

  people
}

#[test]
fn insert_key_bytes_with_value_inmemory() {
  let mut wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(MB)).unwrap();
  insert_key_bytes_with_value(&mut wal);
}

#[test]
fn insert_key_bytes_with_value_map_anon() {
  let mut wal =
    GenericOrderWal::<Person, String>::map_anon(Options::new().with_capacity(MB)).unwrap();
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
  }

  let wal = unsafe { GenericOrderWal::<Person, String>::map(&path, Options::new()).unwrap() };

  for (pbytes, p) in people {
    assert!(wal.contains_key(&p));
    unsafe {
      assert!(wal.contains_key_by_bytes(&pbytes));
    }
    assert_eq!(
      wal.get(&p).unwrap().value(),
      format!("My name is {}", p.name)
    );
  }
}

fn insert_key_with_value_bytes(wal: &mut GenericOrderWal<Person, String>) -> Vec<Person> {
  let people = (0..1000)
    .map(|_| {
      let p = Person::random();
      unsafe {
        wal
          .insert_key_with_value_bytes(&p, format!("My name is {}", p.name).as_bytes())
          .unwrap();
      }
      p
    })
    .collect::<Vec<_>>();

  assert_eq!(wal.len(), 1000);

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
  let mut wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(MB)).unwrap();
  insert_key_with_value_bytes(&mut wal);
}

#[test]
fn insert_key_with_value_bytes_map_anon() {
  let mut wal =
    GenericOrderWal::<Person, String>::map_anon(Options::new().with_capacity(MB)).unwrap();
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
  let people = (0..1000)
    .map(|_| {
      let p = Person::random();
      let pbytes = p.to_vec();
      unsafe {
        wal
          .insert_bytes(&pbytes, format!("My name is {}", p.name).as_bytes())
          .unwrap();
      }
      p
    })
    .collect::<Vec<_>>();

  assert_eq!(wal.len(), 1000);

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
  let mut wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(MB)).unwrap();
  insert_bytes(&mut wal);
}

#[test]
fn insert_bytes_map_anon() {
  let mut wal =
    GenericOrderWal::<Person, String>::map_anon(Options::new().with_capacity(MB)).unwrap();
  insert_bytes(&mut wal);
}

#[test]
#[cfg_attr(miri, ignore)]
fn insert_bytes_map_file() {
  let dir = tempdir().unwrap();
  let path = dir.path().join("generic_wal_insert_bytes_map_file");

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

fn iter(wal: &mut GenericOrderWal<Person, String>) -> Vec<(Person, String)> {
  let mut people = (0..1000)
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
  let people = (0..1000)
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
  let people = (0..1000)
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

// pub(crate) fn get_or_insert(wal: &mut GenericOrderWal<Person, String>) {
//   for i in 0..1000u32 {
//     wal
//       .get_or_insert(&i.to_be_bytes(), &i.to_be_bytes())
//       .unwrap();
//   }

//   for i in 0..1000u32 {
//     wal
//       .get_or_insert(&i.to_be_bytes(), &(i * 2).to_be_bytes())
//       .unwrap();
//   }

//   for i in 0..1000u32 {
//     assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
//   }
// }

// pub(crate) fn get_or_insert_key_with_value_bytes(wal: &mut GenericOrderWal<Person, String>) {
//   for i in 0..1000u32 {
//     wal
//       .get_or_insert_key_with_value_bytes::<()>(
//         &i.to_be_bytes(),
//         ValueBuilder::<_>::new(4, |buf| {
//           let _ = buf.put_u32_be(i);
//           Ok(())
//         }),
//       )
//       .unwrap();
//   }

//   for i in 0..1000u32 {
//     wal
//       .get_or_insert_key_with_value_bytes::<()>(
//         &i.to_be_bytes(),
//         ValueBuilder::<_>::new(4, |buf| {
//           let _ = buf.put_u32_be(i * 2);
//           Ok(())
//         }),
//       )
//       .unwrap();
//   }

//   for i in 0..1000u32 {
//     assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
//   }
// }

fn zero_reserved(wal: &mut GenericOrderWal<Person, String>) {
  unsafe {
    assert_eq!(wal.reserved_slice(), &[]);
    assert_eq!(wal.reserved_slice_mut(), &mut []);
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
  }
}

#[test]
fn reserved_inmemory() {
  let mut wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(MB)).unwrap();
  reserved(&mut wal);
}

#[test]
fn reserved_map_anon() {
  let mut wal =
    GenericOrderWal::<Person, String>::map_anon(Options::new().with_capacity(MB)).unwrap();
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
      Options::new(),
      OpenOptions::new()
        .create_new(Some(MB))
        .write(true)
        .read(true),
    )
    .unwrap()
  };

  reserved(&mut wal);
}
