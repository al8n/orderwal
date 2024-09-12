use arbitrary::Arbitrary;
use dbutils::leb128::{decode_u64_varint, encode_u64_varint, encoded_u64_varint_len};
use tempfile::tempdir;

use super::*;

const MB: u32 = 1024 * 1024;

#[derive(PartialEq, Eq, PartialOrd, Ord, Arbitrary)]
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
    let mut buf = Vec::with_capacity(self.encoded_len());
    self.encode(&mut buf).unwrap();
    buf
  }
}

struct PersonRef<'a> {
  id: u64,
  name: &'a str,
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
    insert_key_bytes_with_value(&mut wal)
  };

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
    insert_key_with_value_bytes(&mut wal)
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

// pub(crate) fn insert_with_builders(wal: &mut GenericOrderWal<Person, String>) {
//   for i in 0..1000u32 {
//     wal
//       .insert_with_builders::<(), ()>(
//         KeyBuilder::<_>::new(4, |buf| {
//           let _ = buf.put_u32_be(i);
//           Ok(())
//         }),
//         ValueBuilder::<_>::new(4, |buf| {
//           let _ = buf.put_u32_be(i);
//           Ok(())
//         }),
//       )
//       .unwrap();
//   }

//   for i in 0..1000u32 {
//     assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
//   }
// }

// pub(crate) fn iter(wal: &mut GenericOrderWal<Person, String>) {
//   for i in 0..1000u32 {
//     wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
//   }

//   let mut iter = wal.iter();
//   for i in 0..1000u32 {
//     let (key, value) = iter.next().unwrap();
//     assert_eq!(key, i.to_be_bytes());
//     assert_eq!(value, i.to_be_bytes());
//   }
// }

// pub(crate) fn range(wal: &mut GenericOrderWal<Person, String>) {
//   for i in 0..1000u32 {
//     wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
//   }

//   let x = 500u32.to_be_bytes();

//   let mut iter = wal.range((Bound::Included(x.as_slice()), Bound::Unbounded));
//   for i in 500..1000u32 {
//     let (key, value) = iter.next().unwrap();
//     assert_eq!(key, i.to_be_bytes());
//     assert_eq!(value, i.to_be_bytes());
//   }

//   assert!(iter.next().is_none());
// }

// pub(crate) fn keys(wal: &mut GenericOrderWal<Person, String>) {
//   for i in 0..1000u32 {
//     wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
//   }

//   let mut iter = wal.keys();
//   for i in 0..1000u32 {
//     let key = iter.next().unwrap();
//     assert_eq!(key, i.to_be_bytes());
//   }
// }

// pub(crate) fn range_keys(wal: &mut GenericOrderWal<Person, String>) {
//   for i in 0..1000u32 {
//     wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
//   }

//   let x = 500u32.to_be_bytes();

//   let mut iter = wal.range_keys((Bound::Included(x.as_slice()), Bound::Unbounded));
//   for i in 500..1000u32 {
//     let key = iter.next().unwrap();
//     assert_eq!(key, i.to_be_bytes());
//   }

//   assert!(iter.next().is_none());
// }

// pub(crate) fn values(wal: &mut GenericOrderWal<Person, String>) {
//   for i in 0..1000u32 {
//     wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
//   }

//   let mut iter = wal.values();
//   for i in 0..1000u32 {
//     let value = iter.next().unwrap();
//     assert_eq!(value, i.to_be_bytes());
//   }
// }

// pub(crate) fn range_values(wal: &mut GenericOrderWal<Person, String>) {
//   for i in 0..1000u32 {
//     wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
//   }

//   let x = 500u32.to_be_bytes();

//   let mut iter = wal.range_values((Bound::Included(x.as_slice()), Bound::Unbounded));
//   for i in 500..1000u32 {
//     let value = iter.next().unwrap();
//     assert_eq!(value, i.to_be_bytes());
//   }

//   assert!(iter.next().is_none());
// }

// pub(crate) fn first(wal: &mut GenericOrderWal<Person, String>) {
//   for i in 0..1000u32 {
//     wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
//   }

//   let (key, value) = wal.first().unwrap();
//   assert_eq!(key, 0u32.to_be_bytes());
//   assert_eq!(value, 0u32.to_be_bytes());
// }

// pub(crate) fn last(wal: &mut GenericOrderWal<Person, String>) {
//   for i in 0..1000u32 {
//     wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
//   }

//   let (key, value) = wal.last().unwrap();
//   assert_eq!(key, 999u32.to_be_bytes());
//   assert_eq!(value, 999u32.to_be_bytes());
// }

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

// pub(crate) fn zero_reserved(wal: &mut GenericOrderWal<Person, String>) {
//   unsafe {
//     assert_eq!(wal.reserved_slice(), &[]);
//     assert_eq!(wal.reserved_slice_mut(), &mut []);
//   }
// }

// pub(crate) fn reserved(wal: &mut GenericOrderWal<Person, String>) {
//   unsafe {
//     let buf = wal.reserved_slice_mut();
//     buf.copy_from_slice(b"al8n");
//     assert_eq!(wal.reserved_slice(), b"al8n");
//     assert_eq!(wal.reserved_slice_mut(), b"al8n");
//   }
// }
