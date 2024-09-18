use std::{collections::BTreeMap, thread::spawn};

use arbitrary::Arbitrary;
use dbutils::leb128::{decode_u64_varint, encode_u64_varint, encoded_u64_varint_len};
use tempfile::tempdir;

use super::*;

const MB: u32 = 1024 * 1024;

#[cfg(all(test, any(test_swmr_generic_constructor, all_tests)))]
mod constructor;

#[cfg(all(test, any(test_swmr_generic_insert, all_tests)))]
mod insert;

#[cfg(all(test, any(test_swmr_generic_iters, all_tests)))]
mod iters;

#[cfg(all(test, any(test_swmr_generic_get, all_tests)))]
mod get;

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Arbitrary)]
pub struct Person {
  #[doc(hidden)]
  pub id: u64,
  #[doc(hidden)]
  pub name: String,
}

impl Person {
  #[doc(hidden)]
  pub fn random() -> Self {
    Self {
      id: rand::random(),
      name: names::Generator::default().next().unwrap(),
    }
  }

  #[doc(hidden)]
  pub fn as_ref(&self) -> PersonRef<'_> {
    PersonRef {
      id: self.id,
      name: &self.name,
    }
  }

  #[doc(hidden)]
  fn to_vec(&self) -> Vec<u8> {
    let mut buf = vec![0; self.encoded_len()];
    self.encode(&mut buf).unwrap();
    buf
  }
}

#[doc(hidden)]
#[derive(Debug)]
pub struct PersonRef<'a> {
  id: u64,
  name: &'a str,
}

impl PartialEq for PersonRef<'_> {
  fn eq(&self, other: &Self) -> bool {
    self.id == other.id && self.name == other.name
  }
}

impl Eq for PersonRef<'_> {}

impl PartialOrd for PersonRef<'_> {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for PersonRef<'_> {
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
    Comparable::compare(a, self).reverse()
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
  unsafe fn from_slice(src: &'a [u8]) -> Self {
    let (id_size, id) = decode_u64_varint(src).unwrap();
    let name = std::str::from_utf8(&src[id_size..]).unwrap();
    PersonRef { id, name }
  }
}

fn insert_batch(wal: &mut GenericOrderWal<Person, String>) -> Vec<(Person, String)> {
  const N: u32 = 100;

  let mut batch = vec![];
  let output = (0..N).map(|i| ({ let mut p = Person::random(); p.id = i as u64; p }, format!("My id is {i}")).clone()).collect::<Vec<_>>();

  for (person, val) in output.iter() {
    if person.id % 3 == 0 {
      batch.push(GenericEntry::new(person.clone(), val.clone()));
    } else if person.id % 3 == 1 {
      batch.push(GenericEntry::new(person, val));
    } else {
      unsafe { batch.push(GenericEntry::new(person, Generic::from_slice(val.as_bytes()))); }
    }
  }

  wal.insert_batch(&mut batch).unwrap();

  for (p, val) in output.iter() {
    assert_eq!(wal.get(p).unwrap().value(), val);
  }

  let wal = wal.reader();
  for (p, val) in output.iter() {
    assert_eq!(wal.get(p).unwrap().value(), val);
  }

  // output
  vec![]
}

#[test]
fn test_insert_batch_inmemory() {
  insert_batch(&mut GenericBuilder::new().with_capacity(MB).alloc::<Person, String>().unwrap());
}

#[test]
fn test_insert_batch_map_anon() {
  insert_batch(&mut GenericBuilder::new().with_capacity(MB).map_anon::<Person, String>().unwrap());
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
    GenericBuilder::new().map_mut::<Person, String, _>(
      &path,
      OpenOptions::new()
        .create_new(Some(MB))
        .write(true)
        .read(true),
    )
    .unwrap()
  };

  insert_batch(&mut map);

  let map = unsafe { GenericBuilder::new().map::<Person, String, _>(&path).unwrap() };

  for i in 0..100u32 {
    assert_eq!(map.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }
}
