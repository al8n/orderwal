use std::{collections::BTreeMap, thread::spawn};

use arbitrary::Arbitrary;
use dbutils::leb128::{decode_u64_varint, encode_u64_varint, encoded_u64_varint_len};
use tempfile::tempdir;

use super::*;

const MB: u32 = 1024 * 1024;

const fn __static_assertion<B: GenericBatch>() {}

const _: () = {
  __static_assertion::<std::collections::BTreeMap<String, String>>();
  __static_assertion::<std::collections::HashMap<String, String>>();
};

#[cfg(all(test, any(test_swmr_generic_constructor, all_tests)))]
mod constructor;

#[cfg(all(test, any(test_swmr_generic_insert, all_tests)))]
mod insert;

#[cfg(all(test, any(test_swmr_generic_iters, all_tests)))]
mod iters;

#[cfg(all(test, any(test_swmr_generic_get, all_tests)))]
mod get;

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

impl PersonRef<'_> {
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
  fn from_slice(src: &'a [u8]) -> Self {
    let (id_size, id) = decode_u64_varint(src).unwrap();
    let name = std::str::from_utf8(&src[id_size..]).unwrap();
    PersonRef { id, name }
  }
}
