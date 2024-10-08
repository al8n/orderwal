use std::{collections::BTreeMap, thread::spawn};

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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Person {
  #[doc(hidden)]
  pub id: u64,
  #[doc(hidden)]
  pub name: String,
}

impl Person {
  #[doc(hidden)]
  #[cfg(test)]
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
  #[cfg(test)]
  #[allow(dead_code)]
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

impl KeyRef<'_, Person> for PersonRef<'_> {
  fn compare<Q>(&self, a: &Q) -> cmp::Ordering
  where
    Q: ?Sized + Ord + Comparable<Self>,
  {
    Comparable::compare(a, self).reverse()
  }

  unsafe fn compare_binary(this: &[u8], other: &[u8]) -> cmp::Ordering {
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
  type Error = dbutils::error::InsufficientBuffer;

  fn encoded_len(&self) -> usize {
    encoded_u64_varint_len(self.id) + self.name.len()
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    let id_size = encode_u64_varint(self.id, buf)?;
    buf[id_size..].copy_from_slice(self.name.as_bytes());
    Ok(id_size + self.name.len())
  }

  #[inline]
  fn encode_to_buffer(
    &self,
    buf: &mut dbutils::buffer::VacantBuffer<'_>,
  ) -> Result<usize, Self::Error> {
    let id_size = buf.put_u64_varint(self.id)?;
    buf.put_slice_unchecked(self.name.as_bytes());
    Ok(id_size + self.name.len())
  }
}

impl<'a> TypeRef<'a> for PersonRef<'a> {
  unsafe fn from_slice(src: &'a [u8]) -> Self {
    let (id_size, id) = decode_u64_varint(src).unwrap();
    let name = std::str::from_utf8(&src[id_size..]).unwrap();
    PersonRef { id, name }
  }
}

impl PersonRef<'_> {
  #[cfg(test)]
  #[allow(dead_code)]
  fn encode_into_vec(&self) -> Result<Vec<u8>, dbutils::error::InsufficientBuffer> {
    let mut buf = vec![0; encoded_u64_varint_len(self.id) + self.name.len()];
    let id_size = encode_u64_varint(self.id, &mut buf)?;
    buf[id_size..].copy_from_slice(self.name.as_bytes());
    Ok(buf)
  }
}
