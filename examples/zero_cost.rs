use std::{cmp, sync::Arc, thread::spawn};

use dbutils::leb128::{decode_u64_varint, encode_u64_varint, encoded_u64_varint_len};
use orderwal::{ 
  generic::{ArenaTable, OrderWal, Reader, Writer}, memtable::MemtableEntry, types::{Type, TypeRef, VacantBuffer}, Builder, Comparable, Equivalent
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct PersonRef<'a> {
  id: u64,
  name: &'a str,
}

impl Equivalent<Person> for PersonRef<'_> {
  fn equivalent(&self, key: &Person) -> bool {
    self.id == key.id && self.name == key.name
  }
}

impl Comparable<Person> for PersonRef<'_> {
  fn compare(&self, key: &Person) -> core::cmp::Ordering {
    self.id.cmp(&key.id).then_with(|| self.name.cmp(&key.name))
  }
}

impl Equivalent<PersonRef<'_>> for Person {
  fn equivalent(&self, key: &PersonRef<'_>) -> bool {
    self.id == key.id && self.name == key.name
  }
}

impl Comparable<PersonRef<'_>> for Person {
  fn compare(&self, key: &PersonRef<'_>) -> core::cmp::Ordering {
    self
      .id
      .cmp(&key.id)
      .then_with(|| self.name.as_str().cmp(key.name))
  }
}

impl Type for Person {
  type Ref<'a> = PersonRef<'a>;
  type Error = dbutils::error::InsufficientBuffer;

  fn encoded_len(&self) -> usize {
    encoded_u64_varint_len(self.id) + self.name.len()
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    let id_size = encode_u64_varint(self.id, buf)?;
    buf[id_size..].copy_from_slice(self.name.as_bytes());
    Ok(id_size + self.name.len())
  }

  #[inline]
  fn encode_to_buffer(
    &self,
    buf: &mut VacantBuffer<'_>,
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

fn main() {
  let dir = tempfile::tempdir().unwrap();
  let path = dir.path().join("zero_copy.wal");

  let people = (0..100)
    .map(|_| {
      let p = Person::random();
      let v = std::format!("My name is {}", p.name);
      (p, v)
    })
    .collect::<Vec<_>>();

  let mut wal = unsafe {
    Builder::new()
      .with_capacity(1024 * 1024)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<OrderWal<ArenaTable<Person, String>>, _>(&path)
      .unwrap()
  };

  // Create 100 readers
  let readers = (0..100).map(|_| wal.reader()).collect::<Vec<_>>();

  let people = Arc::new(people);

  // Spawn 100 threads to read from the wal
  let handles = readers.into_iter().enumerate().map(|(i, reader)| {
    let people = people.clone();
    spawn(move || loop {
      let (person, hello) = people[i].clone();
      let person_ref = PersonRef {
        id: person.id,
        name: &person.name,
      };
      if let Some(p) = reader.get(1, &person) {
        assert_eq!(p.key().id, person.id);
        assert_eq!(p.key().name, person.name);
        assert_eq!(p.value(), hello);
        break;
      }

      if let Some(p) = reader.get(1, &person_ref) {
        assert_eq!(p.key().id, person.id);
        assert_eq!(p.key().name, person.name);
        assert_eq!(p.value(), hello);
        break;
      };
    })
  });

  // Insert 100 people into the wal
  for (p, h) in people.iter() {
    wal.insert(1, p, h).unwrap();
  }

  // Wait for all threads to finish
  for handle in handles {
    handle.join().unwrap();
  }
}
