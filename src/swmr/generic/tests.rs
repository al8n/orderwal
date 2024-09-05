use core::borrow::Borrow;

use super::*;

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Foo {
  a: u32,
  b: u64,
}

struct FooRef<'a> {
  data: &'a [u8],
}

impl<'a> PartialEq for FooRef<'a> {
  fn eq(&self, other: &Self) -> bool {
    self.data == other.data
  }
}

impl<'a> Eq for FooRef<'a> {}

impl<'a> PartialOrd for FooRef<'a> {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<'a> Ord for FooRef<'a> {
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    let a = u32::from_le_bytes(self.data[0..4].try_into().unwrap());
    let b = u64::from_le_bytes(self.data[4..12].try_into().unwrap());
    let other_a = u32::from_le_bytes(other.data[0..4].try_into().unwrap());
    let other_b = u64::from_le_bytes(other.data[4..12].try_into().unwrap());

    Foo { a, b }.cmp(&Foo {
      a: other_a,
      b: other_b,
    })
  }
}

impl Equivalent<Foo> for FooRef<'_> {
  fn equivalent(&self, key: &Foo) -> bool {
    let a = u32::from_be_bytes(self.data[..8].try_into().unwrap());
    let b = u64::from_be_bytes(self.data[8..].try_into().unwrap());
    a == key.a && b == key.b
  }
}

impl Comparable<Foo> for FooRef<'_> {
  fn compare(&self, key: &Foo) -> std::cmp::Ordering {
    let a = u32::from_be_bytes(self.data[..8].try_into().unwrap());
    let b = u64::from_be_bytes(self.data[8..].try_into().unwrap());
    Foo { a, b }.cmp(key)
  }
}

impl Equivalent<FooRef<'_>> for Foo {
  fn equivalent(&self, key: &FooRef<'_>) -> bool {
    let a = u32::from_be_bytes(key.data[..8].try_into().unwrap());
    let b = u64::from_be_bytes(key.data[8..].try_into().unwrap());
    self.a == a && self.b == b
  }
}

impl Comparable<FooRef<'_>> for Foo {
  fn compare(&self, key: &FooRef<'_>) -> std::cmp::Ordering {
    let a = u32::from_be_bytes(key.data[..8].try_into().unwrap());
    let b = u64::from_be_bytes(key.data[8..].try_into().unwrap());
    self.cmp(&Foo { a, b })
  }
}

impl<'a> KeyRef<'a, Foo> for FooRef<'a> {
  fn compare<Q>(&self, a: &Q) -> cmp::Ordering
  where
    Q: ?Sized + Ord + Comparable<Self>,
  {
    Comparable::compare(a, self)
  }

  fn compare_binary(this: &[u8], other: &[u8]) -> cmp::Ordering {
    let a = u32::from_le_bytes(this[0..4].try_into().unwrap());
    let b = u64::from_le_bytes(this[4..12].try_into().unwrap());
    let other_a = u32::from_le_bytes(other[0..4].try_into().unwrap());
    let other_b = u64::from_le_bytes(other[4..12].try_into().unwrap());

    Foo { a, b }.cmp(&Foo {
      a: other_a,
      b: other_b,
    })
  }
}

impl Type for Foo {
  type Ref<'a> = FooRef<'a>;
  type Error = ();

  fn encoded_len(&self) -> usize {
    12
  }

  fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
    buf[0..4].copy_from_slice(&self.a.to_le_bytes());
    buf[4..12].copy_from_slice(&self.b.to_le_bytes());
    Ok(())
  }

  fn from_slice(src: &[u8]) -> Self::Ref<'_> {
    FooRef { data: src }
  }
}

impl<'a> Borrow<[u8]> for FooRef<'a> {
  fn borrow(&self) -> &[u8] {
    self.data
  }
}

#[test]
fn generic_order_wal_flexible_lookup() {
  let wal = GenericOrderWal::<Foo, ()>::new(Options::new().with_capacity(1000));
  assert!(wal
    .get(&FooRef {
      data: &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    })
    .is_none());
  assert!(wal.get(&Foo { a: 0, b: 0 }).is_none());
  assert!(wal
    .get_by_ref([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0].as_slice())
    .is_none());
}
