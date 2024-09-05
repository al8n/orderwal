use core::{borrow::Borrow, cmp, marker::PhantomData, slice};
use std::sync::Arc;

use crossbeam_skiplist::{Comparable, Equivalent, SkipSet};
use dbutils::{Checksumer, Crc32};
use rarena_allocator::{either::Either, sync::Arena};

use crate::{Options, UnsafeCellChecksumer, KEY_LEN_SIZE, STATUS_SIZE, VALUE_LEN_SIZE};

/// The type trait for limiting the types that can be used as keys and values in the [`GenericOrderWal`].
/// 
/// This trait and its implementors can only be used with the [`GenericOrderWal`] type, otherwise
/// the correctness of the implementations is not guaranteed.
pub trait Type {
  /// The reference type for the type.
  type Ref<'a>;
  /// The error type for encoding the type into a binary format.
  type Error;

  /// Returns the length of the encoded type size.
  fn encoded_len(&self) -> usize;

  /// Encodes the type into a binary slice, you can assume that the buf length is equal to the value returned by [`encoded_len`](Type::encoded_len).
  fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error>;

  /// Creates a reference type from a binary slice, when using it with [`GenericOrderWal`],
  /// you can assume that the slice is the same as the one returned by [`encode`](Type::encode).
  fn from_slice(src: &[u8]) -> Self::Ref<'_>;
}

impl Type for () {
  type Ref<'a> = ();
  type Error = ();

  fn encoded_len(&self) -> usize {
    0
  }

  fn encode(&self, _buf: &mut [u8]) -> Result<(), Self::Error> {
    Ok(())
  }

  fn from_slice(_src: &[u8]) -> Self::Ref<'_> {}
}

impl Type for String {
  type Ref<'a> = &'a str;
  type Error = ();

  fn encoded_len(&self) -> usize {
    self.len()
  }

  fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
    buf.copy_from_slice(self.as_bytes());
    Ok(())
  }

  fn from_slice(src: &[u8]) -> Self::Ref<'_> {
    core::str::from_utf8(src).unwrap()
  }
}

impl<'a> KeyRef<'a> for str {
  type Key = String;

  fn compare<Q>(&self, a: &Q) -> cmp::Ordering
  where
    Q: ?Sized + Ord + Comparable<Self>,
  {
    Comparable::compare(a, self).reverse()
  }

  fn compare_binary(a: &[u8], b: &[u8]) -> cmp::Ordering {
    a.cmp(b)
  }
}

impl Type for Vec<u8> {
  type Ref<'a> = &'a [u8];
  type Error = ();

  fn encoded_len(&self) -> usize {
    self.len()
  }

  fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
    buf.copy_from_slice(self.as_slice());
    Ok(())
  }

  fn from_slice(src: &[u8]) -> Self::Ref<'_> {
    src
  }
}

impl<'a> KeyRef<'a> for [u8] {
  type Key = Vec<u8>;

  fn compare<Q>(&self, a: &Q) -> cmp::Ordering
  where
    Q: ?Sized + Ord + Comparable<Self>,
  {
    Comparable::compare(a, self).reverse()
  }

  fn compare_binary(a: &[u8], b: &[u8]) -> cmp::Ordering {
    a.cmp(b)
  }
}

/// The key reference trait for comparing `K` in the [`GenericOrderWal`].
pub trait KeyRef<'a>: Ord + Comparable<Self::Key> {
  type Key: Type;

  /// Compares with a type `Q` which can be borrowed from [`T::Ref`](Type::Ref).
  fn compare<Q>(&self, a: &Q) -> cmp::Ordering
  where
    Q: ?Sized + Ord + Comparable<Self>;

  /// Compares two binary formats of the `K` directly.
  fn compare_binary(a: &[u8], b: &[u8]) -> cmp::Ordering;
}

struct Pointer<K, V> {
  /// The pointer to the start of the entry.
  ptr: *const u8,
  /// The length of the key.
  key_len: usize,
  /// The length of the value.
  value_len: usize,
  
  cached_key: Option<K>,
  cached_value: Option<V>,
}

impl<K: Type, V> PartialEq for Pointer<K, V> {
  fn eq(&self, other: &Self) -> bool {
    self.as_key_slice() == other.as_key_slice()
  }
}

impl<K: Type, V> Eq for Pointer<K, V> {}

impl<K, V> PartialOrd for Pointer<K, V>
where
  K: Type + Ord,
  for<'a> K::Ref<'a>: KeyRef<'a, Key = K>,
{
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<K, V> Ord for Pointer<K, V>
where
  K: Type + Ord,
  for<'a> K::Ref<'a>: KeyRef<'a, Key = K>,
{
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    <K::Ref<'_> as KeyRef>::compare_binary(self.as_key_slice(), other.as_key_slice())
  }
}

unsafe impl<K, V> Send for Pointer<K, V> {}
unsafe impl<K, V> Sync for Pointer<K, V> {}

impl<K, V> Pointer<K, V> {
  #[inline]
  const fn new(key_len: usize, value_len: usize, ptr: *const u8) -> Self {
    Self {
      ptr,
      key_len,
      value_len,
      cached_key: None,
      cached_value: None,
    }
  }

  #[inline]
  fn with_cached_key(mut self, key: K) -> Self {
    self.cached_key = Some(key);
    self
  }

  #[inline]
  fn with_cached_value(mut self, value: V) -> Self {
    self.cached_value = Some(value);
    self
  }

  #[inline]
  const fn as_key_slice<'a>(&self) -> &'a [u8] {
    if self.key_len == 0 {
      return &[];
    }

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    unsafe { slice::from_raw_parts(self.ptr.add(STATUS_SIZE + KEY_LEN_SIZE), self.key_len) }
  }

  #[inline]
  const fn as_value_slice<'a, 'b: 'a>(&'a self) -> &'b [u8] {
    if self.value_len == 0 {
      return &[];
    }

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    unsafe {
      slice::from_raw_parts(
        self
          .ptr
          .add(STATUS_SIZE + KEY_LEN_SIZE + self.key_len + VALUE_LEN_SIZE),
        self.value_len,
      )
    }
  }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Ref<'a, K, Q: ?Sized> {
  key: &'a Q,
  _k: PhantomData<K>,
}

impl<'a, K, Q: ?Sized> Ref<'a, K, Q> {
  #[inline]
  const fn new(key: &'a Q) -> Self {
    Self {
      key,
      _k: PhantomData,
    }
  }
}


impl<'a, K, Q, V> Equivalent<Pointer<K, V>> for Ref<'a, K, Q>
where
  K: Type + Ord,
  K::Ref<'a>: KeyRef<'a, Key = K>,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
{
  fn equivalent(&self, key: &Pointer<K, V>) -> bool {
    self.compare(key).is_eq()
  }
}

impl<'a, K, Q, V> Comparable<Pointer<K, V>> for Ref<'a, K, Q>
where
  K: Type + Ord,
  K::Ref<'a>: KeyRef<'a, Key = K>,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
{
  fn compare(&self, p: &Pointer<K, V>) -> cmp::Ordering {
    let kr = K::from_slice(p.as_key_slice());
    KeyRef::compare(&kr, self.key)
  }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Owned<'a, K, Q: ?Sized> {
  key: &'a Q,
  _k: PhantomData<K>,
}

impl<'a, K, Q: ?Sized> Owned<'a, K, Q> {
  #[inline]
  const fn new(key: &'a Q) -> Self {
    Self {
      key,
      _k: PhantomData,
    }
  }
}

impl<'a, K, Q, V> Equivalent<Pointer<K, V>> for Owned<'a, K, Q>
where
  K: Type + Ord,
  K::Ref<'a>: KeyRef<'a, Key = K>,
  Q: ?Sized + Ord + Comparable<K> + Comparable<K::Ref<'a>>,
{
  fn equivalent(&self, key: &Pointer<K, V>) -> bool {
    self.compare(key).is_eq()
  }
}

impl<'a, K, Q, V> Comparable<Pointer<K, V>> for Owned<'a, K, Q>
where
  K: Type + Ord,
  K::Ref<'a>: KeyRef<'a, Key = K>,
  Q: ?Sized + Ord + Comparable<K> + Comparable<K::Ref<'a>>,
{
  fn compare(&self, p: &Pointer<K, V>) -> cmp::Ordering {
    match p.cached_key.as_ref() {
      Some(k) => {
        Comparable::compare(self.key, k)
      }
      None => {
        let kr = K::from_slice(p.as_key_slice());
        KeyRef::compare(&kr, self.key).reverse()
      }
    }
  }
}

struct GenericOrderWalCore<K, V, S> {
  arena: Arena,
  map: SkipSet<Pointer<K, V>>,
  opts: Options,
  cks: UnsafeCellChecksumer<S>,
}

/// Generic ordered write-ahead log implementation, which supports structured keys and values.
pub struct GenericOrderWal<K, V, S = Crc32> {
  core: Arc<GenericOrderWalCore<K, V, S>>,
  opts: Options,
}

impl<K, V> GenericOrderWal<K, V> {
  /// Creates a new in-memory write-ahead log backed by an aligned vec with the given capacity and options.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::generic::{GenericOrderWal, Options};
  ///
  /// let wal = GenericOrderWal::new(Options::new(), 100).unwrap();
  /// ```
  pub fn new(opts: Options) -> Self {
    todo!()
  }
}

impl<K, V> GenericOrderWal<K, V>
where
  K: Type + Ord,
  for<'a> K::Ref<'a>: KeyRef<'a, Key = K>,
  V: Type,
{
  fn get<'a, Q>(&self, key: &'a Q) -> Option<Either<&V, V::Ref<'_>>>
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>> + Comparable<K>,
  {
    self.core.map.get::<Owned<K, Q>>(&Owned::new(key));
    todo!()
  }

  fn get_by_ref<Q>(&self, key: &Q) -> Option<Either<&V, V::Ref<'_>>>
  where
    Q: ?Sized + Ord + for<'a> Comparable<K::Ref<'a>>,
  {
    self.core.map.get::<Ref<K, Q>>(&Ref::new(key));
    todo!()
  }
}

#[cfg(test)]
mod tests {
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

      Foo { a, b }.cmp(&Foo { a: other_a, b: other_b })
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

  impl<'a> KeyRef<'a> for FooRef<'a>
  {
    type Key = Foo;

    fn compare<Q>(&self, a: &Q) -> cmp::Ordering
    where
      Q: ?Sized + Ord + Comparable<Self> {
      Comparable::compare(a, self)
    }
  
    fn compare_binary(this: &[u8], other: &[u8]) -> cmp::Ordering {
      let a = u32::from_le_bytes(this[0..4].try_into().unwrap());
      let b = u64::from_le_bytes(this[4..12].try_into().unwrap());
      let other_a = u32::from_le_bytes(other[0..4].try_into().unwrap());
      let other_b = u64::from_le_bytes(other[4..12].try_into().unwrap());

      Foo { a, b }.cmp(&Foo { a: other_a, b: other_b })
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
    assert!(wal.get(&FooRef {
      data: &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    }).is_none());
    assert!(wal.get(&Foo {
      a: 0,
      b: 0,
    }).is_none());
    assert!(wal.get_by_ref([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0].as_slice()).is_none());
  }
}
