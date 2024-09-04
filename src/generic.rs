use core::{borrow::Borrow, cmp, marker::PhantomData, slice};
use std::sync::Arc;

use crossbeam_skiplist::{Comparable, Equivalent, SkipMap};
use dbutils::{Checksumer, Crc32};
use rarena_allocator::{either::Either, sync::Arena};

use crate::{Options, UnsafeCellChecksumer, KEY_LEN_SIZE, STATUS_SIZE, VALUE_LEN_SIZE};

// /// Generic comparator for the [`GenericOrderWal`].
// pub trait GenericComparator<T>
// where
//   T: Type + Ord,
//   for<'a> T::Ref<'a>: Ord,
// {
  
// }

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

/// The key reference trait for comparing `K` in the [`GenericOrderWal`].
pub trait KeyRef<K>: Ord {
  /// Compares with a type `Q` which can be borrowed from `T`.
  fn compare<Q>(&self, a: &Q) -> cmp::Ordering
  where
    Q: ?Sized + Ord + Comparable<K>;

  /// Compares with a type `Q` which can be borrowed from [`T::Ref`](Type::Ref).
  fn compare_by_ref<Q>(&self, a: &Q) -> cmp::Ordering
  where
    Q: ?Sized + Ord + for<'a> Comparable<Self>;

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
  _m: PhantomData<(K, V)>,
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
  for<'a> K::Ref<'a>: KeyRef<K>,
{
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<K, V> Ord for Pointer<K, V>
where
  K: Type + Ord,
  for<'a> K::Ref<'a>: KeyRef<K>,
{
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    <K::Ref<'_> as KeyRef<K>>::compare_binary(self.as_key_slice(), other.as_key_slice())
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
      _m: PhantomData,
    }
  }

  #[inline]
  const fn as_key_slice(&self) -> &[u8] {
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
  for<'b> K::Ref<'b>: KeyRef<K>,
  Q: ?Sized + Ord + for<'b> Comparable<K::Ref<'b>>,
{
  fn equivalent(&self, key: &Pointer<K, V>) -> bool {
    self.compare(key).is_eq()
  }
}

impl<'a, K, Q, V> Comparable<Pointer<K, V>> for Ref<'a, K, Q>
where
  K: Type + Ord,
  for<'b> K::Ref<'b>: KeyRef<K>,
  Q: ?Sized + Ord + for<'b> Comparable<K::Ref<'b>>,
{
  fn compare(&self, key: &Pointer<K, V>) -> core::cmp::Ordering {
    let kr = K::from_slice(key.as_key_slice());
    kr.compare_by_ref(self.key).reverse()
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
  for<'b> K::Ref<'b>: KeyRef<K>,
  Q: ?Sized + Ord + Comparable<K>,
{
  fn equivalent(&self, key: &Pointer<K, V>) -> bool {
    self.compare(key).is_eq()
  }
}

impl<'a, K, Q, V> Comparable<Pointer<K, V>> for Owned<'a, K, Q>
where
  K: Type + Ord,
  for<'b> K::Ref<'b>: KeyRef<K>,
  Q: ?Sized + Ord + Comparable<K>,
{
  fn compare(&self, key: &Pointer<K, V>) -> core::cmp::Ordering {
    let kr = K::from_slice(key.as_key_slice());
    KeyRef::compare(&kr, self.key).reverse()
  }
}

struct GenericOrderWalCore<K, V, S> {
  arena: Arena,
  map: SkipMap<Pointer<K, V>, Option<V>>,
  opts: Options,
  cks: UnsafeCellChecksumer<S>,
}

/// Generic ordered write-ahead log implementation, which supports structured keys and values.
pub struct GenericOrderWal<K, V, S = Crc32> {
  core: Arc<GenericOrderWalCore<K, V, S>>,
  opts: Options,
  cache_key: bool,
  cache_value: bool,
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
  for<'a> K::Ref<'a>: KeyRef<K>,
  V: Type,
{
  fn get<Q>(&self, key: &Q) -> Option<Either<&V, V::Ref<'_>>>
  where
    Q: ?Sized + Ord + Comparable<K>,
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

