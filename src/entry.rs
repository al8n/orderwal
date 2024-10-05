use core::borrow::Borrow;

use crossbeam_skiplist::set::Entry as SetEntry;
use dbutils::{
  buffer::VacantBuffer,
  equivalent::{Comparable, Equivalent},
  traits::{KeyRef, Type, TypeRef},
};
use rarena_allocator::either::Either;

use super::{
  pointer::{GenericPointer, Pointer},
  KeyBuilder, ValueBuilder,
};

pub(crate) struct BatchEncodedEntryMeta {
  /// The output of `merge_lengths(klen, vlen)`
  pub(crate) kvlen: u64,
  /// the length of `encoded_u64_varint(merge_lengths(klen, vlen))`
  pub(crate) kvlen_size: usize,
  pub(crate) klen: usize,
  pub(crate) vlen: usize,
}

impl BatchEncodedEntryMeta {
  #[inline]
  pub(crate) const fn new(klen: usize, vlen: usize, kvlen: u64, kvlen_size: usize) -> Self {
    Self {
      klen,
      vlen,
      kvlen,
      kvlen_size,
    }
  }

  #[inline]
  const fn zero() -> Self {
    Self {
      klen: 0,
      vlen: 0,
      kvlen: 0,
      kvlen_size: 0,
    }
  }
}

/// An entry which can be inserted into the [`Wal`](crate::wal::Wal).
pub struct Entry<K, V, C> {
  pub(crate) key: K,
  pub(crate) value: V,
  pub(crate) pointer: Option<Pointer<C>>,
  pub(crate) meta: BatchEncodedEntryMeta,
}

impl<K, V, C> Entry<K, V, C>
where
  K: Borrow<[u8]>,
  V: Borrow<[u8]>,
{
  /// Returns the length of the value.
  #[inline]
  pub fn key_len(&self) -> usize {
    self.key.borrow().len()
  }

  /// Returns the length of the value.
  #[inline]
  pub fn value_len(&self) -> usize {
    self.value.borrow().len()
  }
}

impl<K, V, C> Entry<K, V, C> {
  /// Creates a new entry.
  #[inline]
  pub const fn new(key: K, value: V) -> Self {
    Self {
      key,
      value,
      pointer: None,
      meta: BatchEncodedEntryMeta::zero(),
    }
  }

  /// Returns the key.
  #[inline]
  pub const fn key(&self) -> &K {
    &self.key
  }

  /// Returns the value.
  #[inline]
  pub const fn value(&self) -> &V {
    &self.value
  }

  /// Consumes the entry and returns the key and value.
  #[inline]
  pub fn into_components(self) -> (K, V) {
    (self.key, self.value)
  }
}

/// An entry builder which can build an [`Entry`] to be inserted into the [`Wal`](crate::wal::Wal).
pub struct EntryWithKeyBuilder<KB, V, P> {
  pub(crate) kb: KeyBuilder<KB>,
  pub(crate) value: V,
  pub(crate) pointer: Option<P>,
  pub(crate) meta: BatchEncodedEntryMeta,
}

impl<KB, V, P> EntryWithKeyBuilder<KB, V, P>
where
  V: Borrow<[u8]>,
{
  /// Returns the length of the value.
  #[inline]
  pub(crate) fn value_len(&self) -> usize {
    self.value.borrow().len()
  }
}

impl<KB, V, C> EntryWithKeyBuilder<KB, V, C> {
  /// Creates a new entry.
  #[inline]
  pub const fn new(kb: KeyBuilder<KB>, value: V) -> Self {
    Self {
      kb,
      value,
      pointer: None,
      meta: BatchEncodedEntryMeta::zero(),
    }
  }

  /// Returns the key.
  #[inline]
  pub const fn key_builder(&self) -> &KeyBuilder<KB> {
    &self.kb
  }

  /// Returns the value.
  #[inline]
  pub const fn value(&self) -> &V {
    &self.value
  }

  /// Returns the length of the key.
  #[inline]
  pub const fn key_len(&self) -> usize {
    self.kb.size() as usize
  }

  /// Consumes the entry and returns the key and value.
  #[inline]
  pub fn into_components(self) -> (KeyBuilder<KB>, V) {
    (self.kb, self.value)
  }
}

/// An entry builder which can build an [`Entry`] to be inserted into the [`Wal`](crate::wal::Wal).
pub struct EntryWithValueBuilder<K, VB, P> {
  pub(crate) key: K,
  pub(crate) vb: ValueBuilder<VB>,
  pub(crate) pointer: Option<P>,
  pub(crate) meta: BatchEncodedEntryMeta,
}

impl<K, VB, C> EntryWithValueBuilder<K, VB, C>
where
  K: Borrow<[u8]>,
{
  /// Returns the length of the key.
  #[inline]
  pub(crate) fn key_len(&self) -> usize {
    self.key.borrow().len()
  }
}

impl<K, VB, P> EntryWithValueBuilder<K, VB, P> {
  /// Creates a new entry.
  #[inline]
  pub const fn new(key: K, vb: ValueBuilder<VB>) -> Self {
    Self {
      key,
      vb,
      pointer: None,
      meta: BatchEncodedEntryMeta::zero(),
    }
  }

  /// Returns the key.
  #[inline]
  pub const fn value_builder(&self) -> &ValueBuilder<VB> {
    &self.vb
  }

  /// Returns the value.
  #[inline]
  pub const fn key(&self) -> &K {
    &self.key
  }

  /// Returns the length of the value.
  #[inline]
  pub const fn value_len(&self) -> usize {
    self.vb.size() as usize
  }

  /// Consumes the entry and returns the key and value.
  #[inline]
  pub fn into_components(self) -> (K, ValueBuilder<VB>) {
    (self.key, self.vb)
  }
}

/// A wrapper around a generic type that can be used to construct a [`GenericEntry`].
#[repr(transparent)]
pub struct Generic<'a, T: ?Sized> {
  data: Either<&'a T, &'a [u8]>,
}

impl<'a, T: 'a> PartialEq<T> for Generic<'a, T>
where
  T: ?Sized + PartialEq + Type + for<'b> Equivalent<T::Ref<'b>>,
{
  #[inline]
  fn eq(&self, other: &T) -> bool {
    match &self.data {
      Either::Left(val) => (*val).eq(other),
      Either::Right(val) => {
        let ref_ = unsafe { <T::Ref<'_> as TypeRef<'_>>::from_slice(val) };
        other.equivalent(&ref_)
      }
    }
  }
}

impl<'a, T: 'a> PartialEq for Generic<'a, T>
where
  T: ?Sized + PartialEq + Type + for<'b> Equivalent<T::Ref<'b>>,
{
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    match (&self.data, &other.data) {
      (Either::Left(val), Either::Left(other_val)) => val.eq(other_val),
      (Either::Right(val), Either::Right(other_val)) => val.eq(other_val),
      (Either::Left(val), Either::Right(other_val)) => {
        let ref_ = unsafe { <T::Ref<'_> as TypeRef<'_>>::from_slice(other_val) };
        val.equivalent(&ref_)
      }
      (Either::Right(val), Either::Left(other_val)) => {
        let ref_ = unsafe { <T::Ref<'_> as TypeRef<'_>>::from_slice(val) };
        other_val.equivalent(&ref_)
      }
    }
  }
}

impl<'a, T: 'a> Eq for Generic<'a, T> where T: ?Sized + Eq + Type + for<'b> Equivalent<T::Ref<'b>> {}

impl<'a, T: 'a> PartialOrd for Generic<'a, T>
where
  T: ?Sized + Ord + Type + for<'b> Comparable<T::Ref<'b>>,
  for<'b> T::Ref<'b>: Comparable<T> + Ord,
{
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<'a, T: 'a> PartialOrd<T> for Generic<'a, T>
where
  T: ?Sized + PartialOrd + Type + for<'b> Comparable<T::Ref<'b>>,
{
  #[inline]
  fn partial_cmp(&self, other: &T) -> Option<core::cmp::Ordering> {
    match &self.data {
      Either::Left(val) => (*val).partial_cmp(other),
      Either::Right(val) => {
        let ref_ = unsafe { <T::Ref<'_> as TypeRef<'_>>::from_slice(val) };
        Some(other.compare(&ref_).reverse())
      }
    }
  }
}

impl<'a, T: 'a> Ord for Generic<'a, T>
where
  T: ?Sized + Ord + Type + for<'b> Comparable<T::Ref<'b>>,
  for<'b> T::Ref<'b>: Comparable<T> + Ord,
{
  #[inline]
  fn cmp(&self, other: &Self) -> core::cmp::Ordering {
    match (&self.data, &other.data) {
      (Either::Left(val), Either::Left(other_val)) => (*val).cmp(other_val),
      (Either::Right(val), Either::Right(other_val)) => {
        let this = unsafe { <T::Ref<'_> as TypeRef<'_>>::from_slice(val) };
        let other = unsafe { <T::Ref<'_> as TypeRef<'_>>::from_slice(other_val) };
        this.cmp(&other)
      }
      (Either::Left(val), Either::Right(other_val)) => {
        let other = unsafe { <T::Ref<'_> as TypeRef<'_>>::from_slice(other_val) };
        other.compare(*val).reverse()
      }
      (Either::Right(val), Either::Left(other_val)) => {
        let this = unsafe { <T::Ref<'_> as TypeRef<'_>>::from_slice(val) };
        this.compare(*other_val)
      }
    }
  }
}

impl<'a, T: 'a + Type + ?Sized> Generic<'a, T> {
  /// Returns the encoded length.
  #[inline]
  pub fn encoded_len(&self) -> usize {
    match &self.data {
      Either::Left(val) => val.encoded_len(),
      Either::Right(val) => val.len(),
    }
  }

  /// Encodes the generic into the buffer.
  ///
  /// ## Panics
  /// - if the buffer is not large enough.
  #[inline]
  pub fn encode(&self, buf: &mut [u8]) -> Result<usize, T::Error> {
    match &self.data {
      Either::Left(val) => val.encode(buf),
      Either::Right(val) => {
        buf.copy_from_slice(val);
        Ok(buf.len())
      }
    }
  }

  /// Encodes the generic into the given buffer.
  ///
  /// ## Panics
  /// - if the buffer is not large enough.
  #[inline]
  pub fn encode_to_buffer(&self, buf: &mut VacantBuffer<'_>) -> Result<usize, T::Error> {
    match &self.data {
      Either::Left(val) => val.encode_to_buffer(buf),
      Either::Right(val) => {
        buf.put_slice_unchecked(val);
        Ok(buf.len())
      }
    }
  }
}

impl<'a, T: 'a + ?Sized> Generic<'a, T> {
  /// Returns the value contained in the generic.
  #[inline]
  pub const fn data(&self) -> Either<&T, &'a [u8]> {
    self.data
  }

  /// Creates a new generic from bytes for querying or inserting into the [`GenericOrderWal`](crate::swmr::GenericOrderWal).
  ///
  /// ## Safety
  /// - the `slice` must the same as the one returned by [`T::encode`](Type::encode).
  #[inline]
  pub const unsafe fn from_slice(slice: &'a [u8]) -> Self {
    Self {
      data: Either::Right(slice),
    }
  }
}

impl<'a, T: 'a + ?Sized> From<&'a T> for Generic<'a, T> {
  #[inline]
  fn from(value: &'a T) -> Self {
    Self {
      data: Either::Left(value),
    }
  }
}

/// An entry in the [`GenericOrderWal`](crate::swmr::GenericOrderWal).
pub struct GenericEntry<'a, K: ?Sized, V: ?Sized> {
  pub(crate) key: Generic<'a, K>,
  pub(crate) value: Generic<'a, V>,
  pub(crate) pointer: Option<GenericPointer<K, V>>,
  pub(crate) meta: BatchEncodedEntryMeta,
}

impl<'a, K: ?Sized, V: ?Sized> GenericEntry<'a, K, V> {
  /// Creates a new entry.
  #[inline]
  pub fn new(key: impl Into<Generic<'a, K>>, value: impl Into<Generic<'a, V>>) -> Self {
    Self {
      key: key.into(),
      value: value.into(),
      pointer: None,
      meta: BatchEncodedEntryMeta::zero(),
    }
  }

  /// Returns the length of the key.
  #[inline]
  pub fn key_len(&self) -> usize
  where
    K: Type,
  {
    match self.key.data() {
      Either::Left(val) => val.encoded_len(),
      Either::Right(val) => val.len(),
    }
  }

  /// Returns the length of the value.
  #[inline]
  pub fn value_len(&self) -> usize
  where
    V: Type,
  {
    match self.value.data() {
      Either::Left(val) => val.encoded_len(),
      Either::Right(val) => val.len(),
    }
  }

  /// Returns the key.
  #[inline]
  pub const fn key(&self) -> Either<&K, &[u8]> {
    self.key.data()
  }

  /// Returns the value.
  #[inline]
  pub const fn value(&self) -> Either<&V, &[u8]> {
    self.value.data()
  }

  /// Consumes the entry and returns the key and value.
  #[inline]
  pub fn into_components(self) -> (Generic<'a, K>, Generic<'a, V>) {
    (self.key, self.value)
  }
}

/// An entry builder which can build an [`GenericEntry`] to be inserted into the [`GenericOrderWal`](crate::swmr::generic::GenericOrderWal).
pub struct EntryWithBuilders<KB, VB, P> {
  pub(crate) kb: KeyBuilder<KB>,
  pub(crate) vb: ValueBuilder<VB>,
  pub(crate) pointer: Option<P>,
  pub(crate) meta: BatchEncodedEntryMeta,
}

impl<KB, VB, P> EntryWithBuilders<KB, VB, P> {
  /// Creates a new entry.
  #[inline]
  pub const fn new(kb: KeyBuilder<KB>, vb: ValueBuilder<VB>) -> Self {
    Self {
      kb,
      vb,
      pointer: None,
      meta: BatchEncodedEntryMeta::zero(),
    }
  }

  /// Returns the value builder.
  #[inline]
  pub const fn value_builder(&self) -> &ValueBuilder<VB> {
    &self.vb
  }

  /// Returns the key builder.
  #[inline]
  pub const fn key_builder(&self) -> &KeyBuilder<KB> {
    &self.kb
  }

  /// Returns the length of the key.
  #[inline]
  pub const fn key_len(&self) -> usize {
    self.kb.size() as usize
  }

  /// Returns the length of the value.
  #[inline]
  pub const fn value_len(&self) -> usize {
    self.vb.size() as usize
  }

  /// Consumes the entry and returns the key and value.
  #[inline]
  pub fn into_components(self) -> (KeyBuilder<KB>, ValueBuilder<VB>) {
    (self.kb, self.vb)
  }
}

/// The reference to an entry in the [`GenericOrderWal`](crate::swmr::GenericOrderWal).
pub struct GenericEntryRef<'a, K, V>
where
  K: ?Sized + Type,
  V: ?Sized + Type,
{
  ent: SetEntry<'a, GenericPointer<K, V>>,
  key: K::Ref<'a>,
  value: V::Ref<'a>,
}

impl<'a, K, V> core::fmt::Debug for GenericEntryRef<'a, K, V>
where
  K: Type + ?Sized,
  K::Ref<'a>: core::fmt::Debug,
  V: Type + ?Sized,
  V::Ref<'a>: core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("GenericEntryRef")
      .field("key", &self.key())
      .field("value", &self.value())
      .finish()
  }
}

impl<'a, K, V> Clone for GenericEntryRef<'a, K, V>
where
  K: ?Sized + Type,
  K::Ref<'a>: Clone,
  V: ?Sized + Type,
  V::Ref<'a>: Clone,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      key: self.key.clone(),
      value: self.value.clone(),
    }
  }
}

impl<'a, K, V> GenericEntryRef<'a, K, V>
where
  K: ?Sized + Type,
  V: ?Sized + Type,
{
  #[inline]
  pub(super) fn new(ent: SetEntry<'a, GenericPointer<K, V>>) -> Self {
    Self {
      key: unsafe { TypeRef::from_slice(ent.value().as_key_slice()) },
      value: unsafe { TypeRef::from_slice(ent.value().as_value_slice()) },
      ent,
    }
  }
}

impl<K, V> GenericEntryRef<'_, K, V>
where
  K: Type + Ord + ?Sized,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
  V: ?Sized + Type,
{
  /// Returns the next entry in the [`GenericOrderWal`](crate::swmr::GenericOrderWal).
  ///
  /// This does not move the cursor.
  #[inline]
  #[allow(clippy::should_implement_trait)]
  pub fn next(&self) -> Option<Self> {
    self.ent.next().map(Self::new)
  }

  /// Returns the previous entry in the [`GenericOrderWal`](crate::swmr::GenericOrderWal).
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&self) -> Option<Self> {
    self.ent.prev().map(Self::new)
  }
}

impl<'a, K, V> GenericEntryRef<'a, K, V>
where
  K: ?Sized + Type,
  V: Type + ?Sized,
{
  /// Returns the value of the entry.
  #[inline]
  pub const fn value(&self) -> &V::Ref<'a> {
    &self.value
  }
}

impl<'a, K, V> GenericEntryRef<'a, K, V>
where
  K: Type + ?Sized,
  V: ?Sized + Type,
{
  /// Returns the key of the entry.
  #[inline]
  pub const fn key(&self) -> &K::Ref<'a> {
    &self.key
  }
}
