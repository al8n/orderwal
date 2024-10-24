use dbutils::{
  buffer::VacantBuffer,
  equivalent::{Comparable, Equivalent},
  traits::{KeyRef, MaybeStructured, Type, TypeRef},
};
use rarena_allocator::either::Either;

use crate::{
  memtable::MemtableEntry,
  sealed::{Pointer, WithVersion, WithoutVersion},
};

use super::ty_ref;

impl<'a, T: ?Sized> From<MaybeStructured<'a, T>> for Generic<'a, T> {
  #[inline]
  fn from(value: MaybeStructured<'a, T>) -> Self {
    match value.data() {
      Either::Left(val) => Self {
        data: Either::Left(val),
      },
      Either::Right(val) => Self {
        data: Either::Right(val),
      },
    }
  }
}

impl<'a, T: ?Sized> From<Generic<'a, T>> for MaybeStructured<'a, T> {
  #[inline]
  fn from(value: Generic<'a, T>) -> Self {
    match value.data() {
      Either::Left(val) => Self::from(val),
      Either::Right(val) => unsafe { Self::from_slice(val) },
    }
  }
}

/// A wrapper around a generic type that can be used to construct for insertion.
#[repr(transparent)]
#[derive(Debug)]
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
  pub const fn data(&self) -> Either<&'a T, &'a [u8]> {
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

/// The reference to an entry in the generic WALs.
pub struct Entry<'a, K, V, E>
where
  K: ?Sized + Type,
  V: ?Sized + Type,
{
  ent: E,
  pub(crate) raw_key: &'a [u8],
  key: K::Ref<'a>,
  value: V::Ref<'a>,
  version: Option<u64>,
  query_version: Option<u64>,
}

impl<'a, K, V, E> core::fmt::Debug for Entry<'a, K, V, E>
where
  K: Type + ?Sized,
  K::Ref<'a>: core::fmt::Debug,
  V: Type + ?Sized,
  V::Ref<'a>: core::fmt::Debug,
  E: core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    if let Some(version) = self.version {
      f.debug_struct("Entry")
        .field("key", &self.key())
        .field("value", &self.value())
        .field("version", &version)
        .finish()
    } else {
      f.debug_struct("Entry")
        .field("key", &self.key())
        .field("value", &self.value())
        .finish()
    }
  }
}

impl<'a, K, V, E> Clone for Entry<'a, K, V, E>
where
  K: ?Sized + Type,
  K::Ref<'a>: Clone,
  V: ?Sized + Type,
  V::Ref<'a>: Clone,
  E: Clone,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      raw_key: self.raw_key,
      key: self.key,
      value: self.value,
      version: self.version,
      query_version: self.query_version,
    }
  }
}

impl<'a, K, V, E> Entry<'a, K, V, E>
where
  K: ?Sized + Type,
  V: ?Sized + Type,
  E: MemtableEntry<'a>,
  E::Pointer: Pointer + WithoutVersion,
{
  #[inline]
  pub(super) fn new(ent: E) -> Self {
    Self::with_version_in(ent, None)
  }
}

impl<'a, K, V, E> Entry<'a, K, V, E>
where
  K: ?Sized + Type,
  V: ?Sized + Type,
  E: MemtableEntry<'a>,
  E::Pointer: Pointer + WithVersion,
{
  #[inline]
  pub(super) fn with_version(ent: E, query_version: u64) -> Self {
    Self::with_version_in(ent, Some(query_version))
  }
}

impl<'a, K, V, E> Entry<'a, K, V, E>
where
  K: ?Sized + Type,
  V: ?Sized + Type,
  E: MemtableEntry<'a>,
  E::Pointer: Pointer,
{
  #[inline]
  pub(super) fn with_version_in(ent: E, query_version: Option<u64>) -> Self {
    let ptr = ent.pointer();
    let raw_key = ptr.as_key_slice();
    Self {
      raw_key,
      key: ty_ref::<K>(raw_key),
      value: ty_ref::<V>(ptr.as_value_slice().unwrap()),
      version: if query_version.is_some() {
        Some(ptr.version())
      } else {
        None
      },
      query_version,
      ent,
    }
  }
}

impl<'a, K, V, E> Entry<'a, K, V, E>
where
  K: Type + Ord + ?Sized,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
  V: ?Sized + Type,
  E: MemtableEntry<'a>,
  E::Pointer: Pointer,
{
  /// Returns the next entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  #[allow(clippy::should_implement_trait)]
  pub fn next(&mut self) -> Option<Self> {
    self
      .ent
      .next()
      .map(|ent| Self::with_version_in(ent, self.query_version))
  }

  /// Returns the previous entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    self
      .ent
      .prev()
      .map(|ent| Self::with_version_in(ent, self.query_version))
  }
}

impl<'a, K, V, E> Entry<'a, K, V, E>
where
  K: Type + ?Sized,
  V: ?Sized + Type,
  E: MemtableEntry<'a>,
  E::Pointer: WithVersion,
{
  /// Returns the version of the entry.
  #[inline]
  pub fn version(&self) -> u64 {
    self.version.expect("version must be set")
  }
}

impl<'a, K, V, E> Entry<'a, K, V, E>
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

impl<'a, K, V, E> Entry<'a, K, V, E>
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

/// The reference to a key of the entry in the generic WALs.
pub struct Key<'a, K, E>
where
  K: ?Sized + Type,
{
  ent: E,
  raw_key: &'a [u8],
  key: K::Ref<'a>,
  version: Option<u64>,
  query_version: Option<u64>,
}

impl<'a, K, E> core::fmt::Debug for Key<'a, K, E>
where
  K: Type + ?Sized,
  K::Ref<'a>: core::fmt::Debug,
  E: core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    if let Some(version) = self.version {
      f.debug_struct("Key")
        .field("key", &self.key())
        .field("version", &version)
        .finish()
    } else {
      f.debug_struct("Key").field("key", &self.key()).finish()
    }
  }
}

impl<'a, K, E> Clone for Key<'a, K, E>
where
  K: ?Sized + Type,
  K::Ref<'a>: Clone,
  E: Clone,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      raw_key: self.raw_key,
      key: self.key,
      version: self.version,
      query_version: self.query_version,
    }
  }
}

impl<'a, K, E> Key<'a, K, E>
where
  K: ?Sized + Type,
  E: MemtableEntry<'a>,
  E::Pointer: Pointer,
{
  #[inline]
  pub(super) fn with_version_in(ent: E, query_version: Option<u64>) -> Self {
    let ptr = ent.pointer();
    let raw_key = ptr.as_key_slice();
    Self {
      raw_key,
      key: ty_ref::<K>(raw_key),
      version: if query_version.is_some() {
        Some(ptr.version())
      } else {
        None
      },
      query_version,
      ent,
    }
  }
}

impl<'a, K, E> Key<'a, K, E>
where
  K: Type + Ord + ?Sized,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
  E: MemtableEntry<'a>,
  E::Pointer: Pointer,
{
  /// Returns the next entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  #[allow(clippy::should_implement_trait)]
  pub fn next(&mut self) -> Option<Self> {
    self
      .ent
      .next()
      .map(|ent| Self::with_version_in(ent, self.query_version))
  }

  /// Returns the previous entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    self
      .ent
      .prev()
      .map(|ent| Self::with_version_in(ent, self.query_version))
  }
}

impl<'a, K, E> Key<'a, K, E>
where
  K: Type + ?Sized,
  E: MemtableEntry<'a>,
  E::Pointer: WithVersion,
{
  /// Returns the version of the entry.
  #[inline]
  pub fn version(&self) -> u64 {
    self.version.expect("version must be set")
  }
}

impl<'a, K, E> Key<'a, K, E>
where
  K: Type + ?Sized,
{
  /// Returns the key of the entry.
  #[inline]
  pub const fn key(&self) -> &K::Ref<'a> {
    &self.key
  }
}

/// The reference to a value of the entry in the generic WALs.
pub struct Value<'a, V, E>
where
  V: ?Sized + Type,
{
  ent: E,
  raw_key: &'a [u8],
  value: V::Ref<'a>,
  version: Option<u64>,
  query_version: Option<u64>,
}

impl<'a, V, E> core::fmt::Debug for Value<'a, V, E>
where
  V: Type + ?Sized,
  V::Ref<'a>: core::fmt::Debug,
  E: core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    if let Some(version) = self.version {
      f.debug_struct("Value")
        .field("value", &self.value())
        .field("version", &version)
        .finish()
    } else {
      f.debug_struct("Value")
        .field("value", &self.value())
        .finish()
    }
  }
}

impl<'a, V, E> Clone for Value<'a, V, E>
where
  V: ?Sized + Type,
  V::Ref<'a>: Clone,
  E: Clone,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      raw_key: self.raw_key,
      value: self.value,
      version: self.version,
      query_version: self.query_version,
    }
  }
}

impl<'a, V, E> Value<'a, V, E>
where
  V: ?Sized + Type,
  E: MemtableEntry<'a>,
  E::Pointer: Pointer,
{
  #[inline]
  pub(super) fn with_version_in(ent: E, query_version: Option<u64>) -> Self {
    let ptr = ent.pointer();
    let raw_key = ptr.as_key_slice();
    Self {
      raw_key,
      value: ty_ref::<V>(ptr.as_value_slice().unwrap()),
      version: if query_version.is_some() {
        Some(ptr.version())
      } else {
        None
      },
      query_version,
      ent,
    }
  }
}

impl<'a, V, E> Value<'a, V, E>
where
  V: Type + ?Sized,
  E: MemtableEntry<'a>,
  E::Pointer: Pointer,
{
  /// Returns the next entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  #[allow(clippy::should_implement_trait)]
  pub fn next(&mut self) -> Option<Self> {
    self
      .ent
      .next()
      .map(|ent| Self::with_version_in(ent, self.query_version))
  }

  /// Returns the previous entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    self
      .ent
      .prev()
      .map(|ent| Self::with_version_in(ent, self.query_version))
  }
}

impl<'a, V, E> Value<'a, V, E>
where
  V: Type + ?Sized,
  E: MemtableEntry<'a>,
  E::Pointer: WithVersion,
{
  /// Returns the version of the entry.
  #[inline]
  pub fn version(&self) -> u64 {
    self.version.expect("version must be set")
  }
}

impl<'a, V, E> Value<'a, V, E>
where
  V: Type + ?Sized,
{
  /// Returns the value of the entry.
  #[inline]
  pub const fn value(&self) -> &V::Ref<'a> {
    &self.value
  }
}
