use core::{
  borrow::Borrow,
  cmp,
  marker::PhantomData,
  ops::{Bound, RangeBounds},
};

use among::Among;
use dbutils::{
  buffer::VacantBuffer,
  equivalent::{Comparable, Equivalent},
  traits::{KeyRef, Type, TypeRef},
  CheapClone, Comparator,
};
use rarena_allocator::{either::Either, Allocator};
use ref_cast::RefCast;

use crate::sealed::WithoutVersion;

use super::{
  batch::{Batch, BatchWithBuilders, BatchWithKeyBuilder, BatchWithValueBuilder},
  checksum::BuildChecksumer,
  error::Error,
  sealed::{Base, Constructable, Core, Pointer},
  KeyBuilder, Options, ValueBuilder,
};

mod iter;
pub use iter::{
  GenericIter, GenericKeys, GenericRange, GenericRangeKeys, GenericRangeValues, GenericValues,
};

mod entry;
pub use entry::*;

mod query;
use query::*;

mod pointer;
use pointer::*;

/// An abstract layer for the immutable write-ahead log.
pub trait Reader<K: ?Sized, V: ?Sized, S>: Constructable<GenericComparator<K>, S> {
  /// Returns the reserved space in the WAL.
  ///
  /// ## Safety
  /// - The writer must ensure that the returned slice is not modified.
  /// - This method is not thread-safe, so be careful when using it.
  #[inline]
  unsafe fn reserved_slice(&self) -> &[u8] {
    self.as_core().reserved_slice()
  }

  /// Returns the path of the WAL if it is backed by a file.
  #[inline]
  fn path(
    &self,
  ) -> Option<&<<Self as Constructable<GenericComparator<K>, S>>::Allocator as Allocator>::Path> {
    self.as_core().path()
  }

  /// Returns the number of entries in the WAL.
  #[inline]
  fn len(&self) -> usize {
    self.as_core().len()
  }

  /// Returns `true` if the WAL is empty.
  #[inline]
  fn is_empty(&self) -> bool {
    self.as_core().is_empty()
  }

  /// Returns the maximum key size allowed in the WAL.
  #[inline]
  fn maximum_key_size(&self) -> u32 {
    self.as_core().maximum_key_size()
  }

  /// Returns the maximum value size allowed in the WAL.
  #[inline]
  fn maximum_value_size(&self) -> u32 {
    self.as_core().maximum_value_size()
  }

  /// Returns the remaining capacity of the WAL.
  #[inline]
  fn remaining(&self) -> u32 {
    self.as_core().remaining()
  }

  /// Returns the capacity of the WAL.
  #[inline]
  fn capacity(&self) -> u32 {
    self.as_core().capacity()
  }

  /// Returns the options used to create this WAL instance.
  #[inline]
  fn options(&self) -> &Options {
    self.as_core().options()
  }

  /// Returns an iterator over the entries in the WAL.
  #[inline]
  fn iter(
    &self,
  ) -> GenericIter<
    '_,
    K,
    V,
    <<Self::Core as Core<Self::Pointer, GenericComparator<K>, S>>::Base as Base>::Iterator<'_>,
    Self::Pointer,
  > {
    GenericIter::new(self.as_core().iter(None))
  }

  /// Returns an iterator over a subset of entries in the WAL.
  #[inline]
  fn range<'a, Q, R>(
    &'a self,
    range: R,
  ) -> GenericRange<
    'a,
    K,
    V,
    R,
    Q,
    <Self::Core as Core<Self::Pointer, GenericComparator<K>, S>>::Base,
  >
  where
    R: RangeBounds<Q>,
    K: Type + Ord,
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<Self::Pointer> + Ord,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    GenericRange::new(self.as_core().range(None, GenericQueryRange::new(range)))
  }

  /// Returns an iterator over the keys in the WAL.
  #[inline]
  fn keys(
    &self,
  ) -> GenericKeys<
    '_,
    K,
    <<Self::Core as Core<Self::Pointer, GenericComparator<K>, S>>::Base as Base>::Iterator<'_>,
    Self::Pointer,
  > {
    GenericKeys::new(self.as_core().keys(None))
  }

  /// Returns an iterator over a subset of keys in the WAL.
  #[inline]
  fn range_keys<'a, Q, R>(
    &'a self,
    range: R,
  ) -> GenericRangeKeys<
    'a,
    K,
    R,
    Q,
    <Self::Core as Core<Self::Pointer, GenericComparator<K>, S>>::Base,
  >
  where
    R: RangeBounds<Q>,
    K: Type + Ord,
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<Self::Pointer> + Ord,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    GenericRangeKeys::new(
      self
        .as_core()
        .range_keys(None, GenericQueryRange::new(range)),
    )
  }

  /// Returns an iterator over the values in the WAL.
  #[inline]
  fn values(
    &self,
  ) -> GenericValues<
    '_,
    V,
    <<Self::Core as Core<Self::Pointer, GenericComparator<K>, S>>::Base as Base>::Iterator<'_>,
    Self::Pointer,
  > {
    GenericValues::new(self.as_core().values(None))
  }

  /// Returns an iterator over a subset of values in the WAL.
  #[inline]
  fn range_values<'a, Q, R>(
    &'a self,
    range: R,
  ) -> GenericRangeValues<
    'a,
    K,
    V,
    R,
    Q,
    <Self::Core as Core<Self::Pointer, GenericComparator<K>, S>>::Base,
  >
  where
    R: RangeBounds<Q>,
    K: Type + Ord,
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<Self::Pointer> + Ord,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    GenericRangeValues::new(
      self
        .as_core()
        .range_values(None, GenericQueryRange::new(range)),
    )
  }

  /// Returns the first key-value pair in the map. The key in this pair is the minimum key in the wal.
  #[inline]
  fn first_entry(&self) -> Option<(K::Ref<'_>, V::Ref<'_>)>
  where
    K: Type,
    V: Type,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>> + Ord,
  {
    self.as_core().first(None).map(kv_ref::<K, V>)
  }

  /// Returns the last key-value pair in the map. The key in this pair is the maximum key in the wal.
  #[inline]
  fn last(&self) -> Option<(&[u8], &[u8])>
  where
    K: Type,
    V: Type,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>> + Ord,
  {
    Core::last(self.as_core(), None)
  }

  /// Returns `true` if the key exists in the WAL.
  #[inline]
  fn contains_key<'a, Q>(&'a self, key: &Q) -> bool
  where
    K: Type,
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<Self::Pointer> + Ord,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    self.as_core().contains_key(None, &Query::new(key))
  }

  /// Returns `true` if the key exists in the WAL.
  ///
  /// ## Safety
  /// - The given `key` must be valid to construct to `K::Ref` without remaining.
  #[inline]
  unsafe fn contains_key_by_bytes(&self, key: &[u8]) -> bool
  where
    K: Type,
    for<'a> K::Ref<'a>: KeyRef<'a, K> + Ord,
    Slice<K>: Comparable<Self::Pointer>,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    self.as_core().contains_key(None, Slice::<K>::ref_cast(key))
  }

  /// Gets the value associated with the key.
  #[inline]
  fn get<'a, Q>(&'a self, key: &Q) -> Option<V::Ref<'a>>
  where
    K: Type,
    V: Type,
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<Self::Pointer> + Ord,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    self
      .as_core()
      .get(None, &Query::new(key))
      .map(|p| unsafe { <V::Ref<'_> as TypeRef<'_>>::from_slice(p) })
  }

  /// Returns the key-value pair corresponding to the supplied key.
  ///
  /// The supplied key may be any type which can compare with the WAL's [`K::Ref<'_>`](Type::Ref) type, but the ordering
  /// on the borrowed form *must* match the ordering on the key type.
  #[inline]
  fn get_key_value<'a, Q>(&'a self, key: &Q) -> Option<(K::Ref<'a>, V::Ref<'a>)>
  where
    K: Type,
    V: Type,
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<Self::Pointer> + Ord,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    self
      .as_core()
      .get_entry(None, &Query::new(key))
      .map(kv_ref::<K, V>)
  }

  /// Gets the value associated with the key.
  ///
  /// ## Safety
  /// - The given `key` must be valid to construct to `K::Ref` without remaining.
  #[inline]
  unsafe fn get_by_bytes(&self, key: &[u8]) -> Option<V::Ref<'_>>
  where
    K: Type,
    V: Type,
    for<'a> K::Ref<'a>: KeyRef<'a, K> + Ord,
    Slice<K>: Comparable<Self::Pointer>,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    self
      .as_core()
      .get(None, Slice::<K>::ref_cast(key))
      .map(ty_ref::<V>)
  }

  /// Returns the key-value pair corresponding to the supplied key.
  ///
  /// ## Safety
  /// - The given `key` must be valid to construct to `K::Ref` without remaining.
  #[inline]
  unsafe fn get_key_value_by_bytes(&self, key: &[u8]) -> Option<(K::Ref<'_>, V::Ref<'_>)>
  where
    K: Type,
    V: Type,
    for<'a> K::Ref<'a>: KeyRef<'a, K> + Ord,
    Slice<K>: Comparable<Self::Pointer>,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    self
      .as_core()
      .get_entry(None, Slice::<K>::ref_cast(key))
      .map(kv_ref::<K, V>)
  }

  /// Returns a value associated to the highest element whose key is below the given bound.
  /// If no such element is found then `None` is returned.
  #[inline]
  fn upper_bound<'a, Q>(&'a self, bound: Bound<&Q>) -> Option<V::Ref<'a>>
  where
    K: Type + Ord,
    V: Type,
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<Self::Pointer> + Ord,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    self
      .as_core()
      .upper_bound(None, bound.map(Query::ref_cast))
      .map(ty_ref::<V>)
  }

  /// Returns a value associated to the highest element whose key is below the given bound.
  /// If no such element is found then `None` is returned.
  ///
  /// ## Safety
  /// - The given `key` in `Bound` must be valid to construct to `K::Ref` without remaining.
  #[inline]
  unsafe fn upper_bound_by_bytes(&self, bound: Bound<&[u8]>) -> Option<V::Ref<'_>>
  where
    K: Type,
    V: Type,
    for<'a> K::Ref<'a>: KeyRef<'a, K> + Ord,
    Slice<K>: Comparable<Self::Pointer>,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    self
      .as_core()
      .upper_bound(None, bound.map(Slice::ref_cast))
      .map(ty_ref::<V>)
  }

  /// Returns a value associated to the lowest element whose key is above the given bound.
  /// If no such element is found then `None` is returned.
  #[inline]
  fn lower_bound<'a, Q>(&'a self, bound: Bound<&Q>) -> Option<V::Ref<'a>>
  where
    K: Type + Ord,
    V: Type,
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<Self::Pointer> + Ord,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    self
      .as_core()
      .lower_bound(None, bound.map(Query::ref_cast))
      .map(ty_ref::<V>)
  }

  /// Returns a value associated to the lowest element whose key is above the given bound.
  /// If no such element is found then `None` is returned.
  ///
  /// ## Safety
  /// - The given `key` in `Bound` must be valid to construct to `K::Ref` without remaining.
  #[inline]
  unsafe fn lower_bound_by_bytes(&self, bound: Bound<&[u8]>) -> Option<V::Ref<'_>>
  where
    K: Type,
    V: Type,
    for<'a> K::Ref<'a>: KeyRef<'a, K> + Ord,
    Slice<K>: Comparable<Self::Pointer>,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    self
      .as_core()
      .lower_bound(None, bound.map(Slice::ref_cast))
      .map(ty_ref::<V>)
  }
}

impl<T, K, V, S> Reader<K, V, S> for T
where
  T: Constructable<GenericComparator<K>, S>,
  T::Pointer: WithoutVersion,
  K: ?Sized,
  V: ?Sized,
{
}

/// An abstract layer for the write-ahead log.
pub trait Writer<K: ?Sized, V: ?Sized, S>: Reader<K, V, S> {
  /// The read only reader type for this wal.
  type Reader: Reader<K, V, S, Pointer = Self::Pointer>
  where
    Self::Core: Core<Self::Pointer, GenericComparator<K>, S> + 'static,
    Self::Allocator: 'static;

  /// Returns `true` if this WAL instance is read-only.
  #[inline]
  fn read_only(&self) -> bool {
    self.as_core().read_only()
  }

  /// Returns the mutable reference to the reserved slice.
  ///
  /// ## Safety
  /// - The caller must ensure that the there is no others accessing reserved slice for either read or write.
  /// - This method is not thread-safe, so be careful when using it.
  #[inline]
  unsafe fn reserved_slice_mut<'a>(&'a mut self) -> &'a mut [u8]
  where
    Self::Allocator: 'a,
  {
    self.as_core_mut().reserved_slice_mut()
  }

  /// Flushes the to disk.
  #[inline]
  fn flush(&self) -> Result<(), Error> {
    self.as_core().flush()
  }

  /// Flushes the to disk.
  #[inline]
  fn flush_async(&self) -> Result<(), Error> {
    self.as_core().flush_async()
  }

  /// Returns the read-only view for the WAL.
  fn reader(&self) -> Self::Reader;

  /// Get or insert a new entry into the WAL.
  #[inline]
  fn get_or_insert<'a>(
    &'a mut self,
    key: impl Into<Generic<'a, K>>,
    val: impl Into<Generic<'a, V>>,
  ) -> Result<Option<V::Ref<'a>>, Error>
  where
    K: Type + Ord + for<'b> Comparable<K::Ref<'b>> + 'a,
    for<'b> K::Ref<'b>: KeyRef<'b, K>,
    V: Type + 'a,
    for<'b> Query<'b, K, K>: Comparable<Self::Pointer> + Ord,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>> + Comparable<K>,
  {
    // let key: Generic<'a, K> = key.into();

    // match key.data() {
    //   Either::Left(key) => {
    //     if let Some(val) = self.as_core().get(None, &Query::new(key)) {
    //       return Ok(Some(ty_ref(val)));
    //     }
    //   }
    //   Either::Right(key) => {
    //     if let Some(val) = unsafe { Reader::get(self, ty_ref(key)) } {
    //       return Ok(Some(ty_ref(val)));
    //     }
    //   },
    // }

    // if let Some(val) = base.get(None, Query::new(key)) {
    //   return Ok(Some(val.as_pointer().as_value_slice()));
    // }

    // self
    //   .insert_with_value_builder::<E>(version, key, vb)
    //   .map(|_| None)
    todo!()
  }

  /// Get or insert a new entry into the WAL.
  #[inline]
  fn get_or_insert_with_value_builder<E>(
    &mut self,
    version: u64,
    key: &[u8],
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
  ) -> Result<Option<&[u8]>, Either<E, Error>>
  where
    S: BuildChecksumer,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>> + Borrow<[u8]> + Ord,
  {
    self
      .as_core_mut()
      .get_or_insert_with_value_builder(Some(version), key, vb)
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the key in place.
  ///
  /// See also [`insert_with_value_builder`](Wal::insert_with_value_builder) and [`insert_with_builders`](Wal::insert_with_builders).
  #[inline]
  fn insert_with_key_builder<E>(
    &mut self,
    version: u64,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
    value: &[u8],
  ) -> Result<(), Either<E, Error>>
  where
    S: BuildChecksumer,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>> + Borrow<[u8]> + Ord,
  {
    self
      .as_core_mut()
      .insert_with_key_builder(Some(version), kb, value)
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the value in place.
  ///
  /// See also [`insert_with_key_builder`](Wal::insert_with_key_builder) and [`insert_with_builders`](Wal::insert_with_builders).
  #[inline]
  fn insert_with_value_builder<E>(
    &mut self,
    version: u64,
    key: &[u8],
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
  ) -> Result<(), Either<E, Error>>
  where
    S: BuildChecksumer,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>> + Borrow<[u8]> + Ord,
  {
    self
      .as_core_mut()
      .insert_with_value_builder(Some(version), key, vb)
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the key and value in place.
  #[inline]
  fn insert_with_builders<KE, VE>(
    &mut self,
    version: u64,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), KE>>,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), VE>>,
  ) -> Result<(), Among<KE, VE, Error>>
  where
    S: BuildChecksumer,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>> + Borrow<[u8]> + Ord,
  {
    self
      .as_core_mut()
      .insert_with_builders(Some(version), kb, vb)
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch_with_key_builder<B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Either<B::Error, Error>>
  where
    B: BatchWithKeyBuilder<Self::Pointer>,
    B::Value: Borrow<[u8]>,

    S: BuildChecksumer,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>> + Ord,
  {
    self.as_core_mut().insert_batch_with_key_builder(batch)
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch_with_value_builder<B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Either<B::Error, Error>>
  where
    B: BatchWithValueBuilder<Self::Pointer>,
    B::Key: Borrow<[u8]>,

    S: BuildChecksumer,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>> + Ord,
  {
    self.as_core_mut().insert_batch_with_value_builder(batch)
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch_with_builders<B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Among<B::KeyError, B::ValueError, Error>>
  where
    B: BatchWithBuilders<Self::Pointer>,

    S: BuildChecksumer,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>> + Ord,
  {
    self.as_core_mut().insert_batch_with_builders(batch)
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch<B>(&mut self, batch: &mut B) -> Result<(), Error>
  where
    B: Batch<Pointer = Self::Pointer>,
    B::Key: Borrow<[u8]>,
    B::Value: Borrow<[u8]>,

    S: BuildChecksumer,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>> + Ord,
  {
    self.as_core_mut().insert_batch(batch)
  }

  /// Inserts a key-value pair into the WAL.
  #[inline]
  fn insert(&mut self, version: u64, key: &[u8], value: &[u8]) -> Result<(), Error>
  where
    S: BuildChecksumer,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>> + Ord,
  {
    Core::insert(self.as_core_mut(), Some(version), key, value)
  }
}

#[inline]
fn ty_ref<T: ?Sized + Type>(src: &[u8]) -> T::Ref<'_> {
  unsafe { <T::Ref<'_> as TypeRef<'_>>::from_slice(src) }
}

#[inline]
fn kv_ref<'a, K, V>((k, v): (&'a [u8], &'a [u8])) -> (K::Ref<'a>, V::Ref<'a>)
where
  K: Type + ?Sized,
  V: Type + ?Sized,
{
  (ty_ref::<K>(k), ty_ref::<V>(v))
}
