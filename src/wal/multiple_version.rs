use core::ops::{Bound, RangeBounds};

use among::Among;
use dbutils::{
  buffer::VacantBuffer,
  checksum::BuildChecksumer,
  equivalent::Comparable,
  traits::{KeyRef, MaybeStructured, Type},
};
use rarena_allocator::Allocator;
use ref_cast::RefCast;
use skl::either::Either;

use crate::{
  batch::Batch,
  error::Error,
  memtable::{self, MultipleVersionMemtable},
  sealed::{Constructable, GenericPointer, MultipleVersionWalReader, Pointer, Wal, WithVersion},
  types::{BufWriter, Entry, KeyBuilder, ValueBuilder},
  Options,
};

use super::{iter::*, GenericQueryRange, Query, Slice};

/// An abstract layer for the immutable write-ahead log.
pub trait Reader<K: ?Sized, V: ?Sized>: Constructable<K, V>
where
  <Self::Memtable as memtable::BaseTable>::Pointer: WithVersion + GenericPointer<K, V>,
  Self::Memtable: MultipleVersionMemtable,
{
  /// Returns the reserved space in the WAL.
  ///
  /// ## Safety
  /// - The writer must ensure that the returned slice is not modified.
  /// - This method is not thread-safe, so be careful when using it.
  #[inline]
  unsafe fn reserved_slice(&self) -> &[u8] {
    self.as_wal().reserved_slice()
  }

  /// Returns the path of the WAL if it is backed by a file.
  #[inline]
  fn path(&self) -> Option<&<<Self as Constructable<K, V>>::Allocator as Allocator>::Path> {
    self.as_wal().path()
  }

  /// Returns the maximum key size allowed in the WAL.
  #[inline]
  fn maximum_key_size(&self) -> u32 {
    self.as_wal().maximum_key_size()
  }

  /// Returns the maximum value size allowed in the WAL.
  #[inline]
  fn maximum_value_size(&self) -> u32 {
    self.as_wal().maximum_value_size()
  }

  /// Returns the remaining capacity of the WAL.
  #[inline]
  fn remaining(&self) -> u32 {
    self.as_wal().remaining()
  }

  /// Returns the capacity of the WAL.
  #[inline]
  fn capacity(&self) -> u32 {
    self.as_wal().capacity()
  }

  /// Returns the options used to create this WAL instance.
  #[inline]
  fn options(&self) -> &Options {
    self.as_wal().options()
  }

  /// Returns an iterator over the entries in the WAL.
  #[inline]
  fn iter(
    &self,
    version: u64,
  ) -> Iter<
    '_,
    K,
    V,
    <<Self::Wal as Wal<K, V, Self::Checksumer>>::Memtable as memtable::BaseTable>::Iterator<'_>,
    Self::Memtable,
  >
  where
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + WithVersion,
    Self::Memtable: MultipleVersionMemtable,
  {
    Iter::new(self.as_wal().iter(version))
  }

  /// Returns an iterator over the entries (all versions) in the WAL.
  #[inline]
  fn iter_all_versions(
    &self,
    version: u64,
  ) -> MultipleVersionIter<
    '_,
    K,
    V,
    <<Self::Wal as Wal<K, V, Self::Checksumer>>::Memtable as memtable::MultipleVersionMemtable>::AllIterator<'_>,
    Self::Memtable,
  >
  where
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + WithVersion,
    Self::Memtable: MultipleVersionMemtable,
  {
    MultipleVersionIter::new(self.as_wal().iter_all_versions(version))
  }

  /// Returns an iterator over a subset of entries in the WAL.
  #[inline]
  fn range<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Range<'a, K, V, R, Q, <Self::Wal as Wal<K, V, Self::Checksumer>>::Memtable>
  where
    R: RangeBounds<Q> + 'a,
    K: Type + Ord,
    Q: ?Sized + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<<Self::Memtable as memtable::BaseTable>::Pointer> + Ord,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer,
  {
    Range::new(self.as_wal().range(version, GenericQueryRange::new(range)))
  }

  /// Returns an iterator over a subset of entries (all versions) in the WAL.
  #[inline]
  fn range_all_versions<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> MultipleVersionRange<'a, K, V, R, Q, <Self::Wal as Wal<K, V, Self::Checksumer>>::Memtable>
  where
    R: RangeBounds<Q> + 'a,
    K: Type + Ord,
    Q: ?Sized + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<<Self::Memtable as memtable::BaseTable>::Pointer> + Ord,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer,
  {
    MultipleVersionRange::new(
      self
        .as_wal()
        .range_all_versions(version, GenericQueryRange::new(range)),
    )
  }

  /// Returns an iterator over the keys in the WAL.
  #[inline]
  fn keys(
    &self,
    version: u64,
  ) -> Keys<
    '_,
    K,
    <<Self::Wal as Wal<K, V, Self::Checksumer>>::Memtable as memtable::BaseTable>::Iterator<'_>,
    Self::Memtable,
  >
  where
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer,
  {
    Keys::new(self.as_wal().iter(version))
  }

  /// Returns an iterator over the keys (all versions) in the WAL.
  #[inline]
  fn keys_all_versions(
    &self,
    version: u64,
  ) -> MultipleVersionKeys<
    '_,
    K,
    <<Self::Wal as Wal<K, V, Self::Checksumer>>::Memtable as memtable::MultipleVersionMemtable>::AllIterator<'_>,
    Self::Memtable,
  >
  where
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer,
  {
    MultipleVersionKeys::new(self.as_wal().iter_all_versions(version))
  }

  /// Returns an iterator over a subset of keys in the WAL.
  #[inline]
  fn range_keys<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> RangeKeys<'a, K, R, Q, <Self::Wal as Wal<K, V, Self::Checksumer>>::Memtable>
  where
    R: RangeBounds<Q> + 'a,
    K: Type + Ord,
    Q: ?Sized + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<<Self::Memtable as memtable::BaseTable>::Pointer> + Ord,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer,
  {
    RangeKeys::new(self.as_wal().range(version, GenericQueryRange::new(range)))
  }

  /// Returns an iterator over a subset of keys (all versions) in the WAL.
  #[inline]
  fn range_keys_all_versions<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> MultipleVersionRangeKeys<'a, K, R, Q, <Self::Wal as Wal<K, V, Self::Checksumer>>::Memtable>
  where
    R: RangeBounds<Q> + 'a,
    K: Type + Ord,
    Q: ?Sized + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<<Self::Memtable as memtable::BaseTable>::Pointer> + Ord,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer,
  {
    MultipleVersionRangeKeys::new(
      self
        .as_wal()
        .range_all_versions(version, GenericQueryRange::new(range)),
    )
  }

  /// Returns an iterator over the values in the WAL.
  #[inline]
  fn values(
    &self,
    version: u64,
  ) -> Values<
    '_,
    V,
    <<Self::Wal as Wal<K, V, Self::Checksumer>>::Memtable as memtable::BaseTable>::Iterator<'_>,
    Self::Memtable,
  >
  where
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer,
  {
    Values::new(self.as_wal().iter(version))
  }

  /// Returns an iterator over the values (all versions) in the WAL.
  #[inline]
  fn values_all_versions(
    &self,
    version: u64,
  ) -> MultipleVersionValues<
    '_,
    V,
    <<Self::Wal as Wal<K, V, Self::Checksumer>>::Memtable as memtable::MultipleVersionMemtable>::AllIterator<'_>,
    Self::Memtable,
  >
  where
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer,
  {
    MultipleVersionValues::new(self.as_wal().iter_all_versions(version))
  }

  /// Returns an iterator over a subset of values in the WAL.
  #[inline]
  fn range_values<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> RangeValues<'a, K, V, R, Q, <Self::Wal as Wal<K, V, Self::Checksumer>>::Memtable>
  where
    R: RangeBounds<Q> + 'a,
    K: Type + Ord,
    Q: ?Sized + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<<Self::Memtable as memtable::BaseTable>::Pointer> + Ord,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer,
  {
    RangeValues::new(self.as_wal().range(version, GenericQueryRange::new(range)))
  }

  /// Returns an iterator over a subset of values (all versions) in the WAL.
  #[inline]
  fn range_values_all_versions<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> MultipleVersionRangeValues<
    'a,
    K,
    V,
    R,
    Q,
    <Self::Wal as Wal<K, V, Self::Checksumer>>::Memtable,
  >
  where
    R: RangeBounds<Q> + 'a,
    K: Type + Ord,
    Q: ?Sized + Comparable<K::Ref<'a>>,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer,
  {
    MultipleVersionRangeValues::new(
      self
        .as_wal()
        .range_all_versions(version, GenericQueryRange::new(range)),
    )
  }

  /// Returns the first key-value pair in the map. The key in this pair is the minimum key in the wal.
  #[inline]
  fn first(
    &self,
    version: u64,
  ) -> Option<Entry<'_, K, V, <Self::Memtable as memtable::BaseTable>::Item<'_>>>
  where
    K: Type,
    V: Type,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + Ord,
  {
    self
      .as_wal()
      .first(version)
      .map(|ent| Entry::with_version(ent, version))
  }

  /// Returns the last key-value pair in the map. The key in this pair is the maximum key in the wal.
  #[inline]
  fn last(
    &self,
    version: u64,
  ) -> Option<Entry<'_, K, V, <Self::Memtable as memtable::BaseTable>::Item<'_>>>
  where
    K: Type,
    V: Type,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + Ord,
  {
    MultipleVersionWalReader::last(self.as_wal(), version)
      .map(|ent| Entry::with_version(ent, version))
  }

  /// Returns `true` if the key exists in the WAL.
  #[inline]
  fn contains_key<'a, Q>(&'a self, version: u64, key: &Q) -> bool
  where
    K: Type + 'a,
    Q: ?Sized + Comparable<K::Ref<'a>>,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer,
  {
    self
      .as_wal()
      .contains_key(version, Query::<K, Q>::ref_cast(key))
  }

  /// Returns `true` if the key exists in the WAL.
  ///
  /// ## Safety
  /// - The given `key` must be valid to construct to `K::Ref` without remaining.
  #[inline]
  unsafe fn contains_key_by_bytes(&self, version: u64, key: &[u8]) -> bool
  where
    K: Type,
    for<'a> K::Ref<'a>: KeyRef<'a, K> + Ord,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + WithVersion,
    Self::Memtable: MultipleVersionMemtable,
  {
    self
      .as_wal()
      .contains_key(version, Slice::<K>::ref_cast(key))
  }

  /// Gets the value associated with the key.
  #[inline]
  fn get<'a, Q>(
    &'a self,
    version: u64,
    key: &Q,
  ) -> Option<Entry<'a, K, V, <Self::Memtable as memtable::BaseTable>::Item<'a>>>
  where
    K: Type + 'a,
    V: Type,
    Q: ?Sized + Comparable<K::Ref<'a>>,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer,
  {
    self
      .as_wal()
      .get(version, Query::<K, Q>::ref_cast(key))
      .map(|ent| Entry::with_version(ent, version))
  }

  /// Gets the value associated with the key.
  ///
  /// ## Safety
  /// - The given `key` must be valid to construct to `K::Ref` without remaining.
  #[inline]
  unsafe fn get_by_bytes(
    &self,
    version: u64,
    key: &[u8],
  ) -> Option<Entry<'_, K, V, <Self::Memtable as memtable::BaseTable>::Item<'_>>>
  where
    K: Type,
    V: Type,
    for<'a> K::Ref<'a>: KeyRef<'a, K> + Ord,

    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer,
  {
    self
      .as_wal()
      .get(version, Slice::<K>::ref_cast(key))
      .map(|ent| Entry::with_version(ent, version))
  }

  /// Returns a value associated to the highest element whose key is below the given bound.
  /// If no such element is found then `None` is returned.
  #[inline]
  fn upper_bound<'a, Q>(
    &'a self,
    version: u64,
    bound: Bound<&Q>,
  ) -> Option<Entry<'a, K, V, <Self::Memtable as memtable::BaseTable>::Item<'a>>>
  where
    K: Type + Ord,
    V: Type,
    Q: ?Sized + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<<Self::Memtable as memtable::BaseTable>::Pointer> + Ord,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer,
  {
    self
      .as_wal()
      .upper_bound(version, bound.map(Query::ref_cast))
      .map(|ent| Entry::with_version(ent, version))
  }

  /// Returns a value associated to the highest element whose key is below the given bound.
  /// If no such element is found then `None` is returned.
  ///
  /// ## Safety
  /// - The given `key` in `Bound` must be valid to construct to `K::Ref` without remaining.
  #[inline]
  unsafe fn upper_bound_by_bytes(
    &self,
    version: u64,
    bound: Bound<&[u8]>,
  ) -> Option<Entry<'_, K, V, <Self::Memtable as memtable::BaseTable>::Item<'_>>>
  where
    K: Type,
    V: Type,
    for<'a> K::Ref<'a>: KeyRef<'a, K> + Ord,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer,
  {
    self
      .as_wal()
      .upper_bound(version, bound.map(Slice::<K>::ref_cast))
      .map(|ent| Entry::with_version(ent, version))
  }

  /// Returns a value associated to the lowest element whose key is above the given bound.
  /// If no such element is found then `None` is returned.
  #[inline]
  fn lower_bound<'a, Q>(
    &'a self,
    version: u64,
    bound: Bound<&Q>,
  ) -> Option<Entry<'a, K, V, <Self::Memtable as memtable::BaseTable>::Item<'a>>>
  where
    K: Type + Ord,
    V: Type,
    Q: ?Sized + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<<Self::Memtable as memtable::BaseTable>::Pointer> + Ord,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer,
  {
    self
      .as_wal()
      .lower_bound(version, bound.map(Query::ref_cast))
      .map(|ent| Entry::with_version(ent, version))
  }

  /// Returns a value associated to the lowest element whose key is above the given bound.
  /// If no such element is found then `None` is returned.
  ///
  /// ## Safety
  /// - The given `key` in `Bound` must be valid to construct to `K::Ref` without remaining.
  #[inline]
  unsafe fn lower_bound_by_bytes(
    &self,
    version: u64,
    bound: Bound<&[u8]>,
  ) -> Option<Entry<'_, K, V, <Self::Memtable as memtable::BaseTable>::Item<'_>>>
  where
    K: Type,
    V: Type,
    for<'a> K::Ref<'a>: KeyRef<'a, K> + Ord,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + WithVersion,
    Self::Memtable: MultipleVersionMemtable,
  {
    self
      .as_wal()
      .lower_bound(version, bound.map(Slice::<K>::ref_cast))
      .map(|ent| Entry::with_version(ent, version))
  }
}

impl<T, K, V> Reader<K, V> for T
where
  T: Constructable<K, V>,
  <T::Memtable as memtable::BaseTable>::Pointer: WithVersion + GenericPointer<K, V>,
  T::Memtable: MultipleVersionMemtable,
  K: ?Sized,
  V: ?Sized,
{
}

/// An abstract layer for the write-ahead log.
pub trait Writer<K: ?Sized, V: ?Sized>: Reader<K, V>
where
  <Self::Memtable as memtable::BaseTable>::Pointer: WithVersion + GenericPointer<K, V>,
  Self::Reader: Reader<K, V, Memtable = Self::Memtable>,
  Self::Memtable: MultipleVersionMemtable,
{
  /// Returns `true` if this WAL instance is read-only.
  #[inline]
  fn read_only(&self) -> bool {
    self.as_wal().read_only()
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
    self.as_wal_mut().reserved_slice_mut()
  }

  /// Flushes the to disk.
  #[inline]
  fn flush(&self) -> Result<(), Error<Self::Memtable>> {
    self.as_wal().flush()
  }

  /// Flushes the to disk.
  #[inline]
  fn flush_async(&self) -> Result<(), Error<Self::Memtable>> {
    self.as_wal().flush_async()
  }

  /// Returns the read-only view for the WAL.
  fn reader(&self) -> Self::Reader;

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the key in place.
  ///
  /// See also [`insert_with_value_builder`](Wal::insert_with_value_builder) and [`insert_with_builders`](Wal::insert_with_builders).
  #[inline]
  fn insert_with_key_builder<'a, E>(
    &'a mut self,
    version: u64,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>,
    value: impl Into<MaybeStructured<'a, V>>,
  ) -> Result<(), Among<E, V::Error, Error<Self::Memtable>>>
  where
    K: Type,
    V: Type + 'a,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + Ord + 'static,
  {
    self.as_wal_mut().insert(Some(version), kb, value.into())
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the value in place.
  ///
  /// See also [`insert_with_key_builder`](Wal::insert_with_key_builder) and [`insert_with_builders`](Wal::insert_with_builders).
  #[inline]
  fn insert_with_value_builder<'a, E>(
    &'a mut self,
    version: u64,
    key: impl Into<MaybeStructured<'a, K>>,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>,
  ) -> Result<(), Among<K::Error, E, Error<Self::Memtable>>>
  where
    K: Type + 'a,
    V: Type,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + Ord + 'static,
  {
    self.as_wal_mut().insert(Some(version), key.into(), vb)
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the key and value in place.
  #[inline]
  fn insert_with_builders<KE, VE>(
    &mut self,
    version: u64,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, KE>>,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, VE>>,
  ) -> Result<(), Among<KE, VE, Error<Self::Memtable>>>
  where
    K: Type,
    V: Type,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + Ord + 'static,
  {
    self.as_wal_mut().insert(Some(version), kb, vb)
  }

  /// Inserts a key-value pair into the WAL.
  #[inline]
  fn insert<'a>(
    &mut self,
    version: u64,
    key: impl Into<MaybeStructured<'a, K>>,
    value: impl Into<MaybeStructured<'a, V>>,
  ) -> Result<(), Among<K::Error, V::Error, Error<Self::Memtable>>>
  where
    K: Type + 'a,
    V: Type + 'a,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + Ord + 'static,
  {
    self
      .as_wal_mut()
      .insert(Some(version), key.into(), value.into())
  }

  /// Removes a key-value pair from the WAL. This method
  /// allows the caller to build the key in place.
  #[inline]
  fn remove_with_builder<KE>(
    &mut self,
    version: u64,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, KE>>,
  ) -> Result<(), Either<KE, Error<Self::Memtable>>>
  where
    K: Type,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + Ord + 'static,
  {
    self.as_wal_mut().remove(Some(version), kb)
  }

  /// Removes a key-value pair from the WAL.
  #[inline]
  fn remove<'a>(
    &mut self,
    version: u64,
    key: impl Into<MaybeStructured<'a, K>>,
  ) -> Result<(), Either<K::Error, Error<Self::Memtable>>>
  where
    K: Type + 'a,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + Ord + 'static,
  {
    self.as_wal_mut().remove(Some(version), key.into())
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch<'a, B>(
    &mut self,
    batch: &'a mut B,
  ) -> Result<(), Among<K::Error, V::Error, Error<Self::Memtable>>>
  where
    B: Batch<
      <Self::Memtable as memtable::BaseTable>::Pointer,
      Key = MaybeStructured<'a, K>,
      Value = MaybeStructured<'a, V>,
    >,
    K: Type + 'a,
    V: Type + 'a,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + Ord + 'static,
  {
    self.as_wal_mut().insert_batch::<Self, _>(batch)
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch_with_key_builder<'a, B>(
    &mut self,
    batch: &'a mut B,
  ) -> Result<(), Among<<B::Key as BufWriter>::Error, V::Error, Error<Self::Memtable>>>
  where
    B: Batch<<Self::Memtable as memtable::BaseTable>::Pointer, Value = MaybeStructured<'a, V>>,
    B::Key: BufWriter,
    K: Type + 'a,
    V: Type + 'a,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + Ord + 'static,
  {
    self.as_wal_mut().insert_batch::<Self, _>(batch)
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch_with_value_builder<'a, B>(
    &mut self,
    batch: &'a mut B,
  ) -> Result<(), Among<K::Error, <B::Value as BufWriter>::Error, Error<Self::Memtable>>>
  where
    B: Batch<<Self::Memtable as memtable::BaseTable>::Pointer, Key = MaybeStructured<'a, K>>,
    B::Value: BufWriter,
    K: Type + 'a,
    V: Type + 'a,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + Ord + 'static,
  {
    self.as_wal_mut().insert_batch::<Self, _>(batch)
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch_with_builders<KB, VB, B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Among<KB::Error, VB::Error, Error<Self::Memtable>>>
  where
    B: Batch<<Self::Memtable as memtable::BaseTable>::Pointer, Key = KB, Value = VB>,
    KB: BufWriter,
    VB: BufWriter,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + Ord + 'static,
  {
    self.as_wal_mut().insert_batch::<Self, _>(batch)
  }
}
