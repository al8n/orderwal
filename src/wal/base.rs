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
  memtable::{self, Memtable},
  sealed::{Constructable, GenericPointer, Pointer, Wal, WalReader, WithoutVersion},
  types::{BufWriter, Entry, KeyBuilder, ValueBuilder},
  Options,
};

use super::{iter::*, GenericQueryRange, Query, Slice};

/// An abstract layer for the immutable write-ahead log.
pub trait Reader<K: ?Sized, V: ?Sized>: Constructable<K, V>
where
  <Self::Memtable as memtable::BaseTable>::Pointer: WithoutVersion + GenericPointer<K, V>,
  Self::Memtable: Memtable,
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

  /// Returns the number of entries in the WAL.
  #[inline]
  fn len(&self) -> usize {
    self.as_wal().len()
  }

  /// Returns `true` if the WAL is empty.
  #[inline]
  fn is_empty(&self) -> bool {
    self.as_wal().is_empty()
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
  ) -> Iter<
    '_,
    K,
    V,
    <<Self::Wal as Wal<K, V, Self::Checksumer>>::Memtable as memtable::BaseTable>::Iterator<'_>,
    Self::Memtable,
  >
  where
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + WithoutVersion,
    Self::Memtable: Memtable,
  {
    Iter::new(self.as_wal().iter())
  }

  /// Returns an iterator over a subset of entries in the WAL.
  #[inline]
  fn range<'a, Q, R>(
    &'a self,
    range: R,
  ) -> Range<'a, K, V, R, Q, <Self::Wal as Wal<K, V, Self::Checksumer>>::Memtable>
  where
    R: RangeBounds<Q> + 'a,
    K: Type + Ord,
    Q: ?Sized + Comparable<K::Ref<'a>>,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + WithoutVersion,
    Self::Memtable: Memtable,
  {
    Range::new(self.as_wal().range(GenericQueryRange::new(range)))
  }

  /// Returns an iterator over the keys in the WAL.
  #[inline]
  fn keys(
    &self,
  ) -> Keys<
    '_,
    K,
    <<Self::Wal as Wal<K, V, Self::Checksumer>>::Memtable as memtable::BaseTable>::Iterator<'_>,
    Self::Memtable,
  >
  where
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + WithoutVersion,
    Self::Memtable: Memtable,
  {
    Keys::new(self.as_wal().iter())
  }

  /// Returns an iterator over a subset of keys in the WAL.
  #[inline]
  fn range_keys<'a, Q, R>(
    &'a self,
    range: R,
  ) -> RangeKeys<'a, K, R, Q, <Self::Wal as Wal<K, V, Self::Checksumer>>::Memtable>
  where
    R: RangeBounds<Q> + 'a,
    K: Type + Ord,
    Q: ?Sized + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<<Self::Memtable as memtable::BaseTable>::Pointer> + Ord,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + WithoutVersion,
    Self::Memtable: Memtable,
  {
    RangeKeys::new(self.as_wal().range(GenericQueryRange::new(range)))
  }

  /// Returns an iterator over the values in the WAL.
  #[inline]
  fn values(
    &self,
  ) -> Values<
    '_,
    V,
    <<Self::Wal as Wal<K, V, Self::Checksumer>>::Memtable as memtable::BaseTable>::Iterator<'_>,
    Self::Memtable,
  >
  where
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + WithoutVersion,
    Self::Memtable: Memtable,
  {
    Values::new(self.as_wal().iter())
  }

  /// Returns an iterator over a subset of values in the WAL.
  #[inline]
  fn range_values<'a, Q, R>(
    &'a self,
    range: R,
  ) -> RangeValues<'a, K, V, R, Q, <Self::Wal as Wal<K, V, Self::Checksumer>>::Memtable>
  where
    R: RangeBounds<Q> + 'a,
    K: Type + Ord,
    Q: ?Sized + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<<Self::Memtable as memtable::BaseTable>::Pointer> + Ord,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + WithoutVersion,
    Self::Memtable: Memtable,
  {
    RangeValues::new(self.as_wal().range(GenericQueryRange::new(range)))
  }

  /// Returns the first key-value pair in the map. The key in this pair is the minimum key in the wal.
  #[inline]
  fn first(&self) -> Option<Entry<'_, K, V, <Self::Memtable as memtable::BaseTable>::Item<'_>>>
  where
    K: Type,
    V: Type,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + Ord + WithoutVersion,
    Self::Memtable: Memtable,
  {
    self.as_wal().first().map(Entry::new)
  }

  /// Returns the last key-value pair in the map. The key in this pair is the maximum key in the wal.
  #[inline]
  fn last(&self) -> Option<Entry<'_, K, V, <Self::Memtable as memtable::BaseTable>::Item<'_>>>
  where
    K: Type,
    V: Type,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + Ord + WithoutVersion,
    Self::Memtable: Memtable,
  {
    WalReader::last(self.as_wal()).map(Entry::new)
  }

  /// Returns `true` if the key exists in the WAL.
  #[inline]
  fn contains_key<'a, Q>(&'a self, key: &Q) -> bool
  where
    K: Type + 'a,
    Q: ?Sized + Comparable<K::Ref<'a>>,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + WithoutVersion,
    Self::Memtable: Memtable,
  {
    self.as_wal().contains_key(Query::<K, Q>::ref_cast(key))
  }

  /// Returns `true` if the key exists in the WAL.
  ///
  /// ## Safety
  /// - The given `key` must be valid to construct to `K::Ref` without remaining.
  #[inline]
  unsafe fn contains_key_by_bytes(&self, key: &[u8]) -> bool
  where
    K: Type,
    for<'a> K::Ref<'a>: KeyRef<'a, K>,

    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + WithoutVersion,
    Self::Memtable: Memtable,
  {
    self.as_wal().contains_key(Slice::<K>::ref_cast(key))
  }

  /// Gets the value associated with the key.
  #[inline]
  fn get<'a, Q>(
    &'a self,
    key: &Q,
  ) -> Option<Entry<'a, K, V, <Self::Memtable as memtable::BaseTable>::Item<'a>>>
  where
    K: Type + 'a,
    V: Type,
    Q: ?Sized + Comparable<K::Ref<'a>>,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + WithoutVersion,
    Self::Memtable: Memtable,
  {
    self
      .as_wal()
      .get(Query::<K, Q>::ref_cast(key))
      .map(Entry::new)
  }

  /// Gets the value associated with the key.
  ///
  /// ## Safety
  /// - The given `key` must be valid to construct to `K::Ref` without remaining.
  #[inline]
  unsafe fn get_by_bytes<'a>(
    &'a self,
    key: &[u8],
  ) -> Option<Entry<'a, K, V, <Self::Memtable as memtable::BaseTable>::Item<'a>>>
  where
    K: Type,
    V: Type,
    for<'b> K::Ref<'b>: KeyRef<'b, K> + Ord,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + WithoutVersion,
    Self::Memtable: Memtable,
  {
    self.as_wal().get(Slice::<K>::ref_cast(key)).map(Entry::new)
  }

  /// Returns a value associated to the highest element whose key is below the given bound.
  /// If no such element is found then `None` is returned.
  #[inline]
  fn upper_bound<'a, Q>(
    &'a self,
    bound: Bound<&Q>,
  ) -> Option<Entry<'a, K, V, <Self::Memtable as memtable::BaseTable>::Item<'a>>>
  where
    K: Type + 'a,
    V: Type,
    Q: ?Sized + Comparable<K::Ref<'a>>,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + WithoutVersion,
    Self::Memtable: Memtable,
  {
    self
      .as_wal()
      .upper_bound(bound.map(Query::<K, Q>::ref_cast))
      .map(Entry::new)
  }

  /// Returns a value associated to the highest element whose key is below the given bound.
  /// If no such element is found then `None` is returned.
  ///
  /// ## Safety
  /// - The given `key` in `Bound` must be valid to construct to `K::Ref` without remaining.
  #[inline]
  unsafe fn upper_bound_by_bytes<'a>(
    &'a self,
    bound: Bound<&[u8]>,
  ) -> Option<Entry<'a, K, V, <Self::Memtable as memtable::BaseTable>::Item<'a>>>
  where
    K: Type + 'a,
    V: Type,
    for<'b> K::Ref<'b>: KeyRef<'b, K> + Ord,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + WithoutVersion,
    Self::Memtable: Memtable,
  {
    self
      .as_wal()
      .upper_bound(bound.map(Slice::<K>::ref_cast))
      .map(Entry::new)
  }

  /// Returns a value associated to the lowest element whose key is above the given bound.
  /// If no such element is found then `None` is returned.
  #[inline]
  fn lower_bound<'a, Q>(
    &'a self,
    bound: Bound<&Q>,
  ) -> Option<Entry<'a, K, V, <Self::Memtable as memtable::BaseTable>::Item<'a>>>
  where
    K: Type + 'a,
    V: Type,
    Q: ?Sized + Comparable<K::Ref<'a>>,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + WithoutVersion,
    Self::Memtable: Memtable,
  {
    self
      .as_wal()
      .lower_bound(bound.map(Query::<K, Q>::ref_cast))
      .map(Entry::new)
  }

  /// Returns a value associated to the lowest element whose key is above the given bound.
  /// If no such element is found then `None` is returned.
  ///
  /// ## Safety
  /// - The given `key` in `Bound` must be valid to construct to `K::Ref` without remaining.
  #[inline]
  unsafe fn lower_bound_by_bytes<'a>(
    &'a self,
    bound: Bound<&[u8]>,
  ) -> Option<Entry<'a, K, V, <Self::Memtable as memtable::BaseTable>::Item<'a>>>
  where
    K: Type,
    V: Type,
    for<'b> K::Ref<'b>: KeyRef<'b, K> + Ord,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + WithoutVersion,
    Self::Memtable: Memtable,
  {
    self
      .as_wal()
      .lower_bound(bound.map(Slice::<K>::ref_cast))
      .map(Entry::new)
  }
}

impl<T, K, V> Reader<K, V> for T
where
  T: Constructable<K, V>,
  <T::Memtable as memtable::BaseTable>::Pointer: WithoutVersion + GenericPointer<K, V>,
  T::Memtable: Memtable,
  K: ?Sized,
  V: ?Sized,
{
}

/// An abstract layer for the write-ahead log.
pub trait Writer<K: ?Sized, V: ?Sized>: Reader<K, V>
where
  <Self::Memtable as memtable::BaseTable>::Pointer: WithoutVersion + GenericPointer<K, V>,
  Self::Reader: Reader<K, V, Memtable = Self::Memtable>,
  Self::Memtable: Memtable,
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
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>,
    value: impl Into<MaybeStructured<'a, V>>,
  ) -> Result<(), Among<E, V::Error, Error<Self::Memtable>>>
  where
    K: Type,
    V: Type + 'a,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + Ord + 'static,
  {
    self.as_wal_mut().insert(None, kb, value.into())
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the value in place.
  ///
  /// See also [`insert_with_key_builder`](Wal::insert_with_key_builder) and [`insert_with_builders`](Wal::insert_with_builders).
  #[inline]
  fn insert_with_value_builder<'a, E>(
    &'a mut self,
    key: impl Into<MaybeStructured<'a, K>>,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>,
  ) -> Result<(), Among<K::Error, E, Error<Self::Memtable>>>
  where
    K: Type + 'a,
    V: Type,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + Ord + 'static,
  {
    self.as_wal_mut().insert(None, key.into(), vb)
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the key and value in place.
  #[inline]
  fn insert_with_builders<KE, VE>(
    &mut self,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, KE>>,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, VE>>,
  ) -> Result<(), Among<KE, VE, Error<Self::Memtable>>>
  where
    K: Type,
    V: Type,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + Ord + 'static,
  {
    self.as_wal_mut().insert(None, kb, vb)
  }

  /// Inserts a key-value pair into the WAL.
  #[inline]
  fn insert<'a>(
    &mut self,
    key: impl Into<MaybeStructured<'a, K>>,
    value: impl Into<MaybeStructured<'a, V>>,
  ) -> Result<(), Among<K::Error, V::Error, Error<Self::Memtable>>>
  where
    K: Type + 'a,
    V: Type + 'a,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + Ord + 'static,
  {
    self.as_wal_mut().insert(None, key.into(), value.into())
  }

  /// Removes a key-value pair from the WAL. This method
  /// allows the caller to build the key in place.
  #[inline]
  fn remove_with_builder<KE>(
    &mut self,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, KE>>,
  ) -> Result<(), Either<KE, Error<Self::Memtable>>>
  where
    K: Type,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + Ord + 'static,
  {
    self.as_wal_mut().remove(None, kb)
  }

  /// Removes a key-value pair from the WAL.
  #[inline]
  fn remove<'a>(
    &mut self,
    key: impl Into<MaybeStructured<'a, K>>,
  ) -> Result<(), Either<K::Error, Error<Self::Memtable>>>
  where
    K: Type + 'a,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as memtable::BaseTable>::Pointer: Pointer + Ord + 'static,
  {
    self.as_wal_mut().remove(None, key.into())
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
