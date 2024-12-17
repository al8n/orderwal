use core::ops::{Bound, RangeBounds};

use among::Among;
use dbutils::{
  buffer::VacantBuffer,
  checksum::{BuildChecksumer, Crc32},
};
#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
use rarena_allocator::Allocator;
use skl::{
  either::Either,
  generic::{Type, TypeRefQueryComparator},
  Active, MaybeTombstone,
};

use crate::{
  batch::Batch,
  error::Error,
  log::Log,
  memtable::{generic::unique::bounded, Memtable},
  swmr,
  types::{BufWriter, KeyBuilder, ValueBuilder},
};

pub use crate::memtable::generic::unique::GenericMemtable;

/// A unique versions ordered write-ahead log implementation for concurrent thread environments.
pub type OrderWal<M, S = Crc32> = swmr::OrderWal<M, S>;

/// The read-only view for the ordered write-ahead log [`OrderWal`].
pub type OrderWalReader<M, S = Crc32> = swmr::OrderWalReader<M, S>;

/// The memory table based on bounded ARENA-style `SkipMap` for the ordered write-ahead log [`OrderWal`].
pub type ArenaTable<K, V, C> = bounded::Table<K, V, C>;

/// An abstract layer for the immutable write-ahead log.
pub trait Reader<K, V>
where
  Self: Log,
  K: ?Sized,
  V: ?Sized,
{
  /// Returns the reserved space in the WAL.
  ///
  /// ## Safety
  /// - The writer must ensure that the returned slice is not modified.
  /// - This method is not thread-safe, so be careful when using it.
  #[inline]
  unsafe fn reserved_slice(&self) -> &[u8] {
    self.allocator().reserved_slice()
  }

  /// Returns the path of the WAL if it is backed by a file.
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
  fn path(&self) -> Option<&<<Self as Log>::Allocator as Allocator>::Path> {
    self.allocator().path()
  }

  /// Returns the number of entries in the WAL.
  #[inline]
  fn len(&self) -> usize
  where
    Self::Memtable: Memtable,
  {
    self.memtable().len()
  }

  /// Returns `true` if the WAL is empty.
  #[inline]
  fn is_empty(&self) -> bool
  where
    Self::Memtable: Memtable,
  {
    self.memtable().is_empty()
  }

  /// Returns the maximum key size allowed in the WAL.
  #[inline]
  fn maximum_key_size(&self) -> u32 {
    self.options().maximum_key_size()
  }

  /// Returns the maximum value size allowed in the WAL.
  #[inline]
  fn maximum_value_size(&self) -> u32 {
    self.options().maximum_value_size()
  }

  /// Returns the remaining capacity of the WAL.
  #[inline]
  fn remaining(&self) -> u32 {
    self.allocator().remaining() as u32
  }

  /// Returns the capacity of the WAL.
  #[inline]
  fn capacity(&self) -> u32 {
    self.allocator().capacity() as u32
  }

  /// Returns an iterator over the entries in the WAL.
  #[inline]
  fn iter(&self) -> <Self::Memtable as GenericMemtable<K, V>>::Iterator<'_, Active>
  where
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
  {
    self.memtable().iter()
  }

  /// Returns an iterator over a subset of entries in the WAL.
  #[inline]
  fn range<Q, R>(&self, range: R) -> <Self::Memtable as GenericMemtable<K, V>>::Range<'_, Active, Q, R>
  where
    R: RangeBounds<Q>,
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<K, Q>,
  {
    self.memtable().range(range)
  }

  /// Returns an iterator over point entries in the memtable.
  #[inline]
  fn iter_points(&self) -> <Self::Memtable as GenericMemtable<K, V>>::PointsIterator<'_, Active>
  where
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
  {
    self.memtable().iter_points()
  }

  /// Returns an iterator over all the point entries in the memtable.
  #[inline]
  fn iter_points_with_tombstone(
    &self,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::PointsIterator<'_, MaybeTombstone>
  where
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
  {
    self.memtable().iter_points_with_tombstone()
  }

  /// Returns an iterator over a subset of point entries in the memtable.
  #[inline]
  fn range_points<Q, R>(
    &self,
    range: R,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::RangePoints<'_, Active, Q, R>
  where
    R: RangeBounds<Q>,
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<K, Q>,
  {
    self.memtable().range_points(range)
  }

  /// Returns an iterator over all the point entries in a subset of the memtable.
  #[inline]
  fn range_points_with_tombstone<'a, Q, R>(
    &'a self,
    range: R,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::RangePoints<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<K, Q>,
  {
    self.memtable().range_points_with_tombstone(range)
  }

  /// Returns an iterator over range deletions entries in the memtable.
  #[inline]
  fn iter_bulk_deletions(
    &self,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::BulkDeletionsIterator<'_, Active>
  where
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
  {
    self.memtable().iter_bulk_deletions()
  }

  /// Returns an iterator over all the range deletions entries in the memtable.
  #[inline]
  fn iter_bulk_deletions_with_tombstone(
    &self,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::BulkDeletionsIterator<'_, MaybeTombstone>
  where
    Self::Memtable: GenericMemtable<K, V>,
    K: Type + 'static,
    V: Type + 'static,
  {
    self.memtable().iter_bulk_deletions_with_tombstone()
  }

  /// Returns an iterator over a subset of range deletions entries in the memtable.
  #[inline]
  fn range_bulk_deletions<'a, Q, R>(
    &'a self,
    range: R,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::BulkDeletionsRange<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<K, Q>,
  {
    self.memtable().range_bulk_deletions(range)
  }

  /// Returns an iterator over all the range deletions entries in a subset of the memtable.
  #[inline]
  fn range_bulk_deletions_with_tombstone<'a, Q, R>(
    &'a self,
    range: R,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::BulkDeletionsRange<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<K, Q>,
  {
    self.memtable().range_bulk_deletions_with_tombstone(range)
  }

  /// Returns an iterator over range updates entries in the memtable.
  #[inline]
  fn iter_bulk_updates(
    &self,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::BulkUpdatesIterator<'_, Active>
  where
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
  {
    self.memtable().iter_bulk_updates()
  }

  /// Returns an iterator over all the range updates entries in the memtable.
  #[inline]
  fn iter_bulk_updates_with_tombstone(
    &self,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::BulkUpdatesIterator<'_, MaybeTombstone>
  where
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
  {
    self.memtable().iter_bulk_updates_with_tombstone()
  }

  /// Returns an iterator over a subset of range updates entries in the memtable.
  #[inline]
  fn range_bulk_updates<'a, Q, R>(
    &'a self,
    range: R,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::BulkUpdatesRange<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<K, Q>,
  {
    self.memtable().range_bulk_updates(range)
  }

  /// Returns an iterator over all the range updates entries in a subset of the memtable.
  #[inline]
  fn range_bulk_updates_with_tombstone<'a, Q, R>(
    &'a self,
    range: R,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::BulkUpdatesRange<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<K, Q>,
  {
    self.memtable().range_bulk_updates_with_tombstone(range)
  }

  /// Returns the first key-value pair in the map. The key in this pair is the minimum key in the wal.
  #[inline]
  fn first(&self) -> Option<<Self::Memtable as GenericMemtable<K, V>>::Entry<'_, Active>>
  where
    Self::Memtable: GenericMemtable<K, V>,
    K: Type + 'static,
    V: Type + 'static,
  {
    self.memtable().first()
  }

  /// Returns the last key-value pair in the map. The key in this pair is the maximum key in the wal.
  #[inline]
  fn last(&self) -> Option<<Self::Memtable as GenericMemtable<K, V>>::Entry<'_, Active>>
  where
    Self::Memtable: GenericMemtable<K, V>,
    K: Type + 'static,
    V: Type + 'static,
  {
    self.memtable().last()
  }

  /// Returns `true` if the key exists in the WAL.
  #[inline]
  fn contains_key<Q>(&self, key: &Q) -> bool
  where
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<K, Q>,
  {
    self.memtable().contains(key)
  }

  /// Gets the value associated with the key.
  #[inline]
  fn get<'a, Q>(&'a self, key: &Q) -> Option<<Self::Memtable as GenericMemtable<K, V>>::Entry<'a, Active>>
  where
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<K, Q>,
  {
    self.memtable().get(key)
  }

  /// Returns a value associated to the highest element whose key is below the given bound.
  /// If no such element is found then `None` is returned.
  #[inline]
  fn upper_bound<'a, Q>(
    &'a self,
    bound: Bound<&'a Q>,
  ) -> Option<<Self::Memtable as GenericMemtable<K, V>>::Entry<'a, Active>>
  where
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<K, Q>,
  {
    self.memtable().upper_bound(bound)
  }

  /// Returns a value associated to the lowest element whose key is above the given bound.
  /// If no such element is found then `None` is returned.
  #[inline]
  fn lower_bound<'a, Q>(
    &'a self,
    bound: Bound<&'a Q>,
  ) -> Option<<Self::Memtable as GenericMemtable<K, V>>::Entry<'a, Active>>
  where
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<K, Q>,
  {
    self.memtable().lower_bound(bound)
  }
}

impl<K, V, T> Reader<K, V> for T
where
  T: Log,
  T::Memtable: GenericMemtable<K, V>,
  K: Type + ?Sized + 'static,
  V: Type + ?Sized + 'static,
{
}

/// An abstract layer for the write-ahead log.
pub trait Writer<K, V>: Reader<K, V>
where
  Self::Reader: Reader<K, V, Memtable = Self::Memtable>,
  Self::Memtable: GenericMemtable<K, V>,
  K: Type + ?Sized + 'static,
  V: Type + ?Sized + 'static,
{
  /// Returns `true` if this WAL instance is read-only.
  #[inline]
  fn read_only(&self) -> bool {
    self.allocator().read_only()
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
    self.allocator().reserved_slice_mut()
  }

  /// Flushes the to disk.
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  fn flush(&self) -> Result<(), Error<Self::Memtable>> {
    self.allocator().flush().map_err(Into::into)
  }

  /// Flushes the to disk.
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  fn flush_async(&self) -> Result<(), Error<Self::Memtable>> {
    self.allocator().flush_async().map_err(Into::into)
  }

  /// Returns the read-only view for the WAL.
  fn reader(&self) -> Self::Reader;

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the key in place.
  ///
  /// See also [`insert_with_value_builder`](Writer::insert_with_value_builder) and [`insert_with_builders`](Writer::insert_with_builders).
  #[inline]
  fn insert_with_key_builder<E>(
    &mut self,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>,
    value: &[u8],
  ) -> Result<(), Either<E, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V>,
  {
    Log::insert(self, None, kb, value).map_err(Among::into_left_right)
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the value in place.
  ///
  /// See also [`insert_with_key_builder`](Writer::insert_with_key_builder) and [`insert_with_builders`](Writer::insert_with_builders).
  #[inline]
  fn insert_with_value_builder<E>(
    &mut self,
    key: &[u8],
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>,
  ) -> Result<(), Either<E, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V>,
  {
    Log::insert(self, None, key, vb).map_err(Among::into_middle_right)
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
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V>,
  {
    Log::insert(self, None, kb, vb)
  }

  /// Inserts a key-value pair into the WAL.
  #[inline]
  fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error<Self::Memtable>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V>,
  {
    Log::insert(self, None, key, value).map_err(Among::unwrap_right)
  }

  /// Removes a key-value pair from the WAL. This method
  /// allows the caller to build the key in place.
  #[inline]
  fn remove_with_builder<KE>(
    &mut self,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, KE>>,
  ) -> Result<(), Either<KE, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V>,
  {
    Log::remove(self, None, kb)
  }

  /// Removes a key-value pair from the WAL.
  #[inline]
  fn remove(&mut self, key: &[u8]) -> Result<(), Error<Self::Memtable>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V>,
  {
    Log::remove(self, None, key).map_err(Either::unwrap_right)
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch<B>(&mut self, batch: &mut B) -> Result<(), Error<Self::Memtable>>
  where
    B: Batch<Self::Memtable>,
    B::Key: AsRef<[u8]>,
    B::Value: AsRef<[u8]>,
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V>,
  {
    Log::insert_batch(self, batch).map_err(Among::unwrap_right)
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch_with_key_builder<B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Either<<B::Key as BufWriter>::Error, Error<Self::Memtable>>>
  where
    B: Batch<Self::Memtable>,
    B::Key: BufWriter,
    B::Value: AsRef<[u8]>,
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V>,
  {
    Log::insert_batch(self, batch).map_err(Among::into_left_right)
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch_with_value_builder<B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Either<<B::Value as BufWriter>::Error, Error<Self::Memtable>>>
  where
    B: Batch<Self::Memtable>,
    B::Key: AsRef<[u8]>,
    B::Value: BufWriter,
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V>,
  {
    Log::insert_batch(self, batch).map_err(Among::into_middle_right)
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch_with_builders<KB, VB, B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Among<KB::Error, VB::Error, Error<Self::Memtable>>>
  where
    B: Batch<Self::Memtable, Key = KB, Value = VB>,
    KB: BufWriter,
    VB: BufWriter,
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V>,
  {
    Log::insert_batch(self, batch)
  }
}

impl<K, V, M, S> Writer<K, V> for swmr::OrderWal<M, S>
where
  M: GenericMemtable<K, V> + 'static,
  K: Type + ?Sized + 'static,
  V: Type + ?Sized + 'static,
  S: 'static,
{
  #[inline]
  fn reader(&self) -> Self::Reader {
    swmr::OrderWalReader::from_core(self.core.clone())
  }
}
