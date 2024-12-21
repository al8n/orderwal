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
  generic::{MaybeStructured, Type, TypeRefComparator, TypeRefQueryComparator},
  Active, MaybeTombstone,
};

use crate::{
  batch::Batch,
  error::Error,
  log::Log,
  memtable::{self, generic::bounded, Memtable, MutableMemtable},
  swmr,
  types::{BufWriter, KeyBuilder, ValueBuilder},
};

pub use crate::memtable::generic::GenericMemtable;
pub use skl::generic::{Ascend, Descend};

/// A multiple versions ordered write-ahead log implementation for concurrent thread environments.
pub type OrderWal<M, S = Crc32> = swmr::OrderWal<M, S>;

/// The read-only view for the ordered write-ahead log [`OrderWal`].
pub type OrderWalReader<M, S = Crc32> = swmr::OrderWalReader<M, S>;

/// The memory table based on bounded ARENA-style `SkipMap` for the ordered write-ahead log [`OrderWal`].
pub type ArenaTable<K, V, C = Ascend> = bounded::Table<K, V, C>;

/// The options for the [`ArenaTable`].
pub type ArenaTableOptions<C = Ascend> = memtable::bounded::TableOptions<C>;

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

  /// Returns the maximum version in the WAL.
  #[inline]
  fn maximum_version(&self) -> u64
  where
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
  {
    self.memtable().maximum_version()
  }

  /// Returns the minimum version in the WAL.
  #[inline]
  fn minimum_version(&self) -> u64
  where
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
  {
    self.memtable().minimum_version()
  }

  /// Returns `true` if the WAL may contain an entry whose version is less or equal to the given version.
  #[inline]
  fn may_contain_version(&self, version: u64) -> bool
  where
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
  {
    self.memtable().may_contain_version(version)
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
  fn iter(&self, version: u64) -> <Self::Memtable as GenericMemtable<K, V>>::Iterator<'_, Active>
  where
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
  {
    self.memtable().iter(version)
  }

  /// Returns an iterator over a subset of entries in the WAL.
  #[inline]
  fn range<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::Range<'a, Active, Q, R>
  where
    R: RangeBounds<Q>,
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    self.memtable().range(version, range)
  }

  /// Returns an iterator over the entries in the WAL.
  #[inline]
  fn iter_all(
    &self,
    version: u64,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::Iterator<'_, MaybeTombstone>
  where
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
  {
    self.memtable().iter_all(version)
  }

  /// Returns an iterator over a subset of entries in the WAL.
  #[inline]
  fn range_all<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::Range<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q>,
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    self.memtable().range_all(version, range)
  }

  /// Returns an iterator over point entries in the memtable.
  #[inline]
  fn iter_points(
    &self,
    version: u64,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::PointsIterator<'_, Active>
  where
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
  {
    self.memtable().iter_points(version)
  }

  /// Returns an iterator over all(including all versions and tombstones) the point entries in the memtable.
  #[inline]
  fn iter_all_points(
    &self,
    version: u64,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::PointsIterator<'_, MaybeTombstone>
  where
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
  {
    self.memtable().iter_all_points(version)
  }

  /// Returns an iterator over a subset of point entries in the memtable.
  #[inline]
  fn range_points<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::RangePoints<'a, Active, Q, R>
  where
    R: RangeBounds<Q>,
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    self.memtable().range_points(version, range)
  }

  /// Returns an iterator over all(including all versions and tombstones) the point entries in a subset of the memtable.
  #[inline]
  fn range_all_points<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::RangePoints<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    self.memtable().range_all_points(version, range)
  }

  /// Returns an iterator over range deletions entries in the memtable.
  #[inline]
  fn iter_bulk_deletions(
    &self,
    version: u64,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::BulkDeletionsIterator<'_, Active>
  where
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
  {
    self.memtable().iter_bulk_deletions(version)
  }

  /// Returns an iterator over all(including all versions and tombstones) the range deletions entries in the memtable.
  #[inline]
  fn iter_all_bulk_deletions(
    &self,
    version: u64,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::BulkDeletionsIterator<'_, MaybeTombstone>
  where
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
  {
    self.memtable().iter_all_bulk_deletions(version)
  }

  /// Returns an iterator over a subset of range deletions entries in the memtable.
  #[inline]
  fn range_bulk_deletions<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::BulkDeletionsRange<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    self.memtable().range_bulk_deletions(version, range)
  }

  /// Returns an iterator over all(including all versions and tombstones) the range deletions entries in a subset of the memtable.
  #[inline]
  fn range_all_bulk_deletions<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::BulkDeletionsRange<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    self.memtable().range_all_bulk_deletions(version, range)
  }

  /// Returns an iterator over range updates entries in the memtable.
  #[inline]
  fn iter_bulk_updates(
    &self,
    version: u64,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::BulkUpdatesIterator<'_, Active>
  where
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
  {
    self.memtable().iter_bulk_updates(version)
  }

  /// Returns an iterator over all(including all versions and tombstones) the range updates entries in the memtable.
  #[inline]
  fn iter_all_bulk_updates(
    &self,
    version: u64,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::BulkUpdatesIterator<'_, MaybeTombstone>
  where
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
  {
    self.memtable().iter_all_bulk_updates(version)
  }

  /// Returns an iterator over a subset of range updates entries in the memtable.
  #[inline]
  fn range_bulk_updates<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::BulkUpdatesRange<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    self.memtable().range_bulk_updates(version, range)
  }

  /// Returns an iterator over all(including all versions and tombstones) the range updates entries in a subset of the memtable.
  #[inline]
  fn range_all_bulk_updates<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> <Self::Memtable as GenericMemtable<K, V>>::BulkUpdatesRange<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    self.memtable().range_all_bulk_updates(version, range)
  }

  /// Returns the first key-value pair in the map. The key in this pair is the minimum key in the wal.
  #[inline]
  fn first<'a>(
    &'a self,
    version: u64,
  ) -> Option<<Self::Memtable as GenericMemtable<K, V>>::Entry<'a, Active>>
  where
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefComparator<'a, K>,
  {
    self.memtable().first(version)
  }

  /// Returns the last key-value pair in the map. The key in this pair is the maximum key in the wal.
  #[inline]
  fn last<'a>(
    &'a self,
    version: u64,
  ) -> Option<<Self::Memtable as GenericMemtable<K, V>>::Entry<'a, Active>>
  where
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefComparator<'a, K>,
  {
    self.memtable().last(version)
  }

  /// Returns the first key-value pair in the map. The key in this pair is the minimum key in the wal.
  #[inline]
  fn first_with_tombstone<'a>(
    &'a self,
    version: u64,
  ) -> Option<<Self::Memtable as GenericMemtable<K, V>>::Entry<'a, MaybeTombstone>>
  where
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefComparator<'a, K>,
  {
    self.memtable().first_with_tombstone(version)
  }

  /// Returns the last key-value pair in the map. The key in this pair is the maximum key in the wal.
  #[inline]
  fn last_with_tombstone<'a>(
    &'a self,
    version: u64,
  ) -> Option<<Self::Memtable as GenericMemtable<K, V>>::Entry<'a, MaybeTombstone>>
  where
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefComparator<'a, K>,
  {
    self.memtable().last_with_tombstone(version)
  }

  /// Returns `true` if the key exists in the WAL.
  #[inline]
  fn contains_key<'a, Q>(&'a self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    self.memtable().contains(version, key)
  }

  /// Gets the value associated with the key.
  #[inline]
  fn get<'a, Q>(
    &'a self,
    version: u64,
    key: &Q,
  ) -> Option<<Self::Memtable as GenericMemtable<K, V>>::Entry<'a, Active>>
  where
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    self.memtable().get(version, key)
  }

  /// Returns `true` if the key exists in the WAL.
  #[inline]
  fn contains_key_with_tombstone<'a, Q>(&'a self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    self.memtable().contains_with_tombsone(version, key)
  }

  /// Gets the value associated with the key.
  #[inline]
  fn get_with_tombstone<'a, Q>(
    &'a self,
    version: u64,
    key: &Q,
  ) -> Option<<Self::Memtable as GenericMemtable<K, V>>::Entry<'a, MaybeTombstone>>
  where
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    self.memtable().get_with_tombstone(version, key)
  }

  /// Returns a value associated to the highest element whose key is below the given bound.
  /// If no such element is found then `None` is returned.
  #[inline]
  fn upper_bound<'a, Q>(
    &'a self,
    version: u64,
    bound: Bound<&'a Q>,
  ) -> Option<<Self::Memtable as GenericMemtable<K, V>>::Entry<'a, Active>>
  where
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    self.memtable().upper_bound(version, bound)
  }

  /// Returns a value associated to the lowest element whose key is above the given bound.
  /// If no such element is found then `None` is returned.
  #[inline]
  fn lower_bound<'a, Q>(
    &'a self,
    version: u64,
    bound: Bound<&'a Q>,
  ) -> Option<<Self::Memtable as GenericMemtable<K, V>>::Entry<'a, Active>>
  where
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    self.memtable().lower_bound(version, bound)
  }

  /// Returns a value associated to the highest element whose key is below the given bound.
  /// If no such element is found then `None` is returned.
  #[inline]
  fn upper_bound_with_tombstone<'a, Q>(
    &'a self,
    version: u64,
    bound: Bound<&'a Q>,
  ) -> Option<<Self::Memtable as GenericMemtable<K, V>>::Entry<'a, MaybeTombstone>>
  where
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    self.memtable().upper_bound_with_tombstone(version, bound)
  }

  /// Returns a value associated to the lowest element whose key is above the given bound.
  /// If no such element is found then `None` is returned.
  #[inline]
  fn lower_bound_with_tombstone<'a, Q>(
    &'a self,
    version: u64,
    bound: Bound<&'a Q>,
  ) -> Option<<Self::Memtable as GenericMemtable<K, V>>::Entry<'a, MaybeTombstone>>
  where
    Q: ?Sized,
    K: Type + 'static,
    V: Type + 'static,
    Self::Memtable: GenericMemtable<K, V>,
    <Self::Memtable as GenericMemtable<K, V>>::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    self.memtable().lower_bound_with_tombstone(version, bound)
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
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
  fn flush(&self) -> Result<(), Error<Self::Memtable>> {
    self.allocator().flush().map_err(Into::into)
  }

  /// Flushes the to disk.
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
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
    version: u64,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>,
    value: &[u8],
  ) -> Result<(), Either<E, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::insert::<_, &[u8]>(self, version, kb, value).map_err(Among::into_left_right)
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the value in place.
  ///
  /// See also [`insert_with_key_builder`](Writer::insert_with_key_builder) and [`insert_with_builders`](Writer::insert_with_builders).
  #[inline]
  fn insert_with_value_builder<'a, E>(
    &'a mut self,
    version: u64,
    key: impl Into<MaybeStructured<'a, K>>,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>,
  ) -> Result<(), Among<K::Error, E, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::insert(self, version, key.into(), vb)
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
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::insert(self, version, kb, vb)
  }

  /// Inserts a key-value pair into the WAL.
  #[inline]
  fn insert<'a>(
    &'a mut self,
    version: u64,
    key: impl Into<MaybeStructured<'a, K>>,
    value: impl Into<MaybeStructured<'a, V>>,
  ) -> Result<(), Among<K::Error, V::Error, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::insert(self, version, key.into(), value.into())
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
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::remove(self, version, kb)
  }

  /// Removes a key-value pair from the WAL.
  #[inline]
  fn remove<'a>(
    &'a mut self,
    version: u64,
    key: impl Into<MaybeStructured<'a, K>>,
  ) -> Result<(), Either<K::Error, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::remove(self, version, key.into())
  }

  /// Mark all keys in the range as removed.
  ///
  /// This is not a contra operation to [`range_set`](Writer::range_set).
  /// See also [`range_set`](Writer::range_set) and [`range_set`](Writer::range_unset).
  #[inline]
  fn range_remove<'a>(
    &mut self,
    version: u64,
    start_bound: Bound<impl Into<MaybeStructured<'a, K>>>,
    end_bound: Bound<impl Into<MaybeStructured<'a, K>>>,
  ) -> Result<(), Either<K::Error, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::range_remove(
      self,
      version,
      start_bound.map(Into::into),
      end_bound.map(Into::into),
    )
    .map_err(|e| match e {
      Among::Left(e) => Either::Left(e),
      Among::Middle(e) => Either::Left(e),
      Among::Right(e) => Either::Right(e),
    })
  }

  /// Mark all keys in the range as removed, which allows the caller to build the start bound in place.
  ///
  /// See [`range_remove`](Writer::range_remove).
  #[inline]
  fn range_remove_with_start_bound_builder<'a, E>(
    &'a mut self,
    version: u64,
    start_bound: Bound<KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>>,
    end_bound: Bound<impl Into<MaybeStructured<'a, K>>>,
  ) -> Result<(), Among<E, K::Error, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::range_remove(self, version, start_bound, end_bound.map(Into::into)).map_err(|e| match e {
      Among::Left(e) => Among::Left(e),
      Among::Middle(e) => Among::Middle(e),
      Among::Right(e) => Among::Right(e),
    })
  }

  /// Mark all keys in the range as removed, which allows the caller to build the end bound in place.
  ///
  /// See [`range_remove`](Writer::range_remove).
  #[inline]
  fn range_remove_with_end_bound_builder<'a, E>(
    &'a mut self,
    version: u64,
    start_bound: Bound<impl Into<MaybeStructured<'a, K>>>,
    end_bound: Bound<KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>>,
  ) -> Result<(), Among<K::Error, E, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::range_remove(self, version, start_bound.map(Into::into), end_bound).map_err(|e| match e {
      Among::Left(e) => Among::Left(e),
      Among::Middle(e) => Among::Middle(e),
      Among::Right(e) => Among::Right(e),
    })
  }

  /// Mark all keys in the range as removed, which allows the caller to build both bounds in place.
  ///
  /// See [`range_remove`](Writer::range_remove).
  #[inline]
  fn range_remove_with_builders<S, E>(
    &mut self,
    version: u64,
    start_bound: Bound<KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, S>>>,
    end_bound: Bound<KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>>,
  ) -> Result<(), Among<S, E, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::range_remove(self, version, start_bound, end_bound)
  }

  /// Set all keys in the range to the `value`.
  #[inline]
  fn range_set<'a>(
    &'a mut self,
    version: u64,
    start_bound: Bound<impl Into<MaybeStructured<'a, K>>>,
    end_bound: Bound<impl Into<MaybeStructured<'a, K>>>,
    value: impl Into<MaybeStructured<'a, V>>,
  ) -> Result<(), Among<K::Error, V::Error, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::range_set(
      self,
      version,
      start_bound.map(Into::into),
      end_bound.map(Into::into),
      value.into(),
    )
    .map_err(|e| match e {
      Among::Left(e) => Among::Left(e.into_inner()),
      Among::Middle(e) => Among::Middle(e),
      Among::Right(e) => Among::Right(e),
    })
  }

  /// Set all keys in the range to the `value`, which allows the caller to build the start bound in place.
  ///
  /// See [`range_set`](Writer::range_set).
  #[inline]
  fn range_set_with_start_bound_builder<'a, E>(
    &'a mut self,
    version: u64,
    start_bound: Bound<KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>>,
    end_bound: Bound<impl Into<MaybeStructured<'a, K>>>,
    value: impl Into<MaybeStructured<'a, V>>,
  ) -> Result<(), Among<Either<E, K::Error>, V::Error, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::range_set(
      self,
      version,
      start_bound,
      end_bound.map(Into::into),
      value.into(),
    )
    .map_err(|e| match e {
      Among::Left(e) => Among::Left(e),
      Among::Middle(e) => Among::Middle(e),
      Among::Right(e) => Among::Right(e),
    })
  }

  /// Set all keys in the range to the `value`, which allows the caller to build the end bound in place.
  ///
  /// See [`range_set`](Writer::range_set).
  #[inline]
  fn range_set_with_end_bound_builder<'a, E>(
    &'a mut self,
    version: u64,
    start_bound: Bound<impl Into<MaybeStructured<'a, K>>>,
    end_bound: Bound<KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>>,
    value: impl Into<MaybeStructured<'a, V>>,
  ) -> Result<(), Among<Either<K::Error, E>, V::Error, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::range_set(
      self,
      version,
      start_bound.map(Into::into),
      end_bound,
      value.into(),
    )
    .map_err(|e| match e {
      Among::Left(e) => Among::Left(e),
      Among::Middle(e) => Among::Middle(e),
      Among::Right(e) => Among::Right(e),
    })
  }

  /// Set all keys in the range to the `value`, which allows the caller to build the value in place.
  ///
  /// See [`range_set`](Writer::range_set).
  #[inline]
  fn range_set_with_value_builder<'a, E>(
    &'a mut self,
    version: u64,
    start_bound: Bound<impl Into<MaybeStructured<'a, K>>>,
    end_bound: Bound<impl Into<MaybeStructured<'a, K>>>,
    value: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>,
  ) -> Result<(), Among<K::Error, E, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::range_set(
      self,
      version,
      start_bound.map(Into::into),
      end_bound.map(Into::into),
      value,
    )
    .map_err(|e| match e {
      Among::Left(e) => Among::Left(e.into_inner()),
      Among::Middle(e) => Among::Middle(e),
      Among::Right(e) => Among::Right(e),
    })
  }

  /// Set all keys in the range to the `value`, which allows the caller to build the start bound key and value in place.
  ///
  /// See [`range_set`](Writer::range_set).
  #[inline]
  fn range_set_with_start_bound_builder_and_value_builder<'a, S, VE>(
    &'a mut self,
    version: u64,
    start_bound: Bound<KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, S>>>,
    end_bound: Bound<impl Into<MaybeStructured<'a, K>>>,
    value: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, VE>>,
  ) -> Result<(), Among<Either<S, K::Error>, VE, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::range_set(self, version, start_bound, end_bound.map(Into::into), value).map_err(
      |e| match e {
        Among::Left(e) => Among::Left(e),
        Among::Middle(e) => Among::Middle(e),
        Among::Right(e) => Among::Right(e),
      },
    )
  }

  /// Set all keys in the range to the `value`, which allows the caller to build the end bound key and value in place.
  ///
  /// See [`range_set`](Writer::range_set).
  #[inline]
  fn range_set_with_end_bound_builder_and_value_builder<'a, E, VE>(
    &'a mut self,
    version: u64,
    start_bound: Bound<impl Into<MaybeStructured<'a, K>>>,
    end_bound: Bound<KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>>,
    value: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, VE>>,
  ) -> Result<(), Among<Either<K::Error, E>, VE, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::range_set(self, version, start_bound.map(Into::into), end_bound, value).map_err(
      |e| match e {
        Among::Left(e) => Among::Left(e),
        Among::Middle(e) => Among::Middle(e),
        Among::Right(e) => Among::Right(e),
      },
    )
  }

  /// Set all keys in the range to the `value`, which allows the caller to build both bounds in place.
  ///
  /// See [`range_set`](Writer::range_set).
  #[inline]
  fn range_set_with_bound_builders<'a, S, E>(
    &'a mut self,
    version: u64,
    start_bound: Bound<KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, S>>>,
    end_bound: Bound<KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>>,
    value: impl Into<MaybeStructured<'a, V>>,
  ) -> Result<(), Among<Either<S, E>, V::Error, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::range_set(self, version, start_bound, end_bound, value.into()).map_err(|e| match e {
      Among::Left(e) => Among::Left(e),
      Among::Middle(e) => Among::Middle(e),
      Among::Right(e) => Among::Right(e),
    })
  }

  /// Set all keys in the range to the `value`, which allows the caller to build both bounds and value in place.
  ///
  /// See [`range_set`](Writer::range_set).
  #[inline]
  fn range_set_with_builders<S, E, VE>(
    &mut self,
    version: u64,
    start_bound: Bound<KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, S>>>,
    end_bound: Bound<KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>>,
    value: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, VE>>,
  ) -> Result<(), Among<Either<S, E>, VE, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::range_set(self, version, start_bound, end_bound, value)
  }

  /// Unsets all keys in the range to their original value.
  ///
  /// This is a contra operation to [`range_set`](Writer::range_set).
  #[inline]
  fn range_unset<'a>(
    &'a mut self,
    version: u64,
    start_bound: Bound<impl Into<MaybeStructured<'a, K>>>,
    end_bound: Bound<impl Into<MaybeStructured<'a, K>>>,
  ) -> Result<(), Either<K::Error, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::range_unset(
      self,
      version,
      start_bound.map(Into::into),
      end_bound.map(Into::into),
    )
    .map_err(|e| match e {
      Among::Left(e) => Either::Left(e),
      Among::Middle(e) => Either::Left(e),
      Among::Right(e) => Either::Right(e),
    })
  }

  /// Unsets all keys in the range to their original value, which allows the caller to build the start bound in place.
  ///
  /// See [`range_unset`](Writer::range_unset).
  #[inline]
  fn range_unset_with_start_bound_builder<'a, E>(
    &'a mut self,
    version: u64,
    start_bound: Bound<KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>>,
    end_bound: Bound<impl Into<MaybeStructured<'a, K>>>,
  ) -> Result<(), Among<E, K::Error, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::range_unset(self, version, start_bound, end_bound.map(Into::into)).map_err(|e| match e {
      Among::Left(e) => Among::Left(e),
      Among::Middle(e) => Among::Middle(e),
      Among::Right(e) => Among::Right(e),
    })
  }

  /// Unsets all keys in the range to their original value, which allows the caller to build the end bound in place.
  ///
  /// See [`range_unset`](Writer::range_unset).
  #[inline]
  fn range_unset_with_end_bound_builder<'a, E>(
    &'a mut self,
    version: u64,
    start_bound: Bound<impl Into<MaybeStructured<'a, K>>>,
    end_bound: Bound<KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>>,
  ) -> Result<(), Among<K::Error, E, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::range_unset(self, version, start_bound.map(Into::into), end_bound).map_err(|e| match e {
      Among::Left(e) => Among::Left(e),
      Among::Middle(e) => Among::Middle(e),
      Among::Right(e) => Among::Right(e),
    })
  }

  /// Unsets all keys in the range to their original value, which allows the caller to build both bounds in place.
  ///
  /// See [`range_unset`](Writer::range_unset).
  #[inline]
  fn range_unset_with_builders<S, E>(
    &mut self,
    version: u64,
    start_bound: Bound<KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, S>>>,
    end_bound: Bound<KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>>,
  ) -> Result<(), Among<S, E, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::range_unset(self, version, start_bound, end_bound)
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn apply<KB, VB, B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Among<KB::Error, VB::Error, Error<Self::Memtable>>>
  where
    B: Batch<Self::Memtable, Key = KB, Value = VB>,
    KB: BufWriter,
    VB: BufWriter,
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: GenericMemtable<K, V> + MutableMemtable,
  {
    Log::apply::<B>(self, batch)
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
