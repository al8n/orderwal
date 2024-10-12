use core::ops::{Bound, RangeBounds};

use among::Among;
use dbutils::{
  buffer::VacantBuffer,
  checksum::BuildChecksumer,
  equivalent::Comparable,
  traits::{KeyRef, Type},
};
use rarena_allocator::Allocator;
use ref_cast::RefCast;

use crate::{
  batch::Batch,
  entry::BufWriter,
  error::Error,
  sealed::{self, Constructable, GenericPointer, Pointer, Wal, WithVersion},
  KeyBuilder, Options, ValueBuilder,
};

use super::{
  Generic, GenericComparator, GenericEntry, GenericIter, GenericKeys, GenericQueryRange,
  GenericRange, GenericRangeKeys, GenericRangeValues, GenericValues, Query, Slice,
};

/// An abstract layer for the immutable write-ahead log.
pub trait Reader<K: ?Sized, V: ?Sized>: Constructable<Comparator = GenericComparator<K>>
where
  <Self::Memtable as sealed::Memtable>::Pointer: WithVersion + GenericPointer<K, V>,
{
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
  fn path(&self) -> Option<&<<Self as Constructable>::Allocator as Allocator>::Path> {
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
    version: u64,
  ) -> GenericIter<
    '_,
    K,
    V,
    <<Self::Wal as Wal<GenericComparator<K>, Self::Checksumer>>::Memtable as sealed::Memtable>::Iterator<'_>,
    Self::Memtable,
  >
  where
    <Self::Memtable as sealed::Memtable>::Pointer: Pointer<Comparator = Self::Comparator>
  {
    GenericIter::new(self.as_core().iter(Some(version)))
  }

  /// Returns an iterator over a subset of entries in the WAL.
  #[inline]
  fn range<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> GenericRange<
    'a,
    K,
    V,
    R,
    Q,
    <Self::Wal as Wal<GenericComparator<K>, Self::Checksumer>>::Memtable,
  >
  where
    R: RangeBounds<Q>,
    K: Type + Ord,
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<<Self::Memtable as sealed::Memtable>::Pointer> + Ord,
    <Self::Memtable as sealed::Memtable>::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    GenericRange::new(
      self
        .as_core()
        .range(Some(version), GenericQueryRange::new(range)),
    )
  }

  /// Returns an iterator over the keys in the WAL.
  #[inline]
  fn keys(
    &self,
    version: u64,
  ) -> GenericKeys<
    '_,
    K,
    <<Self::Wal as Wal<GenericComparator<K>, Self::Checksumer>>::Memtable as sealed::Memtable>::Iterator<'_>,
    Self::Memtable,
  >
  where
    <Self::Memtable as sealed::Memtable>::Pointer: Pointer<Comparator = Self::Comparator>
  {
    GenericKeys::new(self.as_core().iter(Some(version)))
  }

  /// Returns an iterator over a subset of keys in the WAL.
  #[inline]
  fn range_keys<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> GenericRangeKeys<
    'a,
    K,
    R,
    Q,
    <Self::Wal as Wal<GenericComparator<K>, Self::Checksumer>>::Memtable,
  >
  where
    R: RangeBounds<Q>,
    K: Type + Ord,
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<<Self::Memtable as sealed::Memtable>::Pointer> + Ord,
    <Self::Memtable as sealed::Memtable>::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    GenericRangeKeys::new(
      self
        .as_core()
        .range(Some(version), GenericQueryRange::new(range)),
    )
  }

  /// Returns an iterator over the values in the WAL.
  #[inline]
  fn values(
    &self,
    version: u64,
  ) -> GenericValues<
    '_,
    V,
    <<Self::Wal as Wal<GenericComparator<K>, Self::Checksumer>>::Memtable as sealed::Memtable>::Iterator<'_>,
    Self::Memtable,
  >
  where
    <Self::Memtable as sealed::Memtable>::Pointer: Pointer<Comparator = Self::Comparator>
  {
    GenericValues::new(self.as_core().iter(Some(version)))
  }

  /// Returns an iterator over a subset of values in the WAL.
  #[inline]
  fn range_values<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> GenericRangeValues<
    'a,
    K,
    V,
    R,
    Q,
    <Self::Wal as Wal<GenericComparator<K>, Self::Checksumer>>::Memtable,
  >
  where
    R: RangeBounds<Q>,
    K: Type + Ord,
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<<Self::Memtable as sealed::Memtable>::Pointer> + Ord,
    <Self::Memtable as sealed::Memtable>::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    GenericRangeValues::new(
      self
        .as_core()
        .range(Some(version), GenericQueryRange::new(range)),
    )
  }

  /// Returns the first key-value pair in the map. The key in this pair is the minimum key in the wal.
  #[inline]
  fn first_entry(
    &self,
    version: u64,
  ) -> Option<GenericEntry<'_, K, V, <Self::Memtable as sealed::Memtable>::Item<'_>>>
  where
    K: Type,
    V: Type,
    <Self::Memtable as sealed::Memtable>::Pointer: Pointer<Comparator = GenericComparator<K>> + Ord,
  {
    self
      .as_core()
      .first(Some(version))
      .map(|ent| GenericEntry::with_version(ent, version))
  }

  /// Returns the last key-value pair in the map. The key in this pair is the maximum key in the wal.
  #[inline]
  fn last(
    &self,
    version: u64,
  ) -> Option<GenericEntry<'_, K, V, <Self::Memtable as sealed::Memtable>::Item<'_>>>
  where
    K: Type,
    V: Type,
    <Self::Memtable as sealed::Memtable>::Pointer: Pointer<Comparator = GenericComparator<K>> + Ord,
  {
    Wal::last(self.as_core(), Some(version)).map(|ent| GenericEntry::with_version(ent, version))
  }

  /// Returns `true` if the key exists in the WAL.
  #[inline]
  fn contains_key<'a, Q>(&'a self, version: u64, key: &Q) -> bool
  where
    K: Type,
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<<Self::Memtable as sealed::Memtable>::Pointer> + Ord,
    <Self::Memtable as sealed::Memtable>::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    self.as_core().contains_key(Some(version), &Query::new(key))
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
    Slice<K>: Comparable<<Self::Memtable as sealed::Memtable>::Pointer>,
    <Self::Memtable as sealed::Memtable>::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    self
      .as_core()
      .contains_key(Some(version), Slice::<K>::ref_cast(key))
  }

  /// Gets the value associated with the key.
  #[inline]
  fn get<'a, Q>(
    &'a self,
    version: u64,
    key: &Q,
  ) -> Option<GenericEntry<'a, K, V, <Self::Memtable as sealed::Memtable>::Item<'a>>>
  where
    K: Type,
    V: Type,
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<<Self::Memtable as sealed::Memtable>::Pointer> + Ord,
    <Self::Memtable as sealed::Memtable>::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    self
      .as_core()
      .get(Some(version), &Query::new(key))
      .map(|ent| GenericEntry::with_version(ent, version))
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
  ) -> Option<GenericEntry<'_, K, V, <Self::Memtable as sealed::Memtable>::Item<'_>>>
  where
    K: Type,
    V: Type,
    for<'a> K::Ref<'a>: KeyRef<'a, K> + Ord,
    Slice<K>: Comparable<<Self::Memtable as sealed::Memtable>::Pointer>,
    <Self::Memtable as sealed::Memtable>::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    self
      .as_core()
      .get(Some(version), Slice::<K>::ref_cast(key))
      .map(|ent| GenericEntry::with_version(ent, version))
  }

  /// Returns a value associated to the highest element whose key is below the given bound.
  /// If no such element is found then `None` is returned.
  #[inline]
  fn upper_bound<'a, Q>(
    &'a self,
    version: u64,
    bound: Bound<&Q>,
  ) -> Option<GenericEntry<'a, K, V, <Self::Memtable as sealed::Memtable>::Item<'a>>>
  where
    K: Type + Ord,
    V: Type,
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<<Self::Memtable as sealed::Memtable>::Pointer> + Ord,
    <Self::Memtable as sealed::Memtable>::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    self
      .as_core()
      .upper_bound(Some(version), bound.map(Query::ref_cast))
      .map(|ent| GenericEntry::with_version(ent, version))
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
  ) -> Option<GenericEntry<'_, K, V, <Self::Memtable as sealed::Memtable>::Item<'_>>>
  where
    K: Type,
    V: Type,
    for<'a> K::Ref<'a>: KeyRef<'a, K> + Ord,
    Slice<K>: Comparable<<Self::Memtable as sealed::Memtable>::Pointer>,
    <Self::Memtable as sealed::Memtable>::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    self
      .as_core()
      .upper_bound(Some(version), bound.map(Slice::ref_cast))
      .map(|ent| GenericEntry::with_version(ent, version))
  }

  /// Returns a value associated to the lowest element whose key is above the given bound.
  /// If no such element is found then `None` is returned.
  #[inline]
  fn lower_bound<'a, Q>(
    &'a self,
    version: u64,
    bound: Bound<&Q>,
  ) -> Option<GenericEntry<'a, K, V, <Self::Memtable as sealed::Memtable>::Item<'a>>>
  where
    K: Type + Ord,
    V: Type,
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
    for<'b> Query<'b, K, Q>: Comparable<<Self::Memtable as sealed::Memtable>::Pointer> + Ord,
    <Self::Memtable as sealed::Memtable>::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    self
      .as_core()
      .lower_bound(Some(version), bound.map(Query::ref_cast))
      .map(|ent| GenericEntry::with_version(ent, version))
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
  ) -> Option<GenericEntry<'_, K, V, <Self::Memtable as sealed::Memtable>::Item<'_>>>
  where
    K: Type,
    V: Type,
    for<'a> K::Ref<'a>: KeyRef<'a, K> + Ord,
    Slice<K>: Comparable<<Self::Memtable as sealed::Memtable>::Pointer>,
    <Self::Memtable as sealed::Memtable>::Pointer: Pointer<Comparator = GenericComparator<K>>,
  {
    self
      .as_core()
      .lower_bound(Some(version), bound.map(Slice::ref_cast))
      .map(|ent| GenericEntry::with_version(ent, version))
  }
}

impl<T, K, V> Reader<K, V> for T
where
  T: Constructable<Comparator = GenericComparator<K>>,
  <T::Memtable as sealed::Memtable>::Pointer: WithVersion + GenericPointer<K, V>,
  K: ?Sized,
  V: ?Sized,
{
}

/// An abstract layer for the write-ahead log.
pub trait Writer<K: ?Sized, V: ?Sized>: Reader<K, V>
where
  <Self::Memtable as sealed::Memtable>::Pointer: WithVersion + GenericPointer<K, V>,
  Self::Reader: Reader<K, V, Memtable = Self::Memtable>,
{
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

  // /// Get or insert a new entry into the WAL.
  // #[inline]
  // fn get_or_insert<'a>(
  //   &'a mut self,
  //   key: impl Into<Generic<'a, K>>,
  //   val: impl Into<Generic<'a, V>>,
  // ) -> Result<Option<V::Ref<'a>>, Among<K::Error, V::Error, Error>>
  // where
  //   K: Type + Ord + for<'b> Comparable<K::Ref<'b>> + 'a,
  //   for<'b> K::Ref<'b>: KeyRef<'b, K>,
  //   V: Type + 'a,
  //   Query<'a, K, Generic<'a, K>>: Comparable<<Self::Memtable as sealed::Memtable>::Pointer> + Ord,
  //   <Self::Memtable as sealed::Memtable>::Pointer: Pointer<Comparator = GenericComparator<K>> + Comparable<K> + Ord,
  //   Self::Checksumer: BuildChecksumer,
  // {

  //   let key: Generic<'a, K> = key.into();
  //   let val: Generic<'a, V> = val.into();

  //   let vb = ValueBuilder::once(val.encoded_len() as u32, |buf| {
  //     val.encode_to_buffer(buf).map(|_| ())
  //   });
  //   self.as_core_mut().get_or_insert_with_value_builder(Some(version), &key, vb)
  //     .map(|res| res.map(ty_ref::<V>))
  // }

  // /// Get or insert a new entry into the WAL.
  // #[inline]
  // fn get_or_insert_with_value_builder<E>(
  //   &mut self,
  //   version: u64,
  //   key: &[u8],
  //   vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
  // ) -> Result<Option<&[u8]>, Either<E, Error>>
  // where
  //   Self::Checksumer: BuildChecksumer,
  //   <Self::Memtable as sealed::Memtable>::Pointer: Pointer<Comparator = GenericComparator<K>> + Ord,
  // {
  //   self
  //     .as_core_mut()
  //     .get_or_insert_with_value_builder(Some(version), key, vb)
  // }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the key in place.
  ///
  /// See also [`insert_with_value_builder`](Wal::insert_with_value_builder) and [`insert_with_builders`](Wal::insert_with_builders).
  #[inline]
  fn insert_with_key_builder<'a, E>(
    &'a mut self,
    version: u64,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
    value: impl Into<Generic<'a, V>>,
  ) -> Result<(), Among<E, V::Error, Error>>
  where
    K: Type,
    V: Type + 'a,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as sealed::Memtable>::Pointer:
      Pointer<Comparator = GenericComparator<K>> + Ord + 'static,
  {
    self.as_core_mut().insert(Some(version), kb, value.into())
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the value in place.
  ///
  /// See also [`insert_with_key_builder`](Wal::insert_with_key_builder) and [`insert_with_builders`](Wal::insert_with_builders).
  #[inline]
  fn insert_with_value_builder<'a, E>(
    &'a mut self,
    version: u64,
    key: impl Into<Generic<'a, K>>,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
  ) -> Result<(), Among<K::Error, E, Error>>
  where
    K: Type + 'a,
    V: Type,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as sealed::Memtable>::Pointer:
      Pointer<Comparator = GenericComparator<K>> + Ord + 'static,
  {
    self.as_core_mut().insert(Some(version), key.into(), vb)
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
    K: Type,
    V: Type,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as sealed::Memtable>::Pointer:
      Pointer<Comparator = GenericComparator<K>> + Ord + 'static,
  {
    self.as_core_mut().insert(Some(version), kb, vb)
  }

  /// Inserts a key-value pair into the WAL.
  #[inline]
  fn insert<'a>(
    &mut self,
    version: u64,
    key: impl Into<Generic<'a, K>>,
    value: impl Into<Generic<'a, V>>,
  ) -> Result<(), Among<K::Error, V::Error, Error>>
  where
    K: Type + 'a,
    V: Type + 'a,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as sealed::Memtable>::Pointer:
      Pointer<Comparator = GenericComparator<K>> + Ord + 'static,
  {
    self
      .as_core_mut()
      .insert(Some(version), key.into(), value.into())
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch<'a, B>(
    &mut self,
    batch: &'a mut B,
  ) -> Result<(), Among<K::Error, V::Error, Error>>
  where
    B: Batch<Self, Key = Generic<'a, K>, Value = Generic<'a, V>>,
    K: Type + 'a,
    V: Type + 'a,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as sealed::Memtable>::Pointer:
      Pointer<Comparator = GenericComparator<K>> + Ord + 'static,
  {
    self.as_core_mut().insert_batch(batch)
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch_with_key_builder<'a, B>(
    &mut self,
    batch: &'a mut B,
  ) -> Result<(), Among<<B::Key as BufWriter>::Error, V::Error, Error>>
  where
    B: Batch<Self, Value = Generic<'a, V>>,
    B::Key: BufWriter,
    K: Type + 'a,
    V: Type + 'a,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as sealed::Memtable>::Pointer:
      Pointer<Comparator = GenericComparator<K>> + Ord + 'static,
  {
    self.as_core_mut().insert_batch(batch)
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch_with_value_builder<'a, B>(
    &mut self,
    batch: &'a mut B,
  ) -> Result<(), Among<K::Error, <B::Value as BufWriter>::Error, Error>>
  where
    B: Batch<Self, Key = Generic<'a, K>>,
    B::Value: BufWriter,
    K: Type + 'a,
    V: Type + 'a,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as sealed::Memtable>::Pointer:
      Pointer<Comparator = GenericComparator<K>> + Ord + 'static,
  {
    self.as_core_mut().insert_batch(batch)
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch_with_builders<KB, VB, B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Among<KB::Error, VB::Error, Error>>
  where
    B: Batch<Self, Key = KB, Value = VB>,
    KB: BufWriter,
    VB: BufWriter,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as sealed::Memtable>::Pointer:
      Pointer<Comparator = GenericComparator<K>> + Ord + 'static,
  {
    self.as_core_mut().insert_batch(batch)
  }
}
