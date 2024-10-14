use core::{
  borrow::Borrow,
  ops::{Bound, RangeBounds},
};

use among::Among;
use dbutils::{buffer::VacantBuffer, equivalent::Comparable, CheapClone};
use rarena_allocator::{either::Either, Allocator};

use crate::{
  batch::Batch,
  checksum::BuildChecksumer,
  entry::BufWriter,
  error::Error,
  memtable::{self, Memtable},
  sealed::{Constructable, Pointer, Wal, WithoutVersion},
  types::{KeyBuilder, ValueBuilder},
  Options,
};

use super::{
  entry::Entry,
  iter::{Iter, Keys, Range, RangeKeys, RangeValues, Values},
};

/// An abstract layer for the immutable write-ahead log.
pub trait Reader: Constructable
where
  <Self::Memtable as Memtable>::Pointer: WithoutVersion,
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

  /// Returns `true` if the WAL contains the specified key.
  #[inline]
  fn contains_key<Q>(&self, key: &Q) -> bool
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    <Self::Memtable as Memtable>::Pointer:
      Borrow<Q> + Borrow<[u8]> + Pointer<Comparator = Self::Comparator> + Ord,
  {
    self.as_core().contains_key(None, key)
  }

  /// Returns an iterator over the entries in the WAL.
  #[inline]
  fn iter(
    &self,
    version: u64,
  ) -> Iter<
    '_,
    <<Self::Wal as Wal<Self::Comparator, Self::Checksumer>>::Memtable as memtable::Memtable>::Iterator<'_>,
     Self::Memtable,
  >
  where
    <Self::Memtable as Memtable>::Pointer: Pointer<Comparator = Self::Comparator>
  {
    Iter::new(self.as_core().iter(Some(version)))
  }

  /// Returns an iterator over a subset of entries in the WAL.
  #[inline]
  fn range<Q, R>(
    &self,
    version: u64,
    range: R,
  ) -> Range<
    '_,
    <<Self::Wal as Wal<Self::Comparator, Self::Checksumer>>::Memtable as memtable::Memtable>::Range<
      '_,
      Q,
      R,
    >,
    Self::Memtable,
  >
  where
    R: RangeBounds<Q>,
    Q: ?Sized + Comparable<<Self::Memtable as Memtable>::Pointer>,
    <Self::Memtable as Memtable>::Pointer: Pointer<Comparator = Self::Comparator>,
  {
    Range::new(self.as_core().range(Some(version), range))
  }

  /// Returns an iterator over the keys in the WAL.
  #[inline]
  fn keys(
    &self,
    version: u64,
  ) -> Keys<
    '_,
    <<Self::Wal as Wal<Self::Comparator, Self::Checksumer>>::Memtable as memtable::Memtable>::Iterator<'_>,
     Self::Memtable,
  >
  where
    <Self::Memtable as Memtable>::Pointer: Pointer<Comparator = Self::Comparator>
  {
    Keys::new(self.as_core().iter(Some(version)))
  }

  /// Returns an iterator over a subset of keys in the WAL.
  #[inline]
  fn range_keys<Q, R>(
    &self,
    version: u64,
    range: R,
  ) -> RangeKeys<
    '_,
    <<Self::Wal as Wal<Self::Comparator, Self::Checksumer>>::Memtable as memtable::Memtable>::Range<
      '_,
      Q,
      R,
    >,
    Self::Memtable,
  >
  where
    R: RangeBounds<Q>,
    Q: ?Sized + Comparable<<Self::Memtable as Memtable>::Pointer>,
    <Self::Memtable as Memtable>::Pointer: Pointer<Comparator = Self::Comparator>,
  {
    RangeKeys::new(self.as_core().range(Some(version), range))
  }

  /// Returns an iterator over the values in the WAL.
  #[inline]
  fn values(
    &self,
    version: u64,
  ) -> Values<
    '_,
    <<Self::Wal as Wal<Self::Comparator, Self::Checksumer>>::Memtable as memtable::Memtable>::Iterator<'_>,
     Self::Memtable,
  >
  where
    <Self::Memtable as Memtable>::Pointer: Pointer<Comparator = Self::Comparator>
  {
    Values::new(self.as_core().iter(Some(version)))
  }

  /// Returns an iterator over a subset of values in the WAL.
  #[inline]
  fn range_values<Q, R>(
    &self,
    version: u64,
    range: R,
  ) -> RangeValues<
    '_,
    <<Self::Wal as Wal<Self::Comparator, Self::Checksumer>>::Memtable as memtable::Memtable>::Range<
      '_,
      Q,
      R,
    >,
    Self::Memtable,
  >
  where
    R: RangeBounds<Q>,
    Q: ?Sized + Comparable<<Self::Memtable as Memtable>::Pointer>,
    <Self::Memtable as Memtable>::Pointer: Pointer<Comparator = Self::Comparator>,
  {
    RangeValues::new(self.as_core().range(Some(version), range))
  }

  /// Returns the first key-value pair in the map. The key in this pair is the minimum key in the wal.
  #[inline]
  fn first(&self) -> Option<Entry<'_, <Self::Memtable as Memtable>::Item<'_>>>
  where
    <Self::Memtable as Memtable>::Pointer: Pointer<Comparator = Self::Comparator> + Ord,
  {
    self.as_core().first(None).map(Entry::new)
  }

  /// Returns the last key-value pair in the map. The key in this pair is the maximum key in the wal.
  #[inline]
  fn last(&self) -> Option<Entry<'_, <Self::Memtable as Memtable>::Item<'_>>>
  where
    <Self::Memtable as Memtable>::Pointer: Pointer<Comparator = Self::Comparator> + Ord,
  {
    Wal::last(self.as_core(), None).map(Entry::new)
  }

  /// Returns the value associated with the key.
  #[inline]
  fn get<Q>(&self, key: &Q) -> Option<Entry<'_, <Self::Memtable as Memtable>::Item<'_>>>
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    <Self::Memtable as Memtable>::Pointer:
      Borrow<Q> + Borrow<[u8]> + Pointer<Comparator = Self::Comparator> + Ord,
  {
    self.as_core().get(None, key).map(Entry::new)
  }

  /// Returns a value associated to the highest element whose key is below the given bound.
  /// If no such element is found then `None` is returned.
  #[inline]
  fn upper_bound<Q>(
    &self,
    bound: Bound<&Q>,
  ) -> Option<Entry<'_, <Self::Memtable as Memtable>::Item<'_>>>
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    <Self::Memtable as Memtable>::Pointer:
      Borrow<Q> + Borrow<[u8]> + Pointer<Comparator = Self::Comparator> + Ord,
  {
    self.as_core().upper_bound(None, bound).map(Entry::new)
  }

  /// Returns a value associated to the lowest element whose key is above the given bound.
  /// If no such element is found then `None` is returned.
  #[inline]
  fn lower_bound<Q>(
    &self,
    bound: Bound<&Q>,
  ) -> Option<Entry<'_, <Self::Memtable as Memtable>::Item<'_>>>
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    <Self::Memtable as Memtable>::Pointer:
      Borrow<Q> + Borrow<[u8]> + Pointer<Comparator = Self::Comparator> + Ord,
  {
    self.as_core().lower_bound(None, bound).map(Entry::new)
  }
}

impl<T> Reader for T
where
  T: Constructable,
  <T::Memtable as Memtable>::Pointer: WithoutVersion,
{
}

/// An abstract layer for the write-ahead log.
pub trait Writer: Reader
where
  <Self::Memtable as Memtable>::Pointer: WithoutVersion,
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

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the key in place.
  ///
  /// See also [`insert_with_value_builder`](Wal::insert_with_value_builder) and [`insert_with_builders`](Wal::insert_with_builders).
  #[inline]
  fn insert_with_key_builder<E>(
    &mut self,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
    value: &[u8],
  ) -> Result<(), Either<E, Error>>
  where
    Self::Comparator: CheapClone,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as Memtable>::Pointer:
      Pointer<Comparator = Self::Comparator> + Borrow<[u8]> + Ord + 'static,
  {
    self
      .as_core_mut()
      .insert(None, kb, value)
      .map_err(Among::into_left_right)
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the value in place.
  ///
  /// See also [`insert_with_key_builder`](Wal::insert_with_key_builder) and [`insert_with_builders`](Wal::insert_with_builders).
  #[inline]
  fn insert_with_value_builder<E>(
    &mut self,
    key: &[u8],
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
  ) -> Result<(), Either<E, Error>>
  where
    Self::Comparator: CheapClone,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as Memtable>::Pointer:
      Pointer<Comparator = Self::Comparator> + Borrow<[u8]> + Ord + 'static,
  {
    self
      .as_core_mut()
      .insert(None, key, vb)
      .map_err(Among::into_middle_right)
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the key and value in place.
  #[inline]
  fn insert_with_builders<KE, VE>(
    &mut self,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), KE>>,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), VE>>,
  ) -> Result<(), Among<KE, VE, Error>>
  where
    Self::Comparator: CheapClone,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as Memtable>::Pointer:
      Pointer<Comparator = Self::Comparator> + Borrow<[u8]> + Ord + 'static,
  {
    self.as_core_mut().insert(None, kb, vb)
  }

  /// Inserts a key-value pair into the WAL.
  #[inline]
  fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error>
  where
    Self::Comparator: CheapClone,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as Memtable>::Pointer: Pointer<Comparator = Self::Comparator> + Ord + 'static,
  {
    self
      .as_core_mut()
      .insert(None, key, value)
      .map_err(Among::unwrap_right)
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
    Self::Comparator: CheapClone,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as Memtable>::Pointer: Pointer<Comparator = Self::Comparator> + Ord + 'static,
  {
    self.as_core_mut().insert_batch(batch)
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch_with_key_builder<B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Either<<B::Key as BufWriter>::Error, Error>>
  where
    B: Batch<Self>,
    B::Key: BufWriter,
    B::Value: Borrow<[u8]>,
    Self::Comparator: CheapClone,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as Memtable>::Pointer: Pointer<Comparator = Self::Comparator> + Ord + 'static,
  {
    self.as_core_mut().insert_batch(batch).map_err(|e| match e {
      Among::Left(e) => Either::Left(e),
      Among::Middle(e) => Either::Right(e.into()),
      Among::Right(e) => Either::Right(e),
    })
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch_with_value_builder<B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Either<<B::Value as BufWriter>::Error, Error>>
  where
    B: Batch<Self>,
    B::Value: BufWriter,
    B::Key: Borrow<[u8]>,
    Self::Comparator: CheapClone,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as Memtable>::Pointer: Pointer<Comparator = Self::Comparator> + Ord + 'static,
  {
    self.as_core_mut().insert_batch(batch).map_err(|e| match e {
      Among::Left(e) => Either::Right(e.into()),
      Among::Middle(e) => Either::Left(e),
      Among::Right(e) => Either::Right(e),
    })
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch<B>(&mut self, batch: &mut B) -> Result<(), Error>
  where
    B: Batch<Self>,
    B::Key: Borrow<[u8]>,
    B::Value: Borrow<[u8]>,
    Self::Comparator: CheapClone,
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as Memtable>::Pointer: Pointer<Comparator = Self::Comparator> + Ord + 'static,
  {
    self.as_core_mut().insert_batch(batch).map_err(Into::into)
  }
}
