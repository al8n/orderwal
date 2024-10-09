use core::{
  borrow::Borrow,
  ops::{Bound, RangeBounds},
};

use among::Among;
use dbutils::{buffer::VacantBuffer, CheapClone, Comparator};
use rarena_allocator::either::Either;

use crate::{error::Error, KeyBuilder, ValueBuilder};

use super::{
  batch::{Batch, BatchWithBuilders, BatchWithKeyBuilder, BatchWithValueBuilder},
  checksum::BuildChecksumer,
  iter::*,
  pointer::Pointer,
  sealed::{Base, Constructor, WalCore},
  Options,
};

/// An abstract layer for the immutable write-ahead log.
pub trait ImmutableWal<C, S>: Constructor<C, S, Pointer = Pointer<C>> {
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
  fn path(&self) -> Option<&std::path::Path> {
    // self.allocator().path().map(|p| p.as_path())
    todo!()
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
    C: Comparator,
  {
    self.as_core().contains_key(None, key)
  }

  /// Returns an iterator over the entries in the WAL.
  #[inline]
  fn iter(
    &self,
  ) -> Iter<'_, <<Self::Core as WalCore<Pointer<C>, C, S>>::Base as Base>::Iterator<'_>, Pointer<C>>
  {
    self.as_core().iter(None)
  }

  /// Returns an iterator over a subset of entries in the WAL.
  #[inline]
  fn range<Q, R>(
    &self,
    range: R,
  ) -> Range<
    '_,
    <<Self::Core as WalCore<Pointer<C>, C, S>>::Base as Base>::Range<'_, Q, R>,
    Pointer<C>,
  >
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    C: Comparator,
  {
    self.as_core().range(None, range)
  }

  /// Returns an iterator over the keys in the WAL.
  #[inline]
  fn keys(
    &self,
  ) -> Keys<'_, <<Self::Core as WalCore<Pointer<C>, C, S>>::Base as Base>::Iterator<'_>, Pointer<C>>
  {
    self.as_core().keys(None)
  }

  /// Returns an iterator over a subset of keys in the WAL.
  #[inline]
  fn range_keys<Q, R>(
    &self,
    range: R,
  ) -> RangeKeys<
    '_,
    <<Self::Core as WalCore<Pointer<C>, C, S>>::Base as Base>::Range<'_, Q, R>,
    Pointer<C>,
  >
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    C: Comparator,
  {
    self.as_core().range_keys(None, range)
  }

  /// Returns an iterator over the values in the WAL.
  #[inline]
  fn values(
    &self,
  ) -> Values<'_, <<Self::Core as WalCore<Pointer<C>, C, S>>::Base as Base>::Iterator<'_>, Pointer<C>>
  {
    self.as_core().values(None)
  }

  /// Returns an iterator over a subset of values in the WAL.
  #[inline]
  fn range_values<Q, R>(
    &self,
    range: R,
  ) -> RangeValues<
    '_,
    <<Self::Core as WalCore<Pointer<C>, C, S>>::Base as Base>::Range<'_, Q, R>,
    Pointer<C>,
  >
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    C: Comparator,
  {
    self.as_core().range_values(None, range)
  }

  /// Returns the first key-value pair in the map. The key in this pair is the minimum key in the wal.
  #[inline]
  fn first(&self) -> Option<(&[u8], &[u8])>
  where
    C: Comparator,
  {
    self.as_core().first(None)
  }

  /// Returns the last key-value pair in the map. The key in this pair is the maximum key in the wal.
  #[inline]
  fn last(&self) -> Option<(&[u8], &[u8])>
  where
    C: Comparator,
  {
    WalCore::last(self.as_core(), None)
  }

  /// Returns the value associated with the key.
  #[inline]
  fn get<Q>(&self, key: &Q) -> Option<&[u8]>
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    C: Comparator,
  {
    self.as_core().get(None, key)
  }

  /// Returns a value associated to the highest element whose key is below the given bound.
  /// If no such element is found then `None` is returned.
  #[inline]
  fn upper_bound<Q>(&self, bound: Bound<&Q>) -> Option<&[u8]>
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    C: Comparator,
  {
    self.as_core().upper_bound(None, bound)
  }

  /// Returns a value associated to the lowest element whose key is above the given bound.
  /// If no such element is found then `None` is returned.
  #[inline]
  fn lower_bound<Q>(&self, bound: Bound<&Q>) -> Option<&[u8]>
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    C: Comparator,
  {
    self.as_core().lower_bound(None, bound)
  }
}

impl<T, C, S> ImmutableWal<C, S> for T where T: Constructor<C, S, Pointer = Pointer<C>> {}

/// An abstract layer for the write-ahead log.
pub trait Wal<C, S>: ImmutableWal<C, S> {
  /// The read only reader type for this wal.
  type Reader: ImmutableWal<C, S, Pointer = Self::Pointer>
  where
    Self::Core: WalCore<Pointer<C>, C, S> + 'static,
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
  fn get_or_insert(&mut self, key: &[u8], value: &[u8]) -> Result<Option<&[u8]>, Error>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    self.as_core_mut().get_or_insert(None, key, value)
  }

  /// Get or insert a new entry into the WAL.
  #[inline]
  fn get_or_insert_with_value_builder<E>(
    &mut self,
    key: &[u8],
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
  ) -> Result<Option<&[u8]>, Either<E, Error>>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    self
      .as_core_mut()
      .get_or_insert_with_value_builder(None, key, vb)
  }

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
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    self.as_core_mut().insert_with_key_builder(None, kb, value)
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
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    self.as_core_mut().insert_with_value_builder(None, key, vb)
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
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    self.as_core_mut().insert_with_builders(None, kb, vb)
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch_with_key_builder<B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Either<B::Error, Error>>
  where
    B: BatchWithKeyBuilder<Pointer<C>>,
    B::Value: Borrow<[u8]>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
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
    B: BatchWithValueBuilder<Pointer<C>>,
    B::Key: Borrow<[u8]>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
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
    B: BatchWithBuilders<Pointer<C>>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    self.as_core_mut().insert_batch_with_builders(batch)
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch<B>(&mut self, batch: &mut B) -> Result<(), Error>
  where
    B: Batch<Pointer = Pointer<C>>,
    B::Key: Borrow<[u8]>,
    B::Value: Borrow<[u8]>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    self.as_core_mut().insert_batch(batch)
  }

  /// Inserts a key-value pair into the WAL.
  #[inline]
  fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    WalCore::insert(self.as_core_mut(), None, key, value)
  }
}
