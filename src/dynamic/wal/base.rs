use core::{
  borrow::Borrow,
  ops::{Bound, RangeBounds},
};

use among::Among;
use dbutils::{buffer::VacantBuffer, checksum::BuildChecksumer};
#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
use rarena_allocator::Allocator;
use skl::{either::Either, KeySize};

use crate::{
  dynamic::{
    batch::Batch,
    memtable::{BaseTable, Memtable},
    sealed::{Constructable, Wal, WalReader},
    types::{Entry, BufWriter},
  },
  error::Error,
  types::{KeyBuilder, ValueBuilder},
  Options, WithoutVersion,
};

use super::iter::{BaseIter, Iter, Range};

/// An abstract layer for the immutable write-ahead log.
pub trait Reader: Constructable {
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
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
  fn path(&self) -> Option<&<<Self as Constructable>::Allocator as Allocator>::Path> {
    self.as_wal().path()
  }

  /// Returns the number of entries in the WAL.
  #[inline]
  fn len(&self) -> usize
  where
    Self::Memtable: Memtable,
  {
    self.as_wal().len()
  }

  /// Returns `true` if the WAL is empty.
  #[inline]
  fn is_empty(&self) -> bool
  where
    Self::Memtable: Memtable,
  {
    self.as_wal().is_empty()
  }

  /// Returns the maximum key size allowed in the WAL.
  #[inline]
  fn maximum_key_size(&self) -> KeySize {
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
  fn iter<'a>(
    &'a self,
  ) -> Iter<
    'a,
    &'a [u8],
    <<Self::Wal as Wal<Self::Checksumer>>::Memtable as BaseTable>::Iterator<'a, &'a [u8]>,
    Self::Memtable,
  >
  where
    Self::Memtable: Memtable,
    <Self::Memtable as BaseTable>::Entry<'a, &'a [u8]>: WithoutVersion,
  {
    let wal = self.as_wal();
    
    Iter::new(BaseIter::new(wal.iter()))
  }

  /// Returns an iterator over a subset of entries in the WAL.
  #[inline]
  fn range<'a, Q, R>(
    &'a self,
    range: R,
  ) -> Range<'a, &'a [u8], R, Q, <Self::Wal as Wal<Self::Checksumer>>::Memtable>
  where
    R: RangeBounds<Q>,
    Q: ?Sized + Borrow<[u8]>,
    Self::Memtable: Memtable,
    <Self::Memtable as BaseTable>::Entry<'a, &'a [u8]>: WithoutVersion,
  {
    let wal = self.as_wal();
    Range::new(BaseIter::new(wal.range(range)))
  }

  /// Returns the first key-value pair in the map. The key in this pair is the minimum key in the wal.
  #[inline]
  fn first<'a>(&'a self) -> Option<Entry<'a, <Self::Memtable as BaseTable>::Entry<'a, &'a [u8]>>>
  where
    Self::Memtable: Memtable,
    <Self::Memtable as BaseTable>::Entry<'a, &'a [u8]>: WithoutVersion,
  {
    self.as_wal().first().map(Entry::new)
  }

  /// Returns the last key-value pair in the map. The key in this pair is the maximum key in the wal.
  #[inline]
  fn last<'a>(&'a self) -> Option<Entry<'a, <Self::Memtable as BaseTable>::Entry<'a, &'a [u8]>>>
  where
    Self::Memtable: Memtable,
    <Self::Memtable as BaseTable>::Entry<'a, &'a [u8]>: WithoutVersion,
  {
    let wal = self.as_wal();
    WalReader::last(wal).map(Entry::new)
  }

  /// Returns `true` if the key exists in the WAL.
  #[inline]
  fn contains_key<Q>(&self, key: &Q) -> bool
  where
    Q: ?Sized + Borrow<[u8]>,
    Self::Memtable: Memtable,
  {
    self.as_wal().contains_key(key)
  }

  /// Gets the value associated with the key.
  #[inline]
  fn get<'a, Q>(&'a self, key: &Q) -> Option<Entry<'a, <Self::Memtable as BaseTable>::Entry<'a, &'a [u8]>>>
  where
    Q: ?Sized + Borrow<[u8]>,
    Self::Memtable: Memtable,
    <Self::Memtable as BaseTable>::Entry<'a, &'a [u8]>: WithoutVersion,
  {
    let wal = self.as_wal();
    wal.get(key).map(Entry::new)
  }

  /// Returns a value associated to the highest element whose key is below the given bound.
  /// If no such element is found then `None` is returned.
  #[inline]
  fn upper_bound<'a, Q>(
    &'a self,
    bound: Bound<&Q>,
  ) -> Option<Entry<'a, <Self::Memtable as BaseTable>::Entry<'a, &'a [u8]>>>
  where
    Q: ?Sized + Borrow<[u8]>,
    Self::Memtable: Memtable,
    <Self::Memtable as BaseTable>::Entry<'a, &'a [u8]>: WithoutVersion,
  {
    let wal = self.as_wal();
    wal.upper_bound(bound).map(Entry::new)
  }

  /// Returns a value associated to the lowest element whose key is above the given bound.
  /// If no such element is found then `None` is returned.
  #[inline]
  fn lower_bound<'a, Q>(
    &'a self,
    bound: Bound<&Q>,
  ) -> Option<Entry<'a, <Self::Memtable as BaseTable>::Entry<'a, &'a [u8]>>>
  where
    Q: ?Sized + Borrow<[u8]>,
    Self::Memtable: Memtable,
    <Self::Memtable as BaseTable>::Entry<'a, &'a [u8]>: WithoutVersion,
  {
    let wal = self.as_wal();
    wal.lower_bound(bound).map(Entry::new)
  }
}

impl<T> Reader for T
where
  T: Constructable,
  T::Memtable: Memtable,
{
}

/// An abstract layer for the write-ahead log.
pub trait Writer: Reader
where
  Self::Reader: Reader<Memtable = Self::Memtable>,
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
    self.as_wal().reserved_slice_mut()
  }

  /// Flushes the to disk.
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  fn flush(&self) -> Result<(), Error<Self::Memtable>> {
    self.as_wal().flush()
  }

  /// Flushes the to disk.
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  fn flush_async(&self) -> Result<(), Error<Self::Memtable>> {
    self.as_wal().flush_async()
  }

  /// Returns the read-only view for the WAL.
  fn reader(&self) -> Self::Reader;

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the key in place.
  ///
  /// See also [`insert_with_value_builder`](Writer::insert_with_value_builder) and [`insert_with_builders`](Writer::insert_with_builders).
  #[inline]
  fn insert_with_key_builder<'a, E>(
    &'a mut self,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>,
    value: &[u8],
  ) -> Result<(), Either<E, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: BaseTable,
    <Self::Memtable as BaseTable>::Entry<'a, &'a [u8]>: WithoutVersion,
  {
    self
      .as_wal()
      .insert(None, kb, value)
      .map_err(Among::into_left_right)
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the value in place.
  ///
  /// See also [`insert_with_key_builder`](Writer::insert_with_key_builder) and [`insert_with_builders`](Writer::insert_with_builders).
  #[inline]
  fn insert_with_value_builder<'a, E>(
    &'a mut self,
    key: &[u8],
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>,
  ) -> Result<(), Either<E, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: BaseTable,
    <Self::Memtable as BaseTable>::Entry<'a, &'a [u8]>: WithoutVersion,
  {
    self
      .as_wal()
      .insert::<&[u8], _>(None, key.into(), vb)
      .map_err(Among::into_middle_right)
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the key and value in place.
  #[inline]
  fn insert_with_builders<'a, KE, VE>(
    &'a mut self,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, KE>>,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, VE>>,
  ) -> Result<(), Among<KE, VE, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: BaseTable,
    <Self::Memtable as BaseTable>::Entry<'a, &'a [u8]>: WithoutVersion,
  {
    self.as_wal().insert(None, kb, vb)
  }

  /// Inserts a key-value pair into the WAL.
  #[inline]
  fn insert<'a>(&'a mut self, key: &[u8], value: &[u8]) -> Result<(), Error<Self::Memtable>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: BaseTable,
    <Self::Memtable as BaseTable>::Entry<'a, &'a [u8]>: WithoutVersion,
  {
    self
      .as_wal()
      .insert(None, key, value)
      .map_err(Among::unwrap_right)
  }

  /// Removes a key-value pair from the WAL. This method
  /// allows the caller to build the key in place.
  #[inline]
  fn remove_with_builder<'a, KE>(
    &'a mut self,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, KE>>,
  ) -> Result<(), Either<KE, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: BaseTable,
    <Self::Memtable as BaseTable>::Entry<'a, &'a [u8]>: WithoutVersion,
  {
    self.as_wal().remove(None, kb)
  }

  /// Removes a key-value pair from the WAL.
  #[inline]
  fn remove<'a>(&'a mut self, key: &[u8]) -> Result<(), Error<Self::Memtable>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: BaseTable,
    <Self::Memtable as BaseTable>::Entry<'a, &'a [u8]>: WithoutVersion,
  {
    self
      .as_wal()
      .remove::<&[u8]>(None, key.into())
      .map_err(Either::unwrap_right)
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch<'a, B>(&'a mut self, batch: &mut B) -> Result<(), Error<Self::Memtable>>
  where
    B: Batch<Self::Memtable>,
    B::Key: AsRef<[u8]>,
    B::Value: AsRef<[u8]>,
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: BaseTable,
    <Self::Memtable as BaseTable>::Entry<'a, &'a [u8]>: WithoutVersion,
  {
    self
      .as_wal()
      .insert_batch::<Self, _>(batch)
      .map_err(Among::unwrap_right)
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch_with_key_builder<'a, B>(
    &'a mut self,
    batch: &mut B,
  ) -> Result<(), Either<<B::Key as BufWriter>::Error, Error<Self::Memtable>>>
  where
    B: Batch<Self::Memtable>,
    B::Key: BufWriter,
    B::Value: AsRef<[u8]>,
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: BaseTable,
    <Self::Memtable as BaseTable>::Entry<'a, &'a [u8]>: WithoutVersion,
  {
    self
      .as_wal()
      .insert_batch::<Self, _>(batch)
      .map_err(Among::into_left_right)
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch_with_value_builder<'a, B>(
    &'a mut self,
    batch: &mut B,
  ) -> Result<(), Either<<B::Value as BufWriter>::Error, Error<Self::Memtable>>>
  where
    B: Batch<Self::Memtable>,
    B::Key: AsRef<[u8]>,
    B::Value: BufWriter,
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: BaseTable,
    <Self::Memtable as BaseTable>::Entry<'a, &'a [u8]>: WithoutVersion,
  {
    self
      .as_wal()
      .insert_batch::<Self, _>(batch)
      .map_err(Among::into_middle_right)
  }

  /// Inserts a batch of key-value pairs into the WAL.
  #[inline]
  fn insert_batch_with_builders<'a, KB, VB, B>(
    &'a mut self,
    batch: &mut B,
  ) -> Result<(), Among<KB::Error, VB::Error, Error<Self::Memtable>>>
  where
    B: Batch<Self::Memtable, Key = KB, Value = VB>,
    KB: BufWriter,
    VB: BufWriter,
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: BaseTable,
    <Self::Memtable as BaseTable>::Entry<'a, &'a [u8]>: WithoutVersion,
  {
    self.as_wal().insert_batch::<Self, _>(batch)
  }
}
