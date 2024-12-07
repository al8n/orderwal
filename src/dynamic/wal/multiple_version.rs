use core::{
  borrow::Borrow,
  ops::{Bound, RangeBounds},
};

use among::Among;
use dbutils::{buffer::VacantBuffer, checksum::BuildChecksumer};
#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
use rarena_allocator::Allocator;
use skl::either::Either;

use crate::{
  dynamic::{
    batch::Batch,
    memtable::{BaseTable, MultipleVersionMemtable},
    sealed::{Constructable, MultipleVersionWalReader, Wal},
    types::BufWriter,
  },
  error::Error,
  types::{KeyBuilder, ValueBuilder},
  Options, WithVersion,
};

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

  /// Returns the maximum version in the WAL.
  #[inline]
  fn maximum_version(&self) -> u64
  where
    Self::Memtable: MultipleVersionMemtable + 'static,
  {
    Wal::memtable(self.as_wal()).maximum_version()
  }

  /// Returns the minimum version in the WAL.
  #[inline]
  fn minimum_version(&self) -> u64
  where
    Self::Memtable: MultipleVersionMemtable + 'static,
  {
    Wal::memtable(self.as_wal()).minimum_version()
  }

  /// Returns `true` if the WAL may contain an entry whose version is less or equal to the given version.
  #[inline]
  fn may_contain_version(&self, version: u64) -> bool
  where
    Self::Memtable: MultipleVersionMemtable + 'static,
  {
    Wal::memtable(self.as_wal()).may_contain_version(version)
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
    version: u64,
  ) -> <<Self::Wal as Wal<Self::Checksumer>>::Memtable as BaseTable>::Iterator<'a>
  where
    Self::Memtable: MultipleVersionMemtable + 'static,
    <Self::Memtable as BaseTable>::Entry<'a>: WithVersion,
  {
    self.as_wal().iter(version)
  }

  /// Returns an iterator over a subset of entries in the WAL.
  #[inline]
  fn range<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> <<Self::Wal as Wal<Self::Checksumer>>::Memtable as BaseTable>::Range<'a, Q, R>
  where
    R: RangeBounds<Q>,
    Q: ?Sized + Borrow<[u8]>,
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Entry<'a>: WithVersion,
  {
    self.as_wal().range(version, range)
  }

  /// Returns the first key-value pair in the map. The key in this pair is the minimum key in the wal.
  #[inline]
  fn first<'a>(&'a self, version: u64) -> Option<<Self::Memtable as BaseTable>::Entry<'a>>
  where
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Entry<'a>: WithVersion,
  {
    self.as_wal().first(version)
  }

  /// Returns the last key-value pair in the map. The key in this pair is the maximum key in the wal.
  #[inline]
  fn last<'a>(&'a self, version: u64) -> Option<<Self::Memtable as BaseTable>::Entry<'a>>
  where
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Entry<'a>: WithVersion,
  {
    self.as_wal().last(version)
  }

  /// Returns `true` if the key exists in the WAL.
  #[inline]
  fn contains_key<'a, Q>(&'a self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Borrow<[u8]>,
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Entry<'a>: WithVersion,
  {
    self.as_wal().contains_key(version, key)
  }

  /// Gets the value associated with the key.
  #[inline]
  fn get<'a, Q>(&'a self, version: u64, key: &Q) -> Option<<Self::Memtable as BaseTable>::Entry<'a>>
  where
    Q: ?Sized + Borrow<[u8]>,
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Entry<'a>: WithVersion,
  {
    self.as_wal().get(version, key)
  }

  /// Returns a value associated to the highest element whose key is below the given bound.
  /// If no such element is found then `None` is returned.
  #[inline]
  fn upper_bound<'a, Q>(
    &'a self,
    version: u64,
    bound: Bound<&'a Q>,
  ) -> Option<<Self::Memtable as BaseTable>::Entry<'a>>
  where
    Q: ?Sized + Borrow<[u8]>,
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Entry<'a>: WithVersion,
  {
    self.as_wal().upper_bound(version, bound)
  }

  /// Returns a value associated to the lowest element whose key is above the given bound.
  /// If no such element is found then `None` is returned.
  #[inline]
  fn lower_bound<'a, Q>(
    &'a self,
    version: u64,
    bound: Bound<&'a Q>,
  ) -> Option<<Self::Memtable as BaseTable>::Entry<'a>>
  where
    Q: ?Sized + Borrow<[u8]>,
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Entry<'a>: WithVersion,
  {
    self.as_wal().lower_bound(version, bound)
  }
}

impl<T> Reader for T
where
  T: Constructable,
  T::Memtable: MultipleVersionMemtable,
{
}

/// An abstract layer for the write-ahead log.
pub trait Writer: Reader
where
  Self::Reader: Reader<Memtable = Self::Memtable>,
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
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
  fn flush(&self) -> Result<(), Error<Self::Memtable>> {
    self.as_wal().flush()
  }

  /// Flushes the to disk.
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
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
    version: u64,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>,
    value: &[u8],
  ) -> Result<(), Either<E, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Entry<'a>: WithVersion,
  {
    Wal::insert::<_, &[u8]>(self.as_wal(), Some(version), kb, value).map_err(Among::into_left_right)
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the value in place.
  ///
  /// See also [`insert_with_key_builder`](Writer::insert_with_key_builder) and [`insert_with_builders`](Writer::insert_with_builders).
  #[inline]
  fn insert_with_value_builder<'a, E>(
    &mut self,
    version: u64,
    key: &[u8],
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, E>>,
  ) -> Result<(), Either<E, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: MultipleVersionMemtable + 'a,
    <Self::Memtable as BaseTable>::Entry<'a>: WithVersion,
  {
    Wal::insert::<&[u8], _>(self.as_wal(), Some(version), key, vb).map_err(Among::into_middle_right)
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the key and value in place.
  #[inline]
  fn insert_with_builders<'a, KE, VE>(
    &'a mut self,
    version: u64,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, KE>>,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, VE>>,
  ) -> Result<(), Among<KE, VE, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Entry<'a>: WithVersion,
  {
    Wal::insert(self.as_wal(), Some(version), kb, vb)
  }

  /// Inserts a key-value pair into the WAL.
  #[inline]
  fn insert<'a>(
    &'a mut self,
    version: u64,
    key: &[u8],
    value: &[u8],
  ) -> Result<(), Error<Self::Memtable>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Entry<'a>: WithVersion,
  {
    Wal::insert(self.as_wal(), Some(version), key, value).map_err(Among::unwrap_right)
  }

  /// Removes a key-value pair from the WAL. This method
  /// allows the caller to build the key in place.
  #[inline]
  fn remove_with_builder<'a, KE>(
    &'a mut self,
    version: u64,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<usize, KE>>,
  ) -> Result<(), Either<KE, Error<Self::Memtable>>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Entry<'a>: WithVersion,
  {
    self.as_wal().remove(Some(version), kb)
  }

  /// Removes a key-value pair from the WAL.
  #[inline]
  fn remove<'a>(&'a mut self, version: u64, key: &[u8]) -> Result<(), Error<Self::Memtable>>
  where
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Entry<'a>: WithVersion,
  {
    self
      .as_wal()
      .remove(Some(version), key)
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
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Entry<'a>: WithVersion,
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
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Entry<'a>: WithVersion,
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
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Entry<'a>: WithVersion,
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
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Entry<'a>: WithVersion,
  {
    self.as_wal().insert_batch::<Self, _>(batch)
  }
}
