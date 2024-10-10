use core::{
  borrow::Borrow, marker::PhantomData, ops::{Bound, RangeBounds}
};

use among::Among;
use dbutils::{buffer::VacantBuffer, equivalent::Comparable, traits::{KeyRef, Type, TypeRef}, CheapClone, Comparator};
use rarena_allocator::{either::Either, Allocator};

use super::{
  batch::{Batch, BatchWithBuilders, BatchWithKeyBuilder, BatchWithValueBuilder},
  checksum::BuildChecksumer,
  error::Error,
  iter::*,
  pointer::WithoutVersion,
  sealed::{Base, Constructable, Pointer, Core},
  KeyBuilder, Options, ValueBuilder,
};


pub struct GenericComparator<K: ?Sized> {
  _k: PhantomData<K>,
}

impl<K: ?Sized> CheapClone for GenericComparator<K> {}

impl<K: ?Sized> Clone for GenericComparator<K> {
  fn clone(&self) -> Self {
    *self
  }
}

impl<K: ?Sized> Copy for GenericComparator<K> {}

impl<K: ?Sized> core::fmt::Debug for GenericComparator<K> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("GenericComparator")
      .finish()
  }
}

impl<K> Comparator for GenericComparator<K>
where
  K: ?Sized + Type,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
{
  fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
    unsafe { <K::Ref<'_> as KeyRef<'_, K>>::compare_binary(a, b) }
  }

  fn contains(&self, start_bound: Bound<&[u8]>, end_bound: Bound<&[u8]>, key: &[u8]) -> bool {
    unsafe {
      let start = start_bound.map(|b| <K::Ref<'_> as TypeRef<'_>>::from_slice(b));
      let end = end_bound.map(|b| <K::Ref<'_> as TypeRef<'_>>::from_slice(b));
      let key = <K::Ref<'_> as TypeRef<'_>>::from_slice(key);
      
      (start, end).contains(&key)
    }
  }
}

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
  fn path(&self) -> Option<&<<Self as Constructable<GenericComparator<K>, S>>::Allocator as Allocator>::Path> {
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
  ) -> Iter<
    '_,
    <<Self::Core as Core<Self::Pointer, GenericComparator<K>, S>>::Base as Base>::Iterator<'_>,
    Self::Pointer,
  > {
    self.as_core().iter(Some(version))
  }

  /// Returns an iterator over a subset of entries in the WAL.
  #[inline]
  fn range<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Range<
    'a,
    <<Self::Core as Core<Self::Pointer, GenericComparator<K>, S>>::Base as Base>::Range<'a, Q, R>,
    Self::Pointer,
  >
  where
    R: RangeBounds<Q>,
    Q: ?Sized + Ord + Comparable<Self::Pointer>,
    K: Type,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>> + Comparable<K::Ref<'a>>,
  {
    self.as_core().range(Some(version), range)
  }

  /// Returns an iterator over the keys in the WAL.
  #[inline]
  fn keys(
    &self,
    version: u64,
  ) -> Keys<
    '_,
    <<Self::Core as Core<Self::Pointer, GenericComparator<K>, S>>::Base as Base>::Iterator<'_>,
    Self::Pointer,
  > {
    self.as_core().keys(Some(version))
  }

  /// Returns an iterator over a subset of keys in the WAL.
  #[inline]
  fn range_keys<Q, R>(
    &self,
    version: u64,
    range: R,
  ) -> RangeKeys<
    '_,
    <<Self::Core as Core<Self::Pointer, GenericComparator<K>, S>>::Base as Base>::Range<'_, Q, R>,
    Self::Pointer,
  >
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    Self::Pointer: Borrow<Q> + Borrow<[u8]> + Pointer<Comparator = GenericComparator<K>> + Ord,
  {
    self.as_core().range_keys(Some(version), range)
  }

  /// Returns an iterator over the values in the WAL.
  #[inline]
  fn values(
    &self,
    version: u64,
  ) -> Values<
    '_,
    <<Self::Core as Core<Self::Pointer, GenericComparator<K>, S>>::Base as Base>::Iterator<'_>,
    Self::Pointer,
  > {
    self.as_core().values(Some(version))
  }

  /// Returns an iterator over a subset of values in the WAL.
  #[inline]
  fn range_values<Q, R>(
    &self,
    version: u64,
    range: R,
  ) -> RangeValues<
    '_,
    <<Self::Core as Core<Self::Pointer, GenericComparator<K>, S>>::Base as Base>::Range<'_, Q, R>,
    Self::Pointer,
  >
  where
    R: RangeBounds<Q>,
    Self::Pointer: Borrow<Q> + Pointer<Comparator = GenericComparator<K>> + Ord,
    Q: Ord + ?Sized,
  {
    self.as_core().range_values(Some(version), range)
  }

  /// Returns the first key-value pair in the map. The key in this pair is the minimum key in the wal.
  #[inline]
  fn first(&self, version: u64) -> Option<(&[u8], &[u8])>
  where
    Self::Pointer: Pointer<Comparator = GenericComparator<K>> + Ord,
  {
    self.as_core().first(Some(version))
  }

  /// Returns the last key-value pair in the map. The key in this pair is the maximum key in the wal.
  #[inline]
  fn last(&self, version: u64) -> Option<(&[u8], &[u8])>
  where
    Self::Pointer: Pointer<Comparator = GenericComparator<K>> + Ord,
  {
    Core::last(self.as_core(), Some(version))
  }

  /// Returns `true` if the key exists in the WAL.
  #[inline]
  fn contains_key<'a, Q>(&'a self, key: &Q) -> bool
  where
    Q: ?Sized + Ord + Comparable<Self::Pointer>,
    K: Type,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>> + Comparable<K::Ref<'a>>,
  {
    self.as_core().contains_key(None, key)
  }

  // /// Returns `true` if the key exists in the WAL.
  // ///
  // /// ## Safety
  // /// - The given `key` must be valid to construct to `K::Ref` without remaining.
  // #[inline]
  // unsafe fn contains_key_by_bytes(&self, key: &[u8]) -> bool {
  //   self.core.contains_key_by_bytes(key)
  // }

  // /// Gets the value associated with the key.
  // #[inline]
  // fn get<'a, Q>(&'a self, key: &Q) -> Option<GenericEntryRef<'a, K, V>>
  // where
  //   Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  //   V: Type,
  // {
  //   self.core.get(key)
  // }

  // /// Gets the value associated with the key.
  // ///
  // /// ## Safety
  // /// - The given `key` must be valid to construct to `K::Ref` without remaining.
  // #[inline]
  // unsafe fn get_by_bytes(&self, key: &[u8]) -> Option<GenericEntryRef<'_, K, V>>
  // where
  //   V: Type,
  // {
  //   self.core.get_by_bytes(key)
  // }

  // /// Returns a value associated to the highest element whose key is below the given bound.
  // /// If no such element is found then `None` is returned.
  // #[inline]
  // fn upper_bound<'a, Q>(&'a self, bound: Bound<&Q>) -> Option<GenericEntryRef<'a, K, V>>
  // where
  //   Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  //   V: Type,
  // {
  //   self.core.upper_bound(bound)
  // }

  // /// Returns a value associated to the highest element whose key is below the given bound.
  // /// If no such element is found then `None` is returned.
  // ///
  // /// ## Safety
  // /// - The given `key` in `Bound` must be valid to construct to `K::Ref` without remaining.
  // #[inline]
  // unsafe fn upper_bound_by_bytes(
  //   &self,
  //   bound: Bound<&[u8]>,
  // ) -> Option<GenericEntryRef<'_, K, V>>
  // where
  //   V: Type,
  // {
  //   self.core.upper_bound_by_bytes(bound)
  // }

  // /// Returns a value associated to the lowest element whose key is above the given bound.
  // /// If no such element is found then `None` is returned.
  // #[inline]
  // pub fn lower_bound<'a, Q>(&'a self, bound: Bound<&Q>) -> Option<GenericEntryRef<'a, K, V>>
  // where
  //   Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  //   V: Type,
  // {
  //   self.core.lower_bound(bound)
  // }

  // /// Returns a value associated to the lowest element whose key is above the given bound.
  // /// If no such element is found then `None` is returned.
  // ///
  // /// ## Safety
  // /// - The given `key` in `Bound` must be valid to construct to `K::Ref` without remaining.
  // #[inline]
  // pub unsafe fn lower_bound_by_bytes(
  //   &self,
  //   bound: Bound<&[u8]>,
  // ) -> Option<GenericEntryRef<'_, K, V>>
  // where
  //   V: Type,
  // {
  //   self.core.lower_bound_by_bytes(bound)
  // }
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
  fn get_or_insert(
    &mut self,
    version: u64,
    key: &[u8],
    value: &[u8],
  ) -> Result<Option<&[u8]>, Error>
  where
    
    S: BuildChecksumer,
    Self::Pointer: Pointer<Comparator = GenericComparator<K>> + Borrow<[u8]> + Ord,
  {
    self.as_core_mut().get_or_insert(Some(version), key, value)
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
