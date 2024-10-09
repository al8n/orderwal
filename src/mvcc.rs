use core::{
  ops::{Bound, RangeBounds},
  ptr::NonNull,
};

use pointer::MvccPointer;

use super::{
  batch::{Batch, BatchWithBuilders, BatchWithKeyBuilder, BatchWithValueBuilder},
  checksum::BuildChecksumer,
  *,
};

/// An abstract layer for the immutable write-ahead log.
pub trait ImmutableWal<C, S>: sealed::Constructor<C, S> {
  /// The iterator type.
  type Iter<'a>: Iterator<Item = (&'a [u8], &'a [u8])> + DoubleEndedIterator
  where
    Self: 'a,
    C: Comparator;

  /// The iterator type over a subset of entries in the WAL.
  type Range<'a, Q, R>: Iterator<Item = (&'a [u8], &'a [u8])> + DoubleEndedIterator
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    Self: 'a,
    C: Comparator;

  /// The keys iterator type.
  type Keys<'a>: Iterator<Item = &'a [u8]> + DoubleEndedIterator
  where
    Self: 'a,
    C: Comparator;

  /// The iterator type over a subset of keys in the WAL.
  type RangeKeys<'a, Q, R>: Iterator<Item = &'a [u8]> + DoubleEndedIterator
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    Self: 'a,
    C: Comparator;

  /// The values iterator type.
  type Values<'a>: Iterator<Item = &'a [u8]> + DoubleEndedIterator
  where
    Self: 'a,
    C: Comparator;

  /// The iterator type over a subset of values in the WAL.
  type RangeValues<'a, Q, R>: Iterator<Item = &'a [u8]> + DoubleEndedIterator
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    Self: 'a,
    C: Comparator;

  /// Returns the reserved space in the WAL.
  ///
  /// ## Safety
  /// - The writer must ensure that the returned slice is not modified.
  /// - This method is not thread-safe, so be careful when using it.
  unsafe fn reserved_slice<'a>(&'a self) -> &'a [u8]
  where
    Self::Allocator: 'a,
  {
    let reserved = self.options().reserved();
    if reserved == 0 {
      return &[];
    }

    let allocator = self.allocator();
    let reserved_slice = allocator.reserved_slice();
    &reserved_slice[HEADER_SIZE..]
  }

  /// Returns the path of the WAL if it is backed by a file.
  fn path(&self) -> Option<&std::path::Path>;

  /// Returns the number of entries in the WAL.
  fn len(&self) -> usize;

  /// Returns `true` if the WAL is empty.
  #[inline]
  fn is_empty(&self) -> bool {
    self.len() == 0
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
    self.options().capacity()
  }

  /// Returns the options used to create this WAL instance.
  fn options(&self) -> &Options;

  /// Returns `true` if the WAL contains the specified key.
  fn contains_key<Q>(&self, key: &Q) -> bool
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    C: Comparator;

  /// Returns an iterator over the entries (version is less or equal to the specified version) the WAL.
  fn iter(&self, version: u64) -> Self::Iter<'_>
  where
    C: Comparator;

  /// Returns an iterator over a subset of entries (version is less or equal to the specified version) in the WAL.
  fn range<Q, R>(&self, version: u64, range: R) -> Self::Range<'_, Q, R>
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    C: Comparator;

  /// Returns an iterator over the keys (version is less or equal to the specified version) in the WAL.
  fn keys(&self, version: u64) -> Self::Keys<'_>
  where
    C: Comparator;

  /// Returns an iterator over a subset of keys (version is less or equal to the specified version) in the WAL.
  fn range_keys<Q, R>(&self, version: u64, range: R) -> Self::RangeKeys<'_, Q, R>
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    C: Comparator;

  /// Returns an iterator over the values (version is less or equal to the specified version) in the WAL.
  fn values(&self, version: u64) -> Self::Values<'_>
  where
    C: Comparator;

  /// Returns an iterator over a subset of values (version is less or equal to the specified version) in the WAL.
  fn range_values<Q, R>(&self, version: u64, range: R) -> Self::RangeValues<'_, Q, R>
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    C: Comparator;

  /// Returns the first key-value pair (version is less or equal to the specified version) in the map. The key in this pair is the minimum key in the wal.
  fn first(&self, version: u64) -> Option<(&[u8], &[u8])>
  where
    C: Comparator;

  /// Returns the last key-value pair (version is less or equal to the specified version) in the map. The key in this pair is the maximum key in the wal.
  fn last(&self, version: u64) -> Option<(&[u8], &[u8])>
  where
    C: Comparator;

  /// Returns the value associated with the key.
  fn get<Q>(&self, key: &Q) -> Option<&[u8]>
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    C: Comparator;

  /// Returns a value associated to the highest element whose key is below the given bound
  /// and version is less or equal to the specified version.
  ///
  /// If no such element is found then `None` is returned.
  // TODO: implement this method for unsync::OrderWal when BTreeMap::upper_bound is stable
  #[inline]
  fn upper_bound<Q>(&self, version: u64, bound: Bound<&Q>) -> Option<&[u8]>
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    C: Comparator,
  {
    self
      .range(version, (Bound::Unbounded, bound))
      .last()
      .map(|ent| ent.0)
  }

  /// Returns a value associated to the lowest element whose key is above the given bound
  /// and version is less or equal to the specified version.
  ///
  /// If no such element is found then `None` is returned.
  // TODO: implement this method for unsync::OrderWal when BTreeMap::lower_bound is stable
  #[inline]
  fn lower_bound<Q>(&self, version: u64, bound: Bound<&Q>) -> Option<&[u8]>
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    C: Comparator,
  {
    self
      .range(version, (bound, Bound::Unbounded))
      .next()
      .map(|ent| ent.0)
  }
}

/// An abstract layer for the write-ahead log.
pub trait Wal<C, S>:
  sealed::Sealed<C, S, Pointer = super::pointer::MvccPointer<C>> + ImmutableWal<C, S>
{
  /// The read only reader type for this wal.
  type Reader: ImmutableWal<C, S, Pointer = Self::Pointer>;

  /// Returns `true` if this WAL instance is read-only.
  fn read_only(&self) -> bool {
    self.allocator().read_only()
  }

  /// Returns the mutable reference to the reserved slice.
  ///
  /// ## Safety
  /// - The caller must ensure that the there is no others accessing reserved slice for either read or write.
  /// - This method is not thread-safe, so be careful when using it.
  unsafe fn reserved_slice_mut<'a>(&'a mut self) -> &'a mut [u8]
  where
    Self::Allocator: 'a,
  {
    let reserved = sealed::Sealed::options(self).reserved();
    if reserved == 0 {
      return &mut [];
    }

    let allocator = self.allocator();
    let reserved_slice = allocator.reserved_slice_mut();
    &mut reserved_slice[HEADER_SIZE..]
  }

  /// Flushes the to disk.
  fn flush(&self) -> Result<(), Error> {
    if !self.read_only() {
      self.allocator().flush().map_err(Into::into)
    } else {
      Err(Error::read_only())
    }
  }

  /// Flushes the to disk.
  fn flush_async(&self) -> Result<(), Error> {
    if !self.read_only() {
      self.allocator().flush_async().map_err(Into::into)
    } else {
      Err(Error::read_only())
    }
  }

  /// Returns the read-only view for the WAL.
  fn reader(&self) -> Self::Reader;

  /// Get (version is less or equal to the specified version) or insert a new entry into the WAL.
  fn get_or_insert(
    &mut self,
    version: u64,
    key: &[u8],
    value: &[u8],
  ) -> Result<Option<&[u8]>, Error>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    self
      .get_or_insert_with_value_builder::<()>(
        version,
        key,
        ValueBuilder::once(value.len() as u32, |buf| {
          buf.put_slice_unchecked(value);
          Ok(())
        }),
      )
      .map_err(|e| e.unwrap_right())
  }

  /// Get (version is less or equal to the specified version) or insert a new entry into the WAL.
  fn get_or_insert_with_value_builder<E>(
    &mut self,
    version: u64,
    key: &[u8],
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
  ) -> Result<Option<&[u8]>, Either<E, Error>>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer;

  /// Inserts a key-value pair into the WAL.
  fn insert(&mut self, version: u64, key: &[u8], value: &[u8]) -> Result<(), Error>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    let klen = VERSION_SIZE + key.len();
    self.check(
      klen,
      value.len(),
      self.maximum_key_size(),
      self.maximum_value_size(),
      self.read_only(),
    )?;

    self
      .insert_with_in::<(), ()>(
        KeyBuilder::once(klen as u32, |buf: &mut VacantBuffer<'_>| {
          buf.put_u64_le_unchecked(version);
          buf.put_slice_unchecked(key);
          Ok(())
        }),
        ValueBuilder::once(value.len() as u32, |buf: &mut VacantBuffer<'_>| {
          buf.put_slice_unchecked(value);
          Ok(())
        }),
      )
      .map(|ptr| self.insert_pointer(ptr))
      .map_err(Among::unwrap_right)
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the key in place.
  ///
  /// See also [`insert_with_value_builder`](Wal::insert_with_value_builder) and [`insert_with_builders`](Wal::insert_with_builders).
  fn insert_with_key_builder<E>(
    &mut self,
    version: u64,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
    value: &[u8],
  ) -> Result<(), Either<E, Error>>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    let (ksize, kb) = kb.into_components();
    let klen = VERSION_SIZE + ksize as usize;
    self
      .check(
        klen,
        value.len(),
        self.maximum_key_size(),
        self.maximum_value_size(),
        self.read_only(),
      )
      .map_err(Either::Right)?;

    let kb = KeyBuilder::once(klen as u32, |buf: &mut VacantBuffer<'_>| {
      buf.put_u64_le_unchecked(version);
      let ptr = buf.as_mut_ptr();
      buf.set_len(klen);
      let mut buf = unsafe { VacantBuffer::new(ksize as usize, NonNull::new_unchecked(ptr)) };
      kb(&mut buf)
    });

    self
      .insert_with_in::<E, ()>(
        kb,
        ValueBuilder::once(value.len() as u32, |buf| {
          buf.put_slice(value).unwrap();
          Ok(())
        }),
      )
      .map(|ptr| self.insert_pointer(ptr))
      .map_err(Among::into_left_right)
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the value in place.
  ///
  /// See also [`insert_with_key_builder`](Wal::insert_with_key_builder) and [`insert_with_builders`](Wal::insert_with_builders).
  fn insert_with_value_builder<E>(
    &mut self,
    version: u64,
    key: &[u8],
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
  ) -> Result<(), Either<E, Error>>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    let klen = VERSION_SIZE + key.len();
    self
      .check(
        klen,
        vb.size() as usize,
        self.maximum_key_size(),
        self.maximum_value_size(),
        self.read_only(),
      )
      .map_err(Either::Right)?;

    self
      .insert_with_in::<(), E>(
        KeyBuilder::once(klen as u32, |buf| {
          buf.put_u64_le_unchecked(version);
          buf.put_slice_unchecked(key);
          Ok(())
        }),
        vb,
      )
      .map(|ptr| self.insert_pointer(ptr))
      .map_err(Among::into_middle_right)
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the key and value in place.
  fn insert_with_builders<KE, VE>(
    &mut self,
    version: u64,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), KE>>,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), VE>>,
  ) -> Result<(), Among<KE, VE, Error>>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    let (ksize, kb) = kb.into_components();
    let klen = VERSION_SIZE + ksize as usize;
    self
      .check(
        klen,
        vb.size() as usize,
        self.maximum_key_size(),
        self.maximum_value_size(),
        self.read_only(),
      )
      .map_err(Among::Right)?;

    let kb = KeyBuilder::once(klen as u32, |buf: &mut VacantBuffer<'_>| {
      buf.put_u64_le_unchecked(version);
      let ptr = buf.as_mut_ptr();
      buf.set_len(klen);
      let mut buf = unsafe { VacantBuffer::new(ksize as usize, NonNull::new_unchecked(ptr)) };
      kb(&mut buf)
    });

    self
      .insert_with_in(kb, vb)
      .map(|ptr| self.insert_pointer(ptr))
  }

  /// Inserts a batch of key-value pairs into the WAL.
  fn insert_batch_with_key_builder<B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Either<B::Error, Error>>
  where
    B: BatchWithKeyBuilder<MvccPointer<C>>,
    B::Value: Borrow<[u8]>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    if self.read_only() {
      return Err(Either::Right(Error::read_only()));
    }

    self
      .insert_batch_with_key_builder_in(batch)
      .map(|_| self.insert_pointers(batch.iter_mut().map(|ent| ent.pointer.take().unwrap())))
  }

  /// Inserts a batch of key-value pairs into the WAL.
  fn insert_batch_with_value_builder<B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Either<B::Error, Error>>
  where
    B: BatchWithValueBuilder<MvccPointer<C>>,
    B::Key: Borrow<[u8]>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    if self.read_only() {
      return Err(Either::Right(Error::read_only()));
    }

    self
      .insert_batch_with_value_builder_in(batch)
      .map(|_| self.insert_pointers(batch.iter_mut().map(|ent| ent.pointer.take().unwrap())))
  }

  /// Inserts a batch of key-value pairs into the WAL.
  fn insert_batch_with_builders<B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Among<B::KeyError, B::ValueError, Error>>
  where
    B: BatchWithBuilders<MvccPointer<C>>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    if self.read_only() {
      return Err(Among::Right(Error::read_only()));
    }

    self
      .insert_batch_with_builders_in(batch)
      .map(|_| self.insert_pointers(batch.iter_mut().map(|ent| ent.pointer.take().unwrap())))
  }

  /// Inserts a batch of key-value pairs into the WAL.
  fn insert_batch<B>(&mut self, batch: &mut B) -> Result<(), Error>
  where
    B: Batch<Pointer = MvccPointer<C>>,
    B::Key: Borrow<[u8]>,
    B::Value: Borrow<[u8]>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    if self.read_only() {
      return Err(Error::read_only());
    }

    self
      .insert_batch_in(batch)
      .map(|_| self.insert_pointers(batch.iter_mut().map(|ent| ent.pointer.take().unwrap())))
  }
}
