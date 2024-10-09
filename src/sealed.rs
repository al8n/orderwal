use core::{
  ops::{Bound, RangeBounds},
  ptr::NonNull,
};

use iter::{Iter, Keys, Range, RangeKeys, RangeValues, Values};
use rarena_allocator::{ArenaPosition, BytesRefMut};

use super::{
  batch::{Batch, BatchWithBuilders, BatchWithKeyBuilder, BatchWithValueBuilder},
  checksum::{BuildChecksumer, Checksumer},
  *,
};

pub trait Pointer: Sized {
  type Comparator;

  fn new(klen: usize, vlen: usize, ptr: *const u8, cmp: Self::Comparator) -> Self;

  fn as_key_slice<'a>(&self) -> &'a [u8];

  fn as_value_slice<'a>(&self) -> &'a [u8];

  fn version(&self) -> u64;
}

pub trait AsPointer<P> {
  fn as_pointer(&self) -> &P;
}

impl<P> AsPointer<P> for crossbeam_skiplist::set::Entry<'_, P> {
  #[inline]
  fn as_pointer(&self) -> &P {
    self.value()
  }
}

impl<P> AsPointer<P> for &P {
  #[inline]
  fn as_pointer(&self) -> &P {
    self
  }
}

pub trait Base: Default {
  type Pointer: Pointer;
  type Item<'a>: AsPointer<Self::Pointer> + 'a
  where
    Self::Pointer: 'a,
    Self: 'a;
  type Iterator<'a>: DoubleEndedIterator<Item = Self::Item<'a>>
  where
    Self::Pointer: 'a,
    Self: 'a;
  type Range<'a, Q, R>: Iterator<Item = Self::Item<'a>>
  where
    Self::Pointer: 'a,
    Self: 'a,
    Self::Pointer: Borrow<Q>,
    R: RangeBounds<Q>,
    Q: ?Sized + Ord;

  fn insert(&mut self, ele: Self::Pointer)
  where
    Self::Pointer: Ord + 'static;

  fn first(&self) -> Option<Self::Item<'_>>;

  fn last(&self) -> Option<Self::Item<'_>>;

  fn get<Q>(&self, key: &Q) -> Option<Self::Item<'_>>
  where
    Self::Pointer: Borrow<Q>,
    Q: Ord + ?Sized;

  fn contains<Q>(&self, key: &Q) -> bool
  where
    Self::Pointer: Borrow<Q>,
    Q: Ord + ?Sized;

  fn iter(&self) -> Self::Iterator<'_>;

  fn range<Q, R>(&self, range: R) -> Self::Range<'_, Q, R>
  where
    R: RangeBounds<Q>,
    Self::Pointer: Borrow<Q>,
    Q: Ord + ?Sized;
}

macro_rules! preprocess_batch {
  ($this:ident($batch:ident)) => {{
    $batch
        .iter_mut()
        .try_fold((0u32, 0u64), |(num_entries, size), ent| {
          let klen = ent.internal_key_len();
          let vlen = ent.value_len();
          $this.check_batch_entry(klen, vlen).map(|_| {
            let merged_len = merge_lengths(klen as u32, vlen as u32);
            let merged_len_size = encoded_u64_varint_len(merged_len);
            let ent_size = klen as u64 + vlen as u64 + merged_len_size as u64;
            ent.meta = BatchEncodedEntryMeta::new(klen, vlen, merged_len, merged_len_size);
            (num_entries + 1, size + ent_size)
          })
        })
        .and_then(|(num_entries, batch_encoded_size)| {
          // safe to cast batch_encoded_size to u32 here, we already checked it's less than capacity (less than u32::MAX).
          let batch_meta = merge_lengths(num_entries, batch_encoded_size as u32);
          let batch_meta_size = encoded_u64_varint_len(batch_meta);
          let allocator = $this.allocator();
          let remaining = allocator.remaining() as u64;
          let total_size =
            STATUS_SIZE as u64 + batch_meta_size as u64 + batch_encoded_size + CHECKSUM_SIZE as u64;
          if total_size > remaining {
            return Err(Error::insufficient_space(total_size, remaining as u32));
          }

          let mut buf = allocator
            .alloc_bytes(total_size as u32)
            .map_err(Error::from_insufficient_space)?;

          let flag = Flags::BATCHING;

          unsafe {
            buf.put_u8_unchecked(flag.bits());
            buf.put_u64_varint_unchecked(batch_meta);
          }

          Ok((1 + batch_meta_size, allocator, buf))
        })
  }};
}

pub trait WalCore<P, C, S> {
  type Allocator: Allocator;
  type Base: Base<Pointer = P>;

  fn construct(
    arena: Self::Allocator,
    base: Self::Base,
    opts: Options,
    cmp: C,
    checksumer: S,
    maximum_version: u64,
    minimum_version: u64,
  ) -> Self;

  fn allocator(&self) -> &Self::Allocator;

  fn options(&self) -> &Options;

  fn base(&self) -> &Self::Base;

  /// Returns `true` if this WAL instance is read-only.
  #[inline]
  fn read_only(&self) -> bool {
    self.allocator().read_only()
  }

  /// Returns the path of the WAL if it is backed by a file.
  #[inline]
  fn path<'a>(&'a self) -> Option<&'a <Self::Allocator as Allocator>::Path>
  where
    Self::Allocator: 'a,
  {
    self.allocator().path()
  }

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

  /// Returns the mutable reference to the reserved slice.
  ///
  /// ## Safety
  /// - The caller must ensure that the there is no others accessing reserved slice for either read or write.
  /// - This method is not thread-safe, so be careful when using it.
  unsafe fn reserved_slice_mut<'a>(&'a mut self) -> &'a mut [u8]
  where
    Self::Allocator: 'a,
  {
    let reserved = self.options().reserved();
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

  fn maximum_version(&self) -> u64;

  fn minimum_version(&self) -> u64;

  #[inline]
  fn contains_version(&self, version: u64) -> bool {
    self.minimum_version() <= version && version <= self.maximum_version()
  }

  #[inline]
  fn iter(&self, version: Option<u64>) -> Iter<'_, <Self::Base as Base>::Iterator<'_>, P> {
    Iter::new(version, self.base().iter())
  }

  #[inline]
  fn range<Q, R>(
    &self,
    version: Option<u64>,
    range: R,
  ) -> Range<'_, <Self::Base as Base>::Range<'_, Q, R>, P>
  where
    R: RangeBounds<Q>,
    P: Borrow<Q> + Pointer,
    Q: Ord + ?Sized,
  {
    Range::new(version, self.base().range(range))
  }

  #[inline]
  fn keys(&self, version: Option<u64>) -> Keys<'_, <Self::Base as Base>::Iterator<'_>, P> {
    Keys::new(version, self.base().iter())
  }

  /// Returns an iterator over a subset of keys in the WAL.
  #[inline]
  fn range_keys<Q, R>(
    &self,
    version: Option<u64>,
    range: R,
  ) -> RangeKeys<'_, <Self::Base as Base>::Range<'_, Q, R>, P>
  where
    R: RangeBounds<Q>,
    P: Borrow<Q> + Pointer,
    Q: Ord + ?Sized,
  {
    RangeKeys::new(version, self.base().range(range))
  }

  #[inline]
  fn values(&self, version: Option<u64>) -> Values<'_, <Self::Base as Base>::Iterator<'_>, P> {
    Values::new(version, self.base().iter())
  }

  #[inline]
  fn range_values<Q, R>(
    &self,
    version: Option<u64>,
    range: R,
  ) -> RangeValues<'_, <Self::Base as Base>::Range<'_, Q, R>, P>
  where
    R: RangeBounds<Q>,
    P: Borrow<Q> + Pointer,
    Q: Ord + ?Sized,
  {
    RangeValues::new(version, self.base().range(range))
  }

  /// Returns the first key-value pair in the map. The key in this pair is the minimum key in the wal.
  #[inline]
  fn first(&self, version: Option<u64>) -> Option<(&[u8], &[u8])>
  where
    P: Pointer,
  {
    match version {
      Some(version) => {
        if !self.contains_version(version) {
          return None;
        }

        self.base().iter().find_map(|p| {
          let p = p.as_pointer();
          if p.version() <= version {
            Some((p.as_key_slice(), p.as_value_slice()))
          } else {
            None
          }
        })
      }
      None => self.base().first().map(|ent| {
        let ent = ent.as_pointer();
        (ent.as_key_slice(), ent.as_value_slice())
      }),
    }
  }

  /// Returns the last key-value pair in the map. The key in this pair is the maximum key in the wal.
  fn last(&self, version: Option<u64>) -> Option<(&[u8], &[u8])>
  where
    P: Pointer,
  {
    match version {
      Some(version) => {
        if !self.contains_version(version) {
          return None;
        }

        self.base().iter().rev().find_map(|p| {
          let p = p.as_pointer();
          if p.version() <= version {
            Some((p.as_key_slice(), p.as_value_slice()))
          } else {
            None
          }
        })
      }
      None => self.base().last().map(|ent| {
        let ent = ent.as_pointer();
        (ent.as_key_slice(), ent.as_value_slice())
      }),
    }
  }

  /// Returns `true` if the WAL contains the specified key.
  fn contains_key<Q>(&self, version: Option<u64>, key: &Q) -> bool
  where
    [u8]: Borrow<P>,
    P: Borrow<Q> + Pointer,
    Q: ?Sized + Ord,
    C: Comparator,
  {
    match version {
      Some(version) => {
        if !self.contains_version(version) {
          return false;
        }

        self.base().iter().any(|p| {
          let p = p.as_pointer();
          p.version() <= version && p.as_key_slice().borrow().borrow() == key
        })
      }
      None => self.base().contains(key),
    }
  }

  /// Returns the value associated with the key.
  #[inline]
  fn get<Q>(&self, version: Option<u64>, key: &Q) -> Option<&[u8]>
  where
    [u8]: Borrow<Q>,
    P: Borrow<Q> + Borrow<[u8]> + Pointer,
    Q: ?Sized + Ord,
  {
    if let Some(version) = version {
      if !self.contains_version(version) {
        return None;
      }

      self.base().iter().find_map(|p| {
        let p = p.as_pointer();
        if p.version() <= version && p.as_key_slice().borrow() == key {
          Some(p.as_value_slice())
        } else {
          None
        }
      })
    } else {
      self
        .base()
        .get(key)
        .map(|ent| ent.as_pointer().as_value_slice())
    }
  }

  fn upper_bound<Q>(&self, version: Option<u64>, bound: Bound<&Q>) -> Option<&[u8]>
  where
    P: Borrow<Q> + Pointer,
    Q: ?Sized + Ord;

  fn lower_bound<Q>(&self, version: Option<u64>, bound: Bound<&Q>) -> Option<&[u8]>
  where
    P: Borrow<Q> + Pointer,
    Q: ?Sized + Ord;

  /// Get or insert a new entry into the WAL.
  fn get_or_insert(
    &mut self,
    version: Option<u64>,
    key: &[u8],
    value: &[u8],
  ) -> Result<Option<&[u8]>, Error>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
    P: Pointer<Comparator = C> + Borrow<[u8]>,
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

  fn get_or_insert_with_value_builder<E>(
    &mut self,
    version: Option<u64>,
    key: &[u8],
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
  ) -> Result<Option<&[u8]>, Either<E, Error>>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
    P: Pointer<Comparator = C> + Borrow<[u8]>,
  {
    let base = self.base();
    match version {
      None => {
        if let Some(val) = base.get(key) {
          return Ok(Some(val.as_pointer().as_value_slice()));
        }

        self
          .insert_with_value_builder::<E>(version, key, vb)
          .map(|_| None)
      }
      Some(version) => {
        if self.contains_version(version) {
          let res = base.iter().find_map(|p| {
            let p = p.as_pointer();
            if p.version() <= version && p.as_key_slice().borrow() == key {
              Some(p.as_value_slice())
            } else {
              None
            }
          });
          if res.is_some() {
            return Ok(res);
          }
        }

        self
          .insert_with_value_builder::<E>(Some(version), key, vb)
          .map(|_| None)
      }
    }
  }

  fn insert_with_key_builder<E>(
    &mut self,
    version: Option<u64>,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
    value: &[u8],
  ) -> Result<(), Either<E, Error>>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
    P: Pointer<Comparator = C>,
  {
    self
      .insert_with_in::<E, ()>(
        version,
        kb,
        ValueBuilder::once(value.len() as u32, |buf| {
          buf.put_slice(value).unwrap();
          Ok(())
        }),
      )
      .map(|ptr| self.insert_pointer(ptr))
      .map_err(Among::into_left_right)
  }

  fn insert_with_value_builder<E>(
    &mut self,
    version: Option<u64>,
    key: &[u8],
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
  ) -> Result<(), Either<E, Error>>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
    P: Pointer<Comparator = C>,
  {
    self
      .insert_with_in::<(), E>(
        version,
        KeyBuilder::once(key.len() as u32, |buf| {
          buf.put_slice_unchecked(key);
          Ok(())
        }),
        vb,
      )
      .map(|ptr| self.insert_pointer(ptr))
      .map_err(Among::into_middle_right)
  }

  fn insert_with_builders<KE, VE>(
    &mut self,
    version: Option<u64>,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), KE>>,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), VE>>,
  ) -> Result<(), Among<KE, VE, Error>>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
    P: Pointer<Comparator = C>,
  {
    self
      .insert_with_in(version, kb, vb)
      .map(|ptr| self.insert_pointer(ptr))
  }

  fn insert(&mut self, version: Option<u64>, key: &[u8], value: &[u8]) -> Result<(), Error>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
    P: Pointer<Comparator = C>,
  {
    self
      .insert_with_in::<(), ()>(
        version,
        KeyBuilder::once(key.len() as u32, |buf: &mut VacantBuffer<'_>| {
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

  fn insert_batch_with_key_builder<B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Either<B::Error, Error>>
  where
    B: BatchWithKeyBuilder<P>,
    B::Value: Borrow<[u8]>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
    P: Pointer<Comparator = C>,
  {
    if self.read_only() {
      return Err(Either::Right(Error::read_only()));
    }

    self
      .insert_batch_with_key_builder_in(batch)
      .map(|_| self.insert_pointers(batch.iter_mut().map(|ent| ent.pointer.take().unwrap())))
  }

  fn insert_batch_with_value_builder<B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Either<B::Error, Error>>
  where
    B: BatchWithValueBuilder<P>,
    B::Key: Borrow<[u8]>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
    P: Pointer<Comparator = C>,
  {
    if self.read_only() {
      return Err(Either::Right(Error::read_only()));
    }

    self
      .insert_batch_with_value_builder_in(batch)
      .map(|_| self.insert_pointers(batch.iter_mut().map(|ent| ent.pointer.take().unwrap())))
  }

  fn insert_batch_with_builders<B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Among<B::KeyError, B::ValueError, Error>>
  where
    B: BatchWithBuilders<P>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
    P: Pointer<Comparator = C>,
  {
    if self.read_only() {
      return Err(Among::Right(Error::read_only()));
    }

    self
      .insert_batch_with_builders_in(batch)
      .map(|_| self.insert_pointers(batch.iter_mut().map(|ent| ent.pointer.take().unwrap())))
  }

  fn insert_batch<B>(&mut self, batch: &mut B) -> Result<(), Error>
  where
    B: Batch<Pointer = P>,
    B::Key: Borrow<[u8]>,
    B::Value: Borrow<[u8]>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
    P: Pointer<Comparator = C>,
  {
    if self.read_only() {
      return Err(Error::read_only());
    }

    self
      .insert_batch_in(batch)
      .map(|_| self.insert_pointers(batch.iter_mut().map(|ent| ent.pointer.take().unwrap())))
  }

  #[inline]
  fn check(
    &self,
    klen: usize,
    vlen: usize,
    max_key_size: u32,
    max_value_size: u32,
    ro: bool,
  ) -> Result<(), Error> {
    crate::check(klen, vlen, max_key_size, max_value_size, ro)
  }

  #[inline]
  fn check_batch_entry(&self, klen: usize, vlen: usize) -> Result<(), Error> {
    let opts = self.options();
    let max_key_size = opts.maximum_key_size();
    let max_value_size = opts.maximum_value_size();

    crate::utils::check_batch_entry(klen, vlen, max_key_size, max_value_size)
  }

  fn hasher(&self) -> &S;

  fn comparator(&self) -> &C;

  fn insert_pointer(&mut self, ptr: P);

  fn insert_pointers(&mut self, ptrs: impl Iterator<Item = P>)
  where
    C: Comparator;

  fn insert_batch_with_key_builder_in<B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Either<B::Error, Error>>
  where
    B: BatchWithKeyBuilder<P>,
    B::Value: Borrow<[u8]>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
    P: Pointer<Comparator = C>,
  {
    let (mut cursor, allocator, mut buf) = preprocess_batch!(self(batch)).map_err(Either::Right)?;

    unsafe {
      let cmp = self.comparator();

      for ent in batch.iter_mut() {
        let klen = ent.internal_key_len();
        let vlen = ent.value_len();
        let merged_kv_len = ent.meta.kvlen;
        let merged_kv_len_size = ent.meta.kvlen_size;
        let remaining = buf.remaining();
        if remaining < merged_kv_len_size + klen + vlen {
          return Err(Either::Right(Error::larger_batch_size(
            buf.capacity() as u32
          )));
        }

        let ent_len_size = buf.put_u64_varint_unchecked(merged_kv_len);
        let ptr = buf.as_mut_ptr().add(cursor + ent_len_size);
        let key_ptr = if let Some(version) = ent.version {
          buf.put_u64_le_unchecked(version);
          ptr.add(VERSION_SIZE)
        } else {
          ptr
        };

        buf.set_len(cursor + ent_len_size + klen);
        let f = ent.key_builder().builder();
        f(&mut VacantBuffer::new(
          klen,
          NonNull::new_unchecked(key_ptr),
        ))
        .map_err(Either::Left)?;

        cursor += ent_len_size + klen;
        cursor += vlen;
        buf.put_slice_unchecked(ent.value().borrow());
        ent.pointer = Some(Pointer::new(klen, vlen, ptr, cmp.cheap_clone()));
      }

      self
        .insert_batch_helper(allocator, buf, cursor)
        .map_err(Either::Right)
    }
  }

  fn insert_batch_with_value_builder_in<B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Either<B::Error, Error>>
  where
    B: BatchWithValueBuilder<P>,
    B::Key: Borrow<[u8]>,
    P: Pointer<Comparator = C>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
    P: Pointer<Comparator = C>,
  {
    let (mut cursor, allocator, mut buf) = preprocess_batch!(self(batch)).map_err(Either::Right)?;

    unsafe {
      let cmp = self.comparator();

      for ent in batch.iter_mut() {
        let klen = ent.internal_key_len();
        let vlen = ent.value_len();
        let merged_kv_len = ent.meta.kvlen;
        let merged_kv_len_size = ent.meta.kvlen_size;
        let remaining = buf.remaining();
        if remaining < merged_kv_len_size + klen + vlen {
          return Err(Either::Right(Error::larger_batch_size(
            buf.capacity() as u32
          )));
        }

        let ent_len_size = buf.put_u64_varint_unchecked(merged_kv_len);
        let ptr = buf.as_mut_ptr().add(cursor + ent_len_size);
        let val_ptr = if let Some(version) = ent.version {
          buf.put_u64_le_unchecked(version);
          ptr.add(klen)
        } else {
          ptr
        };
        cursor += klen + ent_len_size;

        buf.put_slice_unchecked(ent.key().borrow());
        buf.set_len(cursor + vlen);
        let f = ent.vb.builder();
        let mut vacant_buffer = VacantBuffer::new(klen, NonNull::new_unchecked(val_ptr));
        f(&mut vacant_buffer).map_err(Either::Left)?;

        cursor += vlen;
        ent.pointer = Some(Pointer::new(klen, vlen, ptr, cmp.cheap_clone()));
      }

      self
        .insert_batch_helper(allocator, buf, cursor)
        .map_err(Either::Right)
    }
  }

  fn insert_batch_with_builders_in<B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Among<B::KeyError, B::ValueError, Error>>
  where
    B: BatchWithBuilders<P>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
    P: Pointer<Comparator = C>,
  {
    let (mut cursor, allocator, mut buf) = preprocess_batch!(self(batch)).map_err(Among::Right)?;

    unsafe {
      let cmp = self.comparator();

      for ent in batch.iter_mut() {
        let klen = ent.internal_key_len();
        let vlen = ent.value_len();
        let merged_kv_len = ent.meta.kvlen;
        let merged_kv_len_size = ent.meta.kvlen_size;

        let remaining = buf.remaining();
        if remaining < merged_kv_len_size + klen + vlen {
          return Err(Among::Right(
            Error::larger_batch_size(buf.capacity() as u32),
          ));
        }

        let ent_len_size = buf.put_u64_varint_unchecked(merged_kv_len);
        let ptr = buf.as_mut_ptr().add(cursor + ent_len_size);
        let (key_ptr, val_ptr) = if let Some(version) = ent.version {
          buf.put_u64_le_unchecked(version);
          let kptr = ptr.add(VERSION_SIZE);
          (kptr, ptr.add(klen))
        } else {
          (ptr, ptr.add(klen))
        };
        buf.set_len(cursor + ent_len_size + klen);

        let f = ent.key_builder().builder();
        f(&mut VacantBuffer::new(
          klen,
          NonNull::new_unchecked(key_ptr),
        ))
        .map_err(Among::Left)?;
        cursor += ent_len_size + klen;
        buf.set_len(cursor + vlen);
        let f = ent.value_builder().builder();
        f(&mut VacantBuffer::new(
          klen,
          NonNull::new_unchecked(val_ptr),
        ))
        .map_err(Among::Middle)?;
        cursor += vlen;
        ent.pointer = Some(<P as Pointer>::new(klen, vlen, ptr, cmp.cheap_clone()));
      }

      self
        .insert_batch_helper(allocator, buf, cursor)
        .map_err(Among::Right)
    }
  }

  fn insert_batch_in<B>(&mut self, batch: &mut B) -> Result<(), Error>
  where
    B: Batch<Pointer = P>,
    B::Key: Borrow<[u8]>,
    B::Value: Borrow<[u8]>,
    P: Pointer<Comparator = C>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    let (mut cursor, allocator, mut buf) = preprocess_batch!(self(batch))?;

    unsafe {
      let cmp = self.comparator();

      for ent in batch.iter_mut() {
        let klen = ent.internal_key_len();
        let vlen = ent.value_len();
        let merged_kv_len = ent.meta.kvlen;
        let merged_kv_len_size = ent.meta.kvlen_size;

        let remaining = buf.remaining();
        if remaining < merged_kv_len_size + klen + vlen {
          return Err(Error::larger_batch_size(buf.capacity() as u32));
        }

        let ent_len_size = buf.put_u64_varint_unchecked(merged_kv_len);
        let ptr = buf.as_mut_ptr().add(cursor + ent_len_size);
        if let Some(version) = ent.version {
          buf.put_u64_le_unchecked(version);
        }
        cursor += ent_len_size + klen;
        buf.put_slice_unchecked(ent.key().borrow());
        cursor += vlen;
        buf.put_slice_unchecked(ent.value().borrow());
        ent.pointer = Some(Pointer::new(klen, vlen, ptr, cmp.cheap_clone()));
      }

      self.insert_batch_helper(allocator, buf, cursor)
    }
  }

  fn insert_with_in<KE, VE>(
    &mut self,
    version: Option<u64>,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), KE>>,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), VE>>,
  ) -> Result<P, Among<KE, VE, Error>>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
    P: Pointer<Comparator = C>,
  {
    let (klen, kf) = kb.into_components();
    let klen = if version.is_some() {
      klen as usize + VERSION_SIZE
    } else {
      klen as usize
    };

    let (vlen, vf) = vb.into_components();
    let vlen = vlen as usize;
    self
      .check(
        klen,
        vlen,
        self.maximum_key_size(),
        self.maximum_value_size(),
        self.read_only(),
      )
      .map_err(Either::Right)?;

    let (len_size, kvlen, elen) = entry_size(klen as u32, vlen as u32);
    let allocator = self.allocator();
    let is_ondisk = allocator.is_ondisk();
    let buf = allocator.alloc_bytes(elen);
    let mut cks = self.hasher().build_checksumer();

    match buf {
      Err(e) => Err(Among::Right(Error::from_insufficient_space(e))),
      Ok(mut buf) => {
        unsafe {
          // We allocate the buffer with the exact size, so it's safe to write to the buffer.
          let flag = Flags::COMMITTED.bits();

          cks.update(&[flag]);

          buf.put_u8_unchecked(Flags::empty().bits());
          let written = buf.put_u64_varint_unchecked(kvlen);
          debug_assert_eq!(
            written, len_size,
            "the precalculated size should be equal to the written size"
          );

          let ko = STATUS_SIZE + written;
          let ptr = if let Some(version) = version {
            buf.put_u64_le_unchecked(version);
            buf.as_mut_ptr().add(ko + VERSION_SIZE)
          } else {
            buf.as_mut_ptr().add(ko)
          };
          buf.set_len(ko + klen + vlen);

          kf(&mut VacantBuffer::new(klen, NonNull::new_unchecked(ptr))).map_err(Among::Left)?;

          let vo = ko + klen;
          vf(&mut VacantBuffer::new(
            vlen,
            NonNull::new_unchecked(buf.as_mut_ptr().add(vo)),
          ))
          .map_err(Among::Middle)?;

          let cks = {
            cks.update(&buf[1..]);
            cks.digest()
          };
          buf.put_u64_le_unchecked(cks);

          // commit the entry
          buf[0] |= Flags::COMMITTED.bits();

          if self.options().sync() && is_ondisk {
            allocator
              .flush_header_and_range(buf.offset(), elen as usize)
              .map_err(|e| Among::Right(e.into()))?;
          }

          buf.detach();
          let cmp = self.comparator().cheap_clone();
          let ptr = buf.as_ptr().add(ko);
          Ok(Pointer::new(klen, vlen, ptr, cmp))
        }
      }
    }
  }

  unsafe fn insert_batch_helper(
    &self,
    allocator: &Self::Allocator,
    mut buf: BytesRefMut<'_, Self::Allocator>,
    cursor: usize,
  ) -> Result<(), Error>
  where
    S: BuildChecksumer,
  {
    let total_size = buf.capacity();
    if cursor + CHECKSUM_SIZE != total_size {
      return Err(Error::batch_size_mismatch(
        total_size as u32 - CHECKSUM_SIZE as u32,
        cursor as u32,
      ));
    }

    let mut cks = self.hasher().build_checksumer();
    let committed_flag = Flags::BATCHING | Flags::COMMITTED;
    cks.update(&[committed_flag.bits()]);
    cks.update(&buf[1..]);
    let checksum = cks.digest();
    buf.put_u64_le_unchecked(checksum);

    // commit the entry
    buf[0] = committed_flag.bits();
    let buf_cap = buf.capacity();

    if self.options().sync() && allocator.is_ondisk() {
      allocator.flush_header_and_range(buf.offset(), buf_cap)?;
    }
    buf.detach();
    Ok(())
  }
}

pub trait Sealed<C, S>: Constructor<C, S> {
  #[inline]
  fn check(
    &self,
    klen: usize,
    vlen: usize,
    max_key_size: u32,
    max_value_size: u32,
    ro: bool,
  ) -> Result<(), Error> {
    crate::check(klen, vlen, max_key_size, max_value_size, ro)
  }

  #[inline]
  fn check_batch_entry(&self, klen: usize, vlen: usize) -> Result<(), Error> {
    let opts = self.options();
    let max_key_size = opts.maximum_key_size();
    let max_value_size = opts.maximum_value_size();

    crate::utils::check_batch_entry(klen, vlen, max_key_size, max_value_size)
  }

  fn hasher(&self) -> &S;

  fn options(&self) -> &Options;

  fn comparator(&self) -> &C;

  fn insert_pointer(&self, ptr: Self::Pointer);

  fn insert_pointers(&self, ptrs: impl Iterator<Item = Self::Pointer>);

  fn insert_batch_with_key_builder_in<P, B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Either<B::Error, Error>>
  where
    B: BatchWithKeyBuilder<P>,
    B::Value: Borrow<[u8]>,
    P: Pointer<Comparator = C>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    let (mut cursor, allocator, mut buf) = preprocess_batch!(self(batch)).map_err(Either::Right)?;

    unsafe {
      let cmp = self.comparator();

      for ent in batch.iter_mut() {
        let klen = ent.internal_key_len();
        let vlen = ent.value_len();
        let merged_kv_len = ent.meta.kvlen;
        let merged_kv_len_size = ent.meta.kvlen_size;
        let remaining = buf.remaining();
        if remaining < merged_kv_len_size + klen + vlen {
          return Err(Either::Right(Error::larger_batch_size(
            buf.capacity() as u32
          )));
        }

        let ent_len_size = buf.put_u64_varint_unchecked(merged_kv_len);
        let mut ptr = buf.as_mut_ptr().add(cursor + ent_len_size);
        ptr = if let Some(version) = ent.version {
          buf.put_u64_le_unchecked(version);
          ptr.add(VERSION_SIZE)
        } else {
          ptr
        };

        buf.set_len(cursor + ent_len_size + klen);
        let f = ent.key_builder().builder();
        f(&mut VacantBuffer::new(klen, NonNull::new_unchecked(ptr))).map_err(Either::Left)?;

        cursor += ent_len_size + klen;
        cursor += vlen;
        buf.put_slice_unchecked(ent.value().borrow());
        ent.pointer = Some(Pointer::new(klen, vlen, ptr, cmp.cheap_clone()));
      }

      self
        .insert_batch_helper(allocator, buf, cursor)
        .map_err(Either::Right)
    }
  }

  fn insert_batch_with_value_builder_in<P, B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Either<B::Error, Error>>
  where
    B: BatchWithValueBuilder<P>,
    B::Key: Borrow<[u8]>,
    P: Pointer<Comparator = C>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    let (mut cursor, allocator, mut buf) = preprocess_batch!(self(batch)).map_err(Either::Right)?;

    unsafe {
      let cmp = self.comparator();

      for ent in batch.iter_mut() {
        let klen = ent.internal_key_len();
        let vlen = ent.value_len();
        let merged_kv_len = ent.meta.kvlen;
        let merged_kv_len_size = ent.meta.kvlen_size;
        let remaining = buf.remaining();
        if remaining < merged_kv_len_size + klen + vlen {
          return Err(Either::Right(Error::larger_batch_size(
            buf.capacity() as u32
          )));
        }

        let ent_len_size = buf.put_u64_varint_unchecked(merged_kv_len);
        let mut ptr = buf.as_mut_ptr().add(cursor + ent_len_size);
        ptr = if let Some(version) = ent.version {
          buf.put_u64_le_unchecked(version);
          ptr.add(VERSION_SIZE)
        } else {
          ptr
        };
        cursor += klen + ent_len_size;

        buf.put_slice_unchecked(ent.key().borrow());
        buf.set_len(cursor + vlen);
        let f = ent.vb.builder();
        let mut vacant_buffer = VacantBuffer::new(klen, NonNull::new_unchecked(ptr.add(klen)));
        f(&mut vacant_buffer).map_err(Either::Left)?;

        cursor += vlen;
        ent.pointer = Some(Pointer::new(klen, vlen, ptr, cmp.cheap_clone()));
      }

      self
        .insert_batch_helper(allocator, buf, cursor)
        .map_err(Either::Right)
    }
  }

  fn insert_batch_with_builders_in<P, B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Among<B::KeyError, B::ValueError, Error>>
  where
    B: BatchWithBuilders<P>,
    P: Pointer<Comparator = C>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    let (mut cursor, allocator, mut buf) = preprocess_batch!(self(batch)).map_err(Among::Right)?;

    unsafe {
      let cmp = self.comparator();

      for ent in batch.iter_mut() {
        let klen = ent.internal_key_len();
        let vlen = ent.value_len();
        let merged_kv_len = ent.meta.kvlen;
        let merged_kv_len_size = ent.meta.kvlen_size;

        let remaining = buf.remaining();
        if remaining < merged_kv_len_size + klen + vlen {
          return Err(Among::Right(
            Error::larger_batch_size(buf.capacity() as u32),
          ));
        }

        let ent_len_size = buf.put_u64_varint_unchecked(merged_kv_len);
        let mut ptr = buf.as_mut_ptr().add(cursor + ent_len_size);
        ptr = if let Some(version) = ent.version {
          buf.put_u64_le_unchecked(version);
          ptr.add(VERSION_SIZE)
        } else {
          ptr
        };
        buf.set_len(cursor + ent_len_size + klen);

        let f = ent.key_builder().builder();
        f(&mut VacantBuffer::new(klen, NonNull::new_unchecked(ptr))).map_err(Among::Left)?;
        cursor += ent_len_size + klen;
        buf.set_len(cursor + vlen);
        let f = ent.value_builder().builder();
        f(&mut VacantBuffer::new(
          klen,
          NonNull::new_unchecked(ptr.add(klen)),
        ))
        .map_err(Among::Middle)?;
        cursor += vlen;
        ent.pointer = Some(<P as Pointer>::new(klen, vlen, ptr, cmp.cheap_clone()));
      }

      self
        .insert_batch_helper(allocator, buf, cursor)
        .map_err(Among::Right)
    }
  }

  fn insert_batch_in<P, B>(&mut self, batch: &mut B) -> Result<(), Error>
  where
    B: Batch<Pointer = P>,
    B::Key: Borrow<[u8]>,
    B::Value: Borrow<[u8]>,
    P: Pointer<Comparator = C>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    let (mut cursor, allocator, mut buf) = preprocess_batch!(self(batch))?;

    unsafe {
      let cmp = self.comparator();

      for ent in batch.iter_mut() {
        let klen = ent.internal_key_len();
        let vlen = ent.value_len();
        let merged_kv_len = ent.meta.kvlen;
        let merged_kv_len_size = ent.meta.kvlen_size;

        let remaining = buf.remaining();
        if remaining < merged_kv_len_size + klen + vlen {
          return Err(Error::larger_batch_size(buf.capacity() as u32));
        }

        let ent_len_size = buf.put_u64_varint_unchecked(merged_kv_len);
        let mut ptr = buf.as_mut_ptr().add(cursor + ent_len_size);
        ptr = if let Some(version) = ent.version {
          buf.put_u64_le_unchecked(version);
          ptr.add(VERSION_SIZE)
        } else {
          ptr
        };
        cursor += ent_len_size + klen;
        buf.put_slice_unchecked(ent.key().borrow());
        cursor += vlen;
        buf.put_slice_unchecked(ent.value().borrow());
        ent.pointer = Some(Pointer::new(klen, vlen, ptr, cmp.cheap_clone()));
      }

      self.insert_batch_helper(allocator, buf, cursor)
    }
  }

  fn insert_with_in<KE, VE>(
    &mut self,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), KE>>,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), VE>>,
  ) -> Result<Self::Pointer, Among<KE, VE, Error>>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    let (klen, kf) = kb.into_components();
    let (vlen, vf) = vb.into_components();
    let (len_size, kvlen, elen) = entry_size(klen, vlen);
    let klen = klen as usize;
    let vlen = vlen as usize;
    let allocator = self.allocator();
    let is_ondisk = allocator.is_ondisk();
    let buf = allocator.alloc_bytes(elen);
    let mut cks = self.hasher().build_checksumer();

    match buf {
      Err(e) => Err(Among::Right(Error::from_insufficient_space(e))),
      Ok(mut buf) => {
        unsafe {
          // We allocate the buffer with the exact size, so it's safe to write to the buffer.
          let flag = Flags::COMMITTED.bits();

          cks.update(&[flag]);

          buf.put_u8_unchecked(Flags::empty().bits());
          let written = buf.put_u64_varint_unchecked(kvlen);
          debug_assert_eq!(
            written, len_size,
            "the precalculated size should be equal to the written size"
          );

          let ko = STATUS_SIZE + written;
          buf.set_len(ko + klen + vlen);

          kf(&mut VacantBuffer::new(
            klen,
            NonNull::new_unchecked(buf.as_mut_ptr().add(ko)),
          ))
          .map_err(Among::Left)?;

          let vo = ko + klen;
          vf(&mut VacantBuffer::new(
            vlen,
            NonNull::new_unchecked(buf.as_mut_ptr().add(vo)),
          ))
          .map_err(Among::Middle)?;

          let cks = {
            cks.update(&buf[1..]);
            cks.digest()
          };
          buf.put_u64_le_unchecked(cks);

          // commit the entry
          buf[0] |= Flags::COMMITTED.bits();

          if self.options().sync() && is_ondisk {
            allocator
              .flush_header_and_range(buf.offset(), elen as usize)
              .map_err(|e| Among::Right(e.into()))?;
          }

          buf.detach();
          let cmp = self.comparator().cheap_clone();
          let ptr = buf.as_ptr().add(ko);
          Ok(Pointer::new(klen, vlen, ptr, cmp))
        }
      }
    }
  }
}

trait SealedExt<C, S>: Sealed<C, S> {
  unsafe fn insert_batch_helper(
    &self,
    allocator: &Self::Allocator,
    mut buf: BytesRefMut<'_, Self::Allocator>,
    cursor: usize,
  ) -> Result<(), Error>
  where
    S: BuildChecksumer,
  {
    let total_size = buf.capacity();
    if cursor + CHECKSUM_SIZE != total_size {
      return Err(Error::batch_size_mismatch(
        total_size as u32 - CHECKSUM_SIZE as u32,
        cursor as u32,
      ));
    }

    let mut cks = self.hasher().build_checksumer();
    let committed_flag = Flags::BATCHING | Flags::COMMITTED;
    cks.update(&[committed_flag.bits()]);
    cks.update(&buf[1..]);
    let checksum = cks.digest();
    buf.put_u64_le_unchecked(checksum);

    // commit the entry
    buf[0] = committed_flag.bits();
    let buf_cap = buf.capacity();

    if self.options().sync() && allocator.is_ondisk() {
      allocator.flush_header_and_range(buf.offset(), buf_cap)?;
    }
    buf.detach();
    Ok(())
  }
}

impl<C, S, T> SealedExt<C, S> for T where T: Sealed<C, S> {}

pub trait Constructor<C, S>: Sized {
  type Allocator: Allocator;
  type Core: WalCore<Self::Pointer, C, S, Allocator = Self::Allocator>;
  type Pointer: Pointer<Comparator = C>;

  fn allocator(&self) -> &Self::Allocator;

  fn new_in(arena: Self::Allocator, opts: Options, cmp: C, cks: S) -> Result<Self::Core, Error> {
    unsafe {
      let slice = arena.reserved_slice_mut();
      slice[0..6].copy_from_slice(&MAGIC_TEXT);
      slice[6..8].copy_from_slice(&opts.magic_version().to_le_bytes());
    }

    arena
      .flush_range(0, HEADER_SIZE)
      .map(|_| {
        <Self::Core as WalCore<Self::Pointer, C, S>>::construct(
          arena,
          Default::default(),
          opts,
          cmp,
          cks,
          0,
          0,
        )
      })
      .map_err(Into::into)
  }

  fn replay(
    arena: Self::Allocator,
    opts: Options,
    ro: bool,
    cmp: C,
    checksumer: S,
  ) -> Result<Self::Core, Error>
  where
    C: CheapClone,
    S: BuildChecksumer,
    Self::Pointer: Pointer + Ord + 'static,
  {
    let slice = arena.reserved_slice();
    let magic_text = &slice[0..6];
    let magic_version = u16::from_le_bytes(slice[6..8].try_into().unwrap());

    if magic_text != MAGIC_TEXT {
      return Err(Error::magic_text_mismatch());
    }

    if magic_version != opts.magic_version() {
      return Err(Error::magic_version_mismatch());
    }

    let mut set = <Self::Core as WalCore<Self::Pointer, C, S>>::Base::default();

    let mut cursor = arena.data_offset();
    let allocated = arena.allocated();
    let mut minimum_version = u64::MAX;
    let mut maximum_version = 0;

    loop {
      unsafe {
        // we reached the end of the arena, if we have any remaining, then if means two possibilities:
        // 1. the remaining is a partial entry, but it does not be persisted to the disk, so following the write-ahead log principle, we should discard it.
        // 2. our file may be corrupted, so we discard the remaining.
        if cursor + STATUS_SIZE > allocated {
          if !ro && cursor < allocated {
            arena.rewind(ArenaPosition::Start(cursor as u32));
            arena.flush()?;
          }
          break;
        }

        let header = arena.get_u8(cursor).unwrap();
        let flag = Flags::from_bits_retain(header);

        if !flag.contains(Flags::BATCHING) {
          let (readed, encoded_len) = arena.get_u64_varint(cursor + STATUS_SIZE).map_err(|e| {
            #[cfg(feature = "tracing")]
            tracing::error!(err=%e);

            Error::corrupted(e)
          })?;
          let (key_len, value_len) = split_lengths(encoded_len);
          let key_len = key_len as usize;
          let value_len = value_len as usize;
          // Same as above, if we reached the end of the arena, we should discard the remaining.
          let cks_offset = STATUS_SIZE + readed + key_len + value_len;
          if cks_offset + CHECKSUM_SIZE > allocated {
            // If the entry is committed, then it means our file is truncated, so we should report corrupted.
            if flag.contains(Flags::COMMITTED) {
              return Err(Error::corrupted("file is truncated"));
            }

            if !ro {
              arena.rewind(ArenaPosition::Start(cursor as u32));
              arena.flush()?;
            }

            break;
          }

          let cks = arena.get_u64_le(cursor + cks_offset).unwrap();

          if cks != checksumer.checksum_one(arena.get_bytes(cursor, cks_offset)) {
            return Err(Error::corrupted("checksum mismatch"));
          }

          // If the entry is not committed, we should not rewind
          if !flag.contains(Flags::COMMITTED) {
            if !ro {
              arena.rewind(ArenaPosition::Start(cursor as u32));
              arena.flush()?;
            }

            break;
          }

          let pointer: Self::Pointer = Pointer::new(
            key_len,
            value_len,
            arena.get_pointer(cursor + STATUS_SIZE + readed),
            cmp.cheap_clone(),
          );

          let version = pointer.version();
          minimum_version = minimum_version.min(version);
          maximum_version = maximum_version.max(version);

          set.insert(pointer);
          cursor += cks_offset + CHECKSUM_SIZE;
        } else {
          let (readed, encoded_len) = arena.get_u64_varint(cursor + STATUS_SIZE).map_err(|e| {
            #[cfg(feature = "tracing")]
            tracing::error!(err=%e);

            Error::corrupted(e)
          })?;

          let (num_entries, encoded_data_len) = split_lengths(encoded_len);

          // Same as above, if we reached the end of the arena, we should discard the remaining.
          let cks_offset = STATUS_SIZE + readed + encoded_data_len as usize;
          if cks_offset + CHECKSUM_SIZE > allocated {
            // If the entry is committed, then it means our file is truncated, so we should report corrupted.
            if flag.contains(Flags::COMMITTED) {
              return Err(Error::corrupted("file is truncated"));
            }

            if !ro {
              arena.rewind(ArenaPosition::Start(cursor as u32));
              arena.flush()?;
            }

            break;
          }

          let cks = arena.get_u64_le(cursor + cks_offset).unwrap();
          let mut batch_data_buf = arena.get_bytes(cursor, cks_offset);
          if cks != checksumer.checksum_one(batch_data_buf) {
            return Err(Error::corrupted("checksum mismatch"));
          }

          let mut sub_cursor = 0;
          batch_data_buf = &batch_data_buf[1 + readed..];
          for _ in 0..num_entries {
            let (kvlen, ent_len) = decode_u64_varint(batch_data_buf).map_err(|e| {
              #[cfg(feature = "tracing")]
              tracing::error!(err=%e);

              Error::corrupted(e)
            })?;

            let (klen, vlen) = split_lengths(ent_len);
            let klen = klen as usize;
            let vlen = vlen as usize;

            let ptr: Self::Pointer = Pointer::new(
              klen,
              vlen,
              arena.get_pointer(cursor + STATUS_SIZE + readed + sub_cursor + kvlen),
              cmp.cheap_clone(),
            );

            let version = ptr.version();
            minimum_version = minimum_version.min(version);
            maximum_version = maximum_version.max(version);

            set.insert(ptr);
            let ent_len = kvlen + klen + vlen;
            sub_cursor += kvlen + klen + vlen;
            batch_data_buf = &batch_data_buf[ent_len..];
          }

          debug_assert_eq!(
            sub_cursor, encoded_data_len as usize,
            "expected encoded batch data size is not equal to the actual size"
          );

          cursor += cks_offset + CHECKSUM_SIZE;
        }
      }
    }

    Ok(<Self::Core as WalCore<Self::Pointer, C, S>>::construct(
      arena,
      set,
      opts,
      cmp,
      checksumer,
      maximum_version,
      minimum_version,
    ))
  }

  fn from_core(core: Self::Core) -> Self;
}
