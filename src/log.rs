use crate::{
  batch::{Batch, Data},
  checksum::{BuildChecksumer, Checksumer},
  error::Error,
  memtable::Memtable,
  options::Options,
  types::{BoundedKey, EncodedEntryMeta, EncodedRangeEntryMeta, EntryFlags, Flags, RecordPointer},
  utils::merge_lengths,
  CHECKSUM_SIZE, HEADER_SIZE, MAGIC_TEXT, MAGIC_TEXT_SIZE, RECORD_FLAG_SIZE, VERSION_SIZE,
};
use among::Among;
use core::{ops::Bound, ptr::NonNull};
use dbutils::{
  buffer::{BufWriter, BufWriterOnce, VacantBuffer},
  leb128::encoded_u64_varint_len,
};
use rarena_allocator::{either::Either, Allocator, ArenaPosition, Buffer, InsufficientBuffer};

pub trait Log: Sized {
  type Allocator: Allocator + 'static;
  type Memtable: Memtable;
  type Checksumer;
  type Reader;

  fn allocator<'a>(&'a self) -> &'a Self::Allocator
  where
    Self::Allocator: 'a;

  fn new(
    arena: Self::Allocator,
    opts: Options,
    memtable_opts: <Self::Memtable as Memtable>::Options,
    cks: Self::Checksumer,
  ) -> Result<Self, Error<Self::Memtable>> {
    unsafe {
      let slice = arena.reserved_slice_mut();
      let mut cursor = 0;
      slice[0..MAGIC_TEXT_SIZE].copy_from_slice(&MAGIC_TEXT);
      cursor += MAGIC_TEXT_SIZE;
      slice[cursor..HEADER_SIZE].copy_from_slice(&opts.magic_version().to_le_bytes());
    }

    #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
    let this = arena
      .flush_range(0, HEADER_SIZE)
      .map_err(Into::into)
      .and_then(|_| {
        Self::Memtable::new(arena.clone(), memtable_opts)
          .map(|memtable| Self::construct(arena, memtable, opts, cks))
          .map_err(Error::memtable)
      });

    #[cfg(not(all(feature = "memmap", not(target_family = "wasm"))))]
    let this = Self::Memtable::new(memtable_opts)
      .map(|memtable| Self::construct(arena, memtable, opts, cks))
      .map_err(Error::memtable);

    this
  }

  fn construct(
    arena: Self::Allocator,
    base: Self::Memtable,
    opts: Options,
    checksumer: Self::Checksumer,
  ) -> Self;

  fn options(&self) -> &Options;

  fn memtable(&self) -> &Self::Memtable;

  fn hasher(&self) -> &Self::Checksumer;

  /// Returns `true` if this WAL instance is read-only.
  #[inline]
  fn read_only(&self) -> bool {
    self.allocator().read_only()
  }

  /// Returns the path of the WAL if it is backed by a file.
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  #[inline]
  fn path<'a>(&'a self) -> Option<&'a <Self::Allocator as Allocator>::Path>
  where
    Self::Allocator: 'a,
  {
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
  #[allow(clippy::mut_from_ref)]
  unsafe fn reserved_slice_mut<'a>(&'a self) -> &'a mut [u8]
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
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  fn flush(&self) -> Result<(), Error<Self::Memtable>>
  where
    Self::Memtable: Memtable,
  {
    if !self.read_only() {
      self.allocator().flush().map_err(Into::into)
    } else {
      Err(Error::read_only())
    }
  }

  /// Flushes the to disk.
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  fn flush_async(&self) -> Result<(), Error<Self::Memtable>>
  where
    Self::Memtable: Memtable,
  {
    if !self.read_only() {
      self.allocator().flush_async().map_err(Into::into)
    } else {
      Err(Error::read_only())
    }
  }

  #[inline]
  fn insert_pointer(
    &self,
    version: u64,
    flag: EntryFlags,
    kp: RecordPointer,
  ) -> Result<(), Error<Self::Memtable>>
  where
    Self::Memtable: Memtable,
  {
    let t = self.memtable();
    match () {
      _ if flag.contains(EntryFlags::REMOVED) => t.remove(version, kp).map_err(Error::memtable),
      _ if flag.contains(EntryFlags::RANGE_DELETION) => {
        t.range_remove(version, kp).map_err(Error::memtable)
      }
      _ if flag.contains(EntryFlags::RANGE_SET) => {
        t.range_set(version, kp).map_err(Error::memtable)
      }
      _ if flag.contains(EntryFlags::RANGE_UNSET) => {
        t.range_unset(version, kp).map_err(Error::memtable)
      }
      _ => t.insert(version, kp).map_err(Error::memtable),
    }
  }

  #[inline]
  fn insert_pointers(
    &self,
    mut ptrs: impl Iterator<Item = (u64, EntryFlags, RecordPointer)>,
  ) -> Result<(), Error<Self::Memtable>>
  where
    Self::Memtable: Memtable,
  {
    ptrs.try_for_each(|(version, flag, p)| self.insert_pointer(version, flag, p))
  }

  fn range_remove<S, E>(
    &self,
    version: u64,
    start_bound: Bound<S>,
    end_bound: Bound<E>,
  ) -> Result<(), Among<S::Error, E::Error, Error<Self::Memtable>>>
  where
    S: BufWriterOnce,
    E: BufWriterOnce,
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: Memtable,
  {
    self
      .range_update::<_, _, Noop>(
        version,
        EntryFlags::RANGE_DELETION,
        start_bound,
        end_bound,
        None,
      )
      .map_err(|e| match e.into_left_right() {
        Either::Left(Either::Left(e)) => Among::Left(e),
        Either::Left(Either::Right(e)) => Among::Middle(e),
        Either::Right(e) => Among::Right(e),
      })
  }

  fn range_unset<S, E>(
    &self,
    version: u64,
    start_bound: Bound<S>,
    end_bound: Bound<E>,
  ) -> Result<(), Among<S::Error, E::Error, Error<Self::Memtable>>>
  where
    S: BufWriterOnce,
    E: BufWriterOnce,
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: Memtable,
  {
    self
      .range_update::<_, _, Noop>(
        version,
        EntryFlags::RANGE_UNSET,
        start_bound,
        end_bound,
        None,
      )
      .map_err(|e| match e.into_left_right() {
        Either::Left(Either::Left(e)) => Among::Left(e),
        Either::Left(Either::Right(e)) => Among::Middle(e),
        Either::Right(e) => Among::Right(e),
      })
  }

  fn range_set<S, E, V>(
    &self,
    version: u64,
    start_bound: Bound<S>,
    end_bound: Bound<E>,
    value: V,
  ) -> Result<(), Among<Either<S::Error, E::Error>, V::Error, Error<Self::Memtable>>>
  where
    S: BufWriterOnce,
    E: BufWriterOnce,
    V: BufWriterOnce,
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: Memtable,
  {
    self.range_update(
      version,
      EntryFlags::RANGE_SET,
      start_bound,
      end_bound,
      Some(value),
    )
  }

  fn insert<KE, VE>(
    &self,
    version: u64,
    kb: KE,
    vb: VE,
  ) -> Result<(), Among<KE::Error, VE::Error, Error<Self::Memtable>>>
  where
    KE: BufWriterOnce,
    VE: BufWriterOnce,
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: Memtable,
  {
    self.update(version, EntryFlags::empty(), kb, Some(vb))
  }

  fn remove<KE>(&self, version: u64, kb: KE) -> Result<(), Either<KE::Error, Error<Self::Memtable>>>
  where
    KE: BufWriterOnce,
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: Memtable,
  {
    self
      .update::<KE, Noop>(version, EntryFlags::REMOVED, kb, None)
      .map_err(Among::into_left_right)
  }

  fn range_update<S, E, VE>(
    &self,
    version: u64,
    entry_flag: EntryFlags,
    start_bound: Bound<S>,
    end_bound: Bound<E>,
    vb: Option<VE>,
  ) -> Result<(), Among<Either<S::Error, E::Error>, VE::Error, Error<Self::Memtable>>>
  where
    S: BufWriterOnce,
    E: BufWriterOnce,
    VE: BufWriterOnce,
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: Memtable,
  {
    if self.read_only() {
      return Err(Among::Right(Error::read_only()));
    }

    let start_bound = BoundWriterOnce::new(start_bound);
    let end_bound = BoundWriterOnce::new(end_bound);

    let res = {
      let start_key_encoded_len = start_bound.encoded_len();
      let end_key_encoded_len = end_bound.encoded_len();
      let vlen = vb.as_ref().map_or(0, |vb| vb.encoded_len());

      let encoded_entry_meta = check_range(
        start_key_encoded_len,
        end_key_encoded_len,
        vlen,
        self.maximum_key_size(),
        self.maximum_value_size(),
        self.read_only(),
      )
      .map_err(Either::Right)?;

      let allocator = self.allocator();

      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      let is_ondisk = allocator.is_ondisk();

      let buf = allocator.alloc_bytes(encoded_entry_meta.entry_size);
      let mut cks = self.hasher().build_checksumer();

      match buf {
        Err(e) => Err(Among::Right(Error::from_insufficient_space(e))),
        Ok(mut buf) => {
          unsafe {
            // We allocate the buffer with the exact size, so it's safe to write to the buffer.
            let flag = Flags::COMMITTED.bits();

            cks.update(&[flag]);

            buf.put_slice_unchecked(&[Flags::empty().bits(), entry_flag.bits()]);

            buf.put_u64_le_unchecked(version);

            let written = buf.put_u64_varint_unchecked(encoded_entry_meta.packed_kvlen);
            debug_assert_eq!(
              written, encoded_entry_meta.packed_kvlen_size,
              "the precalculated size should be equal to the written size"
            );

            let written = buf.put_u64_varint_unchecked(encoded_entry_meta.range_key_len);
            debug_assert_eq!(
              written, encoded_entry_meta.range_key_len_size,
              "the precalculated size should be equal to the written size"
            );

            let sko = encoded_entry_meta.start_key_offset();
            let ptr = buf.as_mut_ptr().add(sko);
            buf.set_len(encoded_entry_meta.entry_size as usize - CHECKSUM_SIZE);

            let mut start_key_buf = VacantBuffer::new(
              encoded_entry_meta.start_key_len as usize,
              NonNull::new_unchecked(ptr),
            );
            let written = start_bound.write_once(&mut start_key_buf).map_err(|e| {
              let e = e.unwrap_left();
              Among::Left(Either::Left(e))
            })?;

            debug_assert_eq!(
              written, encoded_entry_meta.start_key_len as usize,
              "the actual bytes written to the key buffer not equal to the expected size, expected {} but got {}.",
              encoded_entry_meta.start_key_len, written,
            );

            let eko = encoded_entry_meta.end_key_offset();
            let ptr = buf.as_mut_ptr().add(eko);
            let mut end_key_buf = VacantBuffer::new(
              encoded_entry_meta.end_key_len as usize,
              NonNull::new_unchecked(ptr),
            );
            let written = end_bound.write_once(&mut end_key_buf).map_err(|e| {
              let e = e.unwrap_left();
              Among::Left(Either::Right(e))
            })?;

            debug_assert_eq!(
              written, encoded_entry_meta.end_key_len as usize,
              "the actual bytes written to the key buffer not equal to the expected size, expected {} but got {}.",
              encoded_entry_meta.end_key_len, written,
            );

            if let Some(vb) = vb {
              let vo = encoded_entry_meta.value_offset();
              let mut value_buf = VacantBuffer::new(
                encoded_entry_meta.vlen as usize,
                NonNull::new_unchecked(buf.as_mut_ptr().add(vo)),
              );
              let written = vb.write_once(&mut value_buf).map_err(Among::Middle)?;

              debug_assert_eq!(
                written, encoded_entry_meta.vlen as usize,
                "the actual bytes written to the value buffer not equal to the expected size, expected {} but got {}.",
                encoded_entry_meta.vlen, written,
              );
            }

            let cks = {
              cks.update(&buf[1..]);
              cks.digest()
            };
            buf.put_u64_le_unchecked(cks);

            // commit the entry
            buf[0] |= Flags::COMMITTED.bits();

            #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
            if self.options().sync() && is_ondisk {
              allocator
                .flush_header_and_range(buf.offset(), encoded_entry_meta.entry_size as usize)
                .map_err(|e| Among::Right(e.into()))?;
            }

            buf.detach();
            let eoffset = buf.offset();
            let offset = eoffset + encoded_entry_meta.entry_flag_offset();
            let p = RecordPointer::new(offset as u32, (buf.len() - RECORD_FLAG_SIZE) as u32);
            Ok((buf.buffer_offset(), p, entry_flag))
          }
        }
      }
    };

    res.and_then(|(offset, p, flag)| {
      self.insert_pointer(version, flag, p).map_err(|e| {
        unsafe {
          self.allocator().rewind(ArenaPosition::Start(offset as u32));
        };
        Among::Right(e)
      })
    })
  }

  fn update<KE, VE>(
    &self,
    version: u64,
    entry_flag: EntryFlags,
    kb: KE,
    vb: Option<VE>,
  ) -> Result<(), Among<KE::Error, VE::Error, Error<Self::Memtable>>>
  where
    KE: BufWriterOnce,
    VE: BufWriterOnce,
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: Memtable,
  {
    if self.read_only() {
      return Err(Among::Right(Error::read_only()));
    }

    let res = {
      let klen = kb.encoded_len();
      let vlen = vb.as_ref().map_or(0, |vb| vb.encoded_len());
      let encoded_entry_meta = check(
        klen,
        vlen,
        self.maximum_key_size(),
        self.maximum_value_size(),
        self.read_only(),
      )
      .map_err(Either::Right)?;

      let allocator = self.allocator();

      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      let is_ondisk = allocator.is_ondisk();

      let buf = allocator.alloc_bytes(encoded_entry_meta.entry_size);
      let mut cks = self.hasher().build_checksumer();

      match buf {
        Err(e) => Err(Among::Right(Error::from_insufficient_space(e))),
        Ok(mut buf) => {
          unsafe {
            // We allocate the buffer with the exact size, so it's safe to write to the buffer.
            let flag = Flags::COMMITTED.bits();

            cks.update(&[flag]);

            buf.put_slice_unchecked(&[Flags::empty().bits(), entry_flag.bits()]);

            buf.put_u64_le_unchecked(version);

            let written = buf.put_u64_varint_unchecked(encoded_entry_meta.packed_kvlen);
            debug_assert_eq!(
              written, encoded_entry_meta.packed_kvlen_size,
              "the precalculated size should be equal to the written size"
            );

            let ko = encoded_entry_meta.key_offset();
            let ptr = buf.as_mut_ptr().add(ko);
            buf.set_len(encoded_entry_meta.entry_size as usize - CHECKSUM_SIZE);

            let mut key_buf = VacantBuffer::new(
              encoded_entry_meta.klen as usize,
              NonNull::new_unchecked(ptr),
            );
            let written = kb.write_once(&mut key_buf).map_err(Among::Left)?;
            debug_assert_eq!(
              written, encoded_entry_meta.klen as usize,
              "the actual bytes written to the key buffer not equal to the expected size, expected {} but got {}.",
              encoded_entry_meta.klen, written,
            );

            if let Some(vb) = vb {
              let vo = encoded_entry_meta.value_offset();
              let mut value_buf = VacantBuffer::new(
                encoded_entry_meta.vlen as usize,
                NonNull::new_unchecked(buf.as_mut_ptr().add(vo)),
              );
              let written = vb.write_once(&mut value_buf).map_err(Among::Middle)?;

              debug_assert_eq!(
                written, encoded_entry_meta.vlen as usize,
                "the actual bytes written to the value buffer not equal to the expected size, expected {} but got {}.",
                encoded_entry_meta.vlen, written,
              );
            }

            let cks = {
              cks.update(&buf[1..]);
              cks.digest()
            };
            buf.put_u64_le_unchecked(cks);

            // commit the entry
            buf[0] |= Flags::COMMITTED.bits();

            #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
            if self.options().sync() && is_ondisk {
              allocator
                .flush_header_and_range(buf.offset(), encoded_entry_meta.entry_size as usize)
                .map_err(|e| Among::Right(e.into()))?;
            }

            buf.detach();
            let eoffset = buf.offset();
            let offset = eoffset + encoded_entry_meta.entry_flag_offset();
            let p = RecordPointer::new(offset as u32, (buf.len() - RECORD_FLAG_SIZE) as u32);
            Ok((buf.buffer_offset(), p, entry_flag))
          }
        }
      }
    };

    res.and_then(|(offset, p, flag)| {
      self.insert_pointer(version, flag, p).map_err(|e| {
        unsafe {
          self.allocator().rewind(ArenaPosition::Start(offset as u32));
        };
        Among::Right(e)
      })
    })
  }

  fn apply<B>(
    &self,
    batch: &mut B,
  ) -> Result<
    (),
    Among<<B::Key as BufWriter>::Error, <B::Value as BufWriter>::Error, Error<Self::Memtable>>,
  >
  where
    B: Batch<Self::Memtable>,
    B::Key: BufWriter,
    B::Value: BufWriter,
    Self::Checksumer: BuildChecksumer,
    Self::Memtable: Memtable,
  {
    if self.read_only() {
      return Err(Among::Right(Error::read_only()));
    }

    let opts = self.options();
    let maximum_key_size = opts.maximum_key_size();
    let minimum_value_size = opts.maximum_value_size();
    let start_offset = unsafe {
      let (mut cursor, _allocator, mut buf) = batch
        .iter_mut()
        .try_fold((0u32, 0u64), |(num_entries, size), ent| {
          match &mut ent.data {
            Data::InsertPoint { key, value, meta } => {
              let klen = key.encoded_len();
              let vlen = value.encoded_len();

              check_batch_entry(klen, vlen, maximum_key_size, minimum_value_size).map(|entry_meta| {
                let ent_size = entry_meta.entry_size as u64;
                *meta = entry_meta;
                (num_entries + 1, size + ent_size)
              })
            },
            Data::RemovePoint { key, meta } => {
              let klen = key.encoded_len();
              check_batch_entry(klen, 0, maximum_key_size, minimum_value_size).map(|entry_meta| {
                let ent_size = entry_meta.entry_size as u64;
                *meta = entry_meta;
                (num_entries + 1, size + ent_size)
              })
            },
            Data::RangeRemove { start_bound, end_bound, meta } | Data::RangeUnset { start_bound, end_bound, meta } => {
              let start_key_encoded_len = encode_bound_key_len(start_bound.as_ref());
              let end_key_encoded_len = encode_bound_key_len(end_bound.as_ref());
              check_batch_range_entry(start_key_encoded_len, end_key_encoded_len, 0, maximum_key_size, minimum_value_size,)
                .map(|entry_meta| {
                  let ent_size = entry_meta.entry_size as u64;
                  *meta = entry_meta;
                  (num_entries + 1, size + ent_size)
                })
            },
            Data::RangeSet { start_bound, end_bound, value, meta } => {
              let start_key_encoded_len = encode_bound_key_len(start_bound.as_ref());
              let end_key_encoded_len = encode_bound_key_len(end_bound.as_ref());
              let vlen = value.encoded_len();
              check_batch_range_entry(start_key_encoded_len, end_key_encoded_len, vlen, maximum_key_size, minimum_value_size,)
                .map(|entry_meta| {
                  let ent_size = entry_meta.entry_size as u64;
                  *meta = entry_meta;
                  (num_entries + 1, size + ent_size)
                })
            },
          }
        })
        .and_then(|(num_entries, batch_encoded_size)| {
          // safe to cast batch_encoded_size to u32 here, we already checked it's less than capacity (less than u32::MAX).
          let batch_meta = merge_lengths(num_entries, batch_encoded_size as u32);
          let batch_meta_size = encoded_u64_varint_len(batch_meta);
          let allocator = self.allocator();
          let remaining = allocator.remaining() as u64;
          let total_size = RECORD_FLAG_SIZE as u64
            + batch_meta_size as u64
            + batch_encoded_size
            + CHECKSUM_SIZE as u64;
          if total_size > remaining {
            return Err(Error::insufficient_space(total_size, remaining as u32));
          }

          let mut buf = allocator
            .alloc_bytes(total_size as u32)
            .map_err(Error::from_insufficient_space)?;

          let flag = Flags::BATCHING;

          buf.put_u8_unchecked(flag.bits());
          let size = buf.put_u64_varint_unchecked(batch_meta);
          debug_assert_eq!(
            size, batch_meta_size,
            "the actual encoded u64 varint length ({}) doos not match the length ({}) returned by `dbutils::leb128::encoded_u64_varint_len`, please report bug to https://github.com/al8n/layer0/issues",
            size, batch_meta_size,
          );

          Ok((RECORD_FLAG_SIZE + batch_meta_size, allocator, buf))
        })
        .map_err(Among::Right)?;

      for ent in batch.iter_mut() {
        let meta = ent.encoded_meta();
        match meta {
          Either::Left(meta) => {
            let remaining = buf.remaining();
            if remaining < meta.checksum_offset() {
              return Err(Among::Right(
                Error::larger_batch_size(buf.capacity() as u32),
              ));
            }

            let entry_offset = cursor;
            buf.put_u8_unchecked(ent.flag.bits());
            let (ko, vo) = {
              buf.put_u64_le_unchecked(ent.internal_version());
              (cursor + meta.key_offset(), cursor + meta.value_offset())
            };

            let ent_len_size = buf.put_u64_varint_unchecked(meta.packed_kvlen);
            debug_assert_eq!(
              ent_len_size, meta.packed_kvlen_size,
              "the actual encoded u64 varint length ({}) doos not match the length ({}) returned by `dbutils::leb128::encoded_u64_varint_len`, please report bug to https://github.com/al8n/layer0/issues",
              ent_len_size, meta.packed_kvlen_size,
            );

            let ptr = buf.as_mut_ptr();
            let kp = ptr.add(ko);
            let vp = ptr.add(vo);
            buf.set_len(cursor + meta.value_offset());

            let (kb, vb) = (ent.key(), ent.value());
            let mut key_buf = VacantBuffer::new(meta.klen, NonNull::new_unchecked(kp));
            let written = kb.write(&mut key_buf).map_err(Among::Left)?;
            debug_assert_eq!(
              written, meta.klen,
              "the actual bytes written to the key buffer not equal to the expected size, expected {} but got {}.",
              meta.klen, written,
            );

            buf.set_len(cursor + meta.checksum_offset());
            if let Some(vb) = vb {
              let mut value_buf = VacantBuffer::new(meta.vlen, NonNull::new_unchecked(vp));
              let written = vb.write(&mut value_buf).map_err(Among::Middle)?;

              debug_assert_eq!(
                written, meta.vlen,
                "the actual bytes written to the value buffer not equal to the expected size, expected {} but got {}.",
                meta.vlen, written,
              );
            }

            let entry_size = meta.entry_size as usize;
            ent.set_pointer(RecordPointer::new(entry_offset as u32, meta.entry_size));
            cursor += entry_size;
          }
          Either::Right(meta) => {
            let remaining = buf.remaining();
            if remaining < meta.checksum_offset() {
              return Err(Among::Right(
                Error::larger_batch_size(buf.capacity() as u32),
              ));
            }

            let entry_offset = cursor;
            buf.put_u8_unchecked(ent.flag.bits());
            buf.put_u64_le_unchecked(ent.internal_version());
            let ent_len_size = buf.put_u64_varint_unchecked(meta.packed_kvlen);
            debug_assert_eq!(
              ent_len_size, meta.packed_kvlen_size,
              "the actual encoded u64 varint length ({}) doos not match the length ({}) returned by `dbutils::leb128::encoded_u64_varint_len`, please report bug to https://github.com/al8n/layer0/issues",
              ent_len_size, meta.packed_kvlen_size,
            );

            let range_key_len_size = buf.put_u64_varint_unchecked(meta.range_key_len);
            debug_assert_eq!(
              range_key_len_size, meta.range_key_len_size,
              "the actual encoded u64 varint length ({}) doos not match the length ({}) returned by `dbutils::leb128::encoded_u64_varint_len`, please report bug to https://github.com/al8n/layer0/issues",
              range_key_len_size, meta.range_key_len_size,
            );

            let ptr = buf.as_mut_ptr();
            let start_key_ptr = ptr.add(meta.start_key_offset());
            let end_key_ptr = ptr.add(meta.end_key_offset());
            let value_ptr = ptr.add(meta.value_offset());
            buf.set_len(cursor + meta.checksum_offset());

            let (start_bound, end_bound) = ent.bounds();
            let vb = ent.value();
            let mut start_bound_buf =
              VacantBuffer::new(meta.start_key_len, NonNull::new_unchecked(start_key_ptr));
            let mut end_bound_buf =
              VacantBuffer::new(meta.end_key_len, NonNull::new_unchecked(end_key_ptr));

            let written = BoundWriter::new(start_bound)
              .write(&mut start_bound_buf)
              .map_err(Among::Left)?;
            debug_assert_eq!(
              written, meta.start_key_len,
              "the actual bytes written to the key buffer not equal to the expected size, expected {} but got {}.",
              meta.start_key_len, written,
            );

            let written = BoundWriter::new(end_bound)
              .write(&mut end_bound_buf)
              .map_err(Among::Left)?;
            debug_assert_eq!(
              written, meta.end_key_len,
              "the actual bytes written to the key buffer not equal to the expected size, expected {} but got {}.",
              meta.end_key_len, written,
            );

            if let Some(vb) = vb {
              let mut value_buf = VacantBuffer::new(meta.vlen, NonNull::new_unchecked(value_ptr));
              let written = vb.write(&mut value_buf).map_err(Among::Middle)?;

              debug_assert_eq!(
                written, meta.vlen,
                "the actual bytes written to the value buffer not equal to the expected size, expected {} but got {}.",
                meta.vlen, written,
              );
            }

            let entry_size = meta.entry_size as usize;
            ent.set_pointer(RecordPointer::new(entry_offset as u32, meta.entry_size));
            cursor += entry_size;
          }
        }
      }

      let total_size = buf.capacity();
      if cursor + CHECKSUM_SIZE != total_size {
        return Err(Among::Right(Error::batch_size_mismatch(
          total_size as u32 - CHECKSUM_SIZE as u32,
          cursor as u32,
        )));
      }

      let mut cks = self.hasher().build_checksumer();
      let committed_flag = Flags::BATCHING | Flags::COMMITTED;
      cks.update(&[committed_flag.bits()]);
      cks.update(&buf[1..]);
      let checksum = cks.digest();
      buf.put_u64_le_unchecked(checksum);

      // commit the entry
      buf[0] = committed_flag.bits();

      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      if self.options().sync() && _allocator.is_ondisk() {
        _allocator
          .flush_header_and_range(Buffer::offset(&buf), buf.capacity())
          .map_err(|e| Among::Right(e.into()))?;
      }
      buf.detach();
      Buffer::buffer_offset(&buf)
    };

    self
      .insert_pointers(batch.iter_mut().map(|e| {
        let p = e.take_pointer().unwrap();
        (e.internal_version(), e.flag, p)
      }))
      .map_err(|e| {
        // Safety: the writer is single threaded, the memory chunk in buf cannot be accessed by other threads,
        // so it's safe to rewind the arena.
        unsafe {
          self
            .allocator()
            .rewind(ArenaPosition::Start(start_offset as u32));
        }
        Among::Right(e)
      })
  }

  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  fn replay(
    arena: Self::Allocator,
    opts: Options,
    memtable_opts: <Self::Memtable as Memtable>::Options,
    ro: bool,
    checksumer: Self::Checksumer,
  ) -> Result<Self, Error<Self::Memtable>>
  where
    Self::Checksumer: BuildChecksumer,
  {
    use crate::utils::split_lengths;
    use dbutils::leb128::decode_u64_varint;
    use rarena_allocator::IncompleteBuffer;

    let slice = arena.reserved_slice();
    let mut cursor = 0;
    let magic_text = &slice[0..MAGIC_TEXT_SIZE];
    if magic_text != MAGIC_TEXT {
      return Err(Error::magic_text_mismatch());
    }
    cursor += MAGIC_TEXT_SIZE;

    let magic_version = u16::from_le_bytes(slice[cursor..HEADER_SIZE].try_into().unwrap());
    if magic_version != opts.magic_version() {
      return Err(Error::magic_version_mismatch());
    }

    let set = Self::Memtable::new(arena.clone(), memtable_opts).map_err(Error::memtable)?;

    let mut cursor = arena.data_offset();
    let allocated = arena.allocated();
    let mut minimum_version = u64::MAX;
    let mut maximum_version = 0;

    loop {
      unsafe {
        // we reached the end of the arena, if we have any remaining, then if means two possibilities:
        // 1. the remaining is a partial entry, but it does not be persisted to the disk, so following the write-ahead log principle, we should discard it.
        // 2. our file may be corrupted, so we discard the remaining.
        if cursor + RECORD_FLAG_SIZE > allocated {
          if !ro && cursor < allocated {
            arena.rewind(ArenaPosition::Start(cursor as u32));
            arena.flush()?;
          }
          break;
        }

        let header = arena.get_u8_unchecked(cursor);
        let flag = Flags::from_bits_retain(header);

        if !flag.contains(Flags::BATCHING) {
          let mut sub_cursor = cursor + RECORD_FLAG_SIZE;
          let entry_offset = sub_cursor;
          let entry_flag = arena.get_u8(sub_cursor).map_err(|e| {
            #[cfg(feature = "tracing")]
            tracing::error!(err=%e);

            Error::corrupted(e)
          })?;
          sub_cursor += RECORD_FLAG_SIZE;

          let entry_flag = EntryFlags::from_bits_retain(entry_flag);
          let version = {
            let version = arena.get_u64_le(sub_cursor).map_err(|e| {
              #[cfg(feature = "tracing")]
              tracing::error!(err=%e);

              Error::corrupted(e)
            })?;
            sub_cursor += VERSION_SIZE;
            minimum_version = minimum_version.min(version);
            maximum_version = maximum_version.max(version);
            version
          };

          let (readed, encoded_len) = arena.get_u64_varint(sub_cursor).map_err(|e| {
            #[cfg(feature = "tracing")]
            tracing::error!(err=%e);

            Error::corrupted(e)
          })?;
          let (key_len, value_len) = split_lengths(encoded_len);
          let key_len = key_len as usize;
          let value_len = value_len as usize;
          sub_cursor += readed + key_len + value_len;

          // Same as above, if we reached the end of the arena, we should discard the remaining.
          let cks_offset = sub_cursor - cursor;
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

          let cks = arena.get_u64_le_unchecked(cursor + cks_offset);
          if cks != checksumer.checksum_one(arena.get_bytes(cursor, cks_offset)) {
            return Err(Error::corrupted("checksum mismatch"));
          }

          // If the entry is not committed, we should rewind
          if !flag.contains(Flags::COMMITTED) {
            if !ro {
              arena.rewind(ArenaPosition::Start(cursor as u32));
              arena.flush()?;
            }

            break;
          }

          let pointer = RecordPointer::new(entry_offset as u32, sub_cursor as u32);

          match () {
            _ if entry_flag.contains(EntryFlags::REMOVED) => {
              set.remove(version, pointer).map_err(Error::memtable)?
            }
            _ if entry_flag.contains(EntryFlags::RANGE_DELETION) => set
              .range_remove(version, pointer)
              .map_err(Error::memtable)?,
            _ if entry_flag.contains(EntryFlags::RANGE_SET) => {
              set.range_set(version, pointer).map_err(Error::memtable)?
            }
            _ if entry_flag.contains(EntryFlags::RANGE_UNSET) => {
              set.range_unset(version, pointer).map_err(Error::memtable)?
            }
            _ => set.insert(version, pointer).map_err(Error::memtable)?,
          }

          cursor = sub_cursor + CHECKSUM_SIZE;
        } else {
          let (readed, encoded_len) =
            arena
              .get_u64_varint(cursor + RECORD_FLAG_SIZE)
              .map_err(|e| {
                #[cfg(feature = "tracing")]
                tracing::error!(err=%e);

                Error::corrupted(e)
              })?;

          let (num_entries, encoded_data_len) = split_lengths(encoded_len);
          // Same as above, if we reached the end of the arena, we should discard the remaining.
          let cks_offset = RECORD_FLAG_SIZE + readed + encoded_data_len as usize;
          let total_size = cks_offset + CHECKSUM_SIZE;

          if total_size > allocated {
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
          batch_data_buf = &batch_data_buf[RECORD_FLAG_SIZE + readed..];
          for _ in 0..num_entries {
            if batch_data_buf.len() < EntryFlags::SIZE {
              return Err(Error::corrupted(IncompleteBuffer::new()));
            }

            let mut entry_cursor = 0;
            let entry_flag = EntryFlags::from_bits_retain(batch_data_buf[0]);
            entry_cursor += EntryFlags::SIZE;

            let version = {
              if batch_data_buf.len() < entry_cursor + VERSION_SIZE {
                return Err(Error::corrupted(IncompleteBuffer::new()));
              }

              let version = u64::from_le_bytes(
                batch_data_buf[entry_cursor..entry_cursor + VERSION_SIZE]
                  .try_into()
                  .unwrap(),
              );
              entry_cursor += VERSION_SIZE;
              minimum_version = minimum_version.min(version);
              maximum_version = maximum_version.max(version);
              version
            };

            let (kvlen, ent_len) =
              decode_u64_varint(&batch_data_buf[entry_cursor..]).map_err(|e| {
                #[cfg(feature = "tracing")]
                tracing::error!(err=%e);

                Error::corrupted(e)
              })?;

            let (klen, vlen) = split_lengths(ent_len);
            let klen = klen as usize;
            let vlen = vlen as usize;

            let entry_offset = cursor + RECORD_FLAG_SIZE + readed + sub_cursor;
            entry_cursor += kvlen + klen + vlen;
            sub_cursor += entry_cursor;
            let pointer = RecordPointer::new(entry_offset as u32, entry_cursor as u32);

            match () {
              _ if entry_flag.contains(EntryFlags::REMOVED) => {
                set.remove(version, pointer).map_err(Error::memtable)?
              }
              _ if entry_flag.contains(EntryFlags::RANGE_DELETION) => set
                .range_remove(version, pointer)
                .map_err(Error::memtable)?,
              _ if entry_flag.contains(EntryFlags::RANGE_SET) => {
                set.range_set(version, pointer).map_err(Error::memtable)?
              }
              _ if entry_flag.contains(EntryFlags::RANGE_UNSET) => {
                set.range_unset(version, pointer).map_err(Error::memtable)?
              }
              _ => set.insert(version, pointer).map_err(Error::memtable)?,
            }

            batch_data_buf = &batch_data_buf[entry_cursor..];
          }

          debug_assert_eq!(
            encoded_data_len as usize, sub_cursor,
            "expected encoded batch data size ({}) is not equal to the actual size ({})",
            encoded_data_len, sub_cursor,
          );

          cursor += total_size;
        }
      }
    }

    Ok(Self::construct(arena, set, opts, checksumer))
  }
}

#[inline]
const fn min_u64(a: u64, b: u64) -> u64 {
  if a < b {
    a
  } else {
    b
  }
}

#[inline]
const fn check_range<T: Memtable>(
  start_key_len: usize,
  end_key_len: usize,
  vlen: usize,
  max_key_size: u32,
  max_value_size: u32,
  ro: bool,
) -> Result<EncodedRangeEntryMeta, Error<T>> {
  if ro {
    return Err(Error::read_only());
  }

  let max_ksize = min_u64(max_key_size as u64, u32::MAX as u64);
  let max_vsize = min_u64(max_value_size as u64, u32::MAX as u64);

  if max_ksize < start_key_len as u64 {
    return Err(Error::key_too_large(start_key_len as u64, max_key_size));
  }

  if max_ksize < end_key_len as u64 {
    return Err(Error::key_too_large(end_key_len as u64, max_key_size));
  }

  let range_key_len = merge_lengths(start_key_len as u32, end_key_len as u32);
  let range_key_len_size = encoded_u64_varint_len(range_key_len);
  let total_range_key_size = range_key_len_size + start_key_len + end_key_len;

  if total_range_key_size as u64 > u32::MAX as u64 {
    return Err(Error::range_key_too_large(total_range_key_size as u64));
  }

  if max_vsize < vlen as u64 {
    return Err(Error::value_too_large(vlen as u64, max_value_size));
  }

  let len = merge_lengths(total_range_key_size as u32, vlen as u32);
  let len_size = encoded_u64_varint_len(len);
  let elen = RECORD_FLAG_SIZE as u64
    + EntryFlags::SIZE as u64
    + VERSION_SIZE as u64
    + len_size as u64
    + total_range_key_size as u64
    + vlen as u64
    + CHECKSUM_SIZE as u64;

  if elen > u32::MAX as u64 {
    return Err(Error::entry_too_large(
      elen,
      min_u64(
        RECORD_FLAG_SIZE as u64
          + 10
          + EntryFlags::SIZE as u64
          + VERSION_SIZE as u64
          + max_key_size as u64
          + max_value_size as u64,
        u32::MAX as u64,
      ),
    ));
  }

  Ok(EncodedRangeEntryMeta {
    packed_kvlen_size: len_size,
    packed_kvlen: len,
    entry_size: elen as u32,
    range_key_len,
    range_key_len_size,
    total_range_key_size,
    start_key_len,
    end_key_len,
    vlen,
    batch: false,
  })
}

#[inline]
const fn check<T: Memtable>(
  klen: usize,
  vlen: usize,
  max_key_size: u32,
  max_value_size: u32,
  ro: bool,
) -> Result<EncodedEntryMeta, Error<T>> {
  if ro {
    return Err(Error::read_only());
  }

  let max_ksize = min_u64(max_key_size as u64, u32::MAX as u64);
  let max_vsize = min_u64(max_value_size as u64, u32::MAX as u64);

  if max_ksize < klen as u64 {
    return Err(Error::key_too_large(klen as u64, max_key_size));
  }

  if max_vsize < vlen as u64 {
    return Err(Error::value_too_large(vlen as u64, max_value_size));
  }

  let len = merge_lengths(klen as u32, vlen as u32);
  let len_size = encoded_u64_varint_len(len);
  let elen = RECORD_FLAG_SIZE as u64
    + EntryFlags::SIZE as u64
    + VERSION_SIZE as u64
    + len_size as u64
    + klen as u64
    + vlen as u64
    + CHECKSUM_SIZE as u64;

  if elen > u32::MAX as u64 {
    return Err(Error::entry_too_large(
      elen,
      min_u64(
        RECORD_FLAG_SIZE as u64
          + 10
          + EntryFlags::SIZE as u64
          + VERSION_SIZE as u64
          + max_key_size as u64
          + max_value_size as u64,
        u32::MAX as u64,
      ),
    ));
  }

  Ok(EncodedEntryMeta {
    packed_kvlen_size: len_size,
    batch: false,
    packed_kvlen: len,
    entry_size: elen as u32,
    klen,
    vlen,
  })
}

#[inline]
fn check_batch_entry<T: Memtable>(
  klen: usize,
  vlen: usize,
  max_key_size: u32,
  max_value_size: u32,
) -> Result<EncodedEntryMeta, Error<T>> {
  let max_ksize = min_u64(max_key_size as u64, u32::MAX as u64);
  let max_vsize = min_u64(max_value_size as u64, u32::MAX as u64);

  if max_ksize < klen as u64 {
    return Err(Error::key_too_large(klen as u64, max_key_size));
  }

  if max_vsize < vlen as u64 {
    return Err(Error::value_too_large(vlen as u64, max_value_size));
  }

  let len = merge_lengths(klen as u32, vlen as u32);
  let len_size = encoded_u64_varint_len(len);
  let elen =
    EntryFlags::SIZE as u64 + VERSION_SIZE as u64 + len_size as u64 + klen as u64 + vlen as u64;

  if elen > u32::MAX as u64 {
    return Err(Error::entry_too_large(
      elen,
      min_u64(
        10 + EntryFlags::SIZE as u64
          + VERSION_SIZE as u64
          + max_key_size as u64
          + max_value_size as u64,
        u32::MAX as u64,
      ),
    ));
  }

  Ok(EncodedEntryMeta {
    packed_kvlen_size: len_size,
    packed_kvlen: len,
    entry_size: elen as u32,
    klen,
    vlen,
    batch: true,
  })
}

#[inline]
fn check_batch_range_entry<T: Memtable>(
  start_key_len: usize,
  end_key_len: usize,
  vlen: usize,
  max_key_size: u32,
  max_value_size: u32,
) -> Result<EncodedRangeEntryMeta, Error<T>> {
  let max_ksize = min_u64(max_key_size as u64, u32::MAX as u64);
  let max_vsize = min_u64(max_value_size as u64, u32::MAX as u64);

  if max_ksize < start_key_len as u64 {
    return Err(Error::key_too_large(start_key_len as u64, max_key_size));
  }

  if max_ksize < end_key_len as u64 {
    return Err(Error::key_too_large(end_key_len as u64, max_key_size));
  }

  let range_key_len = merge_lengths(start_key_len as u32, end_key_len as u32);
  let range_key_len_size = encoded_u64_varint_len(range_key_len);
  let total_range_key_size = range_key_len_size + start_key_len + end_key_len;

  if total_range_key_size as u64 > u32::MAX as u64 {
    return Err(Error::range_key_too_large(total_range_key_size as u64));
  }

  if max_vsize < vlen as u64 {
    return Err(Error::value_too_large(vlen as u64, max_value_size));
  }

  let len = merge_lengths(total_range_key_size as u32, vlen as u32);
  let len_size = encoded_u64_varint_len(len);
  let elen = EntryFlags::SIZE as u64
    + VERSION_SIZE as u64
    + len_size as u64
    + total_range_key_size as u64
    + vlen as u64;

  if elen > u32::MAX as u64 {
    return Err(Error::entry_too_large(
      elen,
      min_u64(
        10 + EntryFlags::SIZE as u64
          + VERSION_SIZE as u64
          + max_key_size as u64
          + max_value_size as u64,
        u32::MAX as u64,
      ),
    ));
  }

  Ok(EncodedRangeEntryMeta {
    packed_kvlen_size: len_size,
    packed_kvlen: len,
    entry_size: elen as u32,
    range_key_len,
    range_key_len_size,
    total_range_key_size,
    start_key_len,
    end_key_len,
    vlen,
    batch: true,
  })
}

struct Noop;

impl BufWriterOnce for Noop {
  type Error = ();

  #[inline(never)]
  #[cold]
  fn encoded_len(&self) -> usize {
    0
  }

  #[inline(never)]
  #[cold]
  fn write_once(self, _: &mut VacantBuffer<'_>) -> Result<usize, Self::Error> {
    Ok(0)
  }
}

#[inline]
fn encode_bound_key_len<W: BufWriter>(writer: Bound<&W>) -> usize {
  BoundedKey::encoded_size()
    + match writer {
      Bound::Included(k) => k.encoded_len(),
      Bound::Excluded(k) => k.encoded_len(),
      Bound::Unbounded => 0,
    }
}

struct BoundWriterOnce<W> {
  writer: Bound<W>,
  pointer: bool,
}

impl<W> BoundWriterOnce<W> {
  #[inline]
  fn new(writer: Bound<W>) -> Self {
    Self {
      writer,
      pointer: false,
    }
  }
}

impl<W> BufWriterOnce for BoundWriterOnce<W>
where
  W: BufWriterOnce,
{
  type Error = Either<W::Error, InsufficientBuffer>;

  #[inline]
  fn encoded_len(&self) -> usize {
    BoundedKey::encoded_size()
      + match self.writer.as_ref() {
        Bound::Included(k) => k.encoded_len(),
        Bound::Excluded(k) => k.encoded_len(),
        Bound::Unbounded => 0,
      }
  }

  #[inline]
  fn write_once(self, buf: &mut VacantBuffer<'_>) -> Result<usize, Self::Error> {
    match self.writer {
      Bound::Included(k) => {
        buf
          .put_u8(BoundedKey::new(Bound::Included(()), self.pointer).encode())
          .map_err(Either::Right)?;
        let mut kbuf = buf.split_off(1);
        k.write_once(&mut kbuf).map(|n| n + 1).map_err(Either::Left)
      }
      Bound::Excluded(k) => {
        buf
          .put_u8(BoundedKey::new(Bound::Included(()), self.pointer).encode())
          .map_err(Either::Right)?;
        let mut kbuf = buf.split_off(1);
        k.write_once(&mut kbuf).map(|n| n + 1).map_err(Either::Left)
      }
      Bound::Unbounded => {
        buf
          .put_u8(BoundedKey::new(Bound::Unbounded, false).encode())
          .map_err(Either::Right)?;
        Ok(1)
      }
    }
  }
}

struct BoundWriter<'a, W> {
  writer: Bound<&'a W>,
  pointer: bool,
}

impl<'a, W> BoundWriter<'a, W> {
  #[inline]
  fn new(writer: Bound<&'a W>) -> Self {
    Self {
      writer,
      pointer: false,
    }
  }
}

impl<W> BufWriter for BoundWriter<'_, W>
where
  W: BufWriter,
{
  type Error = W::Error;

  #[inline]
  fn encoded_len(&self) -> usize {
    BoundedKey::encoded_size()
      + match self.writer.as_ref() {
        Bound::Included(k) => k.encoded_len(),
        Bound::Excluded(k) => k.encoded_len(),
        Bound::Unbounded => 0,
      }
  }

  #[inline]
  fn write(&self, buf: &mut VacantBuffer<'_>) -> Result<usize, Self::Error> {
    // use put_u8_unchecked as this method is used internally, and should be have enough space before calling this method.
    match self.writer {
      Bound::Included(k) => {
        buf.put_u8_unchecked(BoundedKey::new(Bound::Included(()), self.pointer).encode());
        let mut kbuf = buf.split_off(1);
        k.write(&mut kbuf).map(|n| n + 1)
      }
      Bound::Excluded(k) => {
        buf.put_u8_unchecked(BoundedKey::new(Bound::Included(()), self.pointer).encode());
        let mut kbuf = buf.split_off(1);
        k.write(&mut kbuf).map(|n| n + 1)
      }
      Bound::Unbounded => {
        buf.put_u8_unchecked(BoundedKey::new(Bound::Unbounded, false).encode());
        Ok(1)
      }
    }
  }
}
