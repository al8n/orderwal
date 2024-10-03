use core::ptr::NonNull;

use checksum::{BuildChecksumer, Checksumer};
use rarena_allocator::{ArenaPosition, BytesRefMut};

use super::*;

pub trait Pointer: Sized {
  type Comparator;

  fn new(klen: usize, vlen: usize, ptr: *const u8, cmp: Self::Comparator) -> Self;
}

pub trait Base: Default {
  type Pointer: Pointer;

  fn insert(&mut self, ele: Self::Pointer)
  where
    Self::Pointer: Ord + 'static;
}

impl<P> Base for SkipSet<P>
where
  P: Pointer + Send,
{
  type Pointer = P;

  fn insert(&mut self, ele: Self::Pointer)
  where
    P: Ord + 'static,
  {
    SkipSet::insert(self, ele);
  }
}

pub trait WalCore<C, S> {
  type Allocator: Allocator;
  type Base: Base<Pointer = Self::Pointer>;
  type Pointer: Pointer;

  fn construct(arena: Self::Allocator, base: Self::Base, opts: Options, cmp: C, cks: S) -> Self;
}

macro_rules! preprocess_batch {
  ($this:ident($batch:ident)) => {{
    $batch
        .iter_mut()
        .try_fold((0u32, 0u64), |(num_entries, size), ent| {
          let klen = ent.key_len();
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
            buf.put_u8_unchecked(flag.bits);
            buf.put_u64_varint_unchecked(batch_meta);
          }

          Ok((1 + batch_meta_size, allocator, buf))
        })
  }};
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

  fn insert_pointer(&self, ptr: Self::Pointer)
  where
    C: Comparator;

  fn insert_pointers(&self, ptrs: impl Iterator<Item = Self::Pointer>)
  where
    C: Comparator;

  fn insert_batch_with_key_builder_in<B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Either<B::Error, Error>>
  where
    B: BatchWithKeyBuilder<crate::pointer::Pointer<C>>,
    B::Value: Borrow<[u8]>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    let (mut cursor, allocator, mut buf) = preprocess_batch!(self(batch)).map_err(Either::Right)?;

    unsafe {
      let cmp = self.comparator();

      for ent in batch.iter_mut() {
        let klen = ent.key_len();
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

  fn insert_batch_with_value_builder_in<B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Either<B::Error, Error>>
  where
    B: BatchWithValueBuilder<crate::pointer::Pointer<C>>,
    B::Key: Borrow<[u8]>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    let (mut cursor, allocator, mut buf) = preprocess_batch!(self(batch)).map_err(Either::Right)?;

    unsafe {
      let cmp = self.comparator();

      for ent in batch.iter_mut() {
        let klen = ent.key_len();
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

  fn insert_batch_with_builders_in<B>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Among<B::KeyError, B::ValueError, Error>>
  where
    B: BatchWithBuilders<crate::pointer::Pointer<C>>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    let (mut cursor, allocator, mut buf) = preprocess_batch!(self(batch)).map_err(Among::Right)?;

    unsafe {
      let cmp = self.comparator();

      for ent in batch.iter_mut() {
        let klen = ent.key_len();
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
        ent.pointer = Some(Pointer::new(klen, vlen, ptr, cmp.cheap_clone()));
      }

      self
        .insert_batch_helper(allocator, buf, cursor)
        .map_err(Among::Right)
    }
  }

  fn insert_batch_in<B: Batch<Comparator = C>>(&mut self, batch: &mut B) -> Result<(), Error>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    let (mut cursor, allocator, mut buf) = preprocess_batch!(self(batch))?;

    unsafe {
      let cmp = self.comparator();

      for ent in batch.iter_mut() {
        let klen = ent.key_len();
        let vlen = ent.value_len();
        let merged_kv_len = ent.meta.kvlen;
        let merged_kv_len_size = ent.meta.kvlen_size;

        let remaining = buf.remaining();
        if remaining < merged_kv_len_size + klen + vlen {
          return Err(Error::larger_batch_size(buf.capacity() as u32));
        }

        let ent_len_size = buf.put_u64_varint_unchecked(merged_kv_len);
        let ptr = buf.as_mut_ptr().add(cursor + ent_len_size);
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
    cks.update(&[committed_flag.bits]);
    cks.update(&buf[1..]);
    let checksum = cks.digest();
    buf.put_u64_le_unchecked(checksum);

    // commit the entry
    buf[0] = committed_flag.bits;
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
  type Core: WalCore<C, S, Allocator = Self::Allocator, Pointer = Self::Pointer>;
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
      .map(|_| <Self::Core as WalCore<C, S>>::construct(arena, Default::default(), opts, cmp, cks))
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
    Self::Pointer: Ord + 'static,
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

    let mut set = <Self::Core as WalCore<C, S>>::Base::default();

    let mut cursor = arena.data_offset();
    let allocated = arena.allocated();

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
        let flag = Flags::from_bits_unchecked(header);

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

          set.insert(Pointer::new(
            key_len,
            value_len,
            arena.get_pointer(cursor + STATUS_SIZE + readed),
            cmp.cheap_clone(),
          ));
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

            let ptr = Pointer::new(
              klen,
              vlen,
              arena.get_pointer(cursor + STATUS_SIZE + readed + sub_cursor + kvlen),
              cmp.cheap_clone(),
            );
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

    Ok(<Self::Core as WalCore<C, S>>::construct(
      arena, set, opts, cmp, checksumer,
    ))
  }

  fn from_core(core: Self::Core) -> Self;
}
