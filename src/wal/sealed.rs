use core::ptr::NonNull;

use rarena_allocator::ArenaPosition;

use super::*;

pub trait Pointer {
  type Comparator;

  fn new(klen: usize, vlen: usize, ptr: *const u8, cmp: Self::Comparator) -> Self;
}

impl<C> Pointer for crate::Pointer<C> {
  type Comparator = C;

  #[inline]
  fn new(klen: usize, vlen: usize, ptr: *const u8, cmp: C) -> Self {
    crate::Pointer::<C>::new(klen, vlen, ptr, cmp)
  }
}

pub trait Base: Default {
  type Pointer: Pointer;

  fn insert(&mut self, ele: Self::Pointer)
  where
    Self::Pointer: Ord;
}

impl<P> Base for SkipSet<P>
where
  P: Pointer + Send + 'static,
{
  type Pointer = P;

  fn insert(&mut self, ele: Self::Pointer)
  where
    P: Ord,
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

    if klen > max_key_size as usize {
      return Err(Error::key_too_large(klen as u64, max_key_size));
    }

    if vlen > max_value_size as usize {
      return Err(Error::value_too_large(vlen as u64, max_value_size));
    }

    Ok(())
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

  fn insert_batch_with_key_builder_in<B: BatchWithKeyBuilder<Comparator = C>>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Either<B::Error, Error>>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    let mut batch_encoded_size = 0u64;

    let mut num_entries = 0u32;
    for ent in batch.iter_mut() {
      let klen = ent.kb.size() as usize;
      let vlen = ent.value.borrow().len();
      self.check_batch_entry(klen, vlen).map_err(Either::Right)?;
      let merged_len = merge_lengths(klen as u32, vlen as u32);
      batch_encoded_size += klen as u64 + vlen as u64 + encoded_u64_varint_len(merged_len) as u64;
      num_entries += 1;
    }

    let cap = self.allocator().remaining() as u64;
    if batch_encoded_size > cap {
      return Err(Either::Right(Error::insufficient_space(
        batch_encoded_size,
        cap as u32,
      )));
    }

    // safe to cast batch_encoded_size to u32 here, we already checked it's less than capacity (less than u32::MAX).
    let batch_meta = merge_lengths(num_entries, batch_encoded_size as u32);
    let batch_meta_size = encoded_u64_varint_len(batch_meta);
    let total_size =
      STATUS_SIZE as u64 + batch_meta_size as u64 + batch_encoded_size + CHECKSUM_SIZE as u64;
    if total_size > cap {
      return Err(Either::Right(Error::insufficient_space(
        total_size, cap as u32,
      )));
    }

    let allocator = self.allocator();

    let mut buf = allocator
      .alloc_bytes(total_size as u32)
      .map_err(|e| Either::Right(Error::from_insufficient_space(e)))?;

    unsafe {
      let committed_flag = Flags::BATCHING | Flags::COMMITTED;
      let mut cks = self.hasher().build_checksumer();
      let flag = Flags::BATCHING;
      buf.put_u8_unchecked(flag.bits);
      buf.put_u64_varint_unchecked(batch_meta);
      let cmp = self.comparator();
      let mut cursor = 1 + batch_meta_size;

      for ent in batch.iter_mut() {
        let klen = ent.kb.size() as usize;
        let value = ent.value.borrow();
        let vlen = value.len();
        let merged_kv_len = merge_lengths(klen as u32, vlen as u32);
        let merged_kv_len_size = encoded_u64_varint_len(merged_kv_len);
        let remaining = buf.remaining();
        if remaining < merged_kv_len_size + klen + vlen {
          return Err(Either::Right(Error::larger_batch_size(total_size as u32)));
        }

        let ent_len_size = buf.put_u64_varint_unchecked(merged_kv_len);
        let ptr = buf.as_mut_ptr().add(cursor as usize + ent_len_size);
        buf.set_len(cursor as usize + ent_len_size + klen);
        let f = ent.kb.builder();
        f(&mut VacantBuffer::new(klen, NonNull::new_unchecked(ptr))).map_err(Either::Left)?;

        cursor += ent_len_size + klen;
        cursor += vlen;
        buf.put_slice_unchecked(value);

        ent.pointer = Some(Pointer::new(klen, vlen, ptr, cmp.cheap_clone()));
      }

      if (cursor + CHECKSUM_SIZE) as u64 != total_size {
        return Err(Either::Right(Error::batch_size_mismatch(
          total_size as u32 - CHECKSUM_SIZE as u32,
          cursor as u32,
        )));
      }

      cks.update(&[committed_flag.bits]);
      cks.update(&buf[1..]);
      buf.put_u64_le_unchecked(cks.digest());

      // commit the entry
      buf[0] = committed_flag.bits;
      let buf_cap = buf.capacity();

      if self.options().sync_on_write() && allocator.is_ondisk() {
        allocator
          .flush_range(buf.offset(), buf_cap)
          .map_err(|e| Either::Right(e.into()))?;
      }
      buf.detach();
      Ok(())
    }
  }

  fn insert_batch_with_value_builder_in<B: BatchWithValueBuilder<Comparator = C>>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Either<B::Error, Error>>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    let mut batch_encoded_size = 0u64;

    let mut num_entries = 0u32;
    for ent in batch.iter_mut() {
      let klen = ent.key.borrow().len();
      let vlen = ent.vb.size() as usize;
      self.check_batch_entry(klen, vlen).map_err(Either::Right)?;
      let merged_len = merge_lengths(klen as u32, vlen as u32);
      batch_encoded_size += klen as u64 + vlen as u64 + encoded_u64_varint_len(merged_len) as u64;
      num_entries += 1;
    }

    let cap = self.allocator().remaining() as u64;
    if batch_encoded_size > cap {
      return Err(Either::Right(Error::insufficient_space(
        batch_encoded_size,
        cap as u32,
      )));
    }

    // safe to cast batch_encoded_size to u32 here, we already checked it's less than capacity (less than u32::MAX).
    let batch_meta = merge_lengths(num_entries, batch_encoded_size as u32);
    let batch_meta_size = encoded_u64_varint_len(batch_meta);
    let total_size =
      STATUS_SIZE as u64 + batch_meta_size as u64 + batch_encoded_size + CHECKSUM_SIZE as u64;
    if total_size > cap {
      return Err(Either::Right(Error::insufficient_space(
        total_size, cap as u32,
      )));
    }

    let allocator = self.allocator();

    let mut buf = allocator
      .alloc_bytes(total_size as u32)
      .map_err(|e| Either::Right(Error::from_insufficient_space(e)))?;

    unsafe {
      let committed_flag = Flags::BATCHING | Flags::COMMITTED;
      let mut cks = self.hasher().build_checksumer();
      let flag = Flags::BATCHING;
      buf.put_u8_unchecked(flag.bits);
      buf.put_u64_varint_unchecked(batch_meta);
      let cmp = self.comparator();
      let mut cursor = 1 + batch_meta_size;

      for ent in batch.iter_mut() {
        let key = ent.key.borrow();
        let klen = key.len();
        let vlen = ent.vb.size() as usize;
        let merged_kv_len = merge_lengths(klen as u32, vlen as u32);
        let merged_kv_len_size = encoded_u64_varint_len(merged_kv_len);
        let remaining = buf.remaining();
        if remaining < merged_kv_len_size + klen + vlen {
          return Err(Either::Right(Error::larger_batch_size(total_size as u32)));
        }

        let ent_len_size = buf.put_u64_varint_unchecked(merged_kv_len);
        let ptr = buf.as_mut_ptr().add(cursor as usize + ent_len_size);
        cursor += klen + ent_len_size;
        buf.put_slice_unchecked(key);
        buf.set_len(cursor + vlen);
        let f = ent.vb.builder();
        let mut vacant_buffer = VacantBuffer::new(klen, NonNull::new_unchecked(ptr.add(klen)));
        f(&mut vacant_buffer).map_err(Either::Left)?;

        cursor += vlen;
        ent.pointer = Some(Pointer::new(klen, vlen, ptr, cmp.cheap_clone()));
      }

      if (cursor + CHECKSUM_SIZE) as u64 != total_size {
        return Err(Either::Right(Error::batch_size_mismatch(
          total_size as u32 - CHECKSUM_SIZE as u32,
          cursor as u32,
        )));
      }

      cks.update(&[committed_flag.bits]);
      cks.update(&buf[1..]);
      buf.put_u64_le_unchecked(cks.digest());

      // commit the entry
      buf[0] = committed_flag.bits;
      let buf_cap = buf.capacity();

      if self.options().sync_on_write() && allocator.is_ondisk() {
        allocator
          .flush_range(buf.offset(), buf_cap)
          .map_err(|e| Either::Right(e.into()))?;
      }
      buf.detach();
      Ok(())
    }
  }

  fn insert_batch_with_builders_in<B: BatchWithBuilders<Comparator = C>>(
    &mut self,
    batch: &mut B,
  ) -> Result<(), Among<B::KeyError, B::ValueError, Error>>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    let mut batch_encoded_size = 0u64;

    let mut num_entries = 0u32;
    for ent in batch.iter_mut() {
      let klen = ent.kb.size() as usize;
      let vlen = ent.vb.size() as usize;
      self.check_batch_entry(klen, vlen).map_err(Either::Right)?;
      let merged_len = merge_lengths(klen as u32, vlen as u32);
      batch_encoded_size += klen as u64 + vlen as u64 + encoded_u64_varint_len(merged_len) as u64;
      num_entries += 1;
    }

    let cap = self.allocator().remaining() as u64;
    if batch_encoded_size > cap {
      return Err(Among::Right(Error::insufficient_space(
        batch_encoded_size,
        cap as u32,
      )));
    }

    // safe to cast batch_encoded_size to u32 here, we already checked it's less than capacity (less than u32::MAX).
    let batch_meta = merge_lengths(num_entries, batch_encoded_size as u32);
    let batch_meta_size = encoded_u64_varint_len(batch_meta);
    let total_size =
      STATUS_SIZE as u64 + batch_meta_size as u64 + batch_encoded_size + CHECKSUM_SIZE as u64;
    if total_size > cap {
      return Err(Among::Right(Error::insufficient_space(
        total_size, cap as u32,
      )));
    }

    let allocator = self.allocator();

    let mut buf = allocator
      .alloc_bytes(total_size as u32)
      .map_err(|e| Either::Right(Error::from_insufficient_space(e)))?;

    unsafe {
      let committed_flag = Flags::BATCHING | Flags::COMMITTED;
      let mut cks = self.hasher().build_checksumer();
      let flag = Flags::BATCHING;
      buf.put_u8_unchecked(flag.bits);
      buf.put_u64_varint_unchecked(batch_meta);
      let cmp = self.comparator();
      let mut cursor = 1 + batch_meta_size;

      for ent in batch.iter_mut() {
        let klen = ent.kb.size() as usize;
        let vlen = ent.vb.size() as usize;
        let merged_kv_len = merge_lengths(klen as u32, vlen as u32);
        let merged_kv_len_size = encoded_u64_varint_len(merged_kv_len);

        let remaining = buf.remaining();
        if remaining < merged_kv_len_size + klen + vlen {
          return Err(Among::Right(Error::larger_batch_size(total_size as u32)));
        }

        let ent_len_size = buf.put_u64_varint_unchecked(merged_kv_len);
        let ptr = buf.as_mut_ptr().add(cursor as usize + ent_len_size);
        buf.set_len(cursor as usize + ent_len_size + klen);
        let f = ent.kb.builder();
        f(&mut VacantBuffer::new(klen, NonNull::new_unchecked(ptr))).map_err(Among::Left)?;
        cursor += ent_len_size + klen;
        buf.set_len(cursor as usize + vlen);
        let f = ent.vb.builder();
        f(&mut VacantBuffer::new(
          klen,
          NonNull::new_unchecked(ptr.add(klen)),
        ))
        .map_err(Among::Middle)?;
        cursor += vlen;
        ent.pointer = Some(Pointer::new(klen, vlen, ptr, cmp.cheap_clone()));
      }

      if (cursor + CHECKSUM_SIZE) as u64 != total_size {
        return Err(Among::Right(Error::batch_size_mismatch(
          total_size as u32 - CHECKSUM_SIZE as u32,
          cursor as u32,
        )));
      }

      cks.update(&[committed_flag.bits]);
      cks.update(&buf[1..]);
      buf.put_u64_le_unchecked(cks.digest());

      // commit the entry
      buf[0] = committed_flag.bits;
      let buf_cap = buf.capacity();

      if self.options().sync_on_write() && allocator.is_ondisk() {
        allocator
          .flush_range(buf.offset(), buf_cap)
          .map_err(|e| Among::Right(e.into()))?;
      }
      buf.detach();
      Ok(())
    }
  }

  fn insert_batch_in<B: Batch<Comparator = C>>(&mut self, batch: &mut B) -> Result<(), Error>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    let mut batch_encoded_size = 0u64;

    let mut num_entries = 0u32;
    for ent in batch.iter_mut() {
      let klen = ent.key.borrow().len();
      let vlen = ent.value.borrow().len();
      self.check_batch_entry(klen, vlen)?;
      let merged_len = merge_lengths(klen as u32, vlen as u32);
      batch_encoded_size += klen as u64 + vlen as u64 + encoded_u64_varint_len(merged_len) as u64;

      num_entries += 1;
    }

    // safe to cast batch_encoded_size to u32 here, we already checked it's less than capacity (less than u32::MAX).
    let batch_meta = merge_lengths(num_entries, batch_encoded_size as u32);
    let batch_meta_size = encoded_u64_varint_len(batch_meta);
    let allocator = self.allocator();
    let remaining = allocator.remaining() as u64;
    let total_size =
      STATUS_SIZE as u64 + batch_meta_size as u64 + batch_encoded_size + CHECKSUM_SIZE as u64;
    if total_size > remaining {
      return Err(Error::insufficient_space(total_size, remaining as u32));
    }

    let mut buf = allocator
      .alloc_bytes(total_size as u32)
      .map_err(Error::from_insufficient_space)?;

    unsafe {
      let committed_flag = Flags::BATCHING | Flags::COMMITTED;
      let mut cks = self.hasher().build_checksumer();
      let flag = Flags::BATCHING;
      buf.put_u8_unchecked(flag.bits);
      buf.put_u64_varint_unchecked(batch_meta);
      let cmp = self.comparator();
      let mut cursor = 1 + batch_meta_size;

      for ent in batch.iter_mut() {
        let key = ent.key.borrow();
        let value = ent.value.borrow();
        let klen = key.len();
        let vlen = value.len();
        let merged_kv_len = merge_lengths(klen as u32, vlen as u32);
        let merged_kv_len_size = encoded_u64_varint_len(merged_kv_len);

        let remaining = buf.remaining();
        if remaining < merged_kv_len_size + klen + vlen {
          return Err(Error::larger_batch_size(total_size as u32));
        }

        let ent_len_size = buf.put_u64_varint_unchecked(merged_kv_len);
        let ptr = buf.as_mut_ptr().add(cursor as usize + ent_len_size);
        cursor += ent_len_size + klen;
        buf.put_slice_unchecked(key);
        cursor += vlen;
        buf.put_slice_unchecked(value);
        ent.pointer = Some(Pointer::new(klen, vlen, ptr, cmp.cheap_clone()));
      }

      if (cursor + CHECKSUM_SIZE) as u64 != total_size {
        return Err(Error::batch_size_mismatch(
          total_size as u32 - CHECKSUM_SIZE as u32,
          cursor as u32,
        ));
      }

      cks.update(&[committed_flag.bits]);
      cks.update(&buf[1..]);
      let checksum = cks.digest();
      buf.put_u64_le_unchecked(checksum);

      // commit the entry
      buf[0] = committed_flag.bits;
      let buf_cap = buf.capacity();

      if self.options().sync_on_write() && allocator.is_ondisk() {
        allocator.flush_range(buf.offset(), buf_cap)?;
      }
      buf.detach();
      Ok(())
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

          if self.options().sync_on_write() && is_ondisk {
            allocator
              .flush_range(buf.offset(), elen as usize)
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
    Self::Pointer: Ord,
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
