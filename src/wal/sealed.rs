use core::ptr::NonNull;

use rarena_allocator::ArenaPosition;

use super::*;

pub trait Base<C>: Default {
  fn insert(&mut self, ele: Pointer<C>)
  where
    C: Comparator;
}

pub trait WalCore<C, S> {
  type Allocator: Allocator;
  type Base: Base<C>;

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

  fn hasher(&self) -> &S;

  fn options(&self) -> &Options;

  fn comparator(&self) -> &C;

  fn insert_pointer(&self, ptr: Pointer<C>)
  where
    C: Comparator;

  fn insert_with_in<KE, VE>(
    &mut self,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), KE>>,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), VE>>,
  ) -> Result<Pointer<C>, Among<KE, VE, Error>>
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
  type Core: WalCore<C, S, Allocator = Self::Allocator>;

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
    C: Comparator + CheapClone,
    S: BuildChecksumer,
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

        let (kvsize, encoded_len) = arena.get_u64_varint(cursor + STATUS_SIZE).map_err(|_e| {
          #[cfg(feature = "tracing")]
          tracing::error!(err=%_e);

          Error::corrupted()
        })?;

        let (key_len, value_len) = split_lengths(encoded_len);
        let key_len = key_len as usize;
        let value_len = value_len as usize;
        // Same as above, if we reached the end of the arena, we should discard the remaining.
        let cks_offset = STATUS_SIZE + kvsize + key_len + value_len;
        if cks_offset + CHECKSUM_SIZE > allocated {
          if !ro {
            arena.rewind(ArenaPosition::Start(cursor as u32));
            arena.flush()?;
          }

          break;
        }

        let cks = arena.get_u64_le(cursor + cks_offset).unwrap();

        if cks != checksumer.checksum_one(arena.get_bytes(cursor, cks_offset)) {
          return Err(Error::corrupted());
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
          arena.get_pointer(cursor + STATUS_SIZE + kvsize),
          cmp.cheap_clone(),
        ));
        cursor += cks_offset + CHECKSUM_SIZE;
      }
    }

    Ok(<Self::Core as WalCore<C, S>>::construct(
      arena, set, opts, cmp, checksumer,
    ))
  }

  fn from_core(core: Self::Core) -> Self;
}
