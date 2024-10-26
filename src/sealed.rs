use core::{
  ops::{Bound, RangeBounds},
  ptr::NonNull,
};

use among::Among;
use dbutils::{
  buffer::VacantBuffer,
  equivalent::Comparable,
  leb128::{decode_u64_varint, encoded_u64_varint_len},
};
use rarena_allocator::{either::Either, Allocator, ArenaPosition, Buffer};

use crate::{merge_lengths, split_lengths};

use super::{
  batch::Batch,
  checksum::{BuildChecksumer, Checksumer},
  error::Error,
  internal_iter::*,
  memtable::{BaseTable, Memtable, MultipleVersionMemtable},
  options::Options,
  types::{BufWriter, EncodedEntryMeta, EntryFlags},
  Flags, CHECKSUM_SIZE, HEADER_SIZE, MAGIC_TEXT, RECORD_FLAG_SIZE, VERSION_SIZE,
};

pub trait Pointer: Sized + Copy {
  fn new(flag: EntryFlags, klen: usize, vlen: usize, ptr: *const u8) -> Self;

  fn as_key_slice<'a>(&self) -> &'a [u8];

  fn as_value_slice<'a>(&self) -> Option<&'a [u8]>;

  fn version(&self) -> Option<u64>;

  #[inline]
  fn is_removed(&self) -> bool {
    self.as_value_slice().is_none()
  }
}

/// A marker trait which indicates that such pointer has a version.
pub trait WithVersion {}

/// A marker trait which indicates that such pointer does not have a version.
pub trait WithoutVersion {}

pub trait GenericPointer<K: ?Sized, V: ?Sized>: Pointer {}

pub trait Immutable {}

pub trait WalReader<K: ?Sized, V: ?Sized, S> {
  type Allocator: Allocator;
  type Memtable;

  fn memtable(&self) -> &Self::Memtable;

  /// Returns the number of entries in the WAL.
  fn len(&self) -> usize
  where
    Self::Memtable: Memtable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + WithoutVersion,
  {
    self.memtable().len()
  }

  /// Returns `true` if the WAL is empty.
  #[inline]
  fn is_empty(&self) -> bool
  where
    Self::Memtable: Memtable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + WithoutVersion,
  {
    self.memtable().is_empty()
  }

  #[inline]
  fn iter(&self) -> Iter<'_, <Self::Memtable as BaseTable>::Iterator<'_>, Self::Memtable>
  where
    Self::Memtable: Memtable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + WithoutVersion,
  {
    Iter::new(None, self.memtable().iter())
  }

  #[inline]
  fn range<Q, R>(
    &self,
    range: R,
  ) -> Iter<'_, <Self::Memtable as BaseTable>::Range<'_, Q, R>, Self::Memtable>
  where
    R: RangeBounds<Q>,
    Q: ?Sized + Comparable<<Self::Memtable as BaseTable>::Pointer>,
    Self::Memtable: Memtable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + WithoutVersion,
  {
    Iter::new(None, self.memtable().range(range))
  }

  /// Returns the first key-value pair in the map. The key in this pair is the minimum key in the wal.
  #[inline]
  fn first(&self) -> Option<<Self::Memtable as BaseTable>::Item<'_>>
  where
    Self::Memtable: Memtable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + Ord + WithoutVersion,
  {
    self.memtable().first()
  }

  /// Returns the last key-value pair in the map. The key in this pair is the maximum key in the wal.
  fn last(&self) -> Option<<Self::Memtable as BaseTable>::Item<'_>>
  where
    Self::Memtable: Memtable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + Ord + WithoutVersion,
  {
    self.memtable().last()
  }

  /// Returns `true` if the WAL contains the specified key.
  fn contains_key<Q>(&self, key: &Q) -> bool
  where
    Q: ?Sized + Comparable<<Self::Memtable as BaseTable>::Pointer>,
    Self::Memtable: Memtable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + WithoutVersion,
  {
    self.memtable().contains(key)
  }

  /// Returns the value associated with the key.
  #[inline]
  fn get<Q>(&self, key: &Q) -> Option<<Self::Memtable as BaseTable>::Item<'_>>
  where
    Q: ?Sized + Comparable<<Self::Memtable as BaseTable>::Pointer>,
    Self::Memtable: Memtable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + WithoutVersion,
  {
    self.memtable().get(key)
  }

  fn upper_bound<Q>(&self, bound: Bound<&Q>) -> Option<<Self::Memtable as BaseTable>::Item<'_>>
  where
    Q: ?Sized + Comparable<<Self::Memtable as BaseTable>::Pointer>,
    Self::Memtable: Memtable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + WithoutVersion,
  {
    self.memtable().upper_bound(bound)
  }

  fn lower_bound<Q>(&self, bound: Bound<&Q>) -> Option<<Self::Memtable as BaseTable>::Item<'_>>
  where
    Q: ?Sized + Comparable<<Self::Memtable as BaseTable>::Pointer>,
    Self::Memtable: Memtable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + WithoutVersion,
  {
    self.memtable().lower_bound(bound)
  }
}

pub trait MultipleVersionWalReader<K: ?Sized, V: ?Sized, S> {
  type Allocator: Allocator;
  type Memtable;

  fn memtable(&self) -> &Self::Memtable;

  #[inline]
  fn iter(
    &self,
    version: u64,
  ) -> Iter<'_, <Self::Memtable as BaseTable>::Iterator<'_>, Self::Memtable>
  where
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + WithVersion,
  {
    Iter::new(Some(version), self.memtable().iter(version))
  }

  #[inline]
  fn range<Q, R>(
    &self,
    version: u64,
    range: R,
  ) -> Iter<'_, <Self::Memtable as BaseTable>::Range<'_, Q, R>, Self::Memtable>
  where
    R: RangeBounds<Q>,
    Q: ?Sized + Comparable<<Self::Memtable as BaseTable>::Pointer>,
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + WithVersion,
  {
    Iter::new(Some(version), self.memtable().range(version, range))
  }

  #[inline]
  fn iter_all_versions(
    &self,
    version: u64,
  ) -> Iter<'_, <Self::Memtable as MultipleVersionMemtable>::AllIterator<'_>, Self::Memtable>
  where
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + WithVersion,
  {
    Iter::new(Some(version), self.memtable().iter_all_versions(version))
  }

  #[inline]
  fn range_all_versions<Q, R>(
    &self,
    version: u64,
    range: R,
  ) -> Iter<'_, <Self::Memtable as MultipleVersionMemtable>::AllRange<'_, Q, R>, Self::Memtable>
  where
    R: RangeBounds<Q>,
    Q: ?Sized + Comparable<<Self::Memtable as BaseTable>::Pointer>,
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + WithVersion,
  {
    Iter::new(
      Some(version),
      self.memtable().range_all_versions(version, range),
    )
  }

  /// Returns the first key-value pair in the map. The key in this pair is the minimum key in the wal.
  #[inline]
  fn first(&self, version: u64) -> Option<<Self::Memtable as BaseTable>::Item<'_>>
  where
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + Ord + WithVersion,
  {
    self.memtable().first(version)
  }

  /// Returns the last key-value pair in the map. The key in this pair is the maximum key in the wal.
  fn last(&self, version: u64) -> Option<<Self::Memtable as BaseTable>::Item<'_>>
  where
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + Ord + WithVersion,
  {
    self.memtable().last(version)
  }

  /// Returns `true` if the WAL contains the specified key.
  fn contains_key<Q>(&self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Comparable<<Self::Memtable as BaseTable>::Pointer>,
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + WithVersion,
  {
    self.memtable().contains(version, key)
  }

  /// Returns the value associated with the key.
  #[inline]
  fn get<Q>(&self, version: u64, key: &Q) -> Option<<Self::Memtable as BaseTable>::Item<'_>>
  where
    Q: ?Sized + Comparable<<Self::Memtable as BaseTable>::Pointer>,
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + WithVersion,
  {
    self.memtable().get(version, key)
  }

  fn upper_bound<Q>(
    &self,
    version: u64,
    bound: Bound<&Q>,
  ) -> Option<<Self::Memtable as BaseTable>::Item<'_>>
  where
    Q: ?Sized + Comparable<<Self::Memtable as BaseTable>::Pointer>,
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + WithVersion,
  {
    self.memtable().upper_bound(version, bound)
  }

  fn lower_bound<Q>(
    &self,
    version: u64,
    bound: Bound<&Q>,
  ) -> Option<<Self::Memtable as BaseTable>::Item<'_>>
  where
    Q: ?Sized + Comparable<<Self::Memtable as BaseTable>::Pointer>,
    Self::Memtable: MultipleVersionMemtable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + WithVersion,
  {
    self.memtable().lower_bound(version, bound)
  }
}

pub trait Wal<K: ?Sized, V: ?Sized, S> {
  type Allocator: Allocator;
  type Memtable;

  fn construct(
    arena: Self::Allocator,
    base: Self::Memtable,
    opts: Options,
    checksumer: S,
    maximum_version: u64,
    minimum_version: u64,
  ) -> Self;

  fn allocator(&self) -> &Self::Allocator;

  fn options(&self) -> &Options;

  fn memtable(&self) -> &Self::Memtable;

  fn memtable_mut(&mut self) -> &mut Self::Memtable;

  fn hasher(&self) -> &S;

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
  fn flush(&self) -> Result<(), Error<Self::Memtable>>
  where
    Self::Memtable: BaseTable,
  {
    if !self.read_only() {
      self.allocator().flush().map_err(Into::into)
    } else {
      Err(Error::read_only())
    }
  }

  /// Flushes the to disk.
  fn flush_async(&self) -> Result<(), Error<Self::Memtable>>
  where
    Self::Memtable: BaseTable,
  {
    if !self.read_only() {
      self.allocator().flush_async().map_err(Into::into)
    } else {
      Err(Error::read_only())
    }
  }

  fn maximum_version(&self) -> u64;

  fn minimum_version(&self) -> u64;

  #[inline]
  fn update_versions(&mut self, version: u64) {
    self.update_maximum_version(version);
    self.update_minimum_version(version);
  }

  fn update_maximum_version(&mut self, version: u64);

  fn update_minimum_version(&mut self, version: u64);

  #[inline]
  fn insert_pointer(
    &self,
    ptr: <Self::Memtable as BaseTable>::Pointer,
  ) -> Result<(), Error<Self::Memtable>>
  where
    Self::Memtable: BaseTable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + Ord + 'static,
  {
    let t = self.memtable();
    if !ptr.is_removed() {
      t.insert(ptr).map_err(Error::memtable)
    } else {
      t.remove(ptr).map_err(Error::memtable)
    }
  }

  #[inline]
  fn insert_pointers(
    &self,
    mut ptrs: impl Iterator<Item = <Self::Memtable as BaseTable>::Pointer>,
  ) -> Result<(), Error<Self::Memtable>>
  where
    Self::Memtable: BaseTable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + Ord + 'static,
  {
    ptrs.try_for_each(|ptr| self.insert_pointer(ptr))
  }

  fn insert<KE, VE>(
    &self,
    version: Option<u64>,
    kb: KE,
    vb: VE,
  ) -> Result<(), Among<KE::Error, VE::Error, Error<Self::Memtable>>>
  where
    KE: super::types::BufWriterOnce,
    VE: super::types::BufWriterOnce,
    S: BuildChecksumer,
    Self::Memtable: BaseTable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + Ord + 'static,
  {
    self.update(version, kb, Some(vb))
  }

  fn remove<KE>(
    &self,
    version: Option<u64>,
    kb: KE,
  ) -> Result<(), Either<KE::Error, Error<Self::Memtable>>>
  where
    KE: super::types::BufWriterOnce,
    S: BuildChecksumer,
    Self::Memtable: BaseTable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + Ord + 'static,
  {
    struct Noop;

    impl super::types::BufWriterOnce for Noop {
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

    self
      .update::<KE, Noop>(version, kb, None)
      .map_err(Among::into_left_right)
  }

  fn update<KE, VE>(
    &self,
    version: Option<u64>,
    kb: KE,
    vb: Option<VE>,
  ) -> Result<(), Among<KE::Error, VE::Error, Error<Self::Memtable>>>
  where
    KE: super::types::BufWriterOnce,
    VE: super::types::BufWriterOnce,
    S: BuildChecksumer,
    Self::Memtable: BaseTable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + Ord + 'static,
  {
    if self.read_only() {
      return Err(Among::Right(Error::read_only()));
    }

    let res = {
      let klen = kb.encoded_len();
      let (vlen, remove) = vb
        .as_ref()
        .map(|vb| (vb.encoded_len(), false))
        .unwrap_or((0, true));
      let encoded_entry_meta = check(
        klen,
        vlen,
        version.is_some(),
        self.maximum_key_size(),
        self.maximum_value_size(),
        self.read_only(),
      )
      .map_err(Either::Right)?;

      let allocator = self.allocator();
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

            buf.put_u8_unchecked(Flags::empty().bits());
            let written = buf.put_u64_varint_unchecked(encoded_entry_meta.packed_kvlen);
            debug_assert_eq!(
              written, encoded_entry_meta.packed_kvlen_size,
              "the precalculated size should be equal to the written size"
            );

            let mut entry_flag = if !remove {
              EntryFlags::empty()
            } else {
              EntryFlags::REMOVED
            };
            buf.put_u8_unchecked(entry_flag.bits());

            if let Some(version) = version {
              buf.put_u64_le_unchecked(version);
              entry_flag |= EntryFlags::VERSIONED;
            }

            let ko = encoded_entry_meta.key_offset();
            let ptr = buf.as_mut_ptr().add(ko);
            buf.set_len(encoded_entry_meta.entry_size as usize - VERSION_SIZE);

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

            if self.options().sync() && is_ondisk {
              allocator
                .flush_header_and_range(buf.offset(), encoded_entry_meta.entry_size as usize)
                .map_err(|e| Among::Right(e.into()))?;
            }

            buf.detach();
            let ptr = buf
              .as_ptr()
              .add(encoded_entry_meta.entry_flag_offset() as usize);
            Ok((
              buf.buffer_offset(),
              Pointer::new(entry_flag, klen, vlen, ptr),
            ))
          }
        }
      }
    };

    res.and_then(|(offset, ptr)| {
      self.insert_pointer(ptr).map_err(|e| {
        unsafe {
          self.allocator().rewind(ArenaPosition::Start(offset as u32));
        };
        Among::Right(e)
      })
    })
  }

  fn insert_batch<W, B>(
    &self,
    batch: &mut B,
  ) -> Result<
    (),
    Among<<B::Key as BufWriter>::Error, <B::Value as BufWriter>::Error, Error<Self::Memtable>>,
  >
  where
    B: Batch<<Self::Memtable as BaseTable>::Pointer>,
    B::Key: BufWriter,
    B::Value: BufWriter,
    S: BuildChecksumer,
    W: Constructable<K, V, Wal = Self, Memtable = Self::Memtable>,
    Self::Memtable: BaseTable,
    <Self::Memtable as BaseTable>::Pointer: Pointer + Ord + 'static,
  {
    if self.read_only() {
      return Err(Among::Right(Error::read_only()));
    }

    let opts = self.options();
    let maximum_key_size = opts.maximum_key_size();
    let minimum_value_size = opts.maximum_value_size();
    let start_offset = unsafe {
      let (mut cursor, allocator, mut buf) = batch
        .iter_mut()
        .try_fold((0u32, 0u64), |(num_entries, size), ent| {
          let klen = ent.encoded_key_len();
          let vlen = ent.value_len();
          check_batch_entry(klen, vlen, maximum_key_size, minimum_value_size, ent.internal_version().is_some()).map(|meta| {
            let ent_size = meta.entry_size as u64;
            ent.set_encoded_meta(meta);
            (num_entries + 1, size + ent_size)
          })
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

      let mut minimum = self.minimum_version();
      let mut maximum = self.maximum_version();

      for ent in batch.iter_mut() {
        let meta = ent.encoded_meta();
        let version_size = if ent.internal_version().is_some() {
          VERSION_SIZE
        } else {
          0
        };

        let remaining = buf.remaining();
        if remaining
          < meta.packed_kvlen_size + EntryFlags::SIZE + version_size + meta.klen + meta.vlen
        {
          return Err(Among::Right(
            Error::larger_batch_size(buf.capacity() as u32),
          ));
        }

        let ent_len_size = buf.put_u64_varint_unchecked(meta.packed_kvlen);
        debug_assert_eq!(
          ent_len_size, meta.packed_kvlen_size,
          "the actual encoded u64 varint length ({}) doos not match the length ({}) returned by `dbutils::leb128::encoded_u64_varint_len`, please report bug to https://github.com/al8n/layer0/issues",
          ent_len_size, meta.packed_kvlen_size,
        );

        buf.put_u8_unchecked(ent.flag.bits());
        let ptr = buf.as_mut_ptr();
        let (key_ptr, val_ptr) = if let Some(version) = ent.internal_version() {
          buf.put_u64_le_unchecked(version);

          if maximum < version {
            maximum = version;
          }

          if minimum > version {
            minimum = version;
          }

          (
            ptr.add(cursor + meta.key_offset()),
            ptr.add(cursor + meta.value_offset()),
          )
        } else {
          (
            ptr.add(cursor + meta.key_offset()),
            ptr.add(cursor + meta.value_offset()),
          )
        };
        buf.set_len(cursor + meta.value_offset());

        let (kb, vb) = (ent.key(), ent.value());
        let mut key_buf = VacantBuffer::new(meta.klen, NonNull::new_unchecked(key_ptr));
        let written = kb.write(&mut key_buf).map_err(Among::Left)?;
        debug_assert_eq!(
          written, meta.klen,
          "the actual bytes written to the key buffer not equal to the expected size, expected {} but got {}.",
          meta.klen, written,
        );

        buf.set_len(cursor + meta.checksum_offset());
        if let Some(vb) = vb {
          let mut value_buf = VacantBuffer::new(meta.vlen, NonNull::new_unchecked(val_ptr));
          let written = vb.write(&mut value_buf).map_err(Among::Middle)?;

          debug_assert_eq!(
            written, meta.vlen,
            "the actual bytes written to the value buffer not equal to the expected size, expected {} but got {}.",
            meta.vlen, written,
          );
        }

        let entry_size = meta.entry_size as usize;
        ent.set_pointer(<<Self::Memtable as BaseTable>::Pointer as Pointer>::new(
          ent.flag,
          meta.klen,
          meta.vlen,
          ptr.add(cursor + meta.entry_flag_offset()),
        ));
        cursor += entry_size;
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
      let buf_cap = buf.capacity();

      if self.options().sync() && allocator.is_ondisk() {
        allocator
          .flush_header_and_range(Buffer::offset(&buf), buf_cap)
          .map_err(|e| Among::Right(e.into()))?;
      }
      buf.detach();
      Buffer::buffer_offset(&buf)
    };

    self
      .insert_pointers(batch.iter_mut().map(|e| e.take_pointer().unwrap()))
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
}

impl<K, V, S, T> WalReader<K, V, S> for T
where
  K: ?Sized,
  V: ?Sized,
  T: Wal<K, V, S>,
  T::Memtable: Memtable,
  <T::Memtable as BaseTable>::Pointer: WithoutVersion,
{
  type Allocator = T::Allocator;

  type Memtable = T::Memtable;

  #[inline]
  fn memtable(&self) -> &Self::Memtable {
    T::memtable(self)
  }
}

impl<K, V, S, T> MultipleVersionWalReader<K, V, S> for T
where
  K: ?Sized,
  V: ?Sized,
  T: Wal<K, V, S>,
  T::Memtable: MultipleVersionMemtable,
  <T::Memtable as BaseTable>::Pointer: WithVersion,
{
  type Allocator = T::Allocator;

  type Memtable = T::Memtable;

  #[inline]
  fn memtable(&self) -> &Self::Memtable {
    T::memtable(self)
  }
}

pub trait Constructable<K: ?Sized, V: ?Sized>: Sized {
  type Allocator: Allocator + 'static;
  type Wal: Wal<K, V, Self::Checksumer, Allocator = Self::Allocator, Memtable = Self::Memtable>
    + 'static;
  type Memtable: BaseTable;
  type Checksumer;
  type Reader;

  #[inline]
  fn allocator<'a>(&'a self) -> &'a Self::Allocator
  where
    Self::Allocator: 'a,
    Self::Wal: 'a,
  {
    self.as_wal().allocator()
  }

  fn as_wal(&self) -> &Self::Wal;

  // fn as_wal_mut(&mut self) -> &mut Self::Wal;

  fn new_in(
    arena: Self::Allocator,
    opts: Options,
    memtable_opts: <Self::Memtable as BaseTable>::Options,
    cks: Self::Checksumer,
  ) -> Result<Self::Wal, Error<Self::Memtable>> {
    unsafe {
      let slice = arena.reserved_slice_mut();
      slice[0..6].copy_from_slice(&MAGIC_TEXT);
      slice[6..8].copy_from_slice(&opts.magic_version().to_le_bytes());
    }

    arena
      .flush_range(0, HEADER_SIZE)
      .map_err(Into::into)
      .and_then(|_| {
        Self::Memtable::new(memtable_opts)
          .map(|memtable| {
            <Self::Wal as Wal<K, V, Self::Checksumer>>::construct(arena, memtable, opts, cks, 0, 0)
          })
          .map_err(Error::memtable)
      })
  }

  fn replay(
    arena: Self::Allocator,
    opts: Options,
    memtable_opts: <Self::Memtable as BaseTable>::Options,
    ro: bool,
    checksumer: Self::Checksumer,
  ) -> Result<Self::Wal, Error<Self::Memtable>>
  where
    Self::Checksumer: BuildChecksumer,
    <Self::Memtable as BaseTable>::Pointer: Pointer + Ord + 'static,
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

    let set = <Self::Wal as Wal<K, V, Self::Checksumer>>::Memtable::new(memtable_opts)
      .map_err(Error::memtable)?;

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

        let header = arena.get_u8(cursor).unwrap();
        let flag = Flags::from_bits_retain(header);

        if !flag.contains(Flags::BATCHING) {
          let (readed, encoded_len) =
            arena
              .get_u64_varint(cursor + RECORD_FLAG_SIZE)
              .map_err(|e| {
                #[cfg(feature = "tracing")]
                tracing::error!(err=%e);

                Error::corrupted(e)
              })?;
          let (key_len, value_len) = split_lengths(encoded_len);
          let key_len = key_len as usize;
          let value_len = value_len as usize;
          let entry_flag = arena
            .get_u8(cursor + RECORD_FLAG_SIZE + readed)
            .map_err(|e| {
              #[cfg(feature = "tracing")]
              tracing::error!(err=%e);

              Error::corrupted(e)
            })?;

          let entry_flag = EntryFlags::from_bits_retain(entry_flag);
          let version_size = if entry_flag.contains(EntryFlags::VERSIONED) {
            VERSION_SIZE
          } else {
            0
          };
          // Same as above, if we reached the end of the arena, we should discard the remaining.
          let cks_offset =
            RECORD_FLAG_SIZE + readed + EntryFlags::SIZE + version_size + key_len + value_len;
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

          let ptr = arena.get_pointer(cursor + RECORD_FLAG_SIZE + readed);

          let flag = EntryFlags::from_bits_retain(*ptr);
          let pointer: <Self::Memtable as BaseTable>::Pointer =
            Pointer::new(flag, key_len, value_len, ptr);

          if flag.contains(EntryFlags::VERSIONED) {
            if let Some(version) = pointer.version() {
              minimum_version = minimum_version.min(version);
              maximum_version = maximum_version.max(version);
            }
          }

          if flag.contains(EntryFlags::REMOVED) {
            set.remove(pointer).map_err(Error::memtable)?;
          } else {
            set.insert(pointer).map_err(Error::memtable)?;
          }

          cursor += cks_offset + CHECKSUM_SIZE;
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
            let (kvlen, ent_len) = decode_u64_varint(batch_data_buf).map_err(|e| {
              #[cfg(feature = "tracing")]
              tracing::error!(err=%e);

              Error::corrupted(e)
            })?;

            let (klen, vlen) = split_lengths(ent_len);
            let klen = klen as usize;
            let vlen = vlen as usize;

            let ptr = arena.get_pointer(cursor + RECORD_FLAG_SIZE + readed + sub_cursor + kvlen);
            let flag = EntryFlags::from_bits_retain(*ptr);
            let ptr: <Self::Memtable as BaseTable>::Pointer = Pointer::new(flag, klen, vlen, ptr);

            let ent_len = if flag.contains(EntryFlags::VERSIONED) {
              let version = ptr.version();
              if let Some(version) = version {
                minimum_version = minimum_version.min(version);
                maximum_version = maximum_version.max(version);
              }
              kvlen + EntryFlags::SIZE + VERSION_SIZE + klen + vlen
            } else {
              kvlen + EntryFlags::SIZE + klen + vlen
            };

            if flag.contains(EntryFlags::REMOVED) {
              set.remove(ptr).map_err(Error::memtable)?;
            } else {
              set.insert(ptr).map_err(Error::memtable)?;
            }

            sub_cursor += ent_len;
            batch_data_buf = &batch_data_buf[ent_len..];
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

    Ok(<Self::Wal as Wal<K, V, Self::Checksumer>>::construct(
      arena,
      set,
      opts,
      checksumer,
      maximum_version,
      minimum_version,
    ))
  }

  fn from_core(core: Self::Wal) -> Self;
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
const fn check<T: BaseTable>(
  klen: usize,
  vlen: usize,
  versioned: bool,
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

  let encoded_entry_meta = EncodedEntryMeta::new(klen, vlen, versioned);
  if encoded_entry_meta.entry_size == u32::MAX {
    let version_size = if versioned { VERSION_SIZE } else { 0 };
    return Err(Error::entry_too_large(
      encoded_entry_meta.entry_size as u64,
      min_u64(
        RECORD_FLAG_SIZE as u64
          + 10
          + EntryFlags::SIZE as u64
          + version_size as u64
          + max_key_size as u64
          + max_value_size as u64,
        u32::MAX as u64,
      ),
    ));
  }

  Ok(encoded_entry_meta)
}

#[inline]
fn check_batch_entry<T: BaseTable>(
  klen: usize,
  vlen: usize,
  max_key_size: u32,
  max_value_size: u32,
  versioned: bool,
) -> Result<EncodedEntryMeta, Error<T>> {
  let max_ksize = min_u64(max_key_size as u64, u32::MAX as u64);
  let max_vsize = min_u64(max_value_size as u64, u32::MAX as u64);

  if max_ksize < klen as u64 {
    return Err(Error::key_too_large(klen as u64, max_key_size));
  }

  if max_vsize < vlen as u64 {
    return Err(Error::value_too_large(vlen as u64, max_value_size));
  }

  let encoded_entry_meta = EncodedEntryMeta::batch(klen, vlen, versioned);
  if encoded_entry_meta.entry_size == u32::MAX {
    let version_size = if versioned { VERSION_SIZE } else { 0 };
    return Err(Error::entry_too_large(
      encoded_entry_meta.entry_size as u64,
      min_u64(
        10 + EntryFlags::SIZE as u64
          + version_size as u64
          + max_key_size as u64
          + max_value_size as u64,
        u32::MAX as u64,
      ),
    ));
  }

  Ok(encoded_entry_meta)
}
