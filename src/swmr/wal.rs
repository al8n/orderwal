use super::super::*;

use among::Among;
use either::Either;
use error::Error;
use wal::{
  sealed::{Base, Constructor, Sealed, WalCore},
  ImmutableWal,
};

use core::ptr::NonNull;
use rarena_allocator::{sync::Arena, Error as ArenaError};
use std::sync::Arc;

mod reader;
pub use reader::*;

mod iter;
pub use iter::*;

#[cfg(test)]
mod tests;

pub struct OrderWalCore<C, S> {
  arena: Arena,
  map: SkipSet<Pointer<C>>,
  opts: Options,
  cmp: C,
  cks: UnsafeCellChecksumer<S>,
}

impl<C: Comparator, S> OrderWalCore<C, S> {
  #[inline]
  fn iter(&self) -> Iter<C> {
    Iter::new(self.map.iter())
  }
}

impl<C: Send + 'static> Base<C> for SkipSet<Pointer<C>> {
  fn insert(&mut self, ele: Pointer<C>)
  where
    C: Comparator,
  {
    SkipSet::insert(self, ele);
  }
}

impl<C: Send + 'static, S> WalCore<C, S> for OrderWalCore<C, S> {
  type Allocator = Arena;
  type Base = SkipSet<Pointer<C>>;

  #[inline]
  fn construct(
    arena: Arena,
    set: SkipSet<Pointer<C>>,
    opts: Options,
    cmp: C,
    checksumer: S,
  ) -> Self {
    Self {
      arena,
      map: set,
      cmp,
      opts,
      cks: UnsafeCellChecksumer::new(checksumer),
    }
  }
}

/// A single writer multiple readers ordered write-ahead log implementation.
///
/// Only the first instance of the WAL can write to the log, while the rest can only read from the log.
// ```text
// +----------------------+--------------------------+--------------------+
// | magic text (6 bytes) | magic version (2 bytes)  |  header (8 bytes)  |
// +----------------------+--------------------------+--------------------+-----------------+--------------------+
// |     flag (1 byte)    | klen & vlen (1-10 bytes) |    key (n bytes)   | value (n bytes) | checksum (8 bytes) |
// +----------------------+--------------------------+--------------------+-----------------|--------------------+
// |     flag (1 byte)    | klen & vlen (1-10 bytes) |    key (n bytes)   | value (n bytes) | checksum (8 bytes) |
// +----------------------+--------------------------+--------------------+-----------------+--------------------+
// |     flag (1 byte)    | klen & vlen (1-10 bytes) |    key (n bytes)   | value (n bytes) | checksum (8 bytes) |
// +----------------------+--------------------------+--------------------+-----------------+-----------------+--------------------+
// |         ...          |            ...           |         ...        |        ...      |        ...      |         ...        |
// +----------------------+--------------------------+--------------------+-----------------+-----------------+--------------------+
// |         ...          |            ...           |         ...        |        ...      |        ...      |         ...        |
// +----------------------+--------------------------+--------------------+-----------------+-----------------+--------------------+
// ```
pub struct OrderWal<C = Ascend, S = Crc32> {
  core: Arc<OrderWalCore<C, S>>,
  ro: bool,
  _s: PhantomData<S>,
}

impl<C, S> Constructor<C, S> for OrderWal<C, S>
where
  C: Send + 'static,
{
  type Allocator = Arena;
  type Core = OrderWalCore<C, S>;

  #[inline]
  fn from_core(core: Self::Core, ro: bool) -> Self {
    Self {
      core: Arc::new(core),
      ro,
      _s: PhantomData,
    }
  }
}

impl<C, S> Sealed<C, S> for OrderWal<C, S>
where
  C: Send + 'static,
{
  fn insert_with_in<KE, VE>(
    &mut self,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), KE>>,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), VE>>,
  ) -> Result<(), Among<KE, VE, Error>>
  where
    C: Comparator + CheapClone,
    S: Checksumer,
  {
    let (klen, kf) = kb.into_components();
    let (vlen, vf) = vb.into_components();
    let (len_size, kvlen, elen) = entry_size(klen, vlen);
    let klen = klen as usize;
    let vlen = vlen as usize;
    let buf = self.core.arena.alloc_bytes(elen);

    match buf {
      Err(e) => {
        let e = match e {
          ArenaError::InsufficientSpace {
            requested,
            available,
          } => error::Error::insufficient_space(requested, available),
          ArenaError::ReadOnly => error::Error::read_only(),
          _ => unreachable!(),
        };
        Err(Among::Right(e))
      }
      Ok(mut buf) => {
        unsafe {
          // We allocate the buffer with the exact size, so it's safe to write to the buffer.
          let flag = Flags::COMMITTED.bits();

          self.core.cks.reset();
          self.core.cks.update(&[flag]);

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
            self.core.cks.update(&buf[1..]);
            self.core.cks.digest()
          };
          buf.put_u64_le_unchecked(cks);

          // commit the entry
          buf[0] |= Flags::COMMITTED.bits();

          if self.core.opts.sync_on_write() && self.core.arena.is_ondisk() {
            self
              .core
              .arena
              .flush_range(buf.offset(), elen as usize)
              .map_err(|e| Among::Right(e.into()))?;
          }
          buf.detach();
          self.core.map.insert(Pointer::new(
            klen,
            vlen,
            buf.as_ptr().add(ko),
            self.core.cmp.cheap_clone(),
          ));
          Ok(())
        }
      }
    }
  }
}

impl<C, S> OrderWal<C, S> {
  /// Returns the read-only view for the WAL.
  #[inline]
  pub fn reader(&self) -> OrderWalReader<C, S> {
    OrderWalReader::new(self.core.clone())
  }

  /// Returns the path of the WAL if it is backed by a file.
  pub fn path_buf(&self) -> Option<&std::sync::Arc<std::path::PathBuf>> {
    self.core.arena.path()
  }
}

impl<C, S> ImmutableWal<C, S> for OrderWal<C, S>
where
  C: Send + 'static,
{
  type Iter<'a> = Iter<'a, C> where Self: 'a, C: Comparator;
  type Range<'a, Q, R> = Range<'a, Q, R, C>
  where
    R: core::ops::RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    Self: 'a,
    C: Comparator;
  type Keys<'a> = Keys<'a, C> where Self: 'a, C: Comparator;

  type RangeKeys<'a, Q, R> = RangeKeys<'a, Q, R, C>
      where
        R: core::ops::RangeBounds<Q>,
        [u8]: Borrow<Q>,
        Q: Ord + ?Sized,
        Self: 'a,
        C: Comparator;

  type Values<'a> = Values<'a, C> where Self: 'a, C: Comparator;

  type RangeValues<'a, Q, R> = RangeValues<'a, Q, R, C>
      where
        R: core::ops::RangeBounds<Q>,
        [u8]: Borrow<Q>,
        Q: Ord + ?Sized,
        Self: 'a,
        C: Comparator;

  #[inline]
  unsafe fn reserved_slice(&self) -> &[u8] {
    if self.core.opts.reserved() == 0 {
      return &[];
    }

    &self.core.arena.reserved_slice()[HEADER_SIZE..]
  }

  #[inline]
  fn path(&self) -> Option<&std::path::Path> {
    self.core.arena.path().map(|p| p.as_ref().as_path())
  }

  #[inline]
  fn read_only(&self) -> bool {
    self.ro
  }

  #[inline]
  fn len(&self) -> usize {
    self.core.map.len()
  }

  #[inline]
  fn maximum_key_size(&self) -> u32 {
    self.core.opts.maximum_key_size()
  }

  #[inline]
  fn maximum_value_size(&self) -> u32 {
    self.core.opts.maximum_value_size()
  }

  #[inline]
  fn contains_key<Q>(&self, key: &Q) -> bool
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    C: Comparator,
  {
    self.core.map.contains(key)
  }

  #[inline]
  fn iter(&self) -> Self::Iter<'_>
  where
    C: Comparator,
  {
    self.core.iter()
  }

  #[inline]
  fn range<Q, R>(&self, range: R) -> Self::Range<'_, Q, R>
  where
    R: core::ops::RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized + crossbeam_skiplist::Comparable<[u8]>,
    C: Comparator,
  {
    Range::new(self.core.map.range(range))
  }

  #[inline]
  fn keys(&self) -> Self::Keys<'_>
  where
    C: Comparator,
  {
    Keys::new(self.core.map.iter())
  }

  #[inline]
  fn range_keys<Q, R>(&self, range: R) -> Self::RangeKeys<'_, Q, R>
  where
    R: core::ops::RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    C: Comparator,
  {
    RangeKeys::new(self.core.map.range(range))
  }

  #[inline]
  fn values(&self) -> Self::Values<'_>
  where
    C: Comparator,
  {
    Values::new(self.core.map.iter())
  }

  #[inline]
  fn range_values<Q, R>(&self, range: R) -> Self::RangeValues<'_, Q, R>
  where
    R: core::ops::RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    C: Comparator,
  {
    RangeValues::new(self.core.map.range(range))
  }

  #[inline]
  fn first(&self) -> Option<(&[u8], &[u8])>
  where
    C: Comparator,
  {
    self
      .core
      .map
      .front()
      .map(|ent| (ent.as_key_slice(), ent.as_value_slice()))
  }

  #[inline]
  fn last(&self) -> Option<(&[u8], &[u8])>
  where
    C: Comparator,
  {
    self
      .core
      .map
      .back()
      .map(|ent| (ent.as_key_slice(), ent.as_value_slice()))
  }

  #[inline]
  fn get<Q>(&self, key: &Q) -> Option<&[u8]>
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    C: Comparator,
  {
    self.core.map.get(key).map(|ent| ent.as_value_slice())
  }
}

impl<C, S> Wal<C, S> for OrderWal<C, S>
where
  C: Send + 'static,
{
  type Reader = OrderWalReader<C, S>;

  #[inline]
  fn flush(&self) -> Result<(), Error> {
    if self.ro {
      return Err(error::Error::read_only());
    }

    self.core.arena.flush().map_err(Into::into)
  }

  #[inline]
  fn flush_async(&self) -> Result<(), Error> {
    if self.ro {
      return Err(error::Error::read_only());
    }

    self.core.arena.flush_async().map_err(Into::into)
  }

  #[inline]
  unsafe fn reserved_slice_mut(&mut self) -> &mut [u8] {
    if self.core.opts.reserved() == 0 {
      return &mut [];
    }

    &mut self.core.arena.reserved_slice_mut()[HEADER_SIZE..]
  }

  fn get_or_insert_with_value_builder<E>(
    &mut self,
    key: &[u8],
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
  ) -> Result<Option<&[u8]>, Either<E, Error>>
  where
    C: Comparator + CheapClone,
    S: Checksumer,
  {
    if self.read_only() {
      return Err(Either::Right(Error::read_only()));
    }

    self
      .check(
        key.len(),
        vb.size() as usize,
        self.maximum_key_size(),
        self.maximum_value_size(),
      )
      .map_err(Either::Right)?;

    if let Some(ent) = self.core.map.get(key) {
      return Ok(Some(ent.as_value_slice()));
    }

    self.insert_with_value_builder::<E>(key, vb).map(|_| None)
  }
}
