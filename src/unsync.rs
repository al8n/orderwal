use core::{cell::UnsafeCell, ops::RangeBounds, ptr::NonNull};
use std::{collections::BTreeSet, rc::Rc};

use super::*;

use among::Among;
use either::Either;
use error::Error;
use rarena_allocator::unsync::Arena;
use wal::{
  sealed::{Constructor, Sealed},
  ImmutableWal,
};

/// Iterators for the `OrderWal`.
pub mod iter;
use iter::*;

mod c;
use c::*;

#[cfg(all(
  test,
  any(
    all_tests,
    test_unsync_constructor,
    test_unsync_insert,
    test_unsync_get,
    test_unsync_iters,
  )
))]
mod tests;

/// An ordered write-ahead log implementation for single thread environments.
///
/// Only the first instance of the WAL can write to the log, while the rest can only read from the log.
// ```text
// +----------------------+-------------------------+--------------------+
// | magic text (6 bytes) | magic version (2 bytes) |  header (8 bytes)  |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// |     flag (1 byte)    |    key len (4 bytes)    |    key (n bytes)   | value len (4 bytes) | value (n bytes) | checksum (8 bytes) |
// +----------------------+-------------------------+--------------------+---------------------+-----------------|--------------------+
// |     flag (1 byte)    |    key len (4 bytes)    |    key (n bytes)   | value len (4 bytes) | value (n bytes) | checksum (8 bytes) |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// |     flag (1 byte)    |    key len (4 bytes)    |    key (n bytes)   | value len (4 bytes) | value (n bytes) | checksum (8 bytes) |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// |         ...          |            ...          |         ...        |          ...        |        ...      |         ...        |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// |         ...          |            ...          |         ...        |          ...        |        ...      |         ...        |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// ```
pub struct OrderWal<C = Ascend, S = Crc32> {
  core: Rc<UnsafeCell<OrderWalCore<C, S>>>,
  ro: bool,
  _s: PhantomData<S>,
}

impl<C, S> Constructor<C, S> for OrderWal<C, S>
where
  C: 'static,
{
  type Allocator = Arena;
  type Core = OrderWalCore<C, S>;

  #[inline]
  fn from_core(core: Self::Core, ro: bool) -> Self {
    Self {
      core: Rc::new(UnsafeCell::new(core)),
      ro,
      _s: PhantomData,
    }
  }
}

impl<C, S> OrderWal<C, S> {
  /// Returns the path of the WAL if it is backed by a file.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::{unsync::OrderWal, Wal, Builder};
  ///
  /// // A in-memory WAL
  /// let wal = OrderWal::new(Builder::new().with_capacity(100)).unwrap();
  ///
  /// assert!(wal.path_buf().is_none());
  /// ```
  pub fn path_buf(&self) -> Option<&std::rc::Rc<std::path::PathBuf>> {
    self.core().arena.path()
  }

  #[inline]
  fn core(&self) -> &OrderWalCore<C, S> {
    unsafe { &*self.core.get() }
  }

  #[inline]
  fn core_mut(&mut self) -> &mut OrderWalCore<C, S> {
    unsafe { &mut *self.core.get() }
  }
}

impl<C, S> Sealed<C, S> for OrderWal<C, S>
where
  C: 'static,
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
    let core = self.core_mut();
    let buf = core.arena.alloc_bytes(elen);

    match buf {
      Err(e) => Err(Among::Right(Error::from_insufficient_space(e))),
      Ok(mut buf) => {
        unsafe {
          // We allocate the buffer with the exact size, so it's safe to write to the buffer.
          let flag = Flags::COMMITTED.bits();

          core.cks.reset();
          core.cks.update(&[flag]);

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
            core.cks.update(&buf[1..]);
            core.cks.digest()
          };
          buf.put_u64_le_unchecked(cks);

          // commit the entry
          buf[0] |= Flags::COMMITTED.bits();

          if core.opts.sync_on_write() && core.arena.is_ondisk() {
            core
              .arena
              .flush_range(buf.offset(), elen as usize)
              .map_err(|e| Among::Right(e.into()))?;
          }
          buf.detach();
          core.map.insert(Pointer::new(
            klen,
            vlen,
            buf.as_ptr().add(ko),
            core.cmp.cheap_clone(),
          ));
          Ok(())
        }
      }
    }
  }
}

impl<C, S> ImmutableWal<C, S> for OrderWal<C, S>
where
  C: 'static,
{
  type Iter<'a> = Iter<'a, C> where Self: 'a, C: Comparator;
  type Range<'a, Q, R> = Range<'a, C>
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    Self: 'a,
    C: Comparator;

  type Keys<'a> = Keys<'a, C> where Self: 'a, C: Comparator;

  type RangeKeys<'a, Q, R> = RangeKeys<'a, C>
      where
        R: RangeBounds<Q>,
        [u8]: Borrow<Q>,
        Q: Ord + ?Sized,
        Self: 'a,
        C: Comparator;

  type Values<'a> = Values<'a, C> where Self: 'a, C: Comparator;

  type RangeValues<'a, Q, R> = RangeValues<'a, C>
      where
        R: RangeBounds<Q>,
        [u8]: Borrow<Q>,
        Q: Ord + ?Sized,
        Self: 'a,
        C: Comparator;

  unsafe fn reserved_slice(&self) -> &[u8] {
    let core = self.core();
    if core.opts.reserved() == 0 {
      return &[];
    }

    &core.arena.reserved_slice()[HEADER_SIZE..]
  }

  #[inline]
  fn path(&self) -> Option<&std::path::Path> {
    self.core().arena.path().map(|p| p.as_ref().as_path())
  }

  /// Returns the number of entries in the WAL.
  #[inline]
  fn len(&self) -> usize {
    self.core().map.len()
  }

  /// Returns `true` if the WAL is empty.
  #[inline]
  fn is_empty(&self) -> bool {
    self.core().map.is_empty()
  }

  #[inline]
  fn maximum_key_size(&self) -> u32 {
    self.core().opts.maximum_key_size()
  }

  #[inline]
  fn maximum_value_size(&self) -> u32 {
    self.core().opts.maximum_value_size()
  }

  #[inline]
  fn contains_key<Q>(&self, key: &Q) -> bool
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    C: Comparator,
  {
    self.core().map.contains(key)
  }

  #[inline]
  fn iter(&self) -> Self::Iter<'_>
  where
    C: Comparator,
  {
    Iter::new(self.core().map.iter())
  }

  #[inline]
  fn range<Q, R>(&self, range: R) -> Self::Range<'_, Q, R>
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    C: Comparator,
  {
    Range::new(self.core().map.range(range))
  }

  #[inline]
  fn keys(&self) -> Self::Keys<'_>
  where
    C: Comparator,
  {
    Keys::new(self.core().map.iter())
  }

  #[inline]
  fn range_keys<Q, R>(&self, range: R) -> Self::RangeKeys<'_, Q, R>
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    C: Comparator,
  {
    RangeKeys::new(self.core().map.range(range))
  }

  #[inline]
  fn values(&self) -> Self::Values<'_>
  where
    C: Comparator,
  {
    Values::new(self.core().map.iter())
  }

  #[inline]
  fn range_values<Q, R>(&self, range: R) -> Self::RangeValues<'_, Q, R>
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    C: Comparator,
  {
    RangeValues::new(self.core().map.range(range))
  }

  #[inline]
  fn first(&self) -> Option<(&[u8], &[u8])>
  where
    C: Comparator,
  {
    self
      .core()
      .map
      .first()
      .map(|ent| (ent.as_key_slice(), ent.as_value_slice()))
  }

  #[inline]
  fn last(&self) -> Option<(&[u8], &[u8])>
  where
    C: Comparator,
  {
    self
      .core()
      .map
      .last()
      .map(|ent| (ent.as_key_slice(), ent.as_value_slice()))
  }

  #[inline]
  fn get<Q>(&self, key: &Q) -> Option<&[u8]>
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    C: Comparator,
  {
    self.core().map.get(key).map(|ent| ent.as_value_slice())
  }
}

impl<C, S> Wal<C, S> for OrderWal<C, S>
where
  C: 'static,
{
  type Reader = Self;

  #[inline]
  fn read_only(&self) -> bool {
    self.ro
  }

  #[inline]
  fn reader(&self) -> Self::Reader {
    Self {
      core: self.core.clone(),
      ro: true,
      _s: PhantomData,
    }
  }

  #[inline]
  unsafe fn reserved_slice_mut(&mut self) -> &mut [u8] {
    let core = self.core_mut();
    if core.opts.reserved() == 0 {
      return &mut [];
    }

    &mut core.arena.reserved_slice_mut()[HEADER_SIZE..]
  }

  #[inline]
  fn flush(&self) -> Result<(), Error> {
    if self.ro {
      return Err(error::Error::read_only());
    }

    self.core().arena.flush().map_err(Into::into)
  }

  #[inline]
  fn flush_async(&self) -> Result<(), Error> {
    if self.ro {
      return Err(error::Error::read_only());
    }

    self.core().arena.flush_async().map_err(Into::into)
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
    self
      .check(
        key.len(),
        vb.size() as usize,
        self.maximum_key_size(),
        self.maximum_value_size(),
        self.read_only(),
      )
      .map_err(Either::Right)?;

    if let Some(ent) = self.core().map.get(key) {
      return Ok(Some(ent.as_value_slice()));
    }

    self.insert_with_value_builder::<E>(key, vb).map(|_| None)
  }
}
