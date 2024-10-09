use core::{cell::UnsafeCell, ops::RangeBounds};
use std::{collections::BTreeSet, rc::Rc};

use super::*;

use checksum::BuildChecksumer;
use either::Either;
use error::Error;
use pointer::Pointer;
use rarena_allocator::unsync::Arena;
use sealed::{Constructor, Sealed};

pub use super::{
  batch::{Batch, BatchWithBuilders, BatchWithKeyBuilder, BatchWithValueBuilder},
  builder::Builder,
  wal::{ImmutableWal, Wal},
  Comparator, KeyBuilder, VacantBuffer, ValueBuilder,
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
  _s: PhantomData<S>,
}

impl<C, S> Constructor<C, S> for OrderWal<C, S>
where
  C: Comparator + 'static,
{
  type Allocator = Arena;
  type Core = OrderWalCore<C, S>;
  type Pointer = Pointer<C>;

  #[inline]
  fn allocator(&self) -> &Self::Allocator {
    &self.core().arena
  }

  #[inline]
  fn from_core(core: Self::Core) -> Self {
    Self {
      core: Rc::new(UnsafeCell::new(core)),
      _s: PhantomData,
    }
  }
}

impl<C, S> OrderWal<C, S> {
  /// Returns the path of the WAL if it is backed by a file.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{unsync::OrderWal, Wal, Builder};
  ///
  /// // A in-memory WAL
  /// let wal = Builder::new().with_capacity(100).alloc::<OrderWal>().unwrap();
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
}

impl<C, S> Sealed<C, S> for OrderWal<C, S>
where
  C: Comparator + 'static,
{
  #[inline]
  fn hasher(&self) -> &S {
    &self.core().cks
  }

  #[inline]
  fn options(&self) -> &Options {
    &self.core().opts
  }

  #[inline]
  fn comparator(&self) -> &C {
    &self.core().cmp
  }

  #[inline]
  fn insert_pointer(&self, ptr: Pointer<C>)
  where
    C: Comparator,
  {
    unsafe {
      (*self.core.get()).map.insert(ptr);
    }
  }

  #[inline]
  fn insert_pointers(&self, ptrs: impl Iterator<Item = Pointer<C>>)
  where
    C: Comparator,
  {
    unsafe {
      (*self.core.get()).map.extend(ptrs);
    }
  }
}

impl<C, S> ImmutableWal<C, S> for OrderWal<C, S>
where
  C: Comparator + 'static,
{
  type Iter<'a>
    = Iter<'a, C>
  where
    Self: 'a,
    C: Comparator;
  type Range<'a, Q, R>
    = Range<'a, C>
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    Self: 'a,
    C: Comparator;

  type Keys<'a>
    = Keys<'a, C>
  where
    Self: 'a,
    C: Comparator;

  type RangeKeys<'a, Q, R>
    = RangeKeys<'a, C>
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    Self: 'a,
    C: Comparator;

  type Values<'a>
    = Values<'a, C>
  where
    Self: 'a,
    C: Comparator;

  type RangeValues<'a, Q, R>
    = RangeValues<'a, C>
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    Self: 'a,
    C: Comparator;

  #[inline]
  fn options(&self) -> &Options {
    &self.core().opts
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
  fn remaining(&self) -> u32 {
    self.core().arena.remaining() as u32
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
  C: Comparator + 'static,
{
  type Reader = Self;

  #[inline]
  fn reader(&self) -> Self::Reader {
    Self {
      core: self.core.clone(),
      _s: PhantomData,
    }
  }

  fn get_or_insert_with_value_builder<E>(
    &mut self,
    key: &[u8],
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
  ) -> Result<Option<&[u8]>, Either<E, Error>>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
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
