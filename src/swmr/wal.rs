use super::super::*;

use either::Either;
use error::Error;
use wal::{
  sealed::{Base, Constructor, Sealed, WalCore},
  ImmutableWal,
};

use rarena_allocator::sync::Arena;
use std::sync::Arc;

mod reader;
pub use reader::*;

mod iter;
pub use iter::*;

#[cfg(all(
  test,
  any(
    all_tests,
    test_swmr_constructor,
    test_swmr_insert,
    test_swmr_get,
    test_swmr_iters,
  )
))]
mod tests;

#[doc(hidden)]
pub struct OrderWalCore<C, S> {
  arena: Arena,
  map: SkipSet<Pointer<C>>,
  opts: Options,
  cmp: C,
  cks: S,
}

impl<C: Comparator, S> OrderWalCore<C, S> {
  #[inline]
  fn iter(&self) -> Iter<'_, C> {
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
  fn construct(arena: Arena, set: SkipSet<Pointer<C>>, opts: Options, cmp: C, cks: S) -> Self {
    Self {
      arena,
      map: set,
      cmp,
      opts,
      cks,
    }
  }
}

/// A single writer multiple readers ordered write-ahead log implementation.
///
/// Both read and write operations of this WAL are zero-cost (no allocation will happen for both read and write).
///
/// Users can create multiple readers from the WAL by [`OrderWal::reader`], but only one writer is allowed.
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
  _s: PhantomData<S>,
}

impl<C, S> Constructor<C, S> for OrderWal<C, S>
where
  C: Send + 'static,
{
  type Allocator = Arena;
  type Core = OrderWalCore<C, S>;

  #[inline]
  fn allocator(&self) -> &Self::Allocator {
    &self.core.arena
  }

  #[inline]
  fn from_core(core: Self::Core) -> Self {
    Self {
      core: Arc::new(core),
      _s: PhantomData,
    }
  }
}

impl<C, S> Sealed<C, S> for OrderWal<C, S>
where
  C: Send + 'static,
{
  fn hasher(&self) -> &S {
    &self.core.cks
  }

  fn options(&self) -> &Options {
    &self.core.opts
  }

  fn comparator(&self) -> &C {
    &self.core.cmp
  }

  fn insert_pointer(&self, ptr: Pointer<C>)
  where
    C: Comparator,
  {
    self.core.map.insert(ptr);
  }
}

impl<C, S> OrderWal<C, S> {
  /// Returns the path of the WAL if it is backed by a file.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::{swmr::OrderWal, Wal, Builder};
  ///
  /// // A in-memory WAL
  /// let wal = OrderWal::new(Builder::new().with_capacity(100)).unwrap();
  ///
  /// assert!(wal.path_buf().is_none());
  /// ```
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
  fn path(&self) -> Option<&std::path::Path> {
    self.core.arena.path().map(|p| p.as_ref().as_path())
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
  fn remaining(&self) -> u32 {
    self.core.arena.remaining() as u32
  }

  #[inline]
  fn options(&self) -> &Options {
    &self.core.opts
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
  fn reader(&self) -> Self::Reader {
    OrderWalReader::new(self.core.clone())
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

    if let Some(ent) = self.core.map.get(key) {
      return Ok(Some(ent.as_value_slice()));
    }

    self.insert_with_value_builder::<E>(key, vb).map(|_| None)
  }
}
