use core::{
  borrow::Borrow,
  ops::{Bound, RangeBounds},
};

use dbutils::Comparator;
use rarena_allocator::sync::Arena;
use std::collections::{btree_set, BTreeSet};

use crate::{
  sealed::{self, WalCore},
  Options,
};

pub struct OrderWalCore<P, C, S> {
  pub(super) arena: Arena,
  pub(super) map: BTreeSet<P>,
  pub(super) max_version: u64,
  pub(super) min_version: u64,
  pub(super) opts: Options,
  pub(super) cmp: C,
  pub(super) cks: S,
}

impl<P> sealed::Base for BTreeSet<P>
where
  P: sealed::Pointer + Ord,
{
  type Pointer = P;

  fn insert(&mut self, ele: Self::Pointer) {
    BTreeSet::insert(self, ele);
  }

  type Item<'a>
    = &'a P
  where
    Self::Pointer: 'a,
    Self: 'a;

  type Iterator<'a>
    = btree_set::Iter<'a, P>
  where
    Self::Pointer: 'a,
    Self: 'a;

  type Range<'a, Q, R>
    = btree_set::Range<'a, P>
  where
    Self::Pointer: 'a,
    Self: 'a,
    Self::Pointer: Borrow<Q>,
    R: RangeBounds<Q>,
    Q: ?Sized + Ord;

  #[inline]
  fn iter(&self) -> Self::Iterator<'_> {
    self.iter()
  }

  #[inline]
  fn range<Q, R>(&self, range: R) -> Self::Range<'_, Q, R>
  where
    R: RangeBounds<Q>,
    Self::Pointer: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    self.range(range)
  }

  #[inline]
  fn get<Q>(&self, key: &Q) -> Option<Self::Item<'_>>
  where
    Self::Pointer: Borrow<Q>,
    Q: ?Sized + Ord,
  {
    self.get(key)
  }

  #[inline]
  fn contains<Q>(&self, key: &Q) -> bool
  where
    Self::Pointer: Borrow<Q>,
    Q: ?Sized + Ord,
  {
    self.contains(key)
  }

  #[inline]
  fn first(&self) -> Option<Self::Item<'_>> {
    self.first()
  }

  #[inline]
  fn last(&self) -> Option<Self::Item<'_>> {
    self.last()
  }
}

impl<P, C, S> WalCore<P, C, S> for OrderWalCore<P, C, S>
where
  C: Comparator,
  P: sealed::Pointer<Comparator = C> + Ord,
{
  type Allocator = Arena;
  type Base = BTreeSet<P>;

  #[inline]
  fn base(&self) -> &Self::Base {
    &self.map
  }

  #[inline]
  fn construct(
    arena: Arena,
    set: BTreeSet<P>,
    opts: Options,
    cmp: C,
    checksumer: S,
    maximum_version: u64,
    minimum_version: u64,
  ) -> Self {
    Self {
      arena,
      map: set,
      cmp,
      opts,
      max_version: maximum_version,
      min_version: minimum_version,
      cks: checksumer,
    }
  }

  #[inline]
  fn options(&self) -> &Options {
    &self.opts
  }

  /// Returns the number of entries in the WAL.
  #[inline]
  fn len(&self) -> usize {
    self.map.len()
  }

  /// Returns `true` if the WAL is empty.
  #[inline]
  fn is_empty(&self) -> bool {
    self.map.is_empty()
  }

  #[inline]
  fn maximum_version(&self) -> u64 {
    self.max_version
  }

  #[inline]
  fn minimum_version(&self) -> u64 {
    self.min_version
  }

  #[inline]
  fn allocator(&self) -> &Self::Allocator {
    &self.arena
  }

  // TODO: implement this method for unsync::OrderWal when BTreeMap::upper_bound is stable
  #[inline]
  fn upper_bound<Q>(&self, version: Option<u64>, bound: Bound<&Q>) -> Option<&[u8]>
  where
    P: Borrow<Q> + sealed::Pointer + Ord,
    Q: ?Sized + Ord,
  {
    self
      .range(version, (Bound::Unbounded, bound))
      .last()
      .map(|ent| ent.0)
  }

  // TODO: implement this method for unsync::OrderWal when BTreeMap::lower_bound is stable
  #[inline]
  fn lower_bound<Q>(&self, version: Option<u64>, bound: core::ops::Bound<&Q>) -> Option<&[u8]>
  where
    P: Borrow<Q> + sealed::Pointer,
    Q: ?Sized + Ord,
  {
    self
      .range(version, (bound, Bound::Unbounded))
      .next()
      .map(|ent| ent.0)
  }

  #[inline]
  fn hasher(&self) -> &S {
    &self.cks
  }

  #[inline]
  fn comparator(&self) -> &C {
    &self.cmp
  }

  #[inline]
  fn insert_pointer(&mut self, ptr: P) {
    self.map.insert(ptr);
  }

  #[inline]
  fn insert_pointers(&mut self, ptrs: impl Iterator<Item = P>) {
    self.map.extend(ptrs);
  }
}

// #[test]
// fn test_() {
//   let core: OrderWalCore<pointer::Pointer<Ascend>, Ascend, Crc32> = todo!();

//   let start: &[u8] = &[0u8, 1u8];
//   let end: &[u8] = &[10u8];
//   core.range::<[u8], _>(None, (Bound::Included(start), Bound::Excluded(end)));
//   core.upper_bound::<[u8]>(None, Bound::Included(start));
//   core.get_or_insert(None, &[0u8], &[1]);
// }
