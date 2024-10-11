use core::ops::{Bound, RangeBounds};

use crossbeam_skiplist::SkipSet;
use dbutils::equivalent::Comparable;
use rarena_allocator::sync::Arena;

use crate::{
  sealed::{self, Pointer, Wal},
  Options,
};

impl<P> sealed::Memtable for SkipSet<P>
where
  P: Send + Ord,
{
  type Pointer = P;

  type Item<'a>
    = crossbeam_skiplist::set::Entry<'a, P>
  where
    Self::Pointer: 'a,
    Self: 'a;

  type Iterator<'a>
    = crossbeam_skiplist::set::Iter<'a, P>
  where
    Self::Pointer: 'a,
    Self: 'a;

  type Range<'a, Q, R>
    = crossbeam_skiplist::set::Range<'a, Q, R, Self::Pointer>
  where
    Self::Pointer: 'a,
    Self: 'a,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized + Comparable<Self::Pointer>;

  fn insert(&mut self, ele: Self::Pointer)
  where
    P: Ord + 'static,
  {
    SkipSet::insert(self, ele);
  }

  #[inline]
  fn first(&self) -> Option<Self::Item<'_>> {
    SkipSet::front(self)
  }

  #[inline]
  fn last(&self) -> Option<Self::Item<'_>> {
    SkipSet::back(self)
  }

  #[inline]
  fn get<Q>(&self, key: &Q) -> Option<Self::Item<'_>>
  where
    Q: Ord + ?Sized + Comparable<P>,
  {
    SkipSet::get(self, key)
  }

  #[inline]
  fn contains<Q>(&self, key: &Q) -> bool
  where
    Q: Ord + ?Sized + Comparable<P>,
  {
    SkipSet::contains(self, key)
  }

  #[inline]
  fn iter(&self) -> Self::Iterator<'_> {
    SkipSet::iter(self)
  }

  #[inline]
  fn range<Q, R>(&self, range: R) -> Self::Range<'_, Q, R>
  where
    R: RangeBounds<Q>,
    Q: Ord + ?Sized + Comparable<P>,
  {
    SkipSet::range(self, range)
  }
}

pub struct OrderCore<P, C, S> {
  pub(super) arena: Arena,
  pub(super) map: SkipSet<P>,
  pub(super) max_version: u64,
  pub(super) min_version: u64,
  pub(super) opts: Options,
  pub(super) cmp: C,
  pub(super) cks: S,
}

impl<P, C, S> Wal<P, C, S> for OrderCore<P, C, S>
where
  P: Ord + Send + 'static,
{
  type Allocator = Arena;
  type Memtable = SkipSet<P>;

  #[inline]
  fn memtable(&self) -> &Self::Memtable {
    &self.map
  }

  #[inline]
  fn construct(
    arena: Arena,
    set: SkipSet<P>,
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
  fn update_maximum_version(&mut self, version: u64) {
    self.max_version = version;
  }

  #[inline]
  fn update_minimum_version(&mut self, version: u64) {
    self.min_version = version;
  }

  #[inline]
  fn allocator(&self) -> &Self::Allocator {
    &self.arena
  }

  #[inline]
  fn upper_bound<Q>(&self, version: Option<u64>, bound: Bound<&Q>) -> Option<&[u8]>
  where
    P: Pointer<Comparator = C>,
    Q: Ord + ?Sized + Comparable<P>,
  {
    match version {
      None => self.map.upper_bound(bound).map(|ent| ent.as_key_slice()),
      Some(version) => {
        let mut ent = self.map.upper_bound(bound);
        loop {
          match ent {
            Some(ent) if ent.version() <= version => return Some(ent.as_key_slice()),
            Some(e) => ent = e.next(),
            None => return None,
          }
        }
      }
    }
  }

  #[inline]
  fn lower_bound<Q>(&self, version: Option<u64>, bound: core::ops::Bound<&Q>) -> Option<&[u8]>
  where
    P: Pointer<Comparator = C>,
    Q: Ord + ?Sized + Comparable<P>,
  {
    match version {
      None => self.map.lower_bound(bound).map(|ent| ent.as_key_slice()),
      Some(version) => {
        let mut ent = self.map.lower_bound(bound);
        loop {
          match ent {
            Some(ent) if ent.version() <= version => return Some(ent.as_key_slice()),
            Some(e) => ent = e.next(),
            None => return None,
          }
        }
      }
    }
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
    for ptr in ptrs {
      self.map.insert(ptr);
    }
  }
}
