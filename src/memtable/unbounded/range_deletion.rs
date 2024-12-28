use core::{
  cell::OnceCell,
  ops::{Bound, RangeBounds},
};

use crossbeam_skiplist_mvcc::nested::{Entry, Iter, Range};
use dbutils::{
  equivalentor::{Comparator, QueryComparator},
  state::State,
};

use crate::types::{
  sealed::{Pointee, RangeComparator},
  Mode, Query, QueryRange, RawRangeRemoveRef, RecordPointer,
};

/// Range deletion entry.
pub struct RangeRemoveEntry<'a, S, C, T>
where
  S: State,
  T: Mode,
{
  pub(crate) ent: Entry<'a, RecordPointer, RecordPointer, S, T::RangeComparator<C>>,
  data: OnceCell<RawRangeRemoveRef<'a>>,
  start_bound: OnceCell<Bound<T::Key<'a>>>,
  end_bound: OnceCell<Bound<T::Key<'a>>>,
}
impl<S, C, T> core::fmt::Debug for RangeRemoveEntry<'_, S, C, T>
where
  C: 'static,
  S: State,
  T: Mode,
  T::RangeComparator<C>: Comparator<RecordPointer> + RangeComparator<C>,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    self
      .data
      .get_or_init(|| self.ent.comparator().fetch_range_deletion(self.ent.key()))
      .write_fmt("RangeRemoveEntry", f)
  }
}
impl<'a, S, C, T> Clone for RangeRemoveEntry<'a, S, C, T>
where
  S: State,
  // S::Data<'a, LazyRef<'a, RecordPointer>>: Clone,
  T: Mode,
  S::Data<'a, T::Value<'a>>: Clone,
  T::Key<'a>: Clone,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      data: self.data.clone(),
      start_bound: self.start_bound.clone(),
      end_bound: self.end_bound.clone(),
    }
  }
}
impl<'a, S, C, T> RangeRemoveEntry<'a, S, C, T>
where
  S: State,
  T: Mode,
{
  pub(in crate::memtable) fn new(
    ent: Entry<'a, RecordPointer, RecordPointer, S, T::RangeComparator<C>>,
  ) -> Self {
    Self {
      ent,
      data: OnceCell::new(),
      start_bound: OnceCell::new(),
      end_bound: OnceCell::new(),
    }
  }
}

impl<'a, S, C, T> crate::memtable::RawRangeEntry<'a> for RangeRemoveEntry<'a, S, C, T>
where
  C: 'static,
  S: State,
  T: Mode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]> + 'a,
  T::RangeComparator<C>: Comparator<RecordPointer> + RangeComparator<C>,
{
  #[inline]
  fn raw_start_bound(&self) -> Bound<&'a [u8]> {
    let ent = self
      .data
      .get_or_init(|| self.ent.comparator().fetch_range_deletion(self.ent.key()));
    ent.start_bound()
  }

  #[inline]
  fn raw_end_bound(&self) -> Bound<&'a [u8]> {
    let ent = self
      .data
      .get_or_init(|| self.ent.comparator().fetch_range_deletion(self.ent.key()));
    ent.end_bound()
  }
}

impl<'a, S, C, T> crate::memtable::RangeEntry<'a> for RangeRemoveEntry<'a, S, C, T>
where
  C: 'static,
  S: State,
  T: Mode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]> + 'a,
  T::RangeComparator<C>: Comparator<RecordPointer> + RangeComparator<C>,
{
  type Key = <T::Key<'a> as Pointee<'a>>::Output;

  #[inline]
  fn start_bound(&self) -> Bound<Self::Key> {
    let start_bound = self.start_bound.get_or_init(|| {
      let ent = self
        .data
        .get_or_init(|| self.ent.comparator().fetch_range_deletion(self.ent.key()));
      ent.start_bound().map(<T::Key<'a> as Pointee>::from_input)
    });
    start_bound.as_ref().map(|k| k.output())
  }

  #[inline]
  fn end_bound(&self) -> Bound<Self::Key> {
    let end_bound = self.end_bound.get_or_init(|| {
      let ent = self
        .data
        .get_or_init(|| self.ent.comparator().fetch_range_deletion(self.ent.key()));
      ent.end_bound().map(<T::Key<'a> as Pointee>::from_input)
    });
    end_bound.as_ref().map(|k| k.output())
  }

  #[inline]
  fn next(&mut self) -> Option<Self> {
    self.ent.next().map(Self::new)
  }

  #[inline]
  fn prev(&mut self) -> Option<Self> {
    self.ent.prev().map(Self::new)
  }

  #[inline]
  fn version(&self) -> u64 {
    self.ent.version()
  }
}
impl<S, C, T> RangeRemoveEntry<'_, S, C, T>
where
  C: 'static,
  S: State,
  T: Mode,
{
  /// Returns the version of the entry.
  #[inline]
  pub fn version(&self) -> u64 {
    self.ent.version()
  }
}

impl<'a, S, C, T> crate::memtable::RangeRemoveEntry<'a> for RangeRemoveEntry<'a, S, C, T>
where
  C: 'static,
  S: State,
  T: Mode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]> + 'a,
  T::RangeComparator<C>: Comparator<RecordPointer> + RangeComparator<C>,
{
}

/// The iterator for point entries.
pub struct IterRangeRemove<'a, S, C, T>
where
  S: State,
  T: Mode,
{
  iter: Iter<'a, RecordPointer, RecordPointer, S, T::RangeComparator<C>>,
}
impl<'a, S, C, T> IterRangeRemove<'a, S, C, T>
where
  S: State,
  T: Mode,
{
  #[inline]
  pub(in crate::memtable) const fn new(
    iter: Iter<'a, RecordPointer, RecordPointer, S, T::RangeComparator<C>>,
  ) -> Self {
    Self { iter }
  }
}
impl<'a, S, C, T> Iterator for IterRangeRemove<'a, S, C, T>
where
  C: 'static,
  S: State,
  T: Mode,
  T::RangeComparator<C>: Comparator<RecordPointer> + 'a,
{
  type Item = RangeRemoveEntry<'a, S, C, T>;
  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(RangeRemoveEntry::new)
  }
}
impl<'a, S, C, T> DoubleEndedIterator for IterRangeRemove<'a, S, C, T>
where
  C: 'static,
  S: State,
  T: Mode,
  T::RangeComparator<C>: Comparator<RecordPointer> + 'a,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(RangeRemoveEntry::new)
  }
}
/// The iterator over a subset of point entries.
pub struct RangeRangeRemove<'a, S, Q, R, C, T>
where
  S: State,
  T: Mode,
  Q: ?Sized,
  R: RangeBounds<Q>,
{
  range:
    Range<'a, RecordPointer, RecordPointer, S, Query<Q>, QueryRange<Q, R>, T::RangeComparator<C>>,
}

impl<'a, S, Q, R, C, T> RangeRangeRemove<'a, S, Q, R, C, T>
where
  S: State,
  T: Mode,
  Q: ?Sized,
  R: RangeBounds<Q>,
{
  #[inline]
  pub(in crate::memtable) const fn new(
    range: Range<
      'a,
      RecordPointer,
      RecordPointer,
      S,
      Query<Q>,
      QueryRange<Q, R>,
      T::RangeComparator<C>,
    >,
  ) -> Self {
    Self { range }
  }
}
impl<'a, S, Q, R, C, T> Iterator for RangeRangeRemove<'a, S, Q, R, C, T>
where
  C: 'static,
  S: State,
  R: RangeBounds<Q>,
  Q: ?Sized,
  T: Mode,
  T::RangeComparator<C>: QueryComparator<RecordPointer, Query<Q>> + 'a,
{
  type Item = RangeRemoveEntry<'a, S, C, T>;
  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.range.next().map(RangeRemoveEntry::new)
  }
}
impl<'a, S, Q, R, C, T> DoubleEndedIterator for RangeRangeRemove<'a, S, Q, R, C, T>
where
  C: 'static,
  S: State,
  R: RangeBounds<Q>,
  Q: ?Sized,
  T: Mode,
  T::RangeComparator<C>: QueryComparator<RecordPointer, Query<Q>> + 'a,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.range.next_back().map(RangeRemoveEntry::new)
  }
}
