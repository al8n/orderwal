use core::{
  cell::OnceCell,
  ops::{Bound, RangeBounds},
};

use crossbeam_skiplist_mvcc::nested::{Entry, Iter, Range};
use dbutils::{
  equivalentor::{Comparator, QueryComparator},
  state::State,
};

use crate::{
  memtable::{sealed, Transfer},
  types::{
    sealed::{Pointee, RangeComparator},
    Query, QueryRange, RawRangeUpdateRef, RecordPointer, TypeMode,
  },
};

/// Range update entry.
pub struct RangeUpdateEntry<'a, S, C, T>
where
  S: State,
  T: TypeMode,
{
  pub(crate) ent: Entry<'a, RecordPointer, RecordPointer, S, T::RangeComparator<C>>,
  data: OnceCell<RawRangeUpdateRef<'a>>,
  start_bound: OnceCell<Bound<T::Key<'a>>>,
  end_bound: OnceCell<Bound<T::Key<'a>>>,
  value: OnceCell<S::Data<'a, T::Value<'a>>>,
}

impl<S, C, T> core::fmt::Debug for RangeUpdateEntry<'_, S, C, T>
where
  C: 'static,
  S: State,
  T: TypeMode,
  T::RangeComparator<C>: Comparator<RecordPointer> + RangeComparator<C>,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    use RangeComparator;
    self
      .data
      .get_or_init(|| self.ent.comparator().fetch_range_update(self.ent.key()))
      .write_fmt("RangeUpdateEntry", f)
  }
}

impl<'a, S, C, T> Clone for RangeUpdateEntry<'a, S, C, T>
where
  S: State,
  T: TypeMode,
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
      value: self.value.clone(),
    }
  }
}

impl<'a, S, C, T> RangeUpdateEntry<'a, S, C, T>
where
  S: State,
  T: TypeMode,
{
  pub(in crate::memtable) fn new(
    ent: Entry<'a, RecordPointer, RecordPointer, S, T::RangeComparator<C>>,
  ) -> Self {
    Self {
      ent,
      data: OnceCell::new(),
      start_bound: OnceCell::new(),
      end_bound: OnceCell::new(),
      value: OnceCell::new(),
    }
  }
}

impl<'a, S, C, T> crate::memtable::RangeEntry<'a> for RangeUpdateEntry<'a, S, C, T>
where
  C: 'static,
  S: State,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]> + 'a,
  T::RangeComparator<C>: Comparator<RecordPointer> + RangeComparator<C>,
{
  type Key = <T::Key<'a> as Pointee<'a>>::Output;

  #[inline]
  fn start_bound(&self) -> Bound<Self::Key> {
    let start_bound = self.start_bound.get_or_init(|| {
      let ent = self
        .data
        .get_or_init(|| self.ent.comparator().fetch_range_update(self.ent.key()));
      ent.start_bound().map(<T::Key<'a> as Pointee>::from_input)
    });
    start_bound.as_ref().map(|k| k.output())
  }

  #[inline]
  fn end_bound(&self) -> Bound<Self::Key> {
    let end_bound = self.end_bound.get_or_init(|| {
      let ent = self
        .data
        .get_or_init(|| self.ent.comparator().fetch_range_update(self.ent.key()));
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
}

impl<S, C, T> RangeUpdateEntry<'_, S, C, T>
where
  C: 'static,
  S: State,
  T: TypeMode,
{
  /// Returns the version of the entry.
  #[inline]
  pub fn version(&self) -> u64 {
    self.ent.version()
  }
}

impl<'a, S, C, T> crate::memtable::RangeUpdateEntry<'a> for RangeUpdateEntry<'a, S, C, T>
where
  C: 'static,
  S: Transfer<'a, T::Value<'a>>,
  S::Data<'a, S::Value>: 'a,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]> + 'a,
  T::RangeComparator<C>: Comparator<RecordPointer> + RangeComparator<C>,
{
  type Value = S::Data<'a, S::Value>;

  #[inline]
  fn value(&self) -> Self::Value {
    let val = self.value.get_or_init(|| {
      let ptr = S::leak(self.ent.value());

      let data = ptr.map(|ptr| {
        let ent = self
          .data
          .get_or_init(|| self.ent.comparator().fetch_range_update(ptr));

        <S as sealed::Sealed<'_, T::Value<'_>>>::from_input(ent.value())
      });
      S::into_state(data)
    });
    <S as sealed::Sealed<'_, T::Value<'_>>>::transfer(val)
  }
}

impl<'a, S, C, T> RangeUpdateEntry<'a, S, C, T>
where
  C: 'static,
  S: Transfer<'a, T::Value<'a>>,
  S::Data<'a, S::Value>: 'a,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]> + 'a,
  T::RangeComparator<C>: Comparator<RecordPointer> + RangeComparator<C>,
{
  #[inline]
  pub(in crate::memtable) fn into_value(self) -> S::Data<'a, T::Value<'a>> {
    self.value.get_or_init(|| {
      let ptr = S::leak(self.ent.value());

      let data = ptr.map(|ptr| {
        let ent = self
          .data
          .get_or_init(|| self.ent.comparator().fetch_range_update(ptr));

        <S as sealed::Sealed<'_, T::Value<'_>>>::from_input(ent.value())
      });
      S::into_state(data)
    });
    self.value.into_inner().unwrap()
  }
}

/// The iterator for point entries.
pub struct IterBulkUpdates<'a, S, C, T>
where
  S: State,
  T: TypeMode,
{
  iter: Iter<'a, RecordPointer, RecordPointer, S, T::RangeComparator<C>>,
}

impl<'a, S, C, T> IterBulkUpdates<'a, S, C, T>
where
  S: State,
  T: TypeMode,
{
  #[inline]
  pub(in crate::memtable) const fn new(
    iter: Iter<'a, RecordPointer, RecordPointer, S, T::RangeComparator<C>>,
  ) -> Self {
    Self { iter }
  }
}

impl<'a, S, C, T> Iterator for IterBulkUpdates<'a, S, C, T>
where
  C: 'static,
  S: State,
  T: TypeMode,
  T::RangeComparator<C>: Comparator<RecordPointer> + 'a,
{
  type Item = RangeUpdateEntry<'a, S, C, T>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(RangeUpdateEntry::new)
  }
}

impl<'a, S, C, T> DoubleEndedIterator for IterBulkUpdates<'a, S, C, T>
where
  C: 'static,
  S: State,
  T: TypeMode,
  T::RangeComparator<C>: Comparator<RecordPointer> + 'a,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(RangeUpdateEntry::new)
  }
}

/// The iterator over a subset of point entries.
pub struct RangeBulkUpdates<'a, S, Q, R, C, T>
where
  S: State,
  Q: ?Sized,
  T: TypeMode,
  R: RangeBounds<Q>,
{
  range:
    Range<'a, RecordPointer, RecordPointer, S, Query<Q>, QueryRange<Q, R>, T::RangeComparator<C>>,
}

impl<'a, S, Q, R, C, T> RangeBulkUpdates<'a, S, Q, R, C, T>
where
  S: State,
  Q: ?Sized,
  T: TypeMode,
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

impl<'a, S, Q, R, C, T> Iterator for RangeBulkUpdates<'a, S, Q, R, C, T>
where
  C: 'static,
  S: State,
  R: RangeBounds<Q>,
  Q: ?Sized,
  T: TypeMode,
  T::RangeComparator<C>: QueryComparator<RecordPointer, Query<Q>> + 'a,
{
  type Item = RangeUpdateEntry<'a, S, C, T>;
  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.range.next().map(RangeUpdateEntry::new)
  }
}

impl<'a, S, Q, R, C, T> DoubleEndedIterator for RangeBulkUpdates<'a, S, Q, R, C, T>
where
  C: 'static,
  S: State,
  R: RangeBounds<Q>,
  Q: ?Sized,
  T: TypeMode,
  T::RangeComparator<C>: QueryComparator<RecordPointer, Query<Q>> + 'a,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.range.next_back().map(RangeUpdateEntry::new)
  }
}
