use skl::generic::multiple_version::sync::{Entry, Iter, Range};
/// Range deletion entry.
pub struct RangeDeletionEntry<'a, S, C, T>
where
  S: crate::State,
  T: crate::types::TypeMode,
{
  pub(crate) ent: Entry<'a, crate::types::RecordPointer, (), S, T::RangeComparator<C>>,
  data: core::cell::OnceCell<crate::types::RawRangeDeletionRef<'a>>,
  start_bound: core::cell::OnceCell<core::ops::Bound<T::Key<'a>>>,
  end_bound: core::cell::OnceCell<core::ops::Bound<T::Key<'a>>>,
  value: core::cell::OnceCell<S::Data<'a, T::Value<'a>>>,
}
impl<S, C, T> core::fmt::Debug for RangeDeletionEntry<'_, S, C, T>
where
  C: 'static,
  S: crate::State,
  T: crate::types::TypeMode,
  T::RangeComparator<C>: dbutils::equivalentor::TypeRefComparator<crate::types::RecordPointer>
    + crate::types::sealed::RangeComparator<C>,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    use crate::types::sealed::RangeComparator;
    self
      .data
      .get_or_init(|| self.ent.comparator().fetch_range_deletion(self.ent.key()))
      .write_fmt("RangeDeletionEntry", f)
  }
}
impl<'a, S, C, T> Clone for RangeDeletionEntry<'a, S, C, T>
where
  S: crate::State,
  S::Data<'a, dbutils::types::LazyRef<'a, ()>>: Clone,
  T: crate::types::TypeMode,
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
impl<'a, S, C, T> RangeDeletionEntry<'a, S, C, T>
where
  S: crate::State,
  T: crate::types::TypeMode,
{
  pub(in crate::memtable) fn new(
    ent: Entry<'a, crate::types::RecordPointer, (), S, T::RangeComparator<C>>,
  ) -> Self {
    Self {
      ent,
      data: core::cell::OnceCell::new(),
      start_bound: core::cell::OnceCell::new(),
      end_bound: core::cell::OnceCell::new(),
      value: core::cell::OnceCell::new(),
    }
  }
}
impl<'a, S, C, T> crate::memtable::RangeEntry<'a> for RangeDeletionEntry<'a, S, C, T>
where
  C: 'static,
  S: crate::State,
  S::Data<'a, dbutils::types::LazyRef<'a, ()>>: skl::Transformable<Input = Option<&'a [u8]>>,
  T: crate::types::TypeMode,
  T::Key<'a>: crate::types::sealed::Pointee<'a, Input = &'a [u8]> + 'a,
  T::RangeComparator<C>: dbutils::equivalentor::TypeRefComparator<crate::types::RecordPointer>
    + crate::types::sealed::RangeComparator<C>,
{
  type Key = <T::Key<'a> as crate::types::sealed::Pointee<'a>>::Output;
  #[inline]
  fn start_bound(&self) -> core::ops::Bound<Self::Key> {
    use crate::types::sealed::{Pointee, RangeComparator};
    let start_bound = self.start_bound.get_or_init(|| {
      let ent = self
        .data
        .get_or_init(|| self.ent.comparator().fetch_range_deletion(self.ent.key()));
      ent
        .start_bound()
        .map(<T::Key<'a> as crate::types::sealed::Pointee>::from_input)
    });
    start_bound.as_ref().map(|k| k.output())
  }
  #[inline]
  fn end_bound(&self) -> core::ops::Bound<Self::Key> {
    use crate::types::sealed::{Pointee, RangeComparator};
    let end_bound = self.end_bound.get_or_init(|| {
      let ent = self
        .data
        .get_or_init(|| self.ent.comparator().fetch_range_deletion(self.ent.key()));
      ent
        .end_bound()
        .map(<T::Key<'a> as crate::types::sealed::Pointee>::from_input)
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
impl<S, C, T> crate::WithVersion for RangeDeletionEntry<'_, S, C, T>
where
  C: 'static,
  S: crate::State,
  T: crate::types::TypeMode,
{
  #[inline]
  fn version(&self) -> u64 {
    self.ent.version()
  }
}
impl<'a, S, C, T> crate::memtable::RangeDeletionEntry<'a> for RangeDeletionEntry<'a, S, C, T>
where
  C: 'static,
  S: crate::State,
  S::Data<'a, dbutils::types::LazyRef<'a, ()>>: skl::Transformable<Input = Option<&'a [u8]>>,
  T: crate::types::TypeMode,
  T::Key<'a>: crate::types::sealed::Pointee<'a, Input = &'a [u8]> + 'a,
  T::RangeComparator<C>: dbutils::equivalentor::TypeRefComparator<crate::types::RecordPointer>
    + crate::types::sealed::RangeComparator<C>,
{
}
/// The iterator for point entries.
pub struct IterBulkDeletions<'a, S, C, T>
where
  S: crate::State,
  T: crate::types::TypeMode,
{
  iter: Iter<'a, crate::types::RecordPointer, (), S, T::RangeComparator<C>>,
}
impl<'a, S, C, T> IterBulkDeletions<'a, S, C, T>
where
  S: crate::State,
  T: crate::types::TypeMode,
{
  #[inline]
  pub(in crate::memtable) const fn new(
    iter: Iter<'a, crate::types::RecordPointer, (), S, T::RangeComparator<C>>,
  ) -> Self {
    Self { iter }
  }
}
impl<'a, S, C, T> Iterator for IterBulkDeletions<'a, S, C, T>
where
  C: 'static,
  S: crate::State,
  S::Data<'a, dbutils::types::LazyRef<'a, ()>>:
    Clone + skl::Transformable<Input = Option<&'a [u8]>>,
  T: crate::types::TypeMode,
  T::RangeComparator<C>: dbutils::equivalentor::TypeRefComparator<crate::types::RecordPointer> + 'a,
{
  type Item = RangeDeletionEntry<'a, S, C, T>;
  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(RangeDeletionEntry::new)
  }
}
impl<'a, S, C, T> DoubleEndedIterator for IterBulkDeletions<'a, S, C, T>
where
  C: 'static,
  S: crate::State,
  S::Data<'a, dbutils::types::LazyRef<'a, ()>>:
    Clone + skl::Transformable<Input = Option<&'a [u8]>>,
  T: crate::types::TypeMode,
  T::RangeComparator<C>: dbutils::equivalentor::TypeRefComparator<crate::types::RecordPointer> + 'a,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(RangeDeletionEntry::new)
  }
}
/// The iterator over a subset of point entries.
pub struct RangeBulkDeletions<'a, S, Q, R, C, T>
where
  S: crate::State,
  Q: ?Sized,
  T: crate::types::TypeMode,
{
  range: Range<
    'a,
    crate::types::RecordPointer,
    (),
    S,
    crate::types::Query<Q>,
    crate::types::QueryRange<Q, R>,
    T::RangeComparator<C>,
  >,
}
impl<'a, S, Q, R, C, T> RangeBulkDeletions<'a, S, Q, R, C, T>
where
  S: crate::State,
  Q: ?Sized,
  T: crate::types::TypeMode,
{
  #[inline]
  pub(in crate::memtable) const fn new(
    range: Range<
      'a,
      crate::types::RecordPointer,
      (),
      S,
      crate::types::Query<Q>,
      crate::types::QueryRange<Q, R>,
      T::RangeComparator<C>,
    >,
  ) -> Self {
    Self { range }
  }
}
impl<'a, S, Q, R, C, T> Iterator for RangeBulkDeletions<'a, S, Q, R, C, T>
where
  C: 'static,
  S: crate::State,
  S::Data<'a, dbutils::types::LazyRef<'a, ()>>:
    Clone + skl::Transformable<Input = Option<&'a [u8]>>,
  R: core::ops::RangeBounds<Q>,
  Q: ?Sized,
  T: crate::types::TypeMode,
  T::RangeComparator<C>: dbutils::equivalentor::TypeRefQueryComparator<
      crate::types::RecordPointer,
      crate::types::Query<Q>,
    > + 'a,
{
  type Item = RangeDeletionEntry<'a, S, C, T>;
  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.range.next().map(RangeDeletionEntry::new)
  }
}
impl<'a, S, Q, R, C, T> DoubleEndedIterator for RangeBulkDeletions<'a, S, Q, R, C, T>
where
  C: 'static,
  S: crate::State,
  S::Data<'a, dbutils::types::LazyRef<'a, ()>>:
    Clone + skl::Transformable<Input = Option<&'a [u8]>>,
  R: core::ops::RangeBounds<Q>,
  Q: ?Sized,
  T: crate::types::TypeMode,
  T::RangeComparator<C>: dbutils::equivalentor::TypeRefQueryComparator<
      crate::types::RecordPointer,
      crate::types::Query<Q>,
    > + 'a,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.range.next_back().map(RangeDeletionEntry::new)
  }
}
