use core::{
  cell::OnceCell,
  ops::{Bound, RangeBounds},
};

use skl::{
  generic::{
    multiple_version::sync::{Entry, Iter, Range},
    LazyRef, TypeRefComparator, TypeRefQueryComparator,
  },
  Active, MaybeTombstone, State, Transformable,
};

use crate::types::{
  sealed::{Pointee, RangeComparator},
  Query, QueryRange, RawRangeUpdateRef, RecordPointer, TypeMode,
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
  T::RangeComparator<C>: TypeRefComparator<RecordPointer> + RangeComparator<C>,
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
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone,
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
  S::Data<'a, LazyRef<'a, RecordPointer>>: Transformable<Input = Option<&'a [u8]>>,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]> + 'a,
  T::RangeComparator<C>: TypeRefComparator<RecordPointer> + RangeComparator<C>,
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

impl<S, C, T> crate::WithVersion for RangeUpdateEntry<'_, S, C, T>
where
  C: 'static,
  S: State,
  T: TypeMode,
{
  #[inline]
  fn version(&self) -> u64 {
    self.ent.version()
  }
}

impl<'a, C, T> crate::memtable::RangeUpdateEntry<'a> for RangeUpdateEntry<'a, Active, C, T>
where
  C: 'static,
  <Active as State>::Data<'a, T::Value<'a>>: Transformable<Input = Option<&'a [u8]>> + 'a,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]> + 'a,
  T::RangeComparator<C>: TypeRefComparator<RecordPointer> + RangeComparator<C>,
{
  type Value = <<Active as State>::Data<'a, T::Value<'a>> as Transformable>::Output;

  #[inline]
  fn value(&self) -> Self::Value {
    self
      .value
      .get_or_init(|| {
        let ent = self
          .data
          .get_or_init(|| self.ent.comparator().fetch_range_update(&self.ent.value()));
        <<Active as State>::Data<'a, T::Value<'a>> as Transformable>::from_input(ent.value())
      })
      .transform()
  }
}

impl<'a, C, T> crate::memtable::RangeUpdateEntry<'a> for RangeUpdateEntry<'a, MaybeTombstone, C, T>
where
  C: 'static,
  <MaybeTombstone as State>::Data<'a, T::Value<'a>>: Transformable<Input = Option<&'a [u8]>> + 'a,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]> + 'a,
  T::Value<'a>: 'a,
  T::RangeComparator<C>: TypeRefComparator<RecordPointer> + RangeComparator<C>,
{
  type Value = <<MaybeTombstone as State>::Data<'a, T::Value<'a>> as Transformable>::Output;

  #[inline]
  fn value(&self) -> Self::Value {
    self
      .value
      .get_or_init(|| match self.ent.value() {
        Some(value) => {
          let ent = self
            .data
            .get_or_init(|| self.ent.comparator().fetch_range_update(&value));
          <<MaybeTombstone as State>::Data<'a, T::Value<'a>> as Transformable>::from_input(
            ent.value(),
          )
        }
        None => None,
      })
      .transform()
  }
}

impl<'a, S, C, T> RangeUpdateEntry<'a, S, C, T>
where
  C: 'static,
  S: State + 'a,
  S::Data<'a, LazyRef<'a, RecordPointer>>: Sized + Transformable<Input = Option<&'a [u8]>>,
  S::Data<'a, T::Value<'a>>: Transformable<Input = Option<&'a [u8]>> + 'a,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]> + 'a,
  T::RangeComparator<C>: TypeRefComparator<RecordPointer> + RangeComparator<C>,
{
  #[inline]
  pub(in crate::memtable) fn into_value(self) -> S::Data<'a, T::Value<'a>> {
    self.value.get_or_init(|| {
      let ent = self
        .data
        .get_or_init(|| self.ent.comparator().fetch_range_update(self.ent.key()));
      <S::Data<'a, T::Value<'a>> as Transformable>::from_input(ent.value())
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
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone + Transformable<Input = Option<&'a [u8]>>,
  T: TypeMode,
  T::RangeComparator<C>: TypeRefComparator<RecordPointer> + 'a,
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
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone + Transformable<Input = Option<&'a [u8]>>,
  T: TypeMode,
  T::RangeComparator<C>: TypeRefComparator<RecordPointer> + 'a,
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
{
  range:
    Range<'a, RecordPointer, RecordPointer, S, Query<Q>, QueryRange<Q, R>, T::RangeComparator<C>>,
}

impl<'a, S, Q, R, C, T> RangeBulkUpdates<'a, S, Q, R, C, T>
where
  S: State,
  Q: ?Sized,
  T: TypeMode,
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
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone + Transformable<Input = Option<&'a [u8]>>,
  R: RangeBounds<Q>,
  Q: ?Sized,
  T: TypeMode,
  T::RangeComparator<C>: TypeRefQueryComparator<RecordPointer, Query<Q>> + 'a,
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
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone + Transformable<Input = Option<&'a [u8]>>,
  R: RangeBounds<Q>,
  Q: ?Sized,
  T: TypeMode,
  T::RangeComparator<C>: TypeRefQueryComparator<RecordPointer, Query<Q>> + 'a,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.range.next_back().map(RangeUpdateEntry::new)
  }
}
