use core::{cell::OnceCell, ops::RangeBounds};

use skl::{
  generic::{
    multiple_version::sync::{Entry, Iter, Range},
    LazyRef, TypeRefComparator, TypeRefQueryComparator,
  },
  Active, MaybeTombstone, State, Transformable,
};

use crate::types::{
  sealed::{PointComparator, Pointee},
  Query, QueryRange, RawEntryRef, RecordPointer, TypeMode,
};

/// Point entry.
pub struct PointEntry<'a, S, C, T>
where
  S: State,
  T: TypeMode,
{
  pub(in crate::memtable) ent: Entry<'a, RecordPointer, RecordPointer, S, T::Comparator<C>>,
  data: OnceCell<RawEntryRef<'a>>,
  key: OnceCell<T::Key<'a>>,
  pub(in crate::memtable) value: OnceCell<S::Data<'a, T::Value<'a>>>,
}

impl<S, C, T> core::fmt::Debug for PointEntry<'_, S, C, T>
where
  S: State,
  T: TypeMode,
  T::Comparator<C>: PointComparator<C>,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    self
      .data
      .get_or_init(|| self.ent.comparator().fetch_entry(self.ent.key()))
      .write_fmt("PointEntry", f)
  }
}

impl<'a, S, C, T> Clone for PointEntry<'a, S, C, T>
where
  S: State,
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone,
  S::Data<'a, T::Value<'a>>: Clone,
  T: TypeMode,
  T::Key<'a>: Clone,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      data: self.data.clone(),
      key: self.key.clone(),
      value: self.value.clone(),
    }
  }
}
impl<'a, S, C, T> PointEntry<'a, S, C, T>
where
  S: State,
  T: TypeMode,
{
  #[inline]
  pub(in crate::memtable) fn new(
    ent: Entry<'a, RecordPointer, RecordPointer, S, T::Comparator<C>>,
  ) -> Self {
    Self {
      ent,
      data: OnceCell::new(),
      key: OnceCell::new(),
      value: OnceCell::new(),
    }
  }
}

impl<'a, C, T> crate::memtable::MemtableEntry<'a> for PointEntry<'a, Active, C, T>
where
  C: 'static,
  <Active as State>::Data<'a, T::Value<'a>>: Transformable<Input = Option<&'a [u8]>> + 'a,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]> + 'a,
  T::Comparator<C>: PointComparator<C> + TypeRefComparator<'a, RecordPointer>,
{
  type Key = <T::Key<'a> as Pointee<'a>>::Output;
  type Value = <<Active as State>::Data<'a, T::Value<'a>> as Transformable>::Output;

  #[inline]
  fn key(&self) -> Self::Key {
    self
      .key
      .get_or_init(|| {
        let ent = self
          .data
          .get_or_init(|| self.ent.comparator().fetch_entry(&self.ent.value()));
        <T::Key<'a> as Pointee<'a>>::from_input(ent.key())
      })
      .output()
  }

  #[inline]
  fn value(&self) -> Self::Value {
    self
      .value
      .get_or_init(|| {
        let ent = self
          .data
          .get_or_init(|| self.ent.comparator().fetch_entry(&self.ent.value()));
        <<Active as State>::Data<'a, _> as Transformable>::from_input(ent.value())
      })
      .transform()
  }

  #[inline]
  fn next(&self) -> Option<Self> {
    self.ent.next().map(Self::new)
  }

  #[inline]
  fn prev(&self) -> Option<Self> {
    self.ent.prev().map(Self::new)
  }
}

impl<'a, C, T> crate::memtable::MemtableEntry<'a> for PointEntry<'a, MaybeTombstone, C, T>
where
  C: 'static,
  <MaybeTombstone as State>::Data<'a, T::Value<'a>>: Transformable<Input = Option<&'a [u8]>> + 'a,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]> + 'a,
  T::Value<'a>: 'a,
  T::Comparator<C>: PointComparator<C> + TypeRefComparator<'a, RecordPointer>,
{
  type Key = <T::Key<'a> as Pointee<'a>>::Output;
  type Value = <<MaybeTombstone as State>::Data<'a, T::Value<'a>> as Transformable>::Output;

  #[inline]
  fn key(&self) -> Self::Key {
    self
      .key
      .get_or_init(|| match self.ent.value() {
        Some(value) => {
          let ent = self
            .data
            .get_or_init(|| self.ent.comparator().fetch_entry(&value));
          <T::Key<'a> as Pointee<'a>>::from_input(ent.key())
        }
        None => {
          let ent = self
            .data
            .get_or_init(|| self.ent.comparator().fetch_entry(self.ent.key()));
          <T::Key<'a> as Pointee<'a>>::from_input(ent.key())
        }
      })
      .output()
  }

  #[inline]
  fn value(&self) -> Self::Value {
    self
      .value
      .get_or_init(|| match self.ent.value() {
        Some(value) => {
          let ent = self
            .data
            .get_or_init(|| self.ent.comparator().fetch_entry(&value));
          <<MaybeTombstone as State>::Data<'a, _> as Transformable>::from_input(ent.value())
        }
        None => None,
      })
      .transform()
  }

  #[inline]
  fn next(&self) -> Option<Self> {
    self.ent.next().map(Self::new)
  }

  #[inline]
  fn prev(&self) -> Option<Self> {
    self.ent.prev().map(Self::new)
  }
}

impl<S, C, T> crate::WithVersion for PointEntry<'_, S, C, T>
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

/// The iterator for point entries.
pub struct IterPoints<'a, S, C, T>
where
  S: State,
  T: TypeMode,
{
  iter: Iter<'a, RecordPointer, RecordPointer, S, T::Comparator<C>>,
}

impl<'a, S, C, T> IterPoints<'a, S, C, T>
where
  S: State,
  T: TypeMode,
{
  #[inline]
  pub(in crate::memtable) const fn new(
    iter: Iter<'a, RecordPointer, RecordPointer, S, T::Comparator<C>>,
  ) -> Self {
    Self { iter }
  }
}

impl<'a, S, C, T> Iterator for IterPoints<'a, S, C, T>
where
  C: 'static,
  S: State,
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone + Transformable<Input = Option<&'a [u8]>>,
  T: TypeMode,
  T::Comparator<C>: TypeRefComparator<'a, RecordPointer> + 'a,
{
  type Item = PointEntry<'a, S, C, T>;
  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(PointEntry::new)
  }
}

impl<'a, S, C, T> DoubleEndedIterator for IterPoints<'a, S, C, T>
where
  C: 'static,
  S: State,
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone + Transformable<Input = Option<&'a [u8]>>,
  T: TypeMode,
  T::Comparator<C>: TypeRefComparator<'a, RecordPointer> + 'a,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(PointEntry::new)
  }
}

/// The iterator over a subset of point entries.
pub struct RangePoints<'a, S, Q, R, C, T>
where
  S: State,
  Q: ?Sized,
  T: TypeMode,
{
  range: Range<'a, RecordPointer, RecordPointer, S, Query<Q>, QueryRange<Q, R>, T::Comparator<C>>,
}

impl<'a, S, Q, R, C, T> RangePoints<'a, S, Q, R, C, T>
where
  S: State,
  Q: ?Sized,
  T: TypeMode,
{
  #[inline]
  pub(in crate::memtable) const fn new(
    range: Range<'a, RecordPointer, RecordPointer, S, Query<Q>, QueryRange<Q, R>, T::Comparator<C>>,
  ) -> Self {
    Self { range }
  }
}

impl<'a, S, Q, R, C, T> Iterator for RangePoints<'a, S, Q, R, C, T>
where
  C: 'static,
  S: State,
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone + Transformable<Input = Option<&'a [u8]>>,
  R: RangeBounds<Q>,
  Q: ?Sized,
  T: TypeMode,
  T::Comparator<C>: TypeRefQueryComparator<'a, RecordPointer, Query<Q>> + 'a,
{
  type Item = PointEntry<'a, S, C, T>;
  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.range.next().map(PointEntry::new)
  }
}

impl<'a, S, Q, R, C, T> DoubleEndedIterator for RangePoints<'a, S, Q, R, C, T>
where
  C: 'static,
  S: State,
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone + Transformable<Input = Option<&'a [u8]>>,
  R: RangeBounds<Q>,
  Q: ?Sized,
  T: TypeMode,
  T::Comparator<C>: TypeRefQueryComparator<'a, RecordPointer, Query<Q>> + 'a,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.range.next_back().map(PointEntry::new)
  }
}
