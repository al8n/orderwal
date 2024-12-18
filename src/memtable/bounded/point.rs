use skl::generic::multiple_version::sync::{Entry, Iter, Range};

/// Point entry.
pub struct PointEntry<'a, S, C, T>
where
  S: crate::State,
  T: crate::types::TypeMode,
{
  pub(in crate::memtable) ent: Entry<'a, crate::types::RecordPointer, (), S, T::Comparator<C>>,
  data: core::cell::OnceCell<crate::types::RawEntryRef<'a>>,
  key: core::cell::OnceCell<T::Key<'a>>,
  pub(in crate::memtable) value: core::cell::OnceCell<S::Data<'a, T::Value<'a>>>,
}
impl<S, C, T> core::fmt::Debug for PointEntry<'_, S, C, T>
where
  S: crate::State,
  T: crate::types::TypeMode,
  T::Comparator<C>: crate::types::sealed::PointComparator<C>,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    use crate::types::sealed::PointComparator;
    self
      .data
      .get_or_init(|| self.ent.comparator().fetch_entry(self.ent.key()))
      .write_fmt("PointEntry", f)
  }
}
impl<'a, S, C, T> Clone for PointEntry<'a, S, C, T>
where
  S: crate::State,
  S::Data<'a, dbutils::types::LazyRef<'a, ()>>: Clone,
  S::Data<'a, T::Value<'a>>: Clone,
  T: crate::types::TypeMode,
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
  S: crate::State,
  T: crate::types::TypeMode,
{
  #[inline]
  pub(in crate::memtable) fn new(
    ent: Entry<'a, crate::types::RecordPointer, (), S, T::Comparator<C>>,
  ) -> Self {
    Self {
      ent,
      data: core::cell::OnceCell::new(),
      key: core::cell::OnceCell::new(),
      value: core::cell::OnceCell::new(),
    }
  }
}
impl<'a, S, C, T> crate::memtable::MemtableEntry<'a> for PointEntry<'a, S, C, T>
where
  C: 'static,
  S: crate::State,
  S::Data<'a, dbutils::types::LazyRef<'a, ()>>: skl::Transformable<Input = Option<&'a [u8]>>,
  S::Data<'a, T::Value<'a>>: skl::Transformable<Input = Option<&'a [u8]>> + 'a,
  T: crate::types::TypeMode,
  T::Key<'a>: crate::types::sealed::Pointee<'a, Input = &'a [u8]> + 'a,
  T::Comparator<C>: crate::types::sealed::PointComparator<C>
    + dbutils::equivalentor::TypeRefComparator<crate::types::RecordPointer>,
{
  type Key = <T::Key<'a> as crate::types::sealed::Pointee<'a>>::Output;
  type Value = <S::Data<'a, T::Value<'a>> as skl::Transformable>::Output;
  #[inline]
  fn key(&self) -> Self::Key {
    use crate::types::sealed::{PointComparator, Pointee};
    self
      .key
      .get_or_init(|| {
        let ent = self
          .data
          .get_or_init(|| self.ent.comparator().fetch_entry(self.ent.key()));
        <T::Key<'a> as Pointee<'a>>::from_input(ent.key())
      })
      .output()
  }
  #[inline]
  fn value(&self) -> Self::Value {
    use crate::types::sealed::PointComparator;
    use skl::Transformable;
    self
      .value
      .get_or_init(|| {
        let ent = self
          .data
          .get_or_init(|| self.ent.comparator().fetch_entry(self.ent.key()));
        <S::Data<'a, _> as skl::Transformable>::from_input(ent.value())
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
  S: crate::State,
  T: crate::types::TypeMode,
{
  #[inline]
  fn version(&self) -> u64 {
    self.ent.version()
  }
}

/// The iterator for point entries.
pub struct IterPoints<'a, S, C, T>
where
  S: crate::State,
  T: crate::types::TypeMode,
{
  iter: Iter<'a, crate::types::RecordPointer, (), S, T::Comparator<C>>,
}
impl<'a, S, C, T> IterPoints<'a, S, C, T>
where
  S: crate::State,
  T: crate::types::TypeMode,
{
  #[inline]
  pub(in crate::memtable) const fn new(
    iter: Iter<'a, crate::types::RecordPointer, (), S, T::Comparator<C>>,
  ) -> Self {
    Self { iter }
  }
}
impl<'a, S, C, T> Iterator for IterPoints<'a, S, C, T>
where
  C: 'static,
  S: crate::State,
  S::Data<'a, dbutils::types::LazyRef<'a, ()>>:
    Clone + skl::Transformable<Input = Option<&'a [u8]>>,
  T: crate::types::TypeMode,
  T::Comparator<C>: dbutils::equivalentor::TypeRefComparator<crate::types::RecordPointer> + 'a,
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
  S: crate::State,
  S::Data<'a, dbutils::types::LazyRef<'a, ()>>:
    Clone + skl::Transformable<Input = Option<&'a [u8]>>,
  T: crate::types::TypeMode,
  T::Comparator<C>: dbutils::equivalentor::TypeRefComparator<crate::types::RecordPointer> + 'a,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(PointEntry::new)
  }
}
/// The iterator over a subset of point entries.
pub struct RangePoints<'a, S, Q, R, C, T>
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
    T::Comparator<C>,
  >,
}
impl<'a, S, Q, R, C, T> RangePoints<'a, S, Q, R, C, T>
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
      T::Comparator<C>,
    >,
  ) -> Self {
    Self { range }
  }
}
impl<'a, S, Q, R, C, T> Iterator for RangePoints<'a, S, Q, R, C, T>
where
  C: 'static,
  S: crate::State,
  S::Data<'a, dbutils::types::LazyRef<'a, ()>>:
    Clone + skl::Transformable<Input = Option<&'a [u8]>>,
  R: core::ops::RangeBounds<Q>,
  Q: ?Sized,
  T: crate::types::TypeMode,
  T::Comparator<C>: dbutils::equivalentor::TypeRefQueryComparator<
      crate::types::RecordPointer,
      crate::types::Query<Q>,
    > + 'a,
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
  S: crate::State,
  S::Data<'a, dbutils::types::LazyRef<'a, ()>>:
    Clone + skl::Transformable<Input = Option<&'a [u8]>>,
  R: core::ops::RangeBounds<Q>,
  Q: ?Sized,
  T: crate::types::TypeMode,
  T::Comparator<C>: dbutils::equivalentor::TypeRefQueryComparator<
      crate::types::RecordPointer,
      crate::types::Query<Q>,
    > + 'a,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.range.next_back().map(PointEntry::new)
  }
}
