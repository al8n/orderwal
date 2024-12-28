use core::{cell::OnceCell, ops::RangeBounds};

use skl::{
  generic::{
    multiple_version::sync::{Entry, Iter, Range},
    LazyRef, TypeRefComparator, TypeRefQueryComparator,
  },
  State,
};

use crate::{
  memtable::{sealed, Transfer},
  types::{
    sealed::{PointComparator, Pointee},
    Mode, Query, QueryRange, RawEntryRef, RecordPointer,
  },
};

/// Point entry.
pub struct PointEntryRef<'a, S, C, T>
where
  S: State,
  T: Mode,
{
  pub(in crate::memtable) ent: Entry<'a, RecordPointer, RecordPointer, S, T::Comparator<C>>,
  data: OnceCell<RawEntryRef<'a>>,
  key: OnceCell<T::Key<'a>>,
  pub(in crate::memtable) value: OnceCell<S::Data<'a, T::Value<'a>>>,
}

impl<S, C, T> core::fmt::Debug for PointEntryRef<'_, S, C, T>
where
  S: State,
  T: Mode,
  T::Comparator<C>: PointComparator<C>,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    self
      .data
      .get_or_init(|| self.ent.comparator().fetch_entry(self.ent.key()))
      .write_fmt("PointEntryRef", f)
  }
}

impl<'a, S, C, T> Clone for PointEntryRef<'a, S, C, T>
where
  S: State,
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone,
  S::Data<'a, T::Value<'a>>: Clone,
  T: Mode,
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
impl<'a, S, C, T> PointEntryRef<'a, S, C, T>
where
  S: State,
  T: Mode,
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

impl<'a, S, C, T> crate::memtable::RawEntry<'a> for PointEntryRef<'a, S, C, T>
where
  C: 'static,
  S: Transfer<'a, T::Value<'a>>,
  S::Data<'a, &'a [u8]>: 'a,
  T: Mode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]> + 'a,
  T::Comparator<C>: PointComparator<C> + TypeRefComparator<'a, RecordPointer>,
{
  type RawValue = S::Data<'a, &'a [u8]>;

  #[inline]
  fn raw_key(&self) -> &'a [u8] {
    let ent = self.data.get_or_init(|| {
      let ptr = S::leak(self.ent.value());

      match ptr {
        Some(ptr) => self.ent.comparator().fetch_entry(&ptr),
        None => self.ent.comparator().fetch_entry(self.ent.key()),
      }
    });

    ent.key()
  }

  #[inline]
  fn raw_value(&self) -> Self::RawValue {
    let ent = self.data.get_or_init(|| {
      let ptr = S::leak(self.ent.value());

      match ptr {
        Some(ptr) => self.ent.comparator().fetch_entry(&ptr),
        None => self.ent.comparator().fetch_entry(self.ent.key()),
      }
    });

    S::raw(ent.value())
  }
}

impl<'a, S, C, T> crate::memtable::Entry<'a> for PointEntryRef<'a, S, C, T>
where
  C: 'static,
  S: Transfer<'a, T::Value<'a>>,
  S::Data<'a, S::Value>: 'a,
  T: Mode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]> + 'a,
  T::Comparator<C>: PointComparator<C> + TypeRefComparator<'a, RecordPointer>,
{
  type Key = <T::Key<'a> as Pointee<'a>>::Output;
  type Value = S::Data<'a, S::Value>;

  #[inline]
  fn key(&self) -> Self::Key {
    self
      .key
      .get_or_init(|| {
        let ptr = S::leak(self.ent.value());

        let ent = match ptr {
          Some(ptr) => self
            .data
            .get_or_init(|| self.ent.comparator().fetch_entry(&ptr)),
          None => self
            .data
            .get_or_init(|| self.ent.comparator().fetch_entry(self.ent.key())),
        };

        <T::Key<'a> as Pointee<'a>>::from_input(ent.key())
      })
      .output()
  }

  #[inline]
  fn value(&self) -> Self::Value {
    let val = self.value.get_or_init(|| {
      let ptr = S::leak(self.ent.value());

      let data = ptr.map(|ptr| {
        let ent = self
          .data
          .get_or_init(|| self.ent.comparator().fetch_entry(&ptr));

        <S as sealed::Sealed<'_, T::Value<'_>>>::from_input(ent.value())
      });
      S::into_state(data)
    });
    <S as sealed::Sealed<'_, T::Value<'_>>>::transfer(val)
  }

  #[inline]
  fn next(&self) -> Option<Self> {
    self.ent.next().map(Self::new)
  }

  #[inline]
  fn prev(&self) -> Option<Self> {
    self.ent.prev().map(Self::new)
  }

  #[inline]
  fn version(&self) -> u64 {
    self.ent.version()
  }
}

impl<S, C, T> PointEntryRef<'_, S, C, T>
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

/// The iterator for point entries.
pub struct IterPoints<'a, S, C, T>
where
  S: State,
  T: Mode,
{
  iter: Iter<'a, RecordPointer, RecordPointer, S, T::Comparator<C>>,
}

impl<'a, S, C, T> IterPoints<'a, S, C, T>
where
  S: State,
  T: Mode,
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
  S: Transfer<'a, T::Value<'a>>,
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone,
  T: Mode,
  T::Comparator<C>: TypeRefComparator<'a, RecordPointer> + 'a,
{
  type Item = PointEntryRef<'a, S, C, T>;
  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(PointEntryRef::new)
  }
}

impl<'a, S, C, T> DoubleEndedIterator for IterPoints<'a, S, C, T>
where
  C: 'static,
  S: Transfer<'a, T::Value<'a>>,
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone,
  T: Mode,
  T::Comparator<C>: TypeRefComparator<'a, RecordPointer> + 'a,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(PointEntryRef::new)
  }
}

/// The iterator over a subset of point entries.
pub struct RangePoints<'a, S, Q, R, C, T>
where
  S: State,
  Q: ?Sized,
  T: Mode,
{
  range: Range<'a, RecordPointer, RecordPointer, S, Query<Q>, QueryRange<Q, R>, T::Comparator<C>>,
}

impl<'a, S, Q, R, C, T> RangePoints<'a, S, Q, R, C, T>
where
  S: State,
  Q: ?Sized,
  T: Mode,
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
  S: Transfer<'a, T::Value<'a>>,
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone,
  R: RangeBounds<Q>,
  Q: ?Sized,
  T: Mode,
  T::Comparator<C>: TypeRefQueryComparator<'a, RecordPointer, Query<Q>> + 'a,
{
  type Item = PointEntryRef<'a, S, C, T>;
  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.range.next().map(PointEntryRef::new)
  }
}

impl<'a, S, Q, R, C, T> DoubleEndedIterator for RangePoints<'a, S, Q, R, C, T>
where
  C: 'static,
  S: Transfer<'a, T::Value<'a>>,
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone,
  R: RangeBounds<Q>,
  Q: ?Sized,
  T: Mode,
  T::Comparator<C>: TypeRefQueryComparator<'a, RecordPointer, Query<Q>> + 'a,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.range.next_back().map(PointEntryRef::new)
  }
}
