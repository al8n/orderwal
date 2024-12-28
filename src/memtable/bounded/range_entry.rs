use core::{
  cell::OnceCell,
  marker::PhantomData,
  ops::{Bound, RangeBounds},
};

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
    sealed::{Pointee, RangeComparator},
    BulkOperation, Mode, Query, QueryRange, RecordPointer, WithValue,
  },
};

/// Range update entry.
pub struct RangeEntryRef<'a, S, O, C, T>
where
  O: BulkOperation,
  S: State,
  T: Mode,
{
  pub(crate) ent: Entry<'a, RecordPointer, RecordPointer, S, T::RangeComparator<C>>,
  data: OnceCell<O::Output<'a>>,
  start_bound: OnceCell<Bound<T::Key<'a>>>,
  end_bound: OnceCell<Bound<T::Key<'a>>>,
  value: OnceCell<S::Data<'a, T::Value<'a>>>,
  _op: PhantomData<O>,
}

impl<'a, S, O, C, T> core::fmt::Debug for RangeEntryRef<'a, S, O, C, T>
where
  C: 'static,
  O: BulkOperation,
  S: State,
  T: Mode,
  T::RangeComparator<C>: TypeRefComparator<'a, RecordPointer> + RangeComparator<C>,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    O::fmt(
      self
        .data
        .get_or_init(|| O::fetch(self.ent.comparator(), self.ent.key())),
      "RangeEntryRef",
      f,
    )
  }
}

impl<'a, S, O, C, T> Clone for RangeEntryRef<'a, S, O, C, T>
where
  O: BulkOperation,
  O::Output<'a>: Clone,
  S: State,
  S::Data<'a, T::Value<'a>>: Clone,
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone,
  T: Mode,
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
      _op: PhantomData,
    }
  }
}

impl<'a, S, O, C, T> RangeEntryRef<'a, S, O, C, T>
where
  O: BulkOperation,
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
      value: OnceCell::new(),
      _op: PhantomData,
    }
  }
}

impl<'a, S, O, C, T> crate::memtable::RawRangeEntry<'a, O> for RangeEntryRef<'a, S, O, C, T>
where
  C: 'static,
  O: BulkOperation,
  S::Data<'a, &'a [u8]>: 'a,
  S: Transfer<'a, T::Value<'a>>,
  T: Mode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]> + 'a,
  T::RangeComparator<C>: TypeRefComparator<'a, RecordPointer> + RangeComparator<C>,
{
  type RawValue = S::Data<'a, &'a [u8]>
  where
    O: WithValue;

  #[inline]
  fn raw_start_bound(&self) -> Bound<&'a [u8]> {
    let ent = self
      .data
      .get_or_init(|| O::fetch(self.ent.comparator(), self.ent.key()));

    O::start_bound(ent)
  }
  
  #[inline]
  fn raw_end_bound(&self) -> Bound<&'a [u8]> {
    let ent = self
      .data
      .get_or_init(|| O::fetch(self.ent.comparator(), self.ent.key()));
    O::end_bound(ent)
  }

  #[inline]
  fn raw_value(&self) -> Self::RawValue
  where
    O: WithValue {
    let ent = self.data.get_or_init(|| {
      let ptr = S::leak(self.ent.value());

      match ptr {
        Some(ptr) => O::fetch(self.ent.comparator(), &ptr),
        None => O::fetch(self.ent.comparator(), self.ent.key()),
      }
    });

    S::raw(O::value(ent))
  }
}

impl<'a, S, O, C, T> crate::memtable::RangeEntry<'a, O> for RangeEntryRef<'a, S, O, C, T>
where
  C: 'static,
  O: BulkOperation,
  S: Transfer<'a, T::Value<'a>>,
  S::Data<'a, S::Value>: 'a,
  T: Mode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]> + 'a,
  T::RangeComparator<C>: TypeRefComparator<'a, RecordPointer> + RangeComparator<C>,
{
  type Key = <T::Key<'a> as Pointee<'a>>::Output;

  type Value = S::Data<'a, S::Value>
  where
    O: WithValue;

  #[inline]
  fn start_bound(&self) -> Bound<Self::Key> {
    let start_bound = self.start_bound.get_or_init(|| {
      let ent = self
        .data
        .get_or_init(|| O::fetch(self.ent.comparator(), self.ent.key()));
      O::start_bound(ent).map(<T::Key<'a> as Pointee>::from_input)
    });
    start_bound.as_ref().map(|k| k.output())
  }

  #[inline]
  fn end_bound(&self) -> Bound<Self::Key> {
    let end_bound = self.end_bound.get_or_init(|| {
      let ent = self
        .data
        .get_or_init(|| O::fetch(self.ent.comparator(), self.ent.key()));
      O::end_bound(ent).map(<T::Key<'a> as Pointee>::from_input)
    });
    end_bound.as_ref().map(|k| k.output())
  }

  #[inline]
  fn value(&self) -> Self::Value
  where
    O: WithValue,
  {
    let val = self.value.get_or_init(|| {
      let ptr = S::leak(self.ent.value());

      let data = ptr.map(|ptr| {
        let ent = self
          .data
          .get_or_init(|| O::fetch(self.ent.comparator(), &ptr));

        <S as sealed::Sealed<'_, T::Value<'_>>>::from_input(O::value(ent))
      });
      S::into_state(data)
    });
    <S as sealed::Sealed<'_, T::Value<'_>>>::transfer(val)
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


impl<'a, S, O, C, T> RangeEntryRef<'a, S, O, C, T>
where
  C: 'static,
  O: WithValue,
  S: Transfer<'a, T::Value<'a>>,
  S::Data<'a, S::Value>: 'a,
  T: Mode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]> + 'a,
  T::RangeComparator<C>: TypeRefComparator<'a, RecordPointer> + RangeComparator<C>,
{
  #[inline]
  pub(in crate::memtable) fn into_value(self) -> S::Data<'a, T::Value<'a>> {
    self.value.get_or_init(|| {
      let ptr = S::leak(self.ent.value());

      let data = ptr.map(|ptr| {
        let ent = self
          .data
          .get_or_init(|| O::fetch(self.ent.comparator(), &ptr));

        <S as sealed::Sealed<'_, T::Value<'_>>>::from_input(O::value(ent))
      });
      S::into_state(data)
    });
    self.value.into_inner().unwrap()
  }
}

/// The iterator for point entries.
pub struct IterBulkOperations<'a, S, O, C, T>
where
  S: State,
  T: Mode,
{
  iter: Iter<'a, RecordPointer, RecordPointer, S, T::RangeComparator<C>>,
  _op: PhantomData<O>,
}

impl<'a, S, O, C, T> IterBulkOperations<'a, S, O, C, T>
where
  S: State,
  T: Mode,
{
  #[inline]
  pub(in crate::memtable) const fn new(
    iter: Iter<'a, RecordPointer, RecordPointer, S, T::RangeComparator<C>>,
  ) -> Self {
    Self {
      iter,
      _op: PhantomData,
    }
  }
}

impl<'a, S, O, C, T> Iterator for IterBulkOperations<'a, S, O, C, T>
where
  C: 'static,
  O: BulkOperation,
  S: Transfer<'a, T::Value<'a>>,
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone,
  T: Mode,
  T::RangeComparator<C>: TypeRefComparator<'a, RecordPointer> + 'a,
{
  type Item = RangeEntryRef<'a, S, O, C, T>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(RangeEntryRef::new)
  }
}

impl<'a, S, O, C, T> DoubleEndedIterator for IterBulkOperations<'a, S, O, C, T>
where
  C: 'static,
  O: BulkOperation,
  S: Transfer<'a, T::Value<'a>>,
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone,
  T: Mode,
  T::RangeComparator<C>: TypeRefComparator<'a, RecordPointer> + 'a,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(RangeEntryRef::new)
  }
}

/// The iterator over a subset of point entries.
pub struct RangeBulkOperations<'a, S, O, Q, R, C, T>
where
  O: BulkOperation,
  S: State,
  Q: ?Sized,
  T: Mode,
{
  range:
    Range<'a, RecordPointer, RecordPointer, S, Query<Q>, QueryRange<Q, R>, T::RangeComparator<C>>,
  _op: PhantomData<O>,
}

impl<'a, S, O, Q, R, C, T> RangeBulkOperations<'a, S, O, Q, R, C, T>
where
  O: BulkOperation,
  S: State,
  Q: ?Sized,
  T: Mode,
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
    Self {
      range,
      _op: PhantomData,
    }
  }
}

impl<'a, S, O, Q, R, C, T> Iterator for RangeBulkOperations<'a, S, O, Q, R, C, T>
where
  C: 'static,
  O: BulkOperation,
  S: Transfer<'a, T::Value<'a>>,
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone,
  R: RangeBounds<Q>,
  Q: ?Sized,
  T: Mode,
  T::RangeComparator<C>: TypeRefQueryComparator<'a, RecordPointer, Query<Q>> + 'a,
{
  type Item = RangeEntryRef<'a, S, O, C, T>;
  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.range.next().map(RangeEntryRef::new)
  }
}

impl<'a, S, O, Q, R, C, T> DoubleEndedIterator for RangeBulkOperations<'a, S, O, Q, R, C, T>
where
  C: 'static,
  O: BulkOperation,
  S: Transfer<'a, T::Value<'a>>,
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone,
  R: RangeBounds<Q>,
  Q: ?Sized,
  T: Mode,
  T::RangeComparator<C>: TypeRefQueryComparator<'a, RecordPointer, Query<Q>> + 'a,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.range.next_back().map(RangeEntryRef::new)
  }
}
