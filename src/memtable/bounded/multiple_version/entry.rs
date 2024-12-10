use super::*;

/// Entry in the memtable.
pub struct Entry<'a, S, C, T>
where
  S: State<'a>,
  T: Kind,
{
  table: &'a Table<C, T>,
  point_ent: PointEntry<'a, S, C, T>,
  key: <T::Key<'a> as Pointee<'a>>::Output,
  val: <T::Value<'a> as Pointee<'a>>::Output,
  version: u64,
  query_version: u64,
}

impl<'a, S, C, T> core::fmt::Debug for Entry<'a, S, C, T>
where
  S: State<'a>,
  C: 'static,
  T: Kind,
  <T::Key<'a> as Pointee<'a>>::Output: core::fmt::Debug,
  <T::Value<'a> as Pointee<'a>>::Output: core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Entry")
      .field("key", &self.key)
      .field("value", &self.val)
      .field("version", &self.version)
      .finish()
  }
}

impl<'a, S, C, T> Clone for Entry<'a, S, C, T>
where
  S: State<'a>,
  T: Kind,
  T::Key<'a>: Clone,
  T::Value<'a>: Clone,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      table: self.table,
      point_ent: self.point_ent.clone(),
      key: self.key,
      val: self.val,
      version: self.version,
      query_version: self.query_version,
    }
  }
}

impl<'a, C, T> MemtableEntry<'a> for Entry<'a, Active, C, T>
where
  C: 'static,
  T: Kind,
  T: Kind,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]>,
  T::Value<'a>: Pointee<'a, Input = &'a [u8]>,
  <T::Key<'a> as Pointee<'a>>::Output: 'a,
  <T::Value<'a> as Pointee<'a>>::Output: 'a,
  T::Comparator<C>: PointComparator<C>
    + TypeRefComparator<'a, RecordPointer>
    + Comparator<<T::Key<'a> as Pointee<'a>>::Output>
    + 'static,
  T::RangeComparator<C>: TypeRefComparator<'a, RecordPointer>
    + TypeRefQueryComparator<'a, RecordPointer, <<T as Sealed>::Key<'a> as Pointee<'a>>::Output>
    + RangeComparator<C>
    + 'static,
  RangeDeletionEntry<'a, Active, C, T>:
    RangeDeletionEntryTrait<'a> + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  RangeUpdateEntry<'a, MaybeTombstone, C, T>: RangeUpdateEntryTrait<'a, Value = Option<<T::Value<'a> as Pointee<'a>>::Output>>
    + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
{
  type Key = <T::Key<'a> as Pointee<'a>>::Output;

  type Value = <T::Value<'a> as Pointee<'a>>::Output;

  #[inline]
  fn key(&self) -> Self::Key {
    self.key
  }

  #[inline]
  fn value(&self) -> Self::Value {
    self.val
  }

  #[inline]
  fn next(&self) -> Option<Self> {
    let mut next = self.point_ent.next();
    while let Some(ent) = next {
      match self.table.validate(self.query_version, ent) {
        ControlFlow::Break(entry) => return entry,
        ControlFlow::Continue(ent) => next = ent.next(),
      }
    }
    None
  }

  #[inline]
  fn prev(&self) -> Option<Self> {
    let mut prev = self.point_ent.prev();
    while let Some(ent) = prev {
      match self.table.validate(self.query_version, ent) {
        ControlFlow::Break(entry) => return entry,
        ControlFlow::Continue(ent) => prev = ent.prev(),
      }
    }
    None
  }
}

impl<'a, S, C, T> Entry<'a, S, C, T>
where
  S: State<'a>,
  T: Kind,
{
  #[inline]
  pub(crate) fn new(
    table: &'a Table<C, T>,
    query_version: u64,
    point_ent: PointEntry<'a, S, C, T>,
    key: <T::Key<'a> as Pointee<'a>>::Output,
    val: <T::Value<'a> as Pointee<'a>>::Output,
    version: u64,
  ) -> Self {
    Self {
      table,
      point_ent,
      key,
      val,
      version,
      query_version,
    }
  }
}

impl<'a, S, C, T> WithVersion for Entry<'a, S, C, T>
where
  S: State<'a>,
  T: Kind,
{
  #[inline]
  fn version(&self) -> u64 {
    self.version
  }
}
