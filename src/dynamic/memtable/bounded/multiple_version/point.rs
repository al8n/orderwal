use skl::generic::multiple_version::sync::{Entry, Iter, Range};

/// Point entry.
pub struct PointEntry<'a, S, C>
where
  S: crate::dynamic::types::State<'a>,
{
  ent: Entry<
    'a,
    crate::types::RecordPointer,
    (),
    S,
    crate::dynamic::memtable::bounded::MemtableComparator<C>,
  >,
  data: core::cell::OnceCell<crate::types::RawEntryRef<'a>>,
}
impl<'a, S, C> Clone for PointEntry<'a, S, C>
where
  S: crate::dynamic::types::State<'a>,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      data: self.data.clone(),
    }
  }
}
impl<'a, S, C> PointEntry<'a, S, C>
where
  S: crate::dynamic::types::State<'a>,
{
  #[inline]
  pub(super) fn new(
    ent: Entry<
      'a,
      crate::types::RecordPointer,
      (),
      S,
      crate::dynamic::memtable::bounded::MemtableComparator<C>,
    >,
  ) -> Self {
    Self {
      ent,
      data: core::cell::OnceCell::new(),
    }
  }
}

impl<'a, C> crate::dynamic::memtable::MemtableEntry<'a>
  for PointEntry<'a, crate::dynamic::types::Active, C>
where
  C: dbutils::equivalentor::BytesComparator,
{
  type Value = &'a [u8];
  #[inline]
  fn key(&self) -> &'a [u8] {
    self
      .data
      .get_or_init(|| self.ent.comparator().fetch_entry(self.ent.key()))
      .key()
  }
  #[inline]
  fn value(&self) -> Self::Value {
    let ent = self
      .data
      .get_or_init(|| self.ent.comparator().fetch_entry(self.ent.key()));
    ent
      .value()
      .expect("entry in Active state must have a value")
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

impl<'a, C> crate::dynamic::memtable::MemtableEntry<'a>
  for PointEntry<'a, crate::dynamic::types::MaybeTombstone, C>
where
  C: dbutils::equivalentor::BytesComparator,
{
  type Value = Option<&'a [u8]>;
  #[inline]
  fn key(&self) -> &'a [u8] {
    self
      .data
      .get_or_init(|| self.ent.comparator().fetch_entry(self.ent.key()))
      .key()
  }
  #[inline]
  fn value(&self) -> Self::Value {
    let ent = self
      .data
      .get_or_init(|| self.ent.comparator().fetch_entry(self.ent.key()));
    ent.value()
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

impl<'a, S, C> crate::WithVersion for PointEntry<'a, S, C>
where
  C: dbutils::equivalentor::BytesComparator,
  S: crate::dynamic::types::State<'a>,
{
  #[inline]
  fn version(&self) -> u64 {
    self.ent.version()
  }
}

iter_wrapper!(
  /// The iterator for point entries.
  IterPoints(Iter) yield PointEntry by MemtableComparator
);

range_wrapper!(
  /// The iterator over a subset of point entries.
  RangePoints(Range) yield PointEntry by MemtableComparator
);
