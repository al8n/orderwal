use {
  super::TableOptions,
  crate::{
    generic::{
      memtable::{
        arena::{
          table::{Entry as ArenaEntry, Iter as ArenaIter, Range as ArenaRange},
          Table as ArenaTable,
        },
        BaseEntry, BaseTable, Memtable, MemtableEntry,
      },
      wal::{RecordPointer, ValuePointer},
    },
    types::Mode,
    WithoutVersion,
  },
  core::ops::{Bound, RangeBounds},
  dbutils::{
    equivalent::Comparable,
    types::{KeyRef, Type},
  },
};

#[cfg(feature = "std")]
use crate::generic::memtable::linked::{
  table::{Entry as LinkedEntry, Iter as LinkedIter, Range as LinkedRange},
  Table as LinkedTable,
};

base_entry!(
  enum Entry {
    Arena(ArenaEntry),
    Linked(LinkedEntry),
  }
);

impl<'a, K, V> MemtableEntry<'a> for Entry<'a, K, V>
where
  K: ?Sized + Type + Ord,
  RecordPointer<K>: Type<Ref<'a> = RecordPointer<K>> + KeyRef<'a, RecordPointer<K>>,
  V: ?Sized + Type,
{
  #[inline]
  fn value(&self) -> ValuePointer<Self::Value> {
    *match_op!(self.value())
  }
}

impl<K: ?Sized, V: ?Sized> WithoutVersion for Entry<'_, K, V> {}

iter!(
  enum Iter {
    Arena(ArenaIter),
    Linked(LinkedIter),
  } -> Entry
);

range!(
  enum Range {
    Arena(ArenaRange),
    Linked(LinkedRange),
  } -> Entry
);

/// A sum type for different memtable implementations.
#[non_exhaustive]
pub enum Table<K: ?Sized, V: ?Sized> {
  /// Arena memtable
  Arena(ArenaTable<K, V>),
  /// Linked memtable
  #[cfg(feature = "std")]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  Linked(LinkedTable<K, V>),
}

impl<K, V> BaseTable for Table<K, V>
where
  K: ?Sized + Type + Ord + 'static,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  for<'a> RecordPointer<K>: Type<Ref<'a> = RecordPointer<K>> + KeyRef<'a, RecordPointer<K>>,
  V: ?Sized + Type + 'static,
{
  type Key = K;

  type Value = V;

  type Options = TableOptions;

  type Error = super::Error;

  type Item<'a>
    = Entry<'a, K, V>
  where
    Self: 'a;

  type Iterator<'a>
    = Iter<'a, K, V>
  where
    Self: 'a;

  type Range<'a, Q, R>
    = Range<'a, K, V, Q, R>
  where
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>;

  #[inline]
  fn new(opts: Self::Options) -> Result<Self, Self::Error>
  where
    Self: Sized,
  {
    match_op!(new(opts))
  }

  #[inline]
  fn insert(
    &self,
    version: Option<u64>,
    kp: RecordPointer<Self::Key>,
    vp: ValuePointer<Self::Value>,
  ) -> Result<(), Self::Error>
  where
    RecordPointer<Self::Key>: Ord + 'static,
  {
    match_op!(update(self.insert(version, kp, vp)))
  }

  #[inline]
  fn remove(&self, version: Option<u64>, key: RecordPointer<Self::Key>) -> Result<(), Self::Error>
  where
    RecordPointer<Self::Key>: Ord + 'static,
  {
    match_op!(update(self.remove(version, key)))
  }

  #[inline]
  fn mode() -> Kind {
    Kind::Plain
  }
}

impl<K, V> Memtable for Table<K, V>
where
  K: ?Sized + Type + Ord + 'static,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  for<'a> RecordPointer<K>: Type<Ref<'a> = RecordPointer<K>> + KeyRef<'a, RecordPointer<K>>,
  V: ?Sized + Type + 'static,
{
  #[inline]
  fn len(&self) -> usize {
    match_op!(self.len())
  }

  #[inline]
  fn upper_bound<Q>(&self, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>,
  {
    match_op!(self.upper_bound(bound).map(Item))
  }

  #[inline]
  fn lower_bound<Q>(&self, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>,
  {
    match_op!(self.lower_bound(bound).map(Item))
  }

  #[inline]
  fn first(&self) -> Option<Self::Item<'_>>
  where
    RecordPointer<Self::Key>: Ord,
  {
    match_op!(self.first().map(Item))
  }

  #[inline]
  fn last(&self) -> Option<Self::Item<'_>>
  where
    RecordPointer<Self::Key>: Ord,
  {
    match_op!(self.last().map(Item))
  }

  #[inline]
  fn get<Q>(&self, key: &Q) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>,
  {
    match_op!(self.get(key).map(Item))
  }

  #[inline]
  fn contains<Q>(&self, key: &Q) -> bool
  where
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>,
  {
    match_op!(self.contains(key))
  }

  #[inline]
  fn iter(&self) -> Self::Iterator<'_> {
    match_op!(Dispatch::Iterator(self.iter()))
  }

  #[inline]
  fn range<'a, Q, R>(&'a self, range: R) -> Self::Range<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>,
  {
    match_op!(Dispatch::Range(self.range(range)))
  }
}
