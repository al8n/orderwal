use {
  crate::{
    generic::{
      memtable::{self, BaseEntry, MultipleVersionMemtableEntry},
      wal::{RecordPointer, ValuePointer},
    },
    types::Mode,
    WithVersion,
  },
  core::{
    convert::Infallible,
    ops::{Bound, RangeBounds},
  },
  crossbeam_skiplist_mvcc::nested::SkipMap,
  dbutils::{
    equivalent::Comparable,
    types::{KeyRef, Type},
  },
};

pub use crossbeam_skiplist_mvcc::nested::{Entry, Iter, IterAll, Range, MultipleVersionRange, VersionedEntry};

/// An memory table implementation based on [`crossbeam_skiplist::SkipSet`].
pub struct MultipleVersionTable<K: ?Sized, V: ?Sized>(SkipMap<RecordPointer<K>, ValuePointer<V>>);

impl<K, V> Default for MultipleVersionTable<K, V>
where
  K: ?Sized,
  V: ?Sized,
{
  #[inline]
  fn default() -> Self {
    Self(SkipMap::new())
  }
}

impl<'a, K, V> BaseEntry<'a> for Entry<'a, RecordPointer<K>, ValuePointer<V>>
where
  K: ?Sized + Type + Ord,
  K::Ref<'a>: KeyRef<'a, K>,
  V: ?Sized,
{
  type Key = K;
  type Value = V;

  #[inline]
  fn next(&mut self) -> Option<Self> {
    Entry::next(self)
  }

  #[inline]
  fn prev(&mut self) -> Option<Self> {
    Entry::prev(self)
  }

  #[inline]
  fn key(&self) -> RecordPointer<K> {
    *self.key()
  }
}

impl<'a, K, V> memtable::MultipleVersionMemtableEntry<'a> for Entry<'a, RecordPointer<K>, ValuePointer<V>>
where
  K: ?Sized + Type + Ord,
  K::Ref<'a>: KeyRef<'a, K>,
  V: ?Sized,
{
  #[inline]
  fn value(&self) -> Option<ValuePointer<V>> {
    Some(*self.value())
  }

  #[inline]
  fn version(&self) -> u64 {
    Entry::version(self)
  }
}

impl<K, V> WithVersion for Entry<'_, RecordPointer<K>, ValuePointer<V>>
where
  K: ?Sized,
  V: ?Sized,
{
}

impl<'a, K, V> BaseEntry<'a> for VersionedEntry<'a, RecordPointer<K>, ValuePointer<V>>
where
  K: ?Sized + Type + Ord,
  K::Ref<'a>: KeyRef<'a, K>,
  V: ?Sized,
{
  type Key = K;
  type Value = V;

  #[inline]
  fn next(&mut self) -> Option<Self> {
    VersionedEntry::next(self)
  }

  #[inline]
  fn prev(&mut self) -> Option<Self> {
    VersionedEntry::prev(self)
  }

  #[inline]
  fn key(&self) -> RecordPointer<K> {
    *self.key()
  }
}

impl<'a, K, V> MultipleVersionMemtableEntry<'a> for VersionedEntry<'a, RecordPointer<K>, ValuePointer<V>>
where
  K: ?Sized + Type + Ord,
  K::Ref<'a>: KeyRef<'a, K>,
  V: ?Sized,
{
  #[inline]
  fn version(&self) -> u64 {
    VersionedEntry::version(self)
  }

  #[inline]
  fn value(&self) -> Option<ValuePointer<V>> {
    self.value().copied()
  }
}

impl<K, V> WithVersion for VersionedEntry<'_, RecordPointer<K>, ValuePointer<V>>
where
  K: ?Sized,
  V: ?Sized,
{
}

impl<K, V> memtable::BaseTable for MultipleVersionTable<K, V>
where
  K: ?Sized + Type + Ord + 'static,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  V: ?Sized + 'static,
{
  type Key = K;
  type Value = V;
  type Item<'a>
    = Entry<'a, RecordPointer<Self::Key>, ValuePointer<Self::Value>>
  where
    Self: 'a;

  type Iterator<'a>
    = Iter<'a, RecordPointer<Self::Key>, ValuePointer<Self::Value>>
  where
    Self: 'a;

  type Range<'a, Q, R>
    = Range<'a, Q, R, RecordPointer<Self::Key>, ValuePointer<Self::Value>>
  where
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>;

  type Options = ();
  type Error = Infallible;

  fn new(_: Self::Options) -> Result<Self, Self::Error>
  where
    Self: Sized,
  {
    Ok(Self(SkipMap::new()))
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
    self.0.insert_unchecked(version.unwrap_or(0), kp, vp);
    Ok(())
  }

  #[inline]
  fn remove(&self, version: Option<u64>, key: RecordPointer<Self::Key>) -> Result<(), Self::Error>
  where
    RecordPointer<Self::Key>: Ord + 'static,
  {
    self.0.remove_unchecked(version.unwrap_or(0), key);
    Ok(())
  }

  #[inline]
  fn mode() -> Kind {
    Kind::MultipleVersion
  }
}

impl<K, V> memtable::MultipleVersionMemtable for MultipleVersionTable<K, V>
where
  K: ?Sized + Type + Ord + 'static,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  V: ?Sized + 'static,
{
  type MultipleVersionEntry<'a>
    = VersionedEntry<'a, RecordPointer<Self::Key>, ValuePointer<Self::Value>>
  where
    Self: 'a;

  type IterAll<'a>
    = IterAll<'a, RecordPointer<Self::Key>, ValuePointer<Self::Value>>
  where
    Self: 'a;

  type MultipleVersionRange<'a, Q, R>
    = MultipleVersionRange<'a, Q, R, RecordPointer<Self::Key>, ValuePointer<Self::Value>>
  where
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>;

  #[inline]
  fn maximum_version(&self) -> u64 {
    self.0.maximum_version()
  }

  #[inline]
  fn minimum_version(&self) -> u64 {
    self.0.minimum_version()
  }

  #[inline]
  fn may_contain_version(&self, version: u64) -> bool {
    self.0.may_contain_version(version)
  }

  fn upper_bound<Q>(&self, version: u64, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>,
  {
    self.0.upper_bound(version, bound)
  }

  fn upper_bound_with_tombstone<Q>(
    &self,
    version: u64,
    bound: Bound<&Q>,
  ) -> Option<Self::MultipleVersionEntry<'_>>
  where
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>,
  {
    self.0.upper_bound_with_tombstone(version, bound)
  }

  fn lower_bound<Q>(&self, version: u64, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>,
  {
    self.0.lower_bound(version, bound)
  }

  fn lower_bound_with_tombstone<Q>(
    &self,
    version: u64,
    bound: Bound<&Q>,
  ) -> Option<Self::MultipleVersionEntry<'_>>
  where
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>,
  {
    self.0.lower_bound_with_tombstone(version, bound)
  }

  fn first(&self, version: u64) -> Option<Self::Item<'_>>
  where
    RecordPointer<Self::Key>: Ord,
  {
    self.0.front(version)
  }

  fn first_with_tombstone(&self, version: u64) -> Option<Self::MultipleVersionEntry<'_>>
  where
    RecordPointer<Self::Key>: Ord,
  {
    self.0.front_with_tombstone(version)
  }

  fn last(&self, version: u64) -> Option<Self::Item<'_>>
  where
    RecordPointer<Self::Key>: Ord,
  {
    self.0.back(version)
  }

  fn last_with_tombstone(&self, version: u64) -> Option<Self::MultipleVersionEntry<'_>>
  where
    RecordPointer<Self::Key>: Ord,
  {
    self.0.back_with_tombstone(version)
  }

  fn get<Q>(&self, version: u64, key: &Q) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>,
  {
    self.0.get(version, key)
  }

  fn get_with_tombstone<Q>(&self, version: u64, key: &Q) -> Option<Self::MultipleVersionEntry<'_>>
  where
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>,
  {
    self.0.get_with_tombstone(version, key)
  }

  fn contains<Q>(&self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>,
  {
    self.0.contains_key(version, key)
  }

  fn contains_with_tombstone<Q>(&self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>,
  {
    self.0.contains_key_with_tombstone(version, key)
  }

  fn iter(&self, version: u64) -> Self::Iterator<'_> {
    self.0.iter(version)
  }

  fn iter_with_tombstone(&self, version: u64) -> Self::IterAll<'_> {
    self.0.iter_with_tombstone(version)
  }

  fn range<'a, Q, R>(&'a self, version: u64, range: R) -> Self::Range<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>,
  {
    self.0.range(version, range)
  }

  fn range_with_tombstone<'a, Q, R>(&'a self, version: u64, range: R) -> Self::MultipleVersionRange<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>,
  {
    self.0.range_with_tombstone(version, range)
  }
}
