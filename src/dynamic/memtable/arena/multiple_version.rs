use {
  super::TableOptions,
  crate::{
    dynamic::{
      memtable::{BaseEntry, BaseTable, MultipleVersionMemtable, MultipleVersionMemtableEntry},
      wal::{RecordPointer, ValuePointer},
    },
    types::Kind,
    WithVersion,
  },
  among::Among,
  core::ops::{Bound, RangeBounds},
  dbutils::{
    equivalent::Comparable,
    types::{KeyRef, Type},
  },
  skl::{
    either::Either,
    generic::multiple_version::{SkipMap, Map as _},
    Options,
  },
};

pub use skl::sync::multiple_version::sync::{Entry, Iter, IterAll, Range, MultipleVersionRange, VersionedEntry};

impl<'a, K, V> BaseEntry<'a> for Entry<'a, RecordPointer<K>, ValuePointer<V>>
where
  K: ?Sized + Type + Ord,
  RecordPointer<K>: Type<Ref<'a> = RecordPointer<K>> + KeyRef<'a, RecordPointer<K>>,
  V: ?Sized + Type,
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
    *Entry::key(self)
  }
}

impl<'a, K, V> MultipleVersionMemtableEntry<'a> for Entry<'a, RecordPointer<K>, ValuePointer<V>>
where
  K: ?Sized + Type + Ord,
  RecordPointer<K>: Type<Ref<'a> = RecordPointer<K>> + KeyRef<'a, RecordPointer<K>>,
  V: ?Sized + Type,
{
  #[inline]
  fn value(&self) -> Option<ValuePointer<Self::Value>> {
    Some(*Entry::value(self))
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
  RecordPointer<K>: Type<Ref<'a> = RecordPointer<K>> + KeyRef<'a, RecordPointer<K>>,
  V: ?Sized + Type,
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
    *VersionedEntry::key(self)
  }
}

impl<'a, K, V> MultipleVersionMemtableEntry<'a> for VersionedEntry<'a, RecordPointer<K>, ValuePointer<V>>
where
  K: ?Sized + Type + Ord,
  RecordPointer<K>: Type<Ref<'a> = RecordPointer<K>> + KeyRef<'a, RecordPointer<K>>,
  V: ?Sized + Type,
{
  #[inline]
  fn version(&self) -> u64 {
    self.version()
  }

  #[inline]
  fn value(&self) -> Option<ValuePointer<V>> {
    VersionedEntry::value(self).copied()
  }
}

impl<K, V> WithVersion for VersionedEntry<'_, RecordPointer<K>, ValuePointer<V>>
where
  K: ?Sized,
  V: ?Sized,
{
}

/// A memory table implementation based on ARENA [`SkipMap`](skl).
pub struct MultipleVersionTable<K: ?Sized, V: ?Sized> {
  map: SkipMap<RecordPointer<K>, ValuePointer<V>>,
}

impl<K, V> BaseTable for MultipleVersionTable<K, V>
where
  K: ?Sized + Type + Ord + 'static,
  for<'a> RecordPointer<K>: Type<Ref<'a> = RecordPointer<K>> + KeyRef<'a, RecordPointer<K>>,
  V: ?Sized + Type + 'static,
{
  type Key = K;
  type Value = V;

  type Item<'a>
    = Entry<'a, RecordPointer<K>, ValuePointer<V>>
  where
    Self: 'a;

  type Iterator<'a>
    = Iter<'a, RecordPointer<K>, ValuePointer<V>>
  where
    Self: 'a;

  type Range<'a, Q, R>
    = Range<'a, RecordPointer<K>, ValuePointer<V>, Q, R>
  where
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<RecordPointer<K>>;

  type Options = TableOptions;

  type Error = skl::error::Error;

  #[inline]
  fn new(opts: Self::Options) -> Result<Self, Self::Error> {
    let arena_opts = Options::new()
      .with_capacity(opts.capacity())
      .with_freelist(skl::Freelist::None)
      .with_unify(false)
      .with_max_height(opts.max_height());

    memmap_or_not!(opts(arena_opts))
  }

  fn insert(
    &self,
    version: Option<u64>,
    kp: RecordPointer<K>,
    vp: ValuePointer<V>,
  ) -> Result<(), Self::Error>
  where
    RecordPointer<K>: Ord + 'static,
  {
    self
      .map
      .insert(version.unwrap_or(0), &kp, &vp)
      .map(|_| ())
      .map_err(|e| match e {
        Among::Right(e) => e,
        _ => unreachable!(),
      })
  }

  fn remove(&self, version: Option<u64>, key: RecordPointer<K>) -> Result<(), Self::Error>
  where
    RecordPointer<K>: Ord + 'static,
  {
    match self.map.get_or_remove(version.unwrap_or(0), &key) {
      Err(Either::Right(e)) => Err(e),
      Err(Either::Left(_)) => unreachable!(),
      _ => Ok(()),
    }
  }

  #[inline]
  fn kind() -> Kind {
    Kind::MultipleVersion
  }
}

impl<K, V> MultipleVersionMemtable for MultipleVersionTable<K, V>
where
  K: ?Sized + Type + Ord + 'static,
  for<'a> RecordPointer<K>: Type<Ref<'a> = RecordPointer<K>> + KeyRef<'a, RecordPointer<K>>,
  V: ?Sized + Type + 'static,
{
  type MultipleVersionEntry<'a>
    = VersionedEntry<'a, RecordPointer<K>, ValuePointer<V>>
  where
    Self: 'a;

  type IterAll<'a>
    = IterAll<'a, RecordPointer<K>, ValuePointer<V>>
  where
    Self: 'a;

  type MultipleVersionRange<'a, Q, R>
    = MultipleVersionRange<'a, RecordPointer<K>, ValuePointer<V>, Q, R>
  where
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<RecordPointer<K>>;

  #[inline]
  fn maximum_version(&self) -> u64 {
    self.map.maximum_version()
  }

  #[inline]
  fn minimum_version(&self) -> u64 {
    self.map.minimum_version()
  }

  #[inline]
  fn may_contain_version(&self, version: u64) -> bool {
    self.map.may_contain_version(version)
  }

  fn upper_bound<Q>(&self, version: u64, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<RecordPointer<K>>,
  {
    self.map.upper_bound(version, bound)
  }

  fn upper_bound_with_tombstone<Q>(
    &self,
    version: u64,
    bound: Bound<&Q>,
  ) -> Option<Self::MultipleVersionEntry<'_>>
  where
    Q: ?Sized + Comparable<RecordPointer<K>>,
  {
    self.map.upper_bound_with_tombstone(version, bound)
  }

  fn lower_bound<Q>(&self, version: u64, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<RecordPointer<K>>,
  {
    self.map.lower_bound(version, bound)
  }

  fn lower_bound_with_tombstone<Q>(
    &self,
    version: u64,
    bound: Bound<&Q>,
  ) -> Option<Self::MultipleVersionEntry<'_>>
  where
    Q: ?Sized + Comparable<RecordPointer<K>>,
  {
    self.map.lower_bound_with_tombstone(version, bound)
  }

  fn first(&self, version: u64) -> Option<Self::Item<'_>>
  where
    RecordPointer<K>: Ord,
  {
    self.map.first(version)
  }

  fn first_with_tombstone(&self, version: u64) -> Option<Self::MultipleVersionEntry<'_>>
  where
    RecordPointer<K>: Ord,
  {
    self.map.first_with_tombstone(version)
  }

  fn last(&self, version: u64) -> Option<Self::Item<'_>>
  where
    RecordPointer<K>: Ord,
  {
    self.map.last(version)
  }

  fn last_with_tombstone(&self, version: u64) -> Option<Self::MultipleVersionEntry<'_>>
  where
    RecordPointer<K>: Ord,
  {
    self.map.last_with_tombstone(version)
  }

  fn get<Q>(&self, version: u64, key: &Q) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<RecordPointer<K>>,
  {
    self.map.get(version, key)
  }

  fn get_with_tombstone<Q>(&self, version: u64, key: &Q) -> Option<Self::MultipleVersionEntry<'_>>
  where
    Q: ?Sized + Comparable<RecordPointer<K>>,
  {
    self.map.get_with_tombstone(version, key)
  }

  fn contains<Q>(&self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Comparable<RecordPointer<K>>,
  {
    self.map.contains_key(version, key)
  }

  fn contains_with_tombstone<Q>(&self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Comparable<RecordPointer<K>>,
  {
    self.map.contains_key_with_tombstone(version, key)
  }

  fn iter(&self, version: u64) -> Self::Iterator<'_> {
    self.map.iter(version)
  }

  fn iter_with_tombstone(&self, version: u64) -> Self::IterAll<'_> {
    self.map.iter_with_tombstone(version)
  }

  fn range<'a, Q, R>(&'a self, version: u64, range: R) -> Self::Range<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<RecordPointer<K>>,
  {
    self.map.range(version, range)
  }

  fn range_with_tombstone<'a, Q, R>(&'a self, version: u64, range: R) -> Self::MultipleVersionRange<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<RecordPointer<K>>,
  {
    self.map.range_with_tombstone(version, range)
  }
}