use core::ops::{Bound, RangeBounds};

use among::Among;
use dbutils::{
  equivalent::Comparable,
  traits::{KeyRef, Type},
};
use skl::{
  either::Either,
  versioned::{
    sync::{AllVersionsIter, AllVersionsRange, Entry, Iter, Range, SkipMap, VersionedEntry},
    VersionedMap as _,
  },
  Options, VersionedContainer as _,
};

use crate::{
  memtable::{BaseEntry, BaseTable, MultipleVersionMemtable, MultipleVersionMemtableEntry},
  sealed::WithVersion,
  wal::{KeyPointer, ValuePointer},
};

use super::TableOptions;

impl<'a, K, V> BaseEntry<'a> for Entry<'a, KeyPointer<K>, ValuePointer<V>>
where
  K: ?Sized + Type + Ord,
  KeyPointer<K>: Type<Ref<'a> = KeyPointer<K>> + KeyRef<'a, KeyPointer<K>>,
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
  fn key(&self) -> KeyPointer<K> {
    *Entry::key(self)
  }
}

impl<'a, K, V> MultipleVersionMemtableEntry<'a> for Entry<'a, KeyPointer<K>, ValuePointer<V>>
where
  K: ?Sized + Type + Ord,
  KeyPointer<K>: Type<Ref<'a> = KeyPointer<K>> + KeyRef<'a, KeyPointer<K>>,
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

impl<K, V> WithVersion for Entry<'_, KeyPointer<K>, ValuePointer<V>>
where
  K: ?Sized,
  V: ?Sized,
{
}

impl<'a, K, V> BaseEntry<'a> for VersionedEntry<'a, KeyPointer<K>, ValuePointer<V>>
where
  K: ?Sized + Type + Ord,
  KeyPointer<K>: Type<Ref<'a> = KeyPointer<K>> + KeyRef<'a, KeyPointer<K>>,
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
  fn key(&self) -> KeyPointer<K> {
    *VersionedEntry::key(self)
  }
}

impl<'a, K, V> MultipleVersionMemtableEntry<'a>
  for VersionedEntry<'a, KeyPointer<K>, ValuePointer<V>>
where
  K: ?Sized + Type + Ord,
  KeyPointer<K>: Type<Ref<'a> = KeyPointer<K>> + KeyRef<'a, KeyPointer<K>>,
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

impl<K, V> WithVersion for VersionedEntry<'_, KeyPointer<K>, ValuePointer<V>>
where
  K: ?Sized,
  V: ?Sized,
{
}

/// A memory table implementation based on ARENA [`SkipMap`](skl).
pub struct MultipleVersionTable<K: ?Sized, V: ?Sized> {
  map: SkipMap<KeyPointer<K>, ValuePointer<V>>,
}

impl<K, V> BaseTable for MultipleVersionTable<K, V>
where
  K: ?Sized + Type + Ord + 'static,
  for<'a> KeyPointer<K>: Type<Ref<'a> = KeyPointer<K>> + KeyRef<'a, KeyPointer<K>>,
  V: ?Sized + Type + 'static,
{
  type Key = K;
  type Value = V;

  type Item<'a>
    = Entry<'a, KeyPointer<K>, ValuePointer<V>>
  where
    Self: 'a;

  type Iterator<'a>
    = Iter<'a, KeyPointer<K>, ValuePointer<V>>
  where
    Self: 'a;

  type Range<'a, Q, R>
    = Range<'a, KeyPointer<K>, ValuePointer<V>, Q, R>
  where
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<KeyPointer<K>>;

  type Options = TableOptions;

  type Error = skl::Error;

  #[inline]
  fn new(opts: Self::Options) -> Result<Self, Self::Error> {
    let arena_opts = Options::new()
      .with_capacity(opts.capacity())
      .with_freelist(skl::Freelist::None)
      .with_unify(false)
      .with_max_height(opts.max_height());

    if opts.map_anon() {
      arena_opts
        .map_anon::<KeyPointer<K>, ValuePointer<V>, SkipMap<_, _>>()
        .map_err(skl::Error::IO)
    } else {
      arena_opts.alloc::<KeyPointer<K>, ValuePointer<V>, SkipMap<_, _>>()
    }
    .map(|map| Self { map })
  }

  fn insert(
    &self,
    version: Option<u64>,
    kp: KeyPointer<K>,
    vp: ValuePointer<V>,
  ) -> Result<(), Self::Error>
  where
    KeyPointer<K>: Ord + 'static,
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

  fn remove(&self, version: Option<u64>, key: KeyPointer<K>) -> Result<(), Self::Error>
  where
    KeyPointer<K>: Ord + 'static,
  {
    match self.map.get_or_remove(version.unwrap_or(0), &key) {
      Err(Either::Right(e)) => Err(e),
      Err(Either::Left(_)) => unreachable!(),
      _ => Ok(()),
    }
  }
}

impl<K, V> MultipleVersionMemtable for MultipleVersionTable<K, V>
where
  K: ?Sized + Type + Ord + 'static,
  for<'a> KeyPointer<K>: Type<Ref<'a> = KeyPointer<K>> + KeyRef<'a, KeyPointer<K>>,
  V: ?Sized + Type + 'static,
{
  type MultipleVersionItem<'a>
    = VersionedEntry<'a, KeyPointer<K>, ValuePointer<V>>
  where
    Self: 'a;

  type AllIterator<'a>
    = AllVersionsIter<'a, KeyPointer<K>, ValuePointer<V>>
  where
    Self: 'a;

  type AllRange<'a, Q, R>
    = AllVersionsRange<'a, KeyPointer<K>, ValuePointer<V>, Q, R>
  where
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<KeyPointer<K>>;

  fn upper_bound<Q>(&self, version: u64, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<KeyPointer<K>>,
  {
    self.map.upper_bound(version, bound)
  }

  fn upper_bound_versioned<Q>(
    &self,
    version: u64,
    bound: Bound<&Q>,
  ) -> Option<Self::MultipleVersionItem<'_>>
  where
    Q: ?Sized + Comparable<KeyPointer<K>>,
  {
    self.map.upper_bound_versioned(version, bound)
  }

  fn lower_bound<Q>(&self, version: u64, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<KeyPointer<K>>,
  {
    self.map.lower_bound(version, bound)
  }

  fn lower_bound_versioned<Q>(
    &self,
    version: u64,
    bound: Bound<&Q>,
  ) -> Option<Self::MultipleVersionItem<'_>>
  where
    Q: ?Sized + Comparable<KeyPointer<K>>,
  {
    self.map.lower_bound_versioned(version, bound)
  }

  fn first(&self, version: u64) -> Option<Self::Item<'_>>
  where
    KeyPointer<K>: Ord,
  {
    self.map.first(version)
  }

  fn first_versioned(&self, version: u64) -> Option<Self::MultipleVersionItem<'_>>
  where
    KeyPointer<K>: Ord,
  {
    self.map.first_versioned(version)
  }

  fn last(&self, version: u64) -> Option<Self::Item<'_>>
  where
    KeyPointer<K>: Ord,
  {
    self.map.last(version)
  }

  fn last_versioned(&self, version: u64) -> Option<Self::MultipleVersionItem<'_>>
  where
    KeyPointer<K>: Ord,
  {
    self.map.last_versioned(version)
  }

  fn get<Q>(&self, version: u64, key: &Q) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<KeyPointer<K>>,
  {
    self.map.get(version, key).inspect(|ent| {
      println!("get arena: {:?} {:?}", ent.version(), ent.key());
    })
  }

  fn get_versioned<Q>(&self, version: u64, key: &Q) -> Option<Self::MultipleVersionItem<'_>>
  where
    Q: ?Sized + Comparable<KeyPointer<K>>,
  {
    self.map.get_versioned(version, key)
  }

  fn contains<Q>(&self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Comparable<KeyPointer<K>>,
  {
    self.map.contains_key(version, key)
  }

  fn contains_versioned<Q>(&self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Comparable<KeyPointer<K>>,
  {
    self.map.contains_key_versioned(version, key)
  }

  fn iter(&self, version: u64) -> Self::Iterator<'_> {
    self.map.iter(version)
  }

  fn iter_all_versions(&self, version: u64) -> Self::AllIterator<'_> {
    self.map.iter_all_versions(version)
  }

  fn range<'a, Q, R>(&'a self, version: u64, range: R) -> Self::Range<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<KeyPointer<K>>,
  {
    self.map.range(version, range)
  }

  fn range_all_versions<'a, Q, R>(&'a self, version: u64, range: R) -> Self::AllRange<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<KeyPointer<K>>,
  {
    self.map.range_all_versions(version, range)
  }
}
