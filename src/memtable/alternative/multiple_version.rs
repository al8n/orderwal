use core::ops::{Bound, RangeBounds};

use crate::{
  memtable::{
    arena::{
      multiple_version::{
        Entry as ArenaEntry, Iter as ArenaIter, IterAll as ArenaIterAll, Range as ArenaRange,
        RangeAll as ArenaRangeAll, VersionedEntry as ArenaVersionedEntry,
      },
      MultipleVersionTable as ArenaTable,
    },
    BaseEntry, BaseTable, MultipleVersionMemtable, VersionedMemtableEntry,
  },
  sealed::WithVersion,
  types::Kind,
  wal::{KeyPointer, ValuePointer},
};

#[cfg(feature = "std")]
use crate::memtable::linked::{
  multiple_version::{
    Entry as LinkedEntry, Iter as LinkedIter, IterAll as LinkedIterAll, Range as LinkedRange,
    RangeAll as LinkedRangeAll, VersionedEntry as LinkedVersionedEntry,
  },
  MultipleVersionTable as LinkedTable,
};

use dbutils::{
  equivalent::Comparable,
  types::{KeyRef, Type},
};

use super::TableOptions;

base_entry!(
  enum Entry {
    Arena(ArenaEntry),
    Linked(LinkedEntry),
  }
);

impl<'a, K, V> VersionedMemtableEntry<'a> for Entry<'a, K, V>
where
  K: ?Sized + Type + Ord,
  KeyPointer<K>: Type<Ref<'a> = KeyPointer<K>> + KeyRef<'a, KeyPointer<K>>,
  V: ?Sized + Type,
{
  #[inline]
  fn value(&self) -> Option<ValuePointer<Self::Value>> {
    Some(*match_op!(self.value()))
  }

  #[inline]
  fn version(&self) -> u64 {
    match_op!(self.version())
  }
}

impl<K: ?Sized, V: ?Sized> WithVersion for Entry<'_, K, V> {}

base_entry!(
  enum VersionedEntry {
    Arena(ArenaVersionedEntry),
    Linked(LinkedVersionedEntry),
  }
);

impl<'a, K, V> VersionedMemtableEntry<'a> for VersionedEntry<'a, K, V>
where
  K: ?Sized + Type + Ord,
  KeyPointer<K>: Type<Ref<'a> = KeyPointer<K>> + KeyRef<'a, KeyPointer<K>>,
  V: ?Sized + Type,
{
  #[inline]
  fn value(&self) -> Option<ValuePointer<Self::Value>> {
    match_op!(self.value()).copied()
  }

  #[inline]
  fn version(&self) -> u64 {
    match_op!(self.version())
  }
}

impl<K: ?Sized, V: ?Sized> WithVersion for VersionedEntry<'_, K, V> {}

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

iter!(
  enum IterAll {
    Arena(ArenaIterAll),
    Linked(LinkedIterAll),
  } -> VersionedEntry
);

range!(
  enum RangeAll {
    Arena(ArenaRangeAll),
    Linked(LinkedRangeAll),
  } -> VersionedEntry
);

/// A sum type for different memtable implementations.
#[non_exhaustive]
pub enum MultipleVersionTable<K: ?Sized, V: ?Sized> {
  /// Arena memtable
  Arena(ArenaTable<K, V>),
  /// Linked memtable
  #[cfg(feature = "std")]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  Linked(LinkedTable<K, V>),
}

impl<K, V> BaseTable for MultipleVersionTable<K, V>
where
  K: ?Sized + Type + Ord + 'static,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  for<'a> KeyPointer<K>: Type<Ref<'a> = KeyPointer<K>> + KeyRef<'a, KeyPointer<K>>,
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
    Q: ?Sized + Comparable<KeyPointer<Self::Key>>;

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
    kp: KeyPointer<Self::Key>,
    vp: ValuePointer<Self::Value>,
  ) -> Result<(), Self::Error>
  where
    KeyPointer<Self::Key>: Ord + 'static,
  {
    match_op!(update(self.insert(version, kp, vp)))
  }

  #[inline]
  fn remove(&self, version: Option<u64>, key: KeyPointer<Self::Key>) -> Result<(), Self::Error>
  where
    KeyPointer<Self::Key>: Ord + 'static,
  {
    match_op!(update(self.remove(version, key)))
  }

  #[inline]
  fn kind() -> Kind {
    Kind::MultipleVersion
  }
}

impl<K, V> MultipleVersionMemtable for MultipleVersionTable<K, V>
where
  K: ?Sized + Type + Ord + 'static,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  for<'a> KeyPointer<K>: Type<Ref<'a> = KeyPointer<K>> + KeyRef<'a, KeyPointer<K>>,
  V: ?Sized + Type + 'static,
{
  type VersionedItem<'a>
    = VersionedEntry<'a, K, V>
  where
    KeyPointer<Self::Key>: 'a,
    Self: 'a;

  type IterAll<'a>
    = IterAll<'a, K, V>
  where
    KeyPointer<Self::Key>: 'a,
    Self: 'a;

  type RangeAll<'a, Q, R>
    = RangeAll<'a, K, V, Q, R>
  where
    KeyPointer<Self::Key>: 'a,
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<KeyPointer<Self::Key>>;

  #[inline]
  fn maximum_version(&self) -> u64 {
    match_op!(self.maximum_version())
  }

  #[inline]
  fn minimum_version(&self) -> u64 {
    match_op!(self.minimum_version())
  }

  #[inline]
  fn may_contain_version(&self, version: u64) -> bool {
    match_op!(self.may_contain_version(version))
  }

  #[inline]
  fn upper_bound<Q>(&self, version: u64, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<KeyPointer<Self::Key>>,
  {
    match_op!(self.upper_bound(version, bound).map(Item))
  }

  fn upper_bound_versioned<Q>(
    &self,
    version: u64,
    bound: Bound<&Q>,
  ) -> Option<Self::VersionedItem<'_>>
  where
    Q: ?Sized + Comparable<KeyPointer<Self::Key>>,
  {
    match_op!(self
      .upper_bound_versioned(version, bound)
      .map(VersionedItem))
  }

  #[inline]
  fn lower_bound<Q>(&self, version: u64, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<KeyPointer<Self::Key>>,
  {
    match_op!(self.lower_bound(version, bound).map(Item))
  }

  fn lower_bound_versioned<Q>(
    &self,
    version: u64,
    bound: Bound<&Q>,
  ) -> Option<Self::VersionedItem<'_>>
  where
    Q: ?Sized + Comparable<KeyPointer<Self::Key>>,
  {
    match_op!(self
      .lower_bound_versioned(version, bound)
      .map(VersionedItem))
  }

  #[inline]
  fn first(&self, version: u64) -> Option<Self::Item<'_>>
  where
    KeyPointer<Self::Key>: Ord,
  {
    match_op!(self.first(version).map(Item))
  }

  fn first_versioned(&self, version: u64) -> Option<Self::VersionedItem<'_>>
  where
    KeyPointer<Self::Key>: Ord,
  {
    match_op!(self.first_versioned(version).map(VersionedItem))
  }

  #[inline]
  fn last(&self, version: u64) -> Option<Self::Item<'_>>
  where
    KeyPointer<Self::Key>: Ord,
  {
    match_op!(self.last(version).map(Item))
  }

  fn last_versioned(&self, version: u64) -> Option<Self::VersionedItem<'_>>
  where
    KeyPointer<Self::Key>: Ord,
  {
    match_op!(self.last_versioned(version).map(VersionedItem))
  }

  #[inline]
  fn get<Q>(&self, version: u64, key: &Q) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<KeyPointer<Self::Key>>,
  {
    match_op!(self.get(version, key).map(Item))
  }

  fn get_versioned<Q>(&self, version: u64, key: &Q) -> Option<Self::VersionedItem<'_>>
  where
    Q: ?Sized + Comparable<KeyPointer<Self::Key>>,
  {
    match_op!(self.get_versioned(version, key).map(VersionedItem))
  }

  #[inline]
  fn contains<Q>(&self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Comparable<KeyPointer<Self::Key>>,
  {
    match_op!(self.contains(version, key))
  }

  fn contains_versioned<Q>(&self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Comparable<KeyPointer<Self::Key>>,
  {
    match_op!(self.contains_versioned(version, key))
  }

  #[inline]
  fn iter(&self, version: u64) -> Self::Iterator<'_> {
    match_op!(Dispatch::Iterator(self.iter(version)))
  }

  fn iter_all_versions(&self, version: u64) -> Self::IterAll<'_> {
    match_op!(Dispatch::IterAll(self.iter_all_versions(version)))
  }

  #[inline]
  fn range<'a, Q, R>(&'a self, version: u64, range: R) -> Self::Range<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<KeyPointer<Self::Key>>,
  {
    match_op!(Dispatch::Range(self.range(version, range)))
  }

  fn range_all_versions<'a, Q, R>(&'a self, version: u64, range: R) -> Self::RangeAll<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<KeyPointer<Self::Key>>,
  {
    match_op!(Dispatch::RangeAll(self.range_all_versions(version, range)))
  }
}
