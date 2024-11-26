use {
  crate::{
    dynamic::{
      memtable,
      wal::{KeyPointer, ValuePointer},
    },
    types::Kind,
    WithoutVersion,
  },
  core::{convert::Infallible, ops::RangeBounds},
  crossbeam_skiplist::SkipMap,
  dbutils::{
    equivalent::Comparable,
    types::{KeyRef, Type},
  },
};

pub use crossbeam_skiplist::map::{Entry, Iter, Range};

/// An memory table implementation based on [`crossbeam_skiplist::SkipMap`].
pub struct Table<K: ?Sized, V: ?Sized>(SkipMap<KeyPointer<K>, ValuePointer<V>>);

impl<K, V> core::fmt::Debug for Table<K, V>
where
  K: ?Sized + Type + Ord,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  V: ?Sized,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_tuple("Table").field(&self.0).finish()
  }
}

impl<K: ?Sized, V: ?Sized> Default for Table<K, V> {
  #[inline]
  fn default() -> Self {
    Self(SkipMap::new())
  }
}

impl<'a, K, V> memtable::BaseEntry<'a> for Entry<'a, KeyPointer<K>, ValuePointer<V>>
where
  K: ?Sized + Type + Ord,
  K::Ref<'a>: KeyRef<'a, K>,
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
    *self.key()
  }
}

impl<'a, K, V> memtable::MemtableEntry<'a> for Entry<'a, KeyPointer<K>, ValuePointer<V>>
where
  K: ?Sized + Type + Ord,
  K::Ref<'a>: KeyRef<'a, K>,
  V: ?Sized + Type,
{
  #[inline]
  fn value(&self) -> ValuePointer<V> {
    *self.value()
  }
}

impl<K, V> WithoutVersion for Entry<'_, KeyPointer<K>, ValuePointer<V>>
where
  K: ?Sized,
  V: ?Sized,
{
}

impl<K, V> memtable::BaseTable for Table<K, V>
where
  K: ?Sized + Type + Ord,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  V: ?Sized + Type + 'static,
{
  type Key = K;
  type Value = V;
  type Item<'a>
    = Entry<'a, KeyPointer<Self::Key>, ValuePointer<Self::Value>>
  where
    Self: 'a;

  type Iterator<'a>
    = Iter<'a, KeyPointer<Self::Key>, ValuePointer<Self::Value>>
  where
    Self: 'a;

  type Range<'a, Q, R>
    = Range<'a, Q, R, KeyPointer<Self::Key>, ValuePointer<Self::Value>>
  where
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<KeyPointer<Self::Key>>;

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
    _: Option<u64>,
    kp: KeyPointer<Self::Key>,
    vp: ValuePointer<Self::Value>,
  ) -> Result<(), Self::Error>
  where
    KeyPointer<Self::Key>: Ord + 'static,
  {
    self.0.insert(kp, vp);
    Ok(())
  }

  #[inline]
  fn remove(&self, _: Option<u64>, key: KeyPointer<Self::Key>) -> Result<(), Self::Error>
  where
    KeyPointer<Self::Key>: Ord + 'static,
  {
    self.0.remove(&key);
    Ok(())
  }

  #[inline]
  fn kind() -> Kind {
    Kind::Plain
  }
}

impl<K, V> memtable::Memtable for Table<K, V>
where
  K: ?Sized + Type + Ord + 'static,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  V: ?Sized + Type + 'static,
{
  #[inline]
  fn len(&self) -> usize {
    self.0.len()
  }

  #[inline]
  fn upper_bound<Q>(&self, bound: core::ops::Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<KeyPointer<Self::Key>>,
  {
    self.0.upper_bound(bound)
  }

  #[inline]
  fn lower_bound<Q>(&self, bound: core::ops::Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<KeyPointer<Self::Key>>,
  {
    self.0.lower_bound(bound)
  }

  #[inline]
  fn first(&self) -> Option<Self::Item<'_>> {
    self.0.front()
  }

  #[inline]
  fn last(&self) -> Option<Self::Item<'_>> {
    self.0.back()
  }

  #[inline]
  fn get<Q>(&self, key: &Q) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<KeyPointer<Self::Key>>,
  {
    self.0.get(key)
  }

  #[inline]
  fn contains<Q>(&self, key: &Q) -> bool
  where
    Q: ?Sized + Comparable<KeyPointer<Self::Key>>,
  {
    self.0.contains_key(key)
  }

  #[inline]
  fn iter(&self) -> Self::Iterator<'_> {
    self.0.iter()
  }

  #[inline]
  fn range<'a, Q, R>(&'a self, range: R) -> Self::Range<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<KeyPointer<Self::Key>>,
  {
    self.0.range(range)
  }
}
