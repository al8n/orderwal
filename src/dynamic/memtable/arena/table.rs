use {
  super::TableOptions,
  crate::{
    dynamic::{
      memtable::{BaseEntry, BaseTable, Memtable, MemtableEntry},
      wal::{RecordPointer, ValuePointer},
    },
    types::Mode,
    WithoutVersion,
  },
  among::Among,
  core::ops::{Bound, RangeBounds},
  dbutils::{
    equivalent::Comparable,
    types::{KeyRef, Type},
  },
  skl::{
    either::Either,
    map::{sync::SkipMap, Map as _},
    Arena as _, EntryRef, Options,
  },
};

pub use skl::map::sync::{Entry, Iter, Range};

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
    *EntryRef::key(self)
  }
}

impl<'a, K, V> MemtableEntry<'a> for Entry<'a, RecordPointer<K>, ValuePointer<V>>
where
  K: ?Sized + Type + Ord,
  RecordPointer<K>: Type<Ref<'a> = RecordPointer<K>> + KeyRef<'a, RecordPointer<K>>,
  V: ?Sized + Type,
{
  #[inline]
  fn value(&self) -> ValuePointer<V> {
    *EntryRef::value(self)
  }
}

impl<K: ?Sized, V: ?Sized> WithoutVersion for Entry<'_, RecordPointer<K>, ValuePointer<V>> {}

/// A memory table implementation based on ARENA [`SkipMap`](skl).
pub struct Table<K: ?Sized, V: ?Sized> {
  map: SkipMap<RecordPointer<K>, ValuePointer<V>>,
}

impl<K, V> BaseTable for Table<K, V>
where
  K: ?Sized + Type + Ord + 'static,
  for<'a> RecordPointer<K>: Type<Ref<'a> = RecordPointer<K>> + KeyRef<'a, RecordPointer<K>>,
  V: ?Sized + Type + 'static,
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
    = Range<'a, RecordPointer<Self::Key>, ValuePointer<Self::Value>, Q, R>
  where
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>;

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
    _: Option<u64>,
    kp: RecordPointer<Self::Key>,
    vp: ValuePointer<Self::Value>,
  ) -> Result<(), Self::Error>
  where
    RecordPointer<Self::Key>: Ord + 'static,
  {
    self.map.insert(&kp, &vp).map(|_| ()).map_err(|e| match e {
      Among::Right(e) => e,
      _ => unreachable!(),
    })
  }

  fn remove(&self, _: Option<u64>, key: RecordPointer<Self::Key>) -> Result<(), Self::Error>
  where
    RecordPointer<Self::Key>: Ord + 'static,
  {
    match self.map.get_or_remove(&key) {
      Err(Either::Right(e)) => Err(e),
      Err(Either::Left(_)) => unreachable!(),
      _ => Ok(()),
    }
  }

  #[inline]
  fn mode() -> Kind {
    Kind::Plain
  }
}

impl<K, V> Memtable for Table<K, V>
where
  K: ?Sized + Type + Ord + 'static,
  for<'a> RecordPointer<K>: Type<Ref<'a> = RecordPointer<K>> + KeyRef<'a, RecordPointer<K>>,
  V: ?Sized + Type + 'static,
{
  #[inline]
  fn len(&self) -> usize {
    self.map.len()
  }

  fn upper_bound<Q>(&self, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>,
  {
    self.map.upper_bound(bound)
  }

  fn lower_bound<Q>(&self, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>,
  {
    self.map.lower_bound(bound)
  }

  fn first(&self) -> Option<Self::Item<'_>>
  where
    RecordPointer<Self::Key>: Ord,
  {
    self.map.first()
  }

  fn last(&self) -> Option<Self::Item<'_>>
  where
    RecordPointer<Self::Key>: Ord,
  {
    self.map.last()
  }

  fn get<Q>(&self, key: &Q) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>,
  {
    self.map.get(key)
  }

  fn contains<Q>(&self, key: &Q) -> bool
  where
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>,
  {
    self.map.contains_key(key)
  }

  fn iter(&self) -> Self::Iterator<'_> {
    self.map.iter()
  }

  fn range<'a, Q, R>(&'a self, range: R) -> Self::Range<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<RecordPointer<Self::Key>>,
  {
    self.map.range(range)
  }
}
