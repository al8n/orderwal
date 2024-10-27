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
  memtable::{BaseTable, MemtableEntry, MultipleVersionMemtable, MultipleVersionMemtableEntry},
  sealed::{Pointer, WithVersion},
};

use super::TableOptions;

impl<'a, P> MemtableEntry<'a> for Entry<'a, P, ()>
where
  P: Type<Ref<'a> = P> + KeyRef<'a, P> + WithVersion,
{
  type Pointer = P;

  #[inline]
  fn pointer(&self) -> &Self::Pointer {
    self.key()
  }

  #[inline]
  fn next(&mut self) -> Option<Self> {
    Entry::next(self)
  }

  #[inline]
  fn prev(&mut self) -> Option<Self> {
    Entry::prev(self)
  }
}

impl<'a, P> MemtableEntry<'a> for VersionedEntry<'a, P, ()>
where
  P: Type<Ref<'a> = P> + KeyRef<'a, P> + WithVersion,
{
  type Pointer = P;

  #[inline]
  fn pointer(&self) -> &Self::Pointer {
    self.key()
  }

  #[inline]
  fn next(&mut self) -> Option<Self> {
    VersionedEntry::next(self)
  }

  #[inline]
  fn prev(&mut self) -> Option<Self> {
    VersionedEntry::prev(self)
  }
}

impl<'a, P> MultipleVersionMemtableEntry<'a> for VersionedEntry<'a, P, ()>
where
  P: Type<Ref<'a> = P> + KeyRef<'a, P> + WithVersion,
{
  #[inline]
  fn version(&self) -> u64 {
    self.version()
  }
}

/// A memory table implementation based on ARENA [`SkipMap`](skl).
pub struct MultipleVersionTable<P> {
  map: SkipMap<P, ()>,
}

impl<P> BaseTable for MultipleVersionTable<P>
where
  for<'a> P: Type<Ref<'a> = P> + KeyRef<'a, P> + 'static + Clone + WithVersion,
{
  type Pointer = P;

  type Item<'a>
    = Entry<'a, Self::Pointer, ()>
  where
    Self::Pointer: 'a,
    Self: 'a;

  type Iterator<'a>
    = Iter<'a, Self::Pointer, ()>
  where
    Self::Pointer: 'a,
    Self: 'a;

  type Range<'a, Q, R>
    = Range<'a, Self::Pointer, (), Q, R>
  where
    Self::Pointer: 'a,
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<Self::Pointer>;

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
        .map_anon::<Self::Pointer, (), SkipMap<_, _>>()
        .map_err(skl::Error::IO)
    } else {
      arena_opts.alloc::<Self::Pointer, (), SkipMap<_, _>>()
    }
    .map(|map| Self { map })
  }

  fn insert(&self, ele: Self::Pointer) -> Result<(), Self::Error>
  where
    Self::Pointer: Pointer + Ord + 'static,
  {
    self
      .map
      .insert(ele.version().unwrap_or(0), &ele, &())
      .map(|_| ())
      .map_err(|e| match e {
        Among::Right(e) => e,
        _ => unreachable!(),
      })
  }

  fn remove(&self, key: Self::Pointer) -> Result<(), Self::Error>
  where
    Self::Pointer: Pointer + Ord + 'static,
  {
    match self.map.get_or_remove(key.version().unwrap_or(0), &key) {
      Err(Either::Right(e)) => Err(e),
      Err(Either::Left(_)) => unreachable!(),
      _ => Ok(()),
    }
  }
}

impl<P> MultipleVersionMemtable for MultipleVersionTable<P>
where
  for<'a> P: Type<Ref<'a> = P> + KeyRef<'a, P> + 'static + Clone + WithVersion,
{
  type MultipleVersionItem<'a>
    = VersionedEntry<'a, Self::Pointer, ()>
  where
    Self::Pointer: 'a,
    Self: 'a;

  type AllIterator<'a>
    = AllVersionsIter<'a, Self::Pointer, ()>
  where
    Self::Pointer: 'a,
    Self: 'a;

  type AllRange<'a, Q, R>
    = AllVersionsRange<'a, Self::Pointer, (), Q, R>
  where
    Self::Pointer: 'a,
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<Self::Pointer>;

  fn upper_bound<Q>(&self, version: u64, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.upper_bound(version, bound)
  }

  fn upper_bound_versioned<Q>(
    &self,
    version: u64,
    bound: Bound<&Q>,
  ) -> Option<Self::MultipleVersionItem<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.upper_bound_versioned(version, bound)
  }

  fn lower_bound<Q>(&self, version: u64, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.lower_bound(version, bound)
  }

  fn lower_bound_versioned<Q>(
    &self,
    version: u64,
    bound: Bound<&Q>,
  ) -> Option<Self::MultipleVersionItem<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.lower_bound_versioned(version, bound)
  }

  fn first(&self, version: u64) -> Option<Self::Item<'_>>
  where
    Self::Pointer: Ord,
  {
    self.map.first(version)
  }

  fn first_versioned(&self, version: u64) -> Option<Self::MultipleVersionItem<'_>>
  where
    Self::Pointer: Ord,
  {
    self.map.first_versioned(version)
  }

  fn last(&self, version: u64) -> Option<Self::Item<'_>>
  where
    Self::Pointer: Ord,
  {
    self.map.last(version)
  }

  fn last_versioned(&self, version: u64) -> Option<Self::MultipleVersionItem<'_>>
  where
    Self::Pointer: Ord,
  {
    self.map.last_versioned(version)
  }

  fn get<Q>(&self, version: u64, key: &Q) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.get(version, key)
  }

  fn get_versioned<Q>(&self, version: u64, key: &Q) -> Option<Self::MultipleVersionItem<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.get_versioned(version, key)
  }

  fn contains<Q>(&self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.contains_key(version, key)
  }

  fn contains_versioned<Q>(&self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Comparable<Self::Pointer>,
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
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.range(version, range)
  }

  fn range_all_versions<'a, Q, R>(&'a self, version: u64, range: R) -> Self::AllRange<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.range_all_versions(version, range)
  }
}
