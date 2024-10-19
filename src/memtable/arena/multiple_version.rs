use core::ops::{Bound, RangeBounds};

use among::Among;
use dbutils::{
  equivalent::Comparable,
  traits::{KeyRef, Type},
};
use skl::{
  versioned::{
    sync::{AllVersionsIter, AllVersionsRange, Entry, Iter, Range, SkipMap, VersionedEntry},
    VersionedMap as _,
  },
  Arena as _, Options, VersionedContainer as _,
};

use crate::{
  error::Error,
  memtable::{MemtableEntry, VersionedMemtable, VersionedMemtableEntry},
  sealed::WithVersion,
};

use super::ArenaTableOptions;

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

impl<'a, P> VersionedMemtableEntry<'a> for VersionedEntry<'a, P, ()>
where
  P: Type<Ref<'a> = P> + KeyRef<'a, P> + WithVersion,
{
  #[inline]
  fn version(&self) -> u64 {
    self.version()
  }
}

/// A memory table implementation based on ARENA [`SkipMap`](skl).
pub struct VersionedArenaTable<P> {
  map: SkipMap<P, ()>,
}

impl<P> VersionedMemtable for VersionedArenaTable<P>
where
  for<'a> P: Type<Ref<'a> = P> + KeyRef<'a, P> + 'static + WithVersion,
{
  type Pointer = P;

  type Item<'a>
    = Entry<'a, Self::Pointer, ()>
  where
    Self::Pointer: 'a,
    Self: 'a;

  type VersionedItem<'a>
    = VersionedEntry<'a, Self::Pointer, ()>
  where
    Self::Pointer: 'a,
    Self: 'a;

  type Iterator<'a>
    = Iter<'a, P, ()>
  where
    Self::Pointer: 'a,
    Self: 'a;

  type AllIterator<'a>
    = AllVersionsIter<'a, P, ()>
  where
    Self::Pointer: 'a,
    Self: 'a;

  type Range<'a, Q, R>
    = Range<'a, P, (), Q, R>
  where
    Self::Pointer: 'a,
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<Self::Pointer>;
  type AllRange<'a, Q, R>
    = AllVersionsRange<'a, P, (), Q, R>
  where
    Self::Pointer: 'a,
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<Self::Pointer>;

  type Options = ArenaTableOptions;

  type ConstructionError = skl::Error;

  #[inline]
  fn new(opts: Self::Options) -> Result<Self, Self::ConstructionError> {
    let arena_opts = Options::new()
      .with_capacity(opts.capacity())
      .with_freelist(skl::Freelist::None)
      .with_unify(false)
      .with_max_height(opts.max_height());

    if opts.map_anon() {
      arena_opts
        .map_anon::<P, (), SkipMap<_, _>>()
        .map_err(skl::Error::IO)
    } else {
      arena_opts.alloc::<P, (), SkipMap<_, _>>()
    }
    .map(|map| Self { map })
  }

  #[inline]
  fn len(&self) -> usize {
    self.map.len()
  }

  fn upper_bound<Q>(&self, version: u64, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.upper_bound(version, bound)
  }

  fn lower_bound<Q>(&self, version: u64, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.lower_bound(version, bound)
  }

  fn insert(&mut self, version: u64, ele: Self::Pointer) -> Result<(), Error>
  where
    Self::Pointer: Ord + 'static,
  {
    self
      .map
      .insert(version, &ele, &())
      .map(|_| ())
      .map_err(|e| match e {
        Among::Right(skl::Error::Arena(skl::ArenaError::InsufficientSpace {
          requested,
          available,
        })) => Error::memtable_insufficient_space(requested as u64, available),
        _ => unreachable!(),
      })
  }

  fn first(&self, version: u64) -> Option<Self::Item<'_>>
  where
    Self::Pointer: Ord,
  {
    self.map.first(version)
  }

  fn last(&self, version: u64) -> Option<Self::Item<'_>>
  where
    Self::Pointer: Ord,
  {
    self.map.last(version)
  }

  fn get<Q>(&self, version: u64, key: &Q) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.get(version, key)
  }

  fn contains<Q>(&self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.contains_key(version, key)
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
