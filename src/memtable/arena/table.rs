use core::ops::{Bound, RangeBounds};

use among::Among;
use dbutils::{
  equivalent::Comparable,
  traits::{KeyRef, Type},
};
use skl::{
  map::{
    sync::{Entry, Iter, Range, SkipMap},
    Map as _,
  },
  Arena as _, Container as _, Options,
};

use crate::error::Error;

use super::{
  super::{Memtable, MemtableEntry},
  ArenaTableOptions,
};

impl<'a, P> MemtableEntry<'a> for Entry<'a, P, ()>
where
  P: Type<Ref<'a> = P> + KeyRef<'a, P>,
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

/// A memory table implementation based on ARENA [`SkipMap`](skl).
pub struct ArenaTable<P> {
  map: SkipMap<P, ()>,
}

impl<P> Memtable for ArenaTable<P>
where
  for<'a> P: Type<Ref<'a> = P> + KeyRef<'a, P> + 'static,
{
  type Pointer = P;

  type Item<'a>
    = Entry<'a, Self::Pointer, ()>
  where
    Self::Pointer: 'a,
    Self: 'a;

  type Iterator<'a>
    = Iter<'a, P, ()>
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

  fn upper_bound<Q>(&self, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.upper_bound(bound)
  }

  fn lower_bound<Q>(&self, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.lower_bound(bound)
  }

  fn insert(&mut self, ele: Self::Pointer) -> Result<(), Error>
  where
    Self::Pointer: Ord + 'static,
  {
    self.map.insert(&ele, &()).map(|_| ()).map_err(|e| match e {
      Among::Right(skl::Error::Arena(skl::ArenaError::InsufficientSpace {
        requested,
        available,
      })) => Error::memtable_insufficient_space(requested as u64, available),
      _ => unreachable!(),
    })
  }

  fn first(&self) -> Option<Self::Item<'_>>
  where
    Self::Pointer: Ord,
  {
    self.map.first()
  }

  fn last(&self) -> Option<Self::Item<'_>>
  where
    Self::Pointer: Ord,
  {
    self.map.last()
  }

  fn get<Q>(&self, key: &Q) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.get(key)
  }

  fn contains<Q>(&self, key: &Q) -> bool
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.contains_key(key)
  }

  fn iter(&self) -> Self::Iterator<'_> {
    self.map.iter()
  }

  fn range<'a, Q, R>(&'a self, range: R) -> Self::Range<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.range(range)
  }
}
