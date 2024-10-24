use core::ops::{Bound, RangeBounds};

use among::Among;
use dbutils::{
  equivalent::Comparable,
  traits::{KeyRef, Type},
};
use skl::{
  either::Either,
  map::{
    sync::{Entry, Iter, Range, SkipMap},
    Map as _,
  },
  Arena as _, Container as _, Options,
};

use crate::{
  memtable::BaseTable,
  sealed::{Pointer, WithoutVersion},
};

use super::{
  super::{Memtable, MemtableEntry},
  TableOptions,
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
pub struct Table<P> {
  map: SkipMap<P, ()>,
}

impl<P> BaseTable for Table<P>
where
  for<'a> P: Type<Ref<'a> = P> + KeyRef<'a, P> + Clone + 'static,
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

  fn insert(&mut self, ele: Self::Pointer) -> Result<(), Self::Error>
  where
    Self::Pointer: Pointer + Ord + 'static,
  {
    self.map.insert(&ele, &()).map(|_| ()).map_err(|e| match e {
      Among::Right(e) => e,
      _ => unreachable!(),
    })
  }

  fn remove(&mut self, key: Self::Pointer) -> Result<(), Self::Error>
  where
    Self::Pointer: Pointer + Ord + 'static,
  {
    match self.map.get_or_remove(&key) {
      Err(Either::Right(e)) => Err(e),
      Err(Either::Left(_)) => unreachable!(),
      _ => Ok(()),
    }
  }
}

impl<P> Memtable for Table<P>
where
  for<'a> P: Type<Ref<'a> = P> + KeyRef<'a, P> + Clone + WithoutVersion + 'static,
{
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