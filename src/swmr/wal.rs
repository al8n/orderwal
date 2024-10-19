use core::{marker::PhantomData, ops::Bound};

use dbutils::equivalent::Comparable;
use rarena_allocator::sync::Arena;

use crate::{
  memtable::{Memtable, MemtableEntry as _},
  sealed::{Pointer, Wal},
  Options,
};
pub struct OrderCore<K: ?Sized, V: ?Sized, M, S> {
  pub(super) arena: Arena,
  pub(super) map: M,
  pub(super) max_version: u64,
  pub(super) min_version: u64,
  pub(super) opts: Options,
  pub(super) cks: S,
  pub(super) _m: PhantomData<(fn() -> K, fn() -> V)>,
}

impl<K, V, M, S> Wal<K, V, S> for OrderCore<K, V, M, S>
where
  K: ?Sized,
  V: ?Sized,
  M: Memtable,
  M::Pointer: Ord + Send + 'static,
{
  type Allocator = Arena;
  type Memtable = M;

  #[inline]
  fn memtable(&self) -> &Self::Memtable {
    &self.map
  }

  #[inline]
  fn memtable_mut(&mut self) -> &mut Self::Memtable {
    &mut self.map
  }

  #[inline]
  fn construct(
    arena: Self::Allocator,
    set: Self::Memtable,
    opts: Options,
    checksumer: S,
    maximum_version: u64,
    minimum_version: u64,
  ) -> Self {
    Self {
      arena,
      map: set,
      opts,
      max_version: maximum_version,
      min_version: minimum_version,
      cks: checksumer,
      _m: PhantomData,
    }
  }

  #[inline]
  fn options(&self) -> &Options {
    &self.opts
  }

  /// Returns the number of entries in the WAL.
  #[inline]
  fn len(&self) -> usize {
    self.map.len()
  }

  /// Returns `true` if the WAL is empty.
  #[inline]
  fn is_empty(&self) -> bool {
    self.map.is_empty()
  }

  #[inline]
  fn maximum_version(&self) -> u64 {
    self.max_version
  }

  #[inline]
  fn minimum_version(&self) -> u64 {
    self.min_version
  }

  #[inline]
  fn update_maximum_version(&mut self, version: u64) {
    self.max_version = version;
  }

  #[inline]
  fn update_minimum_version(&mut self, version: u64) {
    self.min_version = version;
  }

  #[inline]
  fn allocator(&self) -> &Self::Allocator {
    &self.arena
  }

  #[inline]
  fn upper_bound<Q>(&self, version: Option<u64>, bound: Bound<&Q>) -> Option<M::Item<'_>>
  where
    M::Pointer: Pointer,
    Q: ?Sized + Comparable<M::Pointer>,
  {
    match version {
      None => self.map.upper_bound(bound),
      Some(version) => {
        let mut ent = self.map.upper_bound(bound);
        loop {
          match ent {
            Some(ent) if ent.pointer().version() <= version => return Some(ent),
            Some(mut e) => ent = e.next(),
            None => return None,
          }
        }
      }
    }
  }

  #[inline]
  fn lower_bound<Q>(&self, version: Option<u64>, bound: core::ops::Bound<&Q>) -> Option<M::Item<'_>>
  where
    M::Pointer: Pointer,
    Q: ?Sized + Comparable<M::Pointer>,
  {
    match version {
      None => self.map.lower_bound(bound),
      Some(version) => {
        let mut ent = self.map.lower_bound(bound);
        loop {
          match ent {
            Some(ent) if ent.pointer().version() <= version => return Some(ent),
            Some(mut e) => ent = e.next(),
            None => return None,
          }
        }
      }
    }
  }

  #[inline]
  fn hasher(&self) -> &S {
    &self.cks
  }
}
