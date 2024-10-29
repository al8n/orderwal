use core::marker::PhantomData;

use rarena_allocator::sync::Arena;

use crate::{memtable::BaseTable, sealed::Wal, Options};

pub struct OrderCore<K, V, M, S>
where
  K: ?Sized,
  V: ?Sized,
{
  pub(super) arena: Arena,
  pub(super) map: M,
  pub(super) max_version: u64,
  pub(super) min_version: u64,
  pub(super) opts: Options,
  pub(super) cks: S,
  pub(super) _m: PhantomData<(fn() -> K, fn() -> V)>,
}

impl<K, V, M, S> core::fmt::Debug for OrderCore<K, V, M, S>
where
  K: ?Sized,
  V: ?Sized,
{
  #[inline]
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("OrderCore")
      .field("arena", &self.arena)
      .field("max_version", &self.max_version)
      .field("min_version", &self.min_version)
      .field("options", &self.opts)
      .finish()
  }
}

impl<K, V, M, S> Wal<S> for OrderCore<K, V, M, S>
where
  K: ?Sized,
  V: ?Sized,
  M: BaseTable<Key = K, Value = V>,
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
  fn hasher(&self) -> &S {
    &self.cks
  }
}
