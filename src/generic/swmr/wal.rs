use {
  crate::{
    generic::{memtable::BaseTable, sealed::Wal},
    Options,
  },
  core::marker::PhantomData,
  rarena_allocator::sync::Arena,
};

pub struct OrderCore<K, V, M, S>
where
  K: ?Sized,
  V: ?Sized,
{
  pub(super) arena: Arena,
  pub(super) map: M,
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
  fn construct(arena: Self::Allocator, set: Self::Memtable, opts: Options, checksumer: S) -> Self {
    Self {
      arena,
      map: set,
      opts,
      cks: checksumer,
      _m: PhantomData,
    }
  }

  #[inline]
  fn options(&self) -> &Options {
    &self.opts
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
