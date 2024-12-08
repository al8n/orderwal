use crate::{dynamic::sealed::Wal, memtable::Memtable, Options};
use rarena_allocator::sync::Arena;

pub struct OrderCore<M, S> {
  pub(super) arena: Arena,
  pub(super) map: M,
  pub(super) opts: Options,
  pub(super) cks: S,
}

impl<M, S> core::fmt::Debug for OrderCore<M, S> {
  #[inline]
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("OrderCore")
      .field("arena", &self.arena)
      .field("options", &self.opts)
      .finish()
  }
}

impl<M, S> Wal<S> for OrderCore<M, S>
where
  M: Memtable,
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
