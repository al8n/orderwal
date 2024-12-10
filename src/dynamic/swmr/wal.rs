use crate::{memtable::Memtable, Options};
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

impl<M, S> OrderCore<M, S>
where
  M: Memtable,
{
  #[inline]
  pub fn construct(arena: Arena, set: M, opts: Options, checksumer: S) -> Self {
    Self {
      arena,
      map: set,
      opts,
      cks: checksumer,
    }
  }
}
