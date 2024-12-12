use super::{super::swmr::wal::OrderCore, writer::OrderWal};
use crate::{log::Log, memtable::Memtable, Immutable};
use rarena_allocator::sync::Arena;
use triomphe::Arc;

/// An [`OrderWal`] reader.
pub struct OrderWalReader<M, S>(pub(crate) OrderWal<M, S>);

impl<M, S> core::fmt::Debug for OrderWalReader<M, S> {
  #[inline]
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_tuple("OrderWalReader").field(&self.0.core).finish()
  }
}

impl<P, S> Immutable for OrderWalReader<P, S> {}

impl<P, S> OrderWalReader<P, S> {
  /// Creates a new read-only WAL reader.
  #[inline]
  pub(crate) fn from_core(wal: Arc<OrderCore<P, S>>) -> Self {
    Self(OrderWal::from_core(wal))
  }
}

impl<M, S> Log for OrderWalReader<M, S>
where
  S: 'static,
  M: Memtable + 'static,
{
  type Allocator = Arena;
  type Memtable = M;
  type Checksumer = S;
  type Reader = OrderWalReader<M, S>;

  #[inline]
  fn allocator<'a>(&'a self) -> &'a Self::Allocator
  where
    Self::Allocator: 'a,
  {
    self.0.allocator()
  }

  #[inline]
  fn construct(
    arena: Self::Allocator,
    base: Self::Memtable,
    opts: crate::Options,
    checksumer: Self::Checksumer,
  ) -> Self {
    Self(OrderWal::construct(arena, base, opts, checksumer))
  }

  #[inline]
  fn options(&self) -> &crate::Options {
    self.0.options()
  }

  #[inline]
  fn memtable(&self) -> &Self::Memtable {
    self.0.memtable()
  }

  #[inline]
  fn hasher(&self) -> &Self::Checksumer {
    self.0.hasher()
  }
}
