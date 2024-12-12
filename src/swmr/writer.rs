use super::{reader::OrderWalReader, wal::OrderCore};
use crate::{log::Log, memtable::Memtable};
use dbutils::checksum::Crc32;
use rarena_allocator::sync::Arena;
use triomphe::Arc;

#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
use rarena_allocator::Allocator;

/// A ordered write-ahead log implementation for concurrent thread environments.
pub struct OrderWal<M, S = Crc32> {
  pub(crate) core: Arc<OrderCore<M, S>>,
}

impl<M, S> core::fmt::Debug for OrderWal<M, S> {
  #[inline]
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_tuple("OrderWal").field(&self.core).finish()
  }
}

unsafe impl<M: Send, S: Send> Send for OrderWal<M, S> {}
unsafe impl<M: Send + Sync, S: Send + Sync> Sync for OrderWal<M, S> {}

impl<P, S> OrderWal<P, S> {
  #[inline]
  pub(super) const fn from_core(core: Arc<OrderCore<P, S>>) -> Self {
    Self { core }
  }
}

impl<M, S> Log for OrderWal<M, S>
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
    &self.core.arena
  }

  #[inline]
  fn construct(
    arena: Self::Allocator,
    base: Self::Memtable,
    opts: crate::Options,
    checksumer: Self::Checksumer,
  ) -> Self {
    Self {
      core: Arc::new(OrderCore::construct(arena, base, opts, checksumer)),
    }
  }

  #[inline]
  fn options(&self) -> &crate::Options {
    &self.core.opts
  }

  #[inline]
  fn memtable(&self) -> &Self::Memtable {
    &self.core.map
  }

  #[inline]
  fn hasher(&self) -> &Self::Checksumer {
    &self.core.cks
  }
}

impl<M, S> OrderWal<M, S>
where
  S: 'static,
  M: Memtable + 'static,
{
  /// Returns the path of the WAL if it is backed by a file.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{base::OrderWal, Builder};
  ///
  /// // A in-memory WAL
  /// let wal = Builder::new().with_capacity(100).alloc::<OrderWal<[u8], [u8]>>().unwrap();
  ///
  /// assert!(wal.path_buf().is_none());
  /// ```
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "std", not(target_family = "wasm")))))]
  #[inline]
  pub fn path_buf(&self) -> Option<&std::sync::Arc<std::path::PathBuf>> {
    self.core.arena.path()
  }
}
