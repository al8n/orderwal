use super::{reader::OrderWalReader, wal::OrderCore};
use crate::{
  dynamic::sealed::Constructable,
  memtable::{
    dynamic::{multiple_version, unique},
    Memtable,
  },
};
use dbutils::checksum::Crc32;
use rarena_allocator::sync::Arena;
use std::sync::Arc;

#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
use rarena_allocator::Allocator;

/// A ordered write-ahead log implementation for concurrent thread environments.
pub struct OrderWal<M, S = Crc32> {
  pub(super) core: Arc<OrderCore<M, S>>,
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
  pub(super) const fn construct(core: Arc<OrderCore<P, S>>) -> Self {
    Self { core }
  }
}

impl<M, S> Constructable for OrderWal<M, S>
where
  S: 'static,
  M: Memtable + 'static,
{
  type Allocator = Arena;
  type Wal = OrderCore<Self::Memtable, Self::Checksumer>;
  type Memtable = M;
  type Checksumer = S;
  type Reader = OrderWalReader<M, S>;

  #[inline]
  fn as_wal(&self) -> &Self::Wal {
    &self.core
  }

  #[inline]
  fn from_core(core: Self::Wal) -> Self {
    Self {
      core: Arc::new(core),
    }
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
    self.as_wal().arena.path()
  }
}

impl<M, S> crate::dynamic::wal::unique::Writer for OrderWal<M, S>
where
  M: unique::DynamicMemtable + 'static,
  S: 'static,
{
  #[inline]
  fn reader(&self) -> Self::Reader {
    OrderWalReader::new(self.core.clone())
  }
}

impl<M, S> crate::dynamic::wal::multiple_version::Writer for OrderWal<M, S>
where
  M: multiple_version::DynamicMemtable + 'static,
  S: 'static,
{
  #[inline]
  fn reader(&self) -> Self::Reader {
    OrderWalReader::new(self.core.clone())
  }
}
