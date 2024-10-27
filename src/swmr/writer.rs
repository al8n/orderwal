use crate::{
  memtable::{BaseTable, Memtable, MultipleVersionMemtable},
  sealed::{Constructable, WithVersion},
};
use dbutils::{checksum::Crc32, traits::Type};
use rarena_allocator::{sync::Arena, Allocator};

use std::sync::Arc;

use super::{reader::GenericOrderWalReader, wal::OrderCore};

/// A ordered write-ahead log implementation for concurrent thread environments.
pub struct GenericOrderWal<K: ?Sized, V: ?Sized, M, S = Crc32> {
  pub(super) core: Arc<OrderCore<K, V, M, S>>,
}

unsafe impl<K: ?Sized, V: ?Sized, M: Send, S: Send> Send for GenericOrderWal<K, V, M, S> {}
unsafe impl<K: ?Sized, V: ?Sized, M: Send + Sync, S: Send + Sync> Sync
  for GenericOrderWal<K, V, M, S>
{
}

impl<K: ?Sized, V: ?Sized, P, S> GenericOrderWal<K, V, P, S> {
  #[inline]
  pub(super) const fn construct(core: Arc<OrderCore<K, V, P, S>>) -> Self {
    Self { core }
  }
}

impl<K, V, M, S> Constructable for GenericOrderWal<K, V, M, S>
where
  K: ?Sized + 'static,
  V: ?Sized + 'static,
  S: 'static,
  M: BaseTable<Key = K, Value = V> + 'static,
{
  type Allocator = Arena;
  type Wal = OrderCore<K, V, Self::Memtable, Self::Checksumer>;
  type Memtable = M;
  type Checksumer = S;
  type Reader = GenericOrderWalReader<K, V, M, S>;

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

impl<K, V, M, S> GenericOrderWal<K, V, M, S>
where
  K: ?Sized + 'static,
  V: ?Sized + 'static,
  S: 'static,
  M: BaseTable<Key = K, Value = V> + 'static,
{
  /// Returns the path of the WAL if it is backed by a file.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{base::GenericOrderWal, Builder};
  ///
  /// // A in-memory WAL
  /// let wal = Builder::new().with_capacity(100).alloc::<[u8], [u8], GenericOrderWal<_, _>>().unwrap();
  ///
  /// assert!(wal.path_buf().is_none());
  /// ```
  pub fn path_buf(&self) -> Option<&std::sync::Arc<std::path::PathBuf>> {
    self.as_wal().arena.path()
  }
}

impl<K, V, M, S> crate::wal::base::Writer<K, V> for GenericOrderWal<K, V, M, S>
where
  K: ?Sized + Type + Ord + 'static,
  V: ?Sized + Type + 'static,
  M: Memtable<Key = K, Value = V> + 'static,
  S: 'static,
{
  #[inline]
  fn reader(&self) -> Self::Reader {
    GenericOrderWalReader::new(self.core.clone())
  }
}

impl<K, V, M, S> crate::wal::multiple_version::Writer<K, V> for GenericOrderWal<K, V, M, S>
where
  K: ?Sized + Type + Ord + 'static,
  V: ?Sized + Type + 'static,
  M: MultipleVersionMemtable<Key = K, Value = V> + 'static,
  for<'a> M::MultipleVersionItem<'a>: WithVersion,
  for<'a> M::Item<'a>: WithVersion,
  S: 'static,
{
  #[inline]
  fn reader(&self) -> Self::Reader {
    GenericOrderWalReader::new(self.core.clone())
  }
}
