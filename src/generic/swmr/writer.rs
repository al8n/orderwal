use {
  super::{reader::OrderWalReader, wal::OrderCore},
  crate::{
    generic::{
      memtable::{
        BaseTable, Memtable, MemtableEntry, MultipleVersionMemtable, MultipleVersionMemtableEntry,
      },
      sealed::Constructable,
    },
    WithVersion,
  },
  dbutils::{checksum::Crc32, types::Type},
  rarena_allocator::sync::Arena,
  std::sync::Arc,
};

#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
use rarena_allocator::Allocator;

/// A ordered write-ahead log implementation for concurrent thread environments.
pub struct OrderWal<K: ?Sized, V: ?Sized, M, S = Crc32> {
  pub(super) core: Arc<OrderCore<K, V, M, S>>,
}

impl<K, V, M, S> core::fmt::Debug for OrderWal<K, V, M, S>
where
  K: ?Sized,
  V: ?Sized,
{
  #[inline]
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_tuple("OrderWal").field(&self.core).finish()
  }
}

unsafe impl<K: ?Sized, V: ?Sized, M: Send, S: Send> Send for OrderWal<K, V, M, S> {}
unsafe impl<K: ?Sized, V: ?Sized, M: Send + Sync, S: Send + Sync> Sync for OrderWal<K, V, M, S> {}

impl<K: ?Sized, V: ?Sized, P, S> OrderWal<K, V, P, S> {
  #[inline]
  pub(super) const fn construct(core: Arc<OrderCore<K, V, P, S>>) -> Self {
    Self { core }
  }
}

impl<K, V, M, S> Constructable for OrderWal<K, V, M, S>
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
  type Reader = OrderWalReader<K, V, M, S>;

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

impl<K, V, M, S> OrderWal<K, V, M, S>
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

impl<K, V, M, S> crate::generic::wal::base::Writer for OrderWal<K, V, M, S>
where
  K: ?Sized + Type + Ord + 'static,
  V: ?Sized + Type + 'static,
  M: Memtable<Key = K, Value = V> + 'static,
  for<'a> M::Item<'a>: MemtableEntry<'a>,
  S: 'static,
{
  #[inline]
  fn reader(&self) -> Self::Reader {
    OrderWalReader::new(self.core.clone())
  }
}

impl<K, V, M, S> crate::generic::wal::multiple_version::Writer for OrderWal<K, V, M, S>
where
  K: ?Sized + Type + Ord + 'static,
  V: ?Sized + Type + 'static,
  M: MultipleVersionMemtable<Key = K, Value = V> + 'static,
  for<'a> M::Item<'a>: MultipleVersionMemtableEntry<'a>,
  for<'a> M::MultipleVersionEntry<'a>: WithVersion,
  for<'a> M::Item<'a>: WithVersion,
  S: 'static,
{
  #[inline]
  fn reader(&self) -> Self::Reader {
    OrderWalReader::new(self.core.clone())
  }
}
