use core::{cell::UnsafeCell, marker::PhantomData};
use std::rc::Rc;

use dbutils::{checksum::Crc32, Ascend};
use rarena_allocator::{unsync::Arena, Allocator};

use crate::pointer::MvccPointer;

use super::{pointer::Pointer, sealed::Constructor};

pub use super::{
  batch::{Batch, BatchWithBuilders, BatchWithKeyBuilder, BatchWithValueBuilder},
  builder::Builder,
  // wal::{ImmutableWal, Wal},
  Comparator,
  KeyBuilder,
  VacantBuffer,
  ValueBuilder,
};

mod c;
use c::OrderWalCore;

#[cfg(all(
  test,
  any(
    all_tests,
    test_unsync_constructor,
    test_unsync_insert,
    test_unsync_get,
    test_unsync_iters,
  )
))]
mod tests;

/// An ordered write-ahead log implementation for single thread environments.
///
/// Only the first instance of the WAL can write to the log, while the rest can only read from the log.
// ```text
// +----------------------+-------------------------+--------------------+
// | magic text (6 bytes) | magic version (2 bytes) |  header (8 bytes)  |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// |     flag (1 byte)    |    key len (4 bytes)    |    key (n bytes)   | value len (4 bytes) | value (n bytes) | checksum (8 bytes) |
// +----------------------+-------------------------+--------------------+---------------------+-----------------|--------------------+
// |     flag (1 byte)    |    key len (4 bytes)    |    key (n bytes)   | value len (4 bytes) | value (n bytes) | checksum (8 bytes) |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// |     flag (1 byte)    |    key len (4 bytes)    |    key (n bytes)   | value len (4 bytes) | value (n bytes) | checksum (8 bytes) |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// |         ...          |            ...          |         ...        |          ...        |        ...      |         ...        |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// |         ...          |            ...          |         ...        |          ...        |        ...      |         ...        |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// ```
pub struct OrderWal<P, C = Ascend, S = Crc32> {
  core: Rc<UnsafeCell<OrderWalCore<P, C, S>>>,
  _s: PhantomData<S>,
}

impl<P, C, S> Constructor<C, S> for OrderWal<P, C, S>
where
  C: 'static,
  S: 'static,
  P: 'static,
{
  type Allocator = Arena;
  type Core = OrderWalCore<P, C, S>;
  type Pointer = P;

  #[inline]
  fn as_core(&self) -> &Self::Core {
    unsafe { &*self.core.get() }
  }

  #[inline]
  fn as_core_mut(&mut self) -> &mut Self::Core {
    unsafe { &mut *self.core.get() }
  }

  #[inline]
  fn from_core(core: Self::Core) -> Self {
    Self {
      core: Rc::new(UnsafeCell::new(core)),
      _s: PhantomData,
    }
  }
}

impl<P: 'static, C: 'static, S: 'static> OrderWal<P, C, S> {
  /// Returns the path of the WAL if it is backed by a file.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{unsync::OrderWal, Wal, Builder};
  ///
  /// // A in-memory WAL
  /// let wal = Builder::new().with_capacity(100).alloc::<OrderWal>().unwrap();
  ///
  /// assert!(wal.path_buf().is_none());
  /// ```
  pub fn path_buf(&self) -> Option<&std::rc::Rc<std::path::PathBuf>> {
    self.as_core().arena.path()
  }
}

impl<C, S> super::wal::Wal<C, S> for OrderWal<Pointer<C>, C, S>
where
  C: 'static,
  S: 'static,
{
  type Reader = Self;

  #[inline]
  fn reader(&self) -> Self::Reader {
    Self {
      core: {
        let core = self.core.clone();
        unsafe {
          (*core.get()).ro = true;
        }
        core
      },
      _s: PhantomData,
    }
  }
}

impl<C, S> super::mvcc::Wal<C, S> for OrderWal<MvccPointer<C>, C, S>
where
  C: 'static,
  S: 'static,
{
  type Reader = Self;

  #[inline]
  fn reader(&self) -> Self::Reader {
    Self {
      core: {
        let core = self.core.clone();
        unsafe {
          (*core.get()).ro = true;
        }
        core
      },
      _s: PhantomData,
    }
  }
}
