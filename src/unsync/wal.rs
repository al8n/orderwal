use core::{cell::UnsafeCell, marker::PhantomData};
use std::rc::Rc;

use dbutils::{checksum::Crc32, Ascend};
use rarena_allocator::{unsync::Arena, Allocator};

use crate::{pointer::{Pointer, VersionPointer}, sealed::Constructable};

use super::c::OrderCore;

/// An ordered write-ahead log implementation for single thread environments.
pub struct OrderWal<P, C = Ascend, S = Crc32> {
  core: Rc<UnsafeCell<OrderCore<P, C, S>>>,
  _s: PhantomData<S>,
}

impl<P, C, S> Constructable<C, S> for OrderWal<P, C, S>
where
  C: 'static,
  S: 'static,
  P: 'static,
{
  type Allocator = Arena;
  type Core = OrderCore<P, C, S>;
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

impl<C, S> crate::base::Writer<C, S> for OrderWal<Pointer<C>, C, S>
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

impl<C, S> crate::mvcc::Writer<C, S> for OrderWal<VersionPointer<C>, C, S>
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
