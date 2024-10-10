use super::*;

/// An [`OrderWal`] reader.
pub struct OrderWalReader<P, C, S>(OrderWal<P, C, S>);

impl<P, C, S> OrderWalReader<P, C, S> {
  /// Creates a new read-only WAL reader.
  #[inline]
  pub(super) fn new(wal: Arc<UnsafeCell<OrderCore<P, C, S>>>) -> Self {
    Self(OrderWal {
      core: wal.clone(),
      _s: PhantomData,
    })
  }
}

impl<P, C, S> Constructable<C, S> for OrderWalReader<P, C, S>
where
  C: 'static,
  S: 'static,
  P: Ord + Send + 'static,
{
  type Allocator = Arena;
  type Core = OrderCore<P, C, S>;
  type Pointer = P;

  #[inline]
  fn as_core(&self) -> &Self::Core {
    self.0.as_core()
  }

  #[inline]
  fn as_core_mut(&mut self) -> &mut Self::Core {
    self.0.as_core_mut()
  }

  #[inline]
  fn from_core(core: Self::Core) -> Self {
    Self(OrderWal {
      core: Arc::new(UnsafeCell::new(core)),
      _s: PhantomData,
    })
  }
}
