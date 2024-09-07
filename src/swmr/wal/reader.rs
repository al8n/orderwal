use super::*;

pub struct OrderWalReader<C, S>(OrderWal<C, S>);

impl<C, S> OrderWalReader<C, S> {
  /// Creates a new read-only WAL reader.
  #[inline]
  pub(super) fn new(wal: Arc<OrderWalCore<C, S>>) -> Self {
    Self(OrderWal {
      core: wal.clone(),
      ro: true,
      _s: PhantomData,
    })
  }

  /// Returns number of entries in the WAL.
  #[inline]
  pub fn len(&self) -> usize {
    self.0.len()
  }

  /// Returns `true` if the WAL is empty.
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.0.is_empty()
  }
}

impl<C: Comparator, S> OrderWalReader<C, S> {
  /// Returns `true` if the WAL contains the specified key.
  #[inline]
  pub fn contains_key<Q>(&self, key: &Q) -> bool
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
  {
    self.0.contains_key(key)
  }

  /// Returns the value associated with the key.
  #[inline]
  pub fn get<Q>(&self, key: &Q) -> Option<&[u8]>
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
  {
    self.0.get(key)
  }
}
