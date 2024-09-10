use super::*;

/// An [`OrderWal`] reader.
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
}

impl<C: Send + 'static, S> Constructor<C, S> for OrderWalReader<C, S> {
  type Allocator = Arena;

  type Core = OrderWalCore<C, S>;

  fn from_core(core: Self::Core, _ro: bool) -> Self {
    Self(OrderWal {
      core: Arc::new(core),
      ro: true,
      _s: PhantomData,
    })
  }
}

impl<C: Send + 'static, S> ImmutableWal<C, S> for OrderWalReader<C, S> {
  type Iter<'a> = Iter<'a, C> where Self: 'a, C: Comparator;
  type Range<'a, Q, R> = Range<'a, Q, R, C>
  where
    R: core::ops::RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    Self: 'a,
    C: Comparator;
  type Keys<'a> = Keys<'a, C> where Self: 'a, C: Comparator;

  type RangeKeys<'a, Q, R> = RangeKeys<'a, Q, R, C>
      where
        R: core::ops::RangeBounds<Q>,
        [u8]: Borrow<Q>,
        Q: Ord + ?Sized,
        Self: 'a,
        C: Comparator;

  type Values<'a> = Values<'a, C> where Self: 'a, C: Comparator;

  type RangeValues<'a, Q, R> = RangeValues<'a, Q, R, C>
      where
        R: core::ops::RangeBounds<Q>,
        [u8]: Borrow<Q>,
        Q: Ord + ?Sized,
        Self: 'a,
        C: Comparator;

  fn read_only(&self) -> bool {
    self.0.read_only()
  }

  fn len(&self) -> usize {
    self.0.len()
  }

  fn maximum_key_size(&self) -> u32 {
    self.0.maximum_key_size()
  }

  fn maximum_value_size(&self) -> u32 {
    self.0.maximum_value_size()
  }

  fn contains_key<Q>(&self, key: &Q) -> bool
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    C: Comparator,
  {
    self.0.contains_key(key)
  }

  fn iter(&self) -> Self::Iter<'_>
  where
    C: Comparator,
  {
    self.0.iter()
  }

  fn range<Q, R>(&self, range: R) -> Self::Range<'_, Q, R>
  where
    R: core::ops::RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    C: Comparator,
  {
    self.0.range(range)
  }

  fn keys(&self) -> Self::Keys<'_>
  where
    C: Comparator,
  {
    self.0.keys()
  }

  fn range_keys<Q, R>(&self, range: R) -> Self::RangeKeys<'_, Q, R>
  where
    R: core::ops::RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    C: Comparator,
  {
    self.0.range_keys(range)
  }

  fn values(&self) -> Self::Values<'_>
  where
    C: Comparator,
  {
    self.0.values()
  }

  fn range_values<Q, R>(&self, range: R) -> Self::RangeValues<'_, Q, R>
  where
    R: core::ops::RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    C: Comparator,
  {
    self.0.range_values(range)
  }

  fn first(&self) -> Option<(&[u8], &[u8])>
  where
    C: Comparator,
  {
    self.0.first()
  }

  fn last(&self) -> Option<(&[u8], &[u8])>
  where
    C: Comparator,
  {
    self.0.last()
  }

  #[inline]
  fn get<Q>(&self, key: &Q) -> Option<&[u8]>
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    C: Comparator,
  {
    self.0.get(key)
  }
}
