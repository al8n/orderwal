use super::*;

/// An [`OrderWal`] reader.
pub struct OrderWalReader<C, S>(OrderWal<C, S>);

impl<C, S> OrderWalReader<C, S> {
  /// Creates a new read-only WAL reader.
  #[inline]
  pub(super) fn new(wal: Arc<OrderWalCore<C, S>>) -> Self {
    Self(OrderWal {
      core: wal.clone(),
      _s: PhantomData,
    })
  }
}

impl<C, S> Constructor<C, S> for OrderWalReader<C, S>
where
  C: Comparator + CheapClone + Send + 'static,
{
  type Allocator = Arena;
  type Core = OrderWalCore<C, S>;
  type Pointer = Pointer<C>;

  #[inline]
  fn allocator(&self) -> &Self::Allocator {
    self.0.allocator()
  }

  fn from_core(core: Self::Core) -> Self {
    Self(OrderWal {
      core: Arc::new(core),
      _s: PhantomData,
    })
  }
}

impl<C, S> ImmutableWal<C, S> for OrderWalReader<C, S>
where
  C: Comparator + CheapClone + Send + 'static,
{
  type Iter<'a>
    = Iter<'a, C>
  where
    Self: 'a,
    C: Comparator;
  type Range<'a, Q, R>
    = Range<'a, Q, R, C>
  where
    R: core::ops::RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    Self: 'a,
    C: Comparator;
  type Keys<'a>
    = Keys<'a, C>
  where
    Self: 'a,
    C: Comparator;

  type RangeKeys<'a, Q, R>
    = RangeKeys<'a, Q, R, C>
  where
    R: core::ops::RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    Self: 'a,
    C: Comparator;

  type Values<'a>
    = Values<'a, C>
  where
    Self: 'a,
    C: Comparator;

  type RangeValues<'a, Q, R>
    = RangeValues<'a, Q, R, C>
  where
    R: core::ops::RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    Self: 'a,
    C: Comparator;

  #[inline]
  fn path(&self) -> Option<&std::path::Path> {
    self.0.path()
  }

  #[inline]
  fn len(&self) -> usize {
    self.0.len()
  }

  #[inline]
  fn options(&self) -> &Options {
    ImmutableWal::options(&self.0)
  }

  #[inline]
  fn contains_key<Q>(&self, key: &Q) -> bool
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    C: Comparator,
  {
    self.0.contains_key(key)
  }

  #[inline]
  fn iter(&self) -> Self::Iter<'_>
  where
    C: Comparator,
  {
    self.0.iter()
  }

  #[inline]
  fn range<Q, R>(&self, range: R) -> Self::Range<'_, Q, R>
  where
    R: core::ops::RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    C: Comparator,
  {
    self.0.range(range)
  }

  #[inline]
  fn keys(&self) -> Self::Keys<'_>
  where
    C: Comparator,
  {
    self.0.keys()
  }

  #[inline]
  fn range_keys<Q, R>(&self, range: R) -> Self::RangeKeys<'_, Q, R>
  where
    R: core::ops::RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    C: Comparator,
  {
    self.0.range_keys(range)
  }

  #[inline]
  fn values(&self) -> Self::Values<'_>
  where
    C: Comparator,
  {
    self.0.values()
  }

  #[inline]
  fn range_values<Q, R>(&self, range: R) -> Self::RangeValues<'_, Q, R>
  where
    R: core::ops::RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    C: Comparator,
  {
    self.0.range_values(range)
  }

  #[inline]
  fn first(&self) -> Option<(&[u8], &[u8])>
  where
    C: Comparator,
  {
    self.0.first()
  }

  #[inline]
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
