use super::*;

/// A read-only view of a generic single-writer, multi-reader WAL.
pub struct GenericWalReader<K, V>(Arc<GenericOrderWalCore<K, V>>);

impl<K, V> Clone for GenericWalReader<K, V> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<K, V> GenericWalReader<K, V> {
  pub(super) fn new(wal: Arc<GenericOrderWalCore<K, V>>) -> Self {
    Self(wal)
  }

  /// Returns the path of the WAL if it is backed by a file.
  #[inline]
  pub fn path(&self) -> Option<&std::sync::Arc<std::path::PathBuf>> {
    self.0.arena.path()
  }

  /// Returns the reserved space in the WAL.
  ///
  /// # Safety
  /// - The writer must ensure that the returned slice is not modified.
  /// - This method is not thread-safe, so be careful when using it.
  #[inline]
  pub unsafe fn reserved_slice(&self) -> &[u8] {
    if self.0.reserved == 0 {
      return &[];
    }

    &self.0.arena.reserved_slice()[HEADER_SIZE..]
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

impl<K, V> GenericWalReader<K, V>
where
  K: Type + Ord,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
{
  /// Returns the first key-value pair in the map. The key in this pair is the minimum key in the wal.
  #[inline]
  pub fn first(&self) -> Option<EntryRef<K, V>> {
    self.0.first()
  }

  /// Returns the last key-value pair in the map. The key in this pair is the maximum key in the wal.
  #[inline]
  pub fn last(&self) -> Option<EntryRef<K, V>> {
    self.0.last()
  }

  /// Returns an iterator over the entries in the WAL.
  #[inline]
  pub fn iter(&self) -> Iter<K, V> {
    self.0.iter()
  }

  /// Returns an iterator over a subset of the entries in the WAL.
  #[inline]
  pub fn range_by_ref<'a, Q>(
    &'a self,
    start_bound: Bound<&'a Q>,
    end_bound: Bound<&'a Q>,
  ) -> RefRange<'a, Q, K, V>
  where
    Q: Ord + ?Sized + Comparable<K::Ref<'a>>,
  {
    self.0.range_by_ref(start_bound, end_bound)
  }

  /// Returns an iterator over a subset of the entries in the WAL.
  #[inline]
  pub fn range<'a, Q>(
    &'a self,
    start_bound: Bound<&'a Q>,
    end_bound: Bound<&'a Q>,
  ) -> Range<'a, Q, K, V>
  where
    Q: Ord + ?Sized + Comparable<K> + Comparable<K::Ref<'a>>,
  {
    self.0.range(start_bound, end_bound)
  }
}

impl<K, V> GenericWalReader<K, V>
where
  K: Type + Ord,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  V: Type,
{
  /// Returns `true` if the key exists in the WAL.
  #[inline]
  pub fn contains_key<'a, 'b: 'a, Q>(&'a self, key: &'b Q) -> bool
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>> + Comparable<K>,
  {
    self.0.contains_key(key)
  }

  /// Returns `true` if the key exists in the WAL.
  #[inline]
  pub fn contains_key_by_ref<'a, 'b: 'a, Q>(&'a self, key: &'b Q) -> bool
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  {
    self.0.contains_key_by_ref(key)
  }

  /// Gets the value associated with the key.
  #[inline]
  pub fn get<'a, 'b: 'a, Q>(&'a self, key: &'b Q) -> Option<EntryRef<'a, K, V>>
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>> + Comparable<K>,
  {
    self.0.get(key)
  }

  /// Gets the value associated with the key.
  #[inline]
  pub fn get_by_ref<'a, 'b: 'a, Q>(&'a self, key: &'b Q) -> Option<EntryRef<'a, K, V>>
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  {
    self.0.get_by_ref(key)
  }
}
