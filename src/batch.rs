use crate::{
  sealed::{EntryFlags, WithVersion, WithoutVersion},
  VERSION_SIZE,
};

use super::entry::BufWriter;

pub(crate) struct EncodedBatchEntryMeta {
  /// The output of `merge_lengths(klen, vlen)`
  pub(crate) kvlen: u64,
  /// the length of `encoded_u64_varint(merge_lengths(klen, vlen))`
  pub(crate) kvlen_size: usize,
  /// The key length, including version if present.
  pub(crate) klen: usize,
  pub(crate) vlen: usize,
}

impl EncodedBatchEntryMeta {
  #[inline]
  pub(crate) const fn new(klen: usize, vlen: usize, kvlen: u64, kvlen_size: usize) -> Self {
    Self {
      klen,
      vlen,
      kvlen,
      kvlen_size,
    }
  }

  #[inline]
  pub(crate) const fn zero() -> Self {
    Self {
      klen: 0,
      vlen: 0,
      kvlen: 0,
      kvlen_size: 0,
    }
  }
}

/// An entry can be inserted into the WALs through [`Batch`](super::batch::Batch).
pub struct BatchEntry<K, V, P> {
  pub(crate) key: K,
  pub(crate) value: Option<V>,
  pub(crate) flag: EntryFlags,
  pub(crate) meta: EncodedBatchEntryMeta,
  pointer: Option<P>,
  version: Option<u64>,
}

impl<K, V, P> BatchEntry<K, V, P>
where
  P: WithoutVersion,
{
  /// Creates a new entry.
  #[inline]
  pub const fn new(key: K, value: V) -> Self {
    Self {
      key,
      value: Some(value),
      flag: EntryFlags::empty(),
      meta: EncodedBatchEntryMeta::zero(),
      pointer: None,
      version: None,
    }
  }

  /// Creates a tombstone entry.
  #[inline]
  pub const fn tombstone(key: K) -> Self {
    Self {
      key,
      value: None,
      flag: EntryFlags::REMOVED,
      meta: EncodedBatchEntryMeta::zero(),
      pointer: None,
      version: None,
    }
  }
}

impl<K, V, P> BatchEntry<K, V, P>
where
  P: WithVersion,
{
  /// Creates a new entry with version.
  #[inline]
  pub fn with_version(version: u64, key: K, value: V) -> Self {
    Self {
      key,
      value: Some(value),
      flag: EntryFlags::empty(),
      meta: EncodedBatchEntryMeta::zero(),
      pointer: None,
      version: Some(version),
    }
  }

  /// Creates a tombstone entry with version.
  #[inline]
  pub fn tombstone_with_version(version: u64, key: K) -> Self {
    Self {
      key,
      value: None,
      flag: EntryFlags::REMOVED,
      meta: EncodedBatchEntryMeta::zero(),
      pointer: None,
      version: Some(version),
    }
  }

  /// Returns the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    match self.version {
      Some(version) => version,
      None => unreachable!(),
    }
  }

  /// Set the version of the entry.
  #[inline]
  pub fn set_version(&mut self, version: u64) {
    self.version = Some(version);
  }
}

impl<K, V, P> BatchEntry<K, V, P> {
  /// Returns the length of the key.
  #[inline]
  pub fn key_len(&self) -> usize
  where
    K: BufWriter,
  {
    self.key.len()
  }

  /// Returns the length of the value.
  #[inline]
  pub fn value_len(&self) -> usize
  where
    V: BufWriter,
  {
    self.value.as_ref().map_or(0, |v| v.len())
  }

  /// Returns the key.
  #[inline]
  pub const fn key(&self) -> &K {
    &self.key
  }

  /// Returns the value.
  #[inline]
  pub const fn value(&self) -> Option<&V> {
    self.value.as_ref()
  }

  /// Consumes the entry and returns the key and value.
  #[inline]
  pub fn into_components(self) -> (K, Option<V>) {
    (self.key, self.value)
  }

  #[inline]
  pub(crate) fn encoded_key_len(&self) -> usize
  where
    K: BufWriter,
    V: BufWriter,
  {
    // 1 for entry flag
    1 + match self.version {
      Some(_) => self.key.len() + VERSION_SIZE,
      None => self.key.len(),
    }
  }

  #[inline]
  pub(crate) const fn internal_version(&self) -> Option<u64> {
    self.version
  }

  #[inline]
  pub(crate) fn take_pointer(&mut self) -> Option<P> {
    self.pointer.take()
  }

  #[inline]
  pub(crate) fn set_pointer(&mut self, pointer: P) {
    self.pointer = Some(pointer);
  }

  #[inline]
  pub(crate) fn set_encoded_meta(&mut self, meta: EncodedBatchEntryMeta) {
    self.meta = meta;
  }

  #[inline]
  pub(crate) fn encoded_meta(&self) -> &EncodedBatchEntryMeta {
    &self.meta
  }
}

/// A trait for batch insertions.
pub trait Batch<P> {
  /// Any type that can be converted into a key.
  type Key;
  /// Any type that can be converted into a value.
  type Value;

  /// The iterator type.
  type IterMut<'a>: Iterator<Item = &'a mut BatchEntry<Self::Key, Self::Value, P>>
  where
    Self: 'a,
    Self::Key: 'a,
    Self::Value: 'a,
    P: 'a;

  /// Returns an iterator over the keys and values.
  fn iter_mut<'a>(&'a mut self) -> Self::IterMut<'a>
  where
    Self: 'a,
    Self::Key: 'a,
    Self::Value: 'a,
    P: 'a;
}

impl<K, V, P, T> Batch<P> for T
where
  for<'a> &'a mut T: IntoIterator<Item = &'a mut BatchEntry<K, V, P>>,
{
  type Key = K;
  type Value = V;

  type IterMut<'a>
    = <&'a mut T as IntoIterator>::IntoIter
  where
    Self: 'a,
    Self::Key: 'a,
    Self::Value: 'a,
    P: 'a;

  fn iter_mut<'a>(&'a mut self) -> Self::IterMut<'a>
  where
    Self: 'a,
    Self::Key: 'a,
    Self::Value: 'a,
    P: 'a,
  {
    IntoIterator::into_iter(self)
  }
}
