use core::{marker::PhantomData, ops::Bound};

use skl::either::Either;

use crate::{
  memtable::Memtable,
  types::{EncodedEntryMeta, EncodedRangeEntryMeta, EntryFlags, RecordPointer},
};

pub(crate) enum Data<K, V> {
  InsertPoint {
    key: K,
    value: V,
    meta: EncodedEntryMeta,
  },
  RemovePoint {
    key: K,
    meta: EncodedEntryMeta,
  },
  RangeRemove {
    start_bound: Bound<K>,
    end_bound: Bound<K>,
    meta: EncodedRangeEntryMeta,
  },
  RangeUnset {
    start_bound: Bound<K>,
    end_bound: Bound<K>,
    meta: EncodedRangeEntryMeta,
  },
  RangeSet {
    start_bound: Bound<K>,
    end_bound: Bound<K>,
    value: V,
    meta: EncodedRangeEntryMeta,
  },
}

/// An entry can be inserted into the WALs through [`Batch`].
pub struct BatchEntry<K, V, M: Memtable> {
  pub(crate) data: Data<K, V>,
  pub(crate) flag: EntryFlags,
  pointers: Option<RecordPointer>,
  pub(crate) version: u64,
  _m: PhantomData<M>,
}

impl<K, V, M> BatchEntry<K, V, M>
where
  M: Memtable,
{
  /// Creates a new entry with version.
  #[inline]
  pub fn insert(version: u64, key: K, value: V) -> Self {
    Self {
      data: Data::InsertPoint {
        key,
        value,
        meta: EncodedEntryMeta::placeholder(),
      },
      flag: EntryFlags::empty(),
      pointers: None,
      version,
      _m: PhantomData,
    }
  }

  /// Creates a tombstone entry with version.
  #[inline]
  pub fn remove(version: u64, key: K) -> Self {
    Self {
      data: Data::RemovePoint {
        key,
        meta: EncodedEntryMeta::placeholder(),
      },
      flag: EntryFlags::REMOVED,
      pointers: None,
      version,
      _m: PhantomData,
    }
  }

  /// Creates a range remove entry with version.
  #[inline]
  pub fn range_remove(version: u64, start_bound: Bound<K>, end_bound: Bound<K>) -> Self {
    Self {
      data: Data::RangeRemove {
        start_bound,
        end_bound,
        meta: EncodedRangeEntryMeta::placeholder(),
      },
      flag: EntryFlags::RANGE_DELETION,
      pointers: None,
      version,
      _m: PhantomData,
    }
  }

  /// Creates a range unset entry with version.
  #[inline]
  pub fn range_unset(version: u64, start_bound: Bound<K>, end_bound: Bound<K>) -> Self {
    Self {
      data: Data::RangeUnset {
        start_bound,
        end_bound,
        meta: EncodedRangeEntryMeta::placeholder(),
      },
      flag: EntryFlags::RANGE_UNSET,
      pointers: None,
      version,
      _m: PhantomData,
    }
  }

  /// Creates a range set entry with version.
  #[inline]
  pub fn range_set(version: u64, start_bound: Bound<K>, end_bound: Bound<K>, value: V) -> Self {
    Self {
      data: Data::RangeSet {
        start_bound,
        end_bound,
        value,
        meta: EncodedRangeEntryMeta::placeholder(),
      },
      flag: EntryFlags::RANGE_SET,
      pointers: None,
      version,
      _m: PhantomData,
    }
  }

  /// Returns the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }

  /// Set the version of the entry.
  #[inline]
  pub fn set_version(&mut self, version: u64) {
    self.version = version;
  }
}

impl<K, V, M> BatchEntry<K, V, M>
where
  M: Memtable,
{
  /// Returns the key.
  #[inline]
  pub const fn key(&self) -> &K {
    match &self.data {
      Data::InsertPoint { key, .. } | Data::RemovePoint { key, .. } => key,
      Data::RangeRemove { .. } | Data::RangeUnset { .. } | Data::RangeSet { .. } => {
        panic!("try to get key from range entry")
      }
    }
  }

  /// Returns the range key.
  #[inline]
  pub fn bounds(&self) -> (Bound<&K>, Bound<&K>) {
    match &self.data {
      Data::InsertPoint { .. } | Data::RemovePoint { .. } => {
        panic!("try to get range key from point entry")
      }
      Data::RangeRemove {
        start_bound,
        end_bound,
        ..
      }
      | Data::RangeUnset {
        start_bound,
        end_bound,
        ..
      }
      | Data::RangeSet {
        start_bound,
        end_bound,
        ..
      } => (start_bound.as_ref(), end_bound.as_ref()),
    }
  }

  /// Returns the value.
  #[inline]
  pub const fn value(&self) -> Option<&V> {
    match &self.data {
      Data::InsertPoint { value, .. } => Some(value),
      Data::RemovePoint { .. } => None,
      Data::RangeRemove { .. } | Data::RangeUnset { .. } => None,
      Data::RangeSet { value, .. } => Some(value),
    }
  }

  /// Consumes the entry and returns the key and value.
  #[inline]
  pub fn into_components(self) -> (Either<K, (Bound<K>, Bound<K>)>, Option<V>) {
    match self.data {
      Data::InsertPoint { key, value, .. } => (Either::Left(key), Some(value)),
      Data::RemovePoint { key, .. } => (Either::Left(key), None),
      Data::RangeRemove {
        start_bound,
        end_bound,
        ..
      }
      | Data::RangeUnset {
        start_bound,
        end_bound,
        ..
      } => (Either::Right((start_bound, end_bound)), None),
      Data::RangeSet {
        start_bound,
        end_bound,
        value,
        ..
      } => (Either::Right((start_bound, end_bound)), Some(value)),
    }
  }

  #[inline]
  pub(crate) const fn internal_version(&self) -> u64 {
    self.version
  }

  #[inline]
  pub(crate) fn take_pointer(&mut self) -> Option<RecordPointer> {
    self.pointers.take()
  }

  #[inline]
  pub(crate) fn set_pointer(&mut self, p: RecordPointer) {
    self.pointers = Some(p);
  }

  #[inline]
  pub(crate) fn encoded_meta(&self) -> Either<&EncodedEntryMeta, &EncodedRangeEntryMeta> {
    match &self.data {
      Data::InsertPoint { meta, .. } | Data::RemovePoint { meta, .. } => Either::Left(meta),
      Data::RangeRemove { meta, .. }
      | Data::RangeUnset { meta, .. }
      | Data::RangeSet { meta, .. } => Either::Right(meta),
    }
  }
}

/// A trait for batch insertions.
pub trait Batch<M: Memtable> {
  /// Any type that can be converted into a key.
  type Key;
  /// Any type that can be converted into a value.
  type Value;

  /// The iterator type.
  type IterMut<'a>: Iterator<Item = &'a mut BatchEntry<Self::Key, Self::Value, M>>
  where
    Self: 'a,
    Self::Key: 'a,
    Self::Value: 'a,
    M: 'a;

  /// Returns an iterator over the keys and values.
  fn iter_mut<'a>(&'a mut self) -> Self::IterMut<'a>
  where
    Self: 'a,
    Self::Key: 'a,
    Self::Value: 'a,
    M: 'a;
}

impl<K, V, M, T> Batch<M> for T
where
  M: Memtable,
  for<'a> &'a mut T: IntoIterator<Item = &'a mut BatchEntry<K, V, M>>,
{
  type Key = K;
  type Value = V;

  type IterMut<'a>
    = <&'a mut T as IntoIterator>::IntoIter
  where
    Self: 'a,
    Self::Key: 'a,
    Self::Value: 'a,
    M: 'a;

  fn iter_mut<'a>(&'a mut self) -> Self::IterMut<'a>
  where
    Self: 'a,
    Self::Key: 'a,
    Self::Value: 'a,
    M: 'a,
  {
    IntoIterator::into_iter(self)
  }
}
