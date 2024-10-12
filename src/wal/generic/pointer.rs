use core::{cmp, marker::PhantomData, slice};

use dbutils::{
  traits::{KeyRef, Type},
  CheapClone,
};

use crate::{
  sealed::{Pointer, WithVersion, WithoutVersion},
  VERSION_SIZE,
};

use super::GenericComparator;

#[doc(hidden)]
#[derive(Debug)]
pub struct GenericPointer<K: ?Sized, V: ?Sized> {
  /// The pointer to the start of the entry.
  ptr: *const u8,
  /// The length of the key.
  key_len: usize,
  /// The length of the value.
  value_len: usize,
  _m: PhantomData<(fn() -> K, fn() -> V)>,
}

impl<K: ?Sized, V: ?Sized> Clone for GenericPointer<K, V> {
  fn clone(&self) -> Self {
    *self
  }
}

impl<K: ?Sized, V: ?Sized> Copy for GenericPointer<K, V> {}

impl<K: ?Sized, V: ?Sized> CheapClone for GenericPointer<K, V> {
  #[inline]
  fn cheap_clone(&self) -> Self {
    *self
  }
}

impl<K: ?Sized, V: ?Sized> crate::sealed::Pointer for GenericPointer<K, V> {
  type Comparator = GenericComparator<K>;

  #[inline]
  fn new(klen: usize, vlen: usize, ptr: *const u8, _cmp: Self::Comparator) -> Self {
    Self::new(klen, vlen, ptr)
  }

  #[inline]
  fn as_key_slice<'a>(&self) -> &'a [u8] {
    if self.key_len == 0 {
      return &[];
    }

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    unsafe { slice::from_raw_parts(self.ptr, self.key_len) }
  }

  #[inline]
  fn as_value_slice<'a>(&self) -> &'a [u8] {
    if self.value_len == 0 {
      return &[];
    }

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    unsafe { slice::from_raw_parts(self.ptr.add(self.key_len), self.value_len) }
  }

  #[inline]
  fn version(&self) -> u64 {
    0
  }
}

impl<K: Type + ?Sized, V: ?Sized> PartialEq for GenericPointer<K, V> {
  fn eq(&self, other: &Self) -> bool {
    self.as_key_slice() == other.as_key_slice()
  }
}

impl<K: Type + ?Sized, V: ?Sized> Eq for GenericPointer<K, V> {}

impl<K, V> PartialOrd for GenericPointer<K, V>
where
  K: Type + Ord + ?Sized,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  V: ?Sized,
{
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<K, V> Ord for GenericPointer<K, V>
where
  K: Type + Ord + ?Sized,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  V: ?Sized,
{
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    // SAFETY: WALs guarantee that the self and other must be the same as the result returned by `<K as Type>::encode`.
    unsafe { <K::Ref<'_> as KeyRef<K>>::compare_binary(self.as_key_slice(), other.as_key_slice()) }
  }
}

unsafe impl<K, V> Send for GenericPointer<K, V>
where
  K: ?Sized,
  V: ?Sized,
{
}
unsafe impl<K, V> Sync for GenericPointer<K, V>
where
  K: ?Sized,
  V: ?Sized,
{
}

impl<K, V> GenericPointer<K, V>
where
  K: ?Sized,
  V: ?Sized,
{
  #[inline]
  pub(crate) const fn new(key_len: usize, value_len: usize, ptr: *const u8) -> Self {
    Self {
      ptr,
      key_len,
      value_len,
      _m: PhantomData,
    }
  }
}

#[doc(hidden)]
#[derive(Debug)]
pub struct GenericVersionPointer<K: ?Sized, V: ?Sized> {
  /// The pointer to the start of the entry.
  ptr: *const u8,
  /// The length of the key.
  key_len: usize,
  /// The length of the value.
  value_len: usize,
  _m: PhantomData<(fn() -> K, fn() -> V)>,
}

impl<K: ?Sized, V: ?Sized> Clone for GenericVersionPointer<K, V> {
  #[inline]
  fn clone(&self) -> Self {
    *self
  }
}

impl<K: ?Sized, V: ?Sized> Copy for GenericVersionPointer<K, V> {}

impl<K: ?Sized, V: ?Sized> CheapClone for GenericVersionPointer<K, V> {
  #[inline]
  fn cheap_clone(&self) -> Self {
    *self
  }
}

impl<K: ?Sized, V: ?Sized> crate::sealed::Pointer for GenericVersionPointer<K, V> {
  type Comparator = GenericComparator<K>;

  #[inline]
  fn new(klen: usize, vlen: usize, ptr: *const u8, _cmp: Self::Comparator) -> Self {
    Self::new(klen, vlen, ptr)
  }

  #[inline]
  fn as_key_slice<'a>(&self) -> &'a [u8] {
    if self.key_len == 0 {
      return &[];
    }

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    unsafe { slice::from_raw_parts(self.ptr.add(VERSION_SIZE), self.key_len) }
  }

  #[inline]
  fn as_value_slice<'a>(&self) -> &'a [u8] {
    if self.value_len == 0 {
      return &[];
    }

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    unsafe { slice::from_raw_parts(self.ptr.add(VERSION_SIZE + self.key_len), self.value_len) }
  }

  #[inline]
  fn version(&self) -> u64 {
    unsafe {
      let slice = slice::from_raw_parts(self.ptr, VERSION_SIZE);
      u64::from_le_bytes(slice.try_into().unwrap())
    }
  }
}

impl<K: Type + ?Sized, V: ?Sized> PartialEq for GenericVersionPointer<K, V> {
  fn eq(&self, other: &Self) -> bool {
    self.as_key_slice() == other.as_key_slice() && self.version() == other.version()
  }
}

impl<K: Type + ?Sized, V: ?Sized> Eq for GenericVersionPointer<K, V> {}

impl<K, V> PartialOrd for GenericVersionPointer<K, V>
where
  K: Type + Ord + ?Sized,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  V: ?Sized,
{
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<K, V> Ord for GenericVersionPointer<K, V>
where
  K: Type + Ord + ?Sized,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  V: ?Sized,
{
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    // SAFETY: WALs guarantee that the self and other must be the same as the result returned by `<K as Type>::encode`.
    unsafe {
      <K::Ref<'_> as KeyRef<K>>::compare_binary(self.as_key_slice(), other.as_key_slice())
        .then_with(|| other.version().cmp(&self.version()))
    }
  }
}

unsafe impl<K, V> Send for GenericVersionPointer<K, V>
where
  K: ?Sized,
  V: ?Sized,
{
}
unsafe impl<K, V> Sync for GenericVersionPointer<K, V>
where
  K: ?Sized,
  V: ?Sized,
{
}

impl<K, V> GenericVersionPointer<K, V>
where
  K: ?Sized,
  V: ?Sized,
{
  #[inline]
  pub(crate) const fn new(key_len: usize, value_len: usize, ptr: *const u8) -> Self {
    Self {
      ptr,
      key_len: key_len - VERSION_SIZE,
      value_len,
      _m: PhantomData,
    }
  }
}

impl<K: ?Sized, V: ?Sized> WithVersion for GenericVersionPointer<K, V> {}
impl<K: ?Sized, V: ?Sized> crate::sealed::GenericPointer<K, V> for GenericVersionPointer<K, V> {}
impl<K: ?Sized, V: ?Sized> WithoutVersion for GenericPointer<K, V> {}
impl<K: ?Sized, V: ?Sized> crate::sealed::GenericPointer<K, V> for GenericPointer<K, V> {}
