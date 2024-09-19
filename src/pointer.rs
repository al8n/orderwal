use core::{borrow::Borrow, cmp, marker::PhantomData, slice};

use dbutils::Comparator;

use super::wal::r#type::{KeyRef, Type};

#[doc(hidden)]
pub struct Pointer<C> {
  /// The pointer to the start of the entry.
  ptr: *const u8,
  /// The length of the key.
  key_len: usize,
  /// The length of the value.
  value_len: usize,
  cmp: C,
}

unsafe impl<C: Send> Send for Pointer<C> {}
unsafe impl<C: Sync> Sync for Pointer<C> {}

impl<C> Pointer<C> {
  #[inline]
  pub(crate) const fn new(key_len: usize, value_len: usize, ptr: *const u8, cmp: C) -> Self {
    Self {
      ptr,
      key_len,
      value_len,
      cmp,
    }
  }

  #[inline]
  pub const fn as_key_slice<'a>(&self) -> &'a [u8] {
    if self.key_len == 0 {
      return &[];
    }

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    unsafe { slice::from_raw_parts(self.ptr, self.key_len) }
  }

  #[inline]
  pub const fn as_value_slice<'a, 'b: 'a>(&'a self) -> &'b [u8] {
    if self.value_len == 0 {
      return &[];
    }

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    unsafe { slice::from_raw_parts(self.ptr.add(self.key_len), self.value_len) }
  }
}

impl<C: Comparator> PartialEq for Pointer<C> {
  fn eq(&self, other: &Self) -> bool {
    self
      .cmp
      .compare(self.as_key_slice(), other.as_key_slice())
      .is_eq()
  }
}

impl<C: Comparator> Eq for Pointer<C> {}

impl<C: Comparator> PartialOrd for Pointer<C> {
  fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<C: Comparator> Ord for Pointer<C> {
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    self.cmp.compare(self.as_key_slice(), other.as_key_slice())
  }
}

impl<C, Q> Borrow<Q> for Pointer<C>
where
  [u8]: Borrow<Q>,
  Q: ?Sized + Ord,
{
  fn borrow(&self) -> &Q {
    self.as_key_slice().borrow()
  }
}

impl<C> super::wal::sealed::Pointer for Pointer<C> {
  type Comparator = C;

  #[inline]
  fn new(klen: usize, vlen: usize, ptr: *const u8, cmp: C) -> Self {
    Pointer::<C>::new(klen, vlen, ptr, cmp)
  }
}

#[doc(hidden)]
#[derive(Debug)]
pub struct GenericPointer<K, V> {
  /// The pointer to the start of the entry.
  ptr: *const u8,
  /// The length of the key.
  key_len: usize,
  /// The length of the value.
  value_len: usize,
  _m: PhantomData<(fn() -> K, fn() -> V)>,
}

impl<K, V> crate::wal::sealed::Pointer for GenericPointer<K, V> {
  type Comparator = ();

  #[inline]
  fn new(klen: usize, vlen: usize, ptr: *const u8, _cmp: Self::Comparator) -> Self {
    Self::new(klen, vlen, ptr)
  }
}

impl<K: Type, V> PartialEq for GenericPointer<K, V> {
  fn eq(&self, other: &Self) -> bool {
    self.as_key_slice() == other.as_key_slice()
  }
}

impl<K: Type, V> Eq for GenericPointer<K, V> {}

impl<K, V> PartialOrd for GenericPointer<K, V>
where
  K: Type + Ord,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
{
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<K, V> Ord for GenericPointer<K, V>
where
  K: Type + Ord,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
{
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    <K::Ref<'_> as KeyRef<K>>::compare_binary(self.as_key_slice(), other.as_key_slice())
  }
}

unsafe impl<K, V> Send for GenericPointer<K, V> {}
unsafe impl<K, V> Sync for GenericPointer<K, V> {}

impl<K, V> GenericPointer<K, V> {
  #[inline]
  pub(crate) const fn new(key_len: usize, value_len: usize, ptr: *const u8) -> Self {
    Self {
      ptr,
      key_len,
      value_len,
      _m: PhantomData,
    }
  }

  #[inline]
  pub const fn as_key_slice<'a>(&self) -> &'a [u8] {
    if self.key_len == 0 {
      return &[];
    }

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    unsafe { slice::from_raw_parts(self.ptr, self.key_len) }
  }

  #[inline]
  pub const fn as_value_slice<'a, 'b: 'a>(&'a self) -> &'b [u8] {
    if self.value_len == 0 {
      return &[];
    }

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    unsafe { slice::from_raw_parts(self.ptr.add(self.key_len), self.value_len) }
  }
}
