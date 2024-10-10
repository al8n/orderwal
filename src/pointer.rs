use core::{borrow::Borrow, cmp, slice};

use dbutils::{traits::KeyRef, Comparator};

use crate::{
  sealed::{Pointer as _, WithVersion, WithoutVersion},
  VERSION_SIZE,
};

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

impl<C> super::sealed::Pointer for Pointer<C> {
  type Comparator = C;

  #[inline]
  fn new(klen: usize, vlen: usize, ptr: *const u8, cmp: C) -> Self {
    Self {
      ptr,
      key_len: klen,
      value_len: vlen,
      cmp,
    }
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

#[doc(hidden)]
pub struct MvccPointer<C> {
  /// The pointer to the start of the entry.
  ptr: *const u8,
  /// The length of the key.
  key_len: usize,
  /// The length of the value.
  value_len: usize,
  cmp: C,
}

unsafe impl<C: Send> Send for MvccPointer<C> {}
unsafe impl<C: Sync> Sync for MvccPointer<C> {}

impl<C> MvccPointer<C> {
  #[inline]
  pub(crate) const fn new(key_len: usize, value_len: usize, ptr: *const u8, cmp: C) -> Self {
    Self {
      ptr,
      key_len: key_len - VERSION_SIZE,
      value_len,
      cmp,
    }
  }
}

impl<C: Comparator> PartialEq for MvccPointer<C> {
  fn eq(&self, other: &Self) -> bool {
    self
      .cmp
      .compare(self.as_key_slice(), other.as_key_slice())
      .then_with(|| other.version().cmp(&self.version())) // make sure latest version (version with larger number) is present before the older one
      .is_eq()
  }
}

impl<C: Comparator> Eq for MvccPointer<C> {}

impl<C: Comparator> PartialOrd for MvccPointer<C> {
  fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<C: Comparator> Ord for MvccPointer<C> {
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    self
      .cmp
      .compare(self.as_key_slice(), other.as_key_slice())
      .then_with(|| other.version().cmp(&self.version()))
  }
}

impl<C, Q> Borrow<Q> for MvccPointer<C>
where
  [u8]: Borrow<Q>,
  Q: ?Sized + Ord,
{
  fn borrow(&self) -> &Q {
    self.as_key_slice().borrow()
  }
}

impl<C> super::sealed::Pointer for MvccPointer<C> {
  type Comparator = C;

  #[inline]
  fn new(klen: usize, vlen: usize, ptr: *const u8, cmp: C) -> Self {
    MvccPointer::<C>::new(klen, vlen, ptr, cmp)
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

impl<C> WithVersion for MvccPointer<C> {}
impl<C> WithoutVersion for Pointer<C> {}
