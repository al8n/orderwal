use core::{
  borrow::Borrow,
  marker::PhantomData,
  ptr::{self, NonNull},
  slice,
};

use crossbeam_skiplist::{Comparable, Equivalent};

/// Returns when the bytes are too large to be written to the vacant buffer.
#[derive(Debug, Default, Clone, Copy)]
pub struct TooLarge {
  remaining: usize,
  write: usize,
}

impl core::fmt::Display for TooLarge {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(
      f,
      "buffer does not have enough space (remaining {}, want {})",
      self.remaining, self.write
    )
  }
}

#[cfg(feature = "std")]
impl std::error::Error for TooLarge {}

/// A vacant buffer in the WAL.
#[must_use = "vacant buffer must be filled with bytes."]
#[derive(Debug)]
pub struct VacantBuffer<'a> {
  value: NonNull<u8>,
  len: usize,
  cap: usize,
  _m: PhantomData<&'a ()>,
}

impl<'a> VacantBuffer<'a> {
  /// Fill the remaining space with the given byte.
  pub fn fill(&mut self, byte: u8) {
    if self.cap == 0 {
      return;
    }

    // SAFETY: the value's ptr is aligned and the cap is the correct.
    unsafe {
      ptr::write_bytes(self.value.as_ptr(), byte, self.cap);
    }
    self.len = self.cap;
  }

  /// Write bytes to the vacant value.
  pub fn write(&mut self, bytes: &[u8]) -> Result<(), TooLarge> {
    let len = bytes.len();
    let remaining = self.cap - self.len;
    if len > remaining {
      return Err(TooLarge {
        remaining,
        write: len,
      });
    }

    // SAFETY: the value's ptr is aligned and the cap is the correct.
    unsafe {
      self
        .value
        .as_ptr()
        .add(self.len)
        .copy_from(bytes.as_ptr(), len);
    }

    self.len += len;
    Ok(())
  }

  /// Write bytes to the vacant value without bounds checking.
  ///
  /// # Panics
  /// - If a slice is larger than the remaining space.
  pub fn write_unchecked(&mut self, bytes: &[u8]) {
    let len = bytes.len();
    let remaining = self.cap - self.len;
    if len > remaining {
      panic!(
        "buffer does not have enough space (remaining {}, want {})",
        remaining, len
      );
    }

    // SAFETY: the value's ptr is aligned and the cap is the correct.
    unsafe {
      self
        .value
        .as_ptr()
        .add(self.len)
        .copy_from(bytes.as_ptr(), len);
    }
    self.len += len;
  }

  /// Returns the capacity of the vacant value.
  #[inline]
  pub const fn capacity(&self) -> usize {
    self.cap
  }

  /// Returns the length of the vacant value.
  #[inline]
  pub const fn len(&self) -> usize {
    self.len
  }

  /// Returns `true` if the vacant value is empty.
  #[inline]
  pub const fn is_empty(&self) -> bool {
    self.len == 0
  }

  /// Returns the remaining space of the vacant value.
  #[inline]
  pub const fn remaining(&self) -> usize {
    self.cap - self.len
  }

  #[inline]
  pub(crate) fn new(cap: usize, value: NonNull<u8>) -> Self {
    Self {
      value,
      len: 0,
      cap,
      _m: PhantomData,
    }
  }
}

impl<'a> core::ops::Deref for VacantBuffer<'a> {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    if self.cap == 0 {
      return &[];
    }

    unsafe { slice::from_raw_parts(self.value.as_ptr(), self.len) }
  }
}

impl<'a> core::ops::DerefMut for VacantBuffer<'a> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    if self.cap == 0 {
      return &mut [];
    }

    unsafe { slice::from_raw_parts_mut(self.value.as_ptr(), self.len) }
  }
}

impl<'a> AsRef<[u8]> for VacantBuffer<'a> {
  fn as_ref(&self) -> &[u8] {
    self
  }
}

impl<'a> AsMut<[u8]> for VacantBuffer<'a> {
  fn as_mut(&mut self) -> &mut [u8] {
    self
  }
}

impl<'a, Q> PartialEq<Q> for VacantBuffer<'a>
where
  [u8]: Borrow<Q>,
  Q: ?Sized + Eq,
{
  fn eq(&self, other: &Q) -> bool {
    self.as_ref().borrow().eq(other)
  }
}

impl<'a, Q> PartialOrd<Q> for VacantBuffer<'a>
where
  [u8]: Borrow<Q>,
  Q: ?Sized + Ord,
{
  fn partial_cmp(&self, other: &Q) -> Option<core::cmp::Ordering> {
    #[allow(clippy::needless_borrow)]
    Some(self.as_ref().borrow().cmp(&other))
  }
}

impl<'a, Q> Equivalent<Q> for VacantBuffer<'a>
where
  [u8]: Borrow<Q>,
  Q: ?Sized + Eq,
{
  fn equivalent(&self, key: &Q) -> bool {
    self.as_ref().borrow().eq(key)
  }
}

impl<'a, Q> Comparable<Q> for VacantBuffer<'a>
where
  [u8]: Borrow<Q>,
  Q: ?Sized + Ord,
{
  fn compare(&self, other: &Q) -> core::cmp::Ordering {
    self.as_ref().borrow().compare(other)
  }
}

impl<'a> PartialEq<VacantBuffer<'a>> for [u8] {
  fn eq(&self, other: &VacantBuffer<'a>) -> bool {
    self.as_ref().eq(other.as_ref())
  }
}

impl<'a> PartialOrd<VacantBuffer<'a>> for [u8] {
  fn partial_cmp(&self, other: &VacantBuffer<'a>) -> Option<core::cmp::Ordering> {
    Some(self.as_ref().cmp(other.as_ref()))
  }
}

impl<'a> PartialEq<[u8]> for &VacantBuffer<'a> {
  fn eq(&self, other: &[u8]) -> bool {
    self.as_ref().eq(other)
  }
}

impl<'a> PartialOrd<[u8]> for &VacantBuffer<'a> {
  fn partial_cmp(&self, other: &[u8]) -> Option<core::cmp::Ordering> {
    Some(self.as_ref().cmp(other))
  }
}

impl<'a> PartialEq<&VacantBuffer<'a>> for [u8] {
  fn eq(&self, other: &&VacantBuffer<'a>) -> bool {
    self.eq(other.as_ref())
  }
}

impl<'a> PartialOrd<&VacantBuffer<'a>> for [u8] {
  fn partial_cmp(&self, other: &&VacantBuffer<'a>) -> Option<core::cmp::Ordering> {
    Some(self.cmp(other.as_ref()))
  }
}

impl<'a, const N: usize> PartialEq<VacantBuffer<'a>> for [u8; N] {
  fn eq(&self, other: &VacantBuffer<'a>) -> bool {
    self.as_ref().eq(other.as_ref())
  }
}

impl<'a, const N: usize> PartialOrd<VacantBuffer<'a>> for [u8; N] {
  fn partial_cmp(&self, other: &VacantBuffer<'a>) -> Option<core::cmp::Ordering> {
    Some(self.as_ref().cmp(other.as_ref()))
  }
}

impl<'a, const N: usize> PartialEq<&VacantBuffer<'a>> for [u8; N] {
  fn eq(&self, other: &&VacantBuffer<'a>) -> bool {
    self.as_ref().eq(other.as_ref())
  }
}

impl<'a, const N: usize> PartialEq<[u8; N]> for &VacantBuffer<'a> {
  fn eq(&self, other: &[u8; N]) -> bool {
    self.as_ref().eq(other.as_ref())
  }
}

impl<'a, const N: usize> PartialEq<&mut VacantBuffer<'a>> for [u8; N] {
  fn eq(&self, other: &&mut VacantBuffer<'a>) -> bool {
    self.as_ref().eq(other.as_ref())
  }
}

impl<'a, const N: usize> PartialEq<[u8; N]> for &mut VacantBuffer<'a> {
  fn eq(&self, other: &[u8; N]) -> bool {
    self.as_ref().eq(other.as_ref())
  }
}

macro_rules! builder {
  ($($name:ident($size:ident)),+ $(,)?) => {
    $(
      paste::paste! {
        #[doc = "A " [< $name: snake>] " builder for the wal, which requires the " [< $name: snake>] " size for accurate allocation and a closure to build the " [< $name: snake>]]
        #[derive(Copy, Clone, Debug)]
        pub struct [< $name Builder >] <F> {
          size: $size,
          f: F,
        }

        impl<F> [< $name Builder >]<F> {
          #[doc = "Creates a new `" [<$name Builder>] "` with the given size and builder closure."]
          #[inline]
          pub const fn new<E>(size: $size, f: F) -> Self
          where
            F: for<'a> FnOnce(&mut VacantBuffer<'a>) -> Result<(), E>,
          {
            Self { size, f }
          }

          #[doc = "Returns the required" [< $name: snake>] "size."]
          #[inline]
          pub const fn size(&self) -> $size {
            self.size
          }

          #[doc = "Returns the " [< $name: snake>] "builder closure."]
          #[inline]
          pub const fn builder(&self) -> &F {
            &self.f
          }

          /// Deconstructs the value builder into the size and the builder closure.
          #[inline]
          pub fn into_components(self) -> ($size, F) {
            (self.size, self.f)
          }
        }
      }
    )*
  };
}

builder!(Value(u32), Key(u32));
