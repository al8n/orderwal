use core::{borrow::Borrow, cmp, mem, slice};

use dbutils::{
  traits::{Type, TypeRef},
  CheapClone, Comparator, StaticComparator,
};

use crate::{
  sealed::{Pointer as _, WithVersion, WithoutVersion},
  VERSION_SIZE,
};

const PTR_SIZE: usize = mem::size_of::<usize>();
const U32_SIZE: usize = mem::size_of::<u32>();

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

impl<C> core::fmt::Debug for Pointer<C> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Pointer")
      .field("ptr", &self.ptr)
      .field("key_len", &self.key_len)
      .field("value_len", &self.value_len)
      .finish()
  }
}

impl<C: Clone> Clone for Pointer<C> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ptr: self.ptr,
      key_len: self.key_len,
      value_len: self.value_len,
      cmp: self.cmp.clone(),
    }
  }
}

impl<C: Copy> Copy for Pointer<C> {}

impl<C: CheapClone> CheapClone for Pointer<C> {}

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

impl<C> crate::sealed::Pointer for Pointer<C> {
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

impl<C> Type for Pointer<C>
where
  C: Copy + StaticComparator + Default,
{
  type Ref<'a> = Self;

  type Error = ();

  #[inline]
  fn encoded_len(&self) -> usize {
    const SIZE: usize = PTR_SIZE + 2 * U32_SIZE;
    SIZE
  }

  #[inline]
  fn encode_to_buffer(&self, buf: &mut skl::VacantBuffer<'_>) -> Result<usize, Self::Error> {
    // Safe to cast to u32 here, because the key and value length are guaranteed to be less than or equal to u32::MAX.
    let key_len = self.key_len as u32;
    let value_len = self.value_len as u32;
    let ptr = self.ptr as usize;

    buf.set_len(self.encoded_len());

    buf[0..PTR_SIZE].copy_from_slice(&ptr.to_le_bytes());

    let mut offset = PTR_SIZE;
    buf[offset..offset + U32_SIZE].copy_from_slice(&key_len.to_le_bytes());
    offset += U32_SIZE;
    buf[offset..offset + U32_SIZE].copy_from_slice(&value_len.to_le_bytes());

    Ok(offset + U32_SIZE)
  }
}

impl<'a, C> TypeRef<'a> for Pointer<C>
where
  C: Copy + StaticComparator + Default,
{
  unsafe fn from_slice(src: &'a [u8]) -> Self {
    let ptr = usize::from_le_bytes((&src[..PTR_SIZE]).try_into().unwrap()) as *const u8;
    let mut offset = PTR_SIZE;
    let key_len =
      u32::from_le_bytes((&src[offset..offset + U32_SIZE]).try_into().unwrap()) as usize;
    offset += U32_SIZE;
    let value_len =
      u32::from_le_bytes((&src[offset..offset + U32_SIZE]).try_into().unwrap()) as usize;

    Self {
      ptr,
      key_len,
      value_len,
      cmp: Default::default(),
    }
  }
}

#[doc(hidden)]
pub struct VersionPointer<C> {
  /// The pointer to the start of the entry.
  ptr: *const u8,
  /// The length of the key.
  key_len: usize,
  /// The length of the value.
  value_len: usize,
  cmp: C,
}

impl<C> core::fmt::Debug for VersionPointer<C> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("VersionPointer")
      .field("ptr", &self.ptr)
      .field("key_len", &(self.key_len - VERSION_SIZE))
      .field("value_len", &self.value_len)
      .finish()
  }
}

impl<C: Clone> Clone for VersionPointer<C> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ptr: self.ptr,
      key_len: self.key_len,
      value_len: self.value_len,
      cmp: self.cmp.clone(),
    }
  }
}

impl<C: Copy> Copy for VersionPointer<C> {}

impl<C: CheapClone> CheapClone for VersionPointer<C> {}

unsafe impl<C: Send> Send for VersionPointer<C> {}
unsafe impl<C: Sync> Sync for VersionPointer<C> {}

impl<C> VersionPointer<C> {
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

impl<C: Comparator> PartialEq for VersionPointer<C> {
  fn eq(&self, other: &Self) -> bool {
    self
      .cmp
      .compare(self.as_key_slice(), other.as_key_slice())
      .then_with(|| other.version().cmp(&self.version())) // make sure latest version (version with larger number) is present before the older one
      .is_eq()
  }
}

impl<C: Comparator> Eq for VersionPointer<C> {}

impl<C: Comparator> PartialOrd for VersionPointer<C> {
  fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<C: Comparator> Ord for VersionPointer<C> {
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    self
      .cmp
      .compare(self.as_key_slice(), other.as_key_slice())
      .then_with(|| other.version().cmp(&self.version()))
  }
}

impl<C, Q> Borrow<Q> for VersionPointer<C>
where
  [u8]: Borrow<Q>,
  Q: ?Sized + Ord,
{
  fn borrow(&self) -> &Q {
    self.as_key_slice().borrow()
  }
}

impl<C> crate::sealed::Pointer for VersionPointer<C> {
  type Comparator = C;

  #[inline]
  fn new(klen: usize, vlen: usize, ptr: *const u8, cmp: C) -> Self {
    VersionPointer::<C>::new(klen, vlen, ptr, cmp)
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

impl<C> Type for VersionPointer<C>
where
  C: Copy + StaticComparator + Default,
{
  type Ref<'a> = Self;

  type Error = ();

  #[inline]
  fn encoded_len(&self) -> usize {
    const SIZE: usize = PTR_SIZE + 2 * U32_SIZE;
    SIZE
  }

  #[inline]
  fn encode_to_buffer(&self, buf: &mut skl::VacantBuffer<'_>) -> Result<usize, Self::Error> {
    // Safe to cast to u32 here, because the key and value length are guaranteed to be less than or equal to u32::MAX.
    let key_len = self.key_len as u32;
    let value_len = self.value_len as u32;
    let ptr = self.ptr as usize;

    buf.set_len(self.encoded_len());

    buf[0..PTR_SIZE].copy_from_slice(&ptr.to_le_bytes());

    let mut offset = PTR_SIZE;
    buf[offset..offset + U32_SIZE].copy_from_slice(&key_len.to_le_bytes());
    offset += U32_SIZE;
    buf[offset..offset + U32_SIZE].copy_from_slice(&value_len.to_le_bytes());

    Ok(offset + U32_SIZE)
  }
}

impl<'a, C> TypeRef<'a> for VersionPointer<C>
where
  C: Copy + StaticComparator + Default,
{
  unsafe fn from_slice(src: &'a [u8]) -> Self {
    let ptr = usize::from_le_bytes((&src[..PTR_SIZE]).try_into().unwrap()) as *const u8;
    let mut offset = PTR_SIZE;
    let key_len =
      u32::from_le_bytes((&src[offset..offset + U32_SIZE]).try_into().unwrap()) as usize;
    offset += U32_SIZE;
    let value_len =
      u32::from_le_bytes((&src[offset..offset + U32_SIZE]).try_into().unwrap()) as usize;

    Self {
      ptr,
      key_len,
      value_len,
      cmp: Default::default(),
    }
  }
}

impl<C> WithVersion for VersionPointer<C> {}
impl<C> WithoutVersion for Pointer<C> {}
