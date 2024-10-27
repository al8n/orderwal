use core::{cmp, marker::PhantomData, mem, slice};

use dbutils::{
  buffer::VacantBuffer,
  equivalent::Comparable,
  traits::{KeyRef, Type, TypeRef},
  CheapClone,
};

use crate::types::EntryFlags;

const PTR_SIZE: usize = mem::size_of::<usize>();
const U32_SIZE: usize = mem::size_of::<u32>();

pub struct ValuePointer<V: ?Sized> {
  ptr: *const u8,
  len: usize,
  _m: PhantomData<V>,
}

impl<V: ?Sized> core::fmt::Debug for ValuePointer<V> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("ValuePointer")
      .field("ptr", &self.ptr)
      .field("value", &self.as_slice())
      .finish()
  }
}

impl<V: ?Sized> Clone for ValuePointer<V> {
  #[inline]
  fn clone(&self) -> Self {
    *self
  }
}

impl<V: ?Sized> Copy for ValuePointer<V> {}

impl<V: ?Sized> CheapClone for ValuePointer<V> {
  #[inline]
  fn cheap_clone(&self) -> Self {
    *self
  }
}

impl<V: ?Sized> ValuePointer<V> {
  #[inline]
  pub(crate) fn new(len: usize, ptr: *const u8) -> Self {
    Self {
      ptr,
      len,
      _m: PhantomData,
    }
  }

  #[inline]
  pub(crate) fn as_slice<'a>(&self) -> &'a [u8] {
    if self.len == 0 {
      return &[];
    }

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    unsafe { slice::from_raw_parts(self.ptr, self.len) }
  }
}

impl<V> Type for ValuePointer<V>
where
  V: ?Sized,
{
  type Ref<'a> = Self;

  type Error = ();

  #[inline]
  fn encoded_len(&self) -> usize {
    const SIZE: usize = PTR_SIZE + U32_SIZE;
    SIZE
  }

  #[inline]
  fn encode_to_buffer(&self, buf: &mut VacantBuffer<'_>) -> Result<usize, Self::Error> {
    // Safe to cast to u32 here, because the key and value length are guaranteed to be less than or equal to u32::MAX.
    let val_len = self.len as u32;
    let ptr = self.ptr as usize;

    buf.set_len(self.encoded_len());
    buf[0..PTR_SIZE].copy_from_slice(&ptr.to_le_bytes());

    buf[PTR_SIZE..PTR_SIZE + U32_SIZE].copy_from_slice(&val_len.to_le_bytes());

    Ok(PTR_SIZE + U32_SIZE)
  }
}

impl<'a, V: ?Sized> TypeRef<'a> for ValuePointer<V> {
  unsafe fn from_slice(src: &'a [u8]) -> Self {
    let ptr = usize_to_addr(usize::from_le_bytes((&src[..PTR_SIZE]).try_into().unwrap()));
    let len =
      u32::from_le_bytes((&src[PTR_SIZE..PTR_SIZE + U32_SIZE]).try_into().unwrap()) as usize;

    Self::new(len, ptr)
  }
}

#[doc(hidden)]
pub struct KeyPointer<K: ?Sized> {
  flag: EntryFlags,
  ptr: *const u8,
  len: usize,
  _m: PhantomData<K>,
}

impl<K: ?Sized> core::fmt::Debug for KeyPointer<K> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("KeyPointer")
      .field("ptr", &self.ptr)
      .field("flag", &self.flag)
      .field("key", &self.as_slice())
      .finish()
  }
}

impl<K: ?Sized> Clone for KeyPointer<K> {
  #[inline]
  fn clone(&self) -> Self {
    *self
  }
}

impl<K: ?Sized> Copy for KeyPointer<K> {}

impl<K: ?Sized> CheapClone for KeyPointer<K> {
  #[inline]
  fn cheap_clone(&self) -> Self {
    *self
  }
}

impl<K: ?Sized> KeyPointer<K> {
  #[inline]
  pub(crate) fn new(flag: EntryFlags, len: usize, ptr: *const u8) -> Self {
    Self {
      ptr,
      flag,
      len,
      _m: PhantomData,
    }
  }

  #[inline]
  pub(crate) fn as_slice<'a>(&self) -> &'a [u8] {
    if self.len == 0 {
      return &[];
    }

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    unsafe { slice::from_raw_parts(self.ptr, self.len) }
  }
}

impl<K: Type + ?Sized> PartialEq for KeyPointer<K> {
  fn eq(&self, other: &Self) -> bool {
    self.as_slice() == other.as_slice()
  }
}

impl<K: Type + ?Sized> Eq for KeyPointer<K> {}

impl<K> PartialOrd for KeyPointer<K>
where
  K: Type + Ord + ?Sized,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
{
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<K> Ord for KeyPointer<K>
where
  K: Type + Ord + ?Sized,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
{
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    // SAFETY: WALs guarantee that the self and other must be the same as the result returned by `<K as Type>::encode`.
    unsafe { <K::Ref<'_> as KeyRef<K>>::compare_binary(self.as_slice(), other.as_slice()) }
  }
}

unsafe impl<K> Send for KeyPointer<K> where K: ?Sized {}
unsafe impl<K> Sync for KeyPointer<K> where K: ?Sized {}

impl<K> Type for KeyPointer<K>
where
  K: ?Sized,
{
  type Ref<'a> = Self;

  type Error = ();

  #[inline]
  fn encoded_len(&self) -> usize {
    const SIZE: usize = PTR_SIZE + U32_SIZE + mem::size_of::<EntryFlags>();
    SIZE
  }

  #[inline]
  fn encode_to_buffer(&self, buf: &mut VacantBuffer<'_>) -> Result<usize, Self::Error> {
    // Safe to cast to u32 here, because the key and value length are guaranteed to be less than or equal to u32::MAX.
    let key_len = self.len as u32;
    let ptr = self.ptr as usize;

    buf.set_len(self.encoded_len());
    buf[0..PTR_SIZE].copy_from_slice(&ptr.to_le_bytes());

    let mut offset = PTR_SIZE;
    buf[offset] = self.flag.bits();
    offset += 1;
    buf[offset..offset + U32_SIZE].copy_from_slice(&key_len.to_le_bytes());

    Ok(offset + U32_SIZE)
  }
}

impl<'a, K: ?Sized> TypeRef<'a> for KeyPointer<K> {
  unsafe fn from_slice(src: &'a [u8]) -> Self {
    let ptr = usize_to_addr(usize::from_le_bytes((&src[..PTR_SIZE]).try_into().unwrap()));
    let mut offset = PTR_SIZE;
    let flag = EntryFlags::from_bits_retain(src[offset]);
    offset += 1;
    let key_len =
      u32::from_le_bytes((&src[offset..offset + U32_SIZE]).try_into().unwrap()) as usize;

    Self::new(flag, key_len, ptr)
  }
}

impl<K> KeyRef<'_, Self> for KeyPointer<K>
where
  K: Type + Ord + ?Sized,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
{
  #[inline]
  fn compare<Q>(&self, a: &Q) -> cmp::Ordering
  where
    Q: ?Sized + Ord + Comparable<Self>,
  {
    Comparable::compare(a, self).reverse()
  }

  #[inline]
  unsafe fn compare_binary(a: &[u8], b: &[u8]) -> cmp::Ordering {
    <K::Ref<'_> as KeyRef<K>>::compare_binary(a, b)
  }
}

#[inline]
const fn usize_to_addr<T>(addr: usize) -> *const T {
  addr as *const T
}
