use core::{cmp, marker::PhantomData, mem, slice};

use dbutils::{
  equivalent::Comparable,
  traits::{KeyRef, Type, TypeRef},
  CheapClone,
};

use crate::{
  sealed::{Pointer, WithVersion, WithoutVersion},
  types::EntryFlags,
  VERSION_SIZE,
};

const PTR_SIZE: usize = mem::size_of::<usize>();
const U32_SIZE: usize = mem::size_of::<u32>();
#[doc(hidden)]
pub struct GenericPointer<K: ?Sized, V: ?Sized> {
  /// The pointer to the start of the entry.
  ///
  /// | flag (1 byte) | version (8 bytes, optional) | key | value (optional) |
  ptr: *const u8,
  flag: EntryFlags,
  key_ptr: *const u8,
  /// The length of the key.
  key_len: usize,
  value_ptr: *const u8,
  /// The length of the value.
  value_len: usize,
  _m: PhantomData<(fn() -> K, fn() -> V)>,
}

impl<K: ?Sized, V: ?Sized> core::fmt::Debug for GenericPointer<K, V> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("GenericPointer")
      .field("ptr", &self.ptr)
      .field("key_len", &self.key_len)
      .field("value_len", &self.value_len)
      .finish()
  }
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
  #[inline]
  fn new(flag: EntryFlags, klen: usize, vlen: usize, ptr: *const u8) -> Self {
    Self {
      key_ptr: unsafe { ptr.add(EntryFlags::SIZE) },
      value_ptr: unsafe { ptr.add(EntryFlags::SIZE + klen) },
      ptr,
      key_len: klen,
      value_len: vlen,
      flag,
      _m: PhantomData,
    }
  }

  #[inline]
  fn as_key_slice<'a>(&self) -> &'a [u8] {
    if self.key_len == 0 {
      return &[];
    }

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    unsafe { slice::from_raw_parts(self.key_ptr, self.key_len) }
  }

  #[inline]
  fn as_value_slice<'a>(&self) -> Option<&'a [u8]> {
    if self.flag.contains(EntryFlags::REMOVED) {
      return None;
    }

    if self.value_len == 0 {
      return Some(&[]);
    }

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    Some(unsafe { slice::from_raw_parts(self.value_ptr, self.value_len) })
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

impl<K, V> Type for GenericPointer<K, V>
where
  K: ?Sized,
  V: ?Sized,
{
  type Ref<'a> = Self;

  type Error = ();

  #[inline]
  fn encoded_len(&self) -> usize {
    const SIZE: usize = PTR_SIZE + 2 * U32_SIZE + mem::size_of::<EntryFlags>();
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
    buf[offset] = self.flag.bits();
    offset += 1;
    buf[offset..offset + U32_SIZE].copy_from_slice(&key_len.to_le_bytes());
    offset += U32_SIZE;
    buf[offset..offset + U32_SIZE].copy_from_slice(&value_len.to_le_bytes());

    Ok(offset + U32_SIZE)
  }
}

impl<'a, K: ?Sized, V: ?Sized> TypeRef<'a> for GenericPointer<K, V> {
  unsafe fn from_slice(src: &'a [u8]) -> Self {
    let ptr = usize::from_le_bytes((&src[..PTR_SIZE]).try_into().unwrap()) as *const u8;
    let mut offset = PTR_SIZE;
    let flag = EntryFlags::from_bits_retain(src[offset]);
    offset += 1;
    let key_len =
      u32::from_le_bytes((&src[offset..offset + U32_SIZE]).try_into().unwrap()) as usize;
    offset += U32_SIZE;
    let value_len =
      u32::from_le_bytes((&src[offset..offset + U32_SIZE]).try_into().unwrap()) as usize;

    Self::new(flag, key_len, value_len, ptr)
  }
}

impl<K, V> KeyRef<'_, Self> for GenericPointer<K, V>
where
  K: Type + Ord + ?Sized,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
  V: ?Sized,
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

#[doc(hidden)]
pub struct GenericVersionPointer<K: ?Sized, V: ?Sized> {
  /// The pointer to the start of the entry.
  ///
  /// | flag (1 byte) | version (8 bytes, optional) | key | value (optional) |
  ptr: *const u8,
  flag: EntryFlags,
  version: u64,
  key_ptr: *const u8,
  /// The length of the key.
  key_len: usize,
  value_ptr: *const u8,
  /// The length of the value.
  value_len: usize,
  _m: PhantomData<(fn() -> K, fn() -> V)>,
}

impl<K: ?Sized, V: ?Sized> core::fmt::Debug for GenericVersionPointer<K, V> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("GenericVersionPointer")
      .field("ptr", &self.ptr)
      .field("key_len", &(self.key_len - VERSION_SIZE))
      .field("value_len", &self.value_len)
      .finish()
  }
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
  #[inline]
  fn new(flag: EntryFlags, klen: usize, vlen: usize, ptr: *const u8) -> Self {
    Self::new(flag, klen, vlen, ptr)
  }

  #[inline]
  fn as_key_slice<'a>(&self) -> &'a [u8] {
    if self.key_len == 0 {
      return &[];
    }

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    unsafe { slice::from_raw_parts(self.key_ptr, self.key_len) }
  }

  #[inline]
  fn as_value_slice<'a>(&self) -> Option<&'a [u8]> {
    if self.flag.contains(EntryFlags::REMOVED) {
      return None;
    }

    if self.value_len == 0 {
      return Some(&[]);
    }

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    Some(unsafe { slice::from_raw_parts(self.value_ptr, self.value_len) })
  }

  #[inline]
  fn version(&self) -> u64 {
    self.version
  }

  #[inline]
  fn is_removed(&self) -> bool {
    self.flag.contains(EntryFlags::REMOVED)
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
  pub(crate) fn new(flag: EntryFlags, key_len: usize, value_len: usize, ptr: *const u8) -> Self {
    Self {
      flag,
      key_ptr: unsafe { ptr.add(EntryFlags::SIZE + VERSION_SIZE) },
      value_ptr: unsafe { ptr.add(EntryFlags::SIZE + VERSION_SIZE + key_len) },
      version: unsafe {
        let slice = slice::from_raw_parts(ptr.add(EntryFlags::SIZE), VERSION_SIZE);
        u64::from_le_bytes(slice.try_into().unwrap())
      },
      ptr,
      key_len: key_len - VERSION_SIZE,
      value_len,
      _m: PhantomData,
    }
  }
}

impl<K, V> Type for GenericVersionPointer<K, V>
where
  K: ?Sized,
  V: ?Sized,
{
  type Ref<'a> = Self;

  type Error = ();

  #[inline]
  fn encoded_len(&self) -> usize {
    const SIZE: usize = PTR_SIZE + 2 * U32_SIZE + mem::size_of::<EntryFlags>();
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
    buf[offset] = self.flag.bits();
    offset += 1;
    buf[offset..offset + U32_SIZE].copy_from_slice(&key_len.to_le_bytes());
    offset += U32_SIZE;
    buf[offset..offset + U32_SIZE].copy_from_slice(&value_len.to_le_bytes());

    Ok(offset + U32_SIZE)
  }
}

impl<'a, K: ?Sized, V: ?Sized> TypeRef<'a> for GenericVersionPointer<K, V> {
  unsafe fn from_slice(src: &'a [u8]) -> Self {
    let ptr = usize::from_le_bytes((&src[..PTR_SIZE]).try_into().unwrap()) as *const u8;
    let mut offset = PTR_SIZE;
    let flag = EntryFlags::from_bits_retain(src[offset]);
    offset += 1;
    let key_len =
      u32::from_le_bytes((&src[offset..offset + U32_SIZE]).try_into().unwrap()) as usize;
    offset += U32_SIZE;
    let value_len =
      u32::from_le_bytes((&src[offset..offset + U32_SIZE]).try_into().unwrap()) as usize;

    Self::new(flag, key_len, value_len, ptr)
  }
}

impl<K, V> KeyRef<'_, Self> for GenericVersionPointer<K, V>
where
  K: Type + Ord + ?Sized,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
  V: ?Sized,
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

impl<K: ?Sized, V: ?Sized> WithVersion for GenericVersionPointer<K, V> {}
impl<K: ?Sized, V: ?Sized> crate::sealed::GenericPointer<K, V> for GenericVersionPointer<K, V> {}
impl<K: ?Sized, V: ?Sized> WithoutVersion for GenericPointer<K, V> {}
impl<K: ?Sized, V: ?Sized> crate::sealed::GenericPointer<K, V> for GenericPointer<K, V> {}
