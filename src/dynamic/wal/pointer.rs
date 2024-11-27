use core::mem;

use dbutils::{
  buffer::VacantBuffer,
  error::InsufficientBuffer,
  types::{Type, TypeRef},
};

use crate::types::EntryFlags;

const PTR_SIZE: usize = mem::size_of::<usize>();
const U32_SIZE: usize = mem::size_of::<u32>();

#[derive(Clone, Copy)]
pub struct ValuePointer {
  offset: u32,
  len: u32,
}

impl core::fmt::Debug for ValuePointer {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("ValuePointer")
      .field("offset", &self.offset)
      .field("len", &self.len)
      .finish()
  }
}

impl ValuePointer {
  const SIZE: usize = mem::size_of::<Self>();

  #[inline]
  pub(crate) const fn new(offset: u32, len: u32) -> Self {
    Self { offset, len }
  }

  #[inline]
  pub const fn offset(&self) -> usize {
    self.offset as usize
  }

  #[inline]
  pub const fn len(&self) -> usize {
    self.len as usize
  }

  #[inline]
  pub(crate) fn as_array(&self) -> [u8; Self::SIZE] {
    let mut array = [0; Self::SIZE];
    {
      let mut buf = VacantBuffer::from(array.as_mut());
      self.encode_to_buffer(&mut buf).unwrap();
    }
    array
  }
}

impl Type for ValuePointer {
  type Ref<'a> = Self;

  type Error = InsufficientBuffer;

  #[inline]
  fn encoded_len(&self) -> usize {
    Self::SIZE
  }

  #[inline]
  fn encode_to_buffer(&self, buf: &mut VacantBuffer<'_>) -> Result<usize, Self::Error> {
    buf
      .put_u32_le(self.offset)
      .and_then(|_| buf.put_u32_le(self.len))
      .map(|_| Self::SIZE)
  }
}

impl<'a> TypeRef<'a> for ValuePointer {
  #[inline]
  unsafe fn from_slice(src: &'a [u8]) -> Self {
    let offset = u32::from_le_bytes(src[..4].try_into().unwrap());
    let len = u32::from_le_bytes(src[4..Self::SIZE].try_into().unwrap());
    Self { offset, len }
  }
}

#[doc(hidden)]
pub struct KeyPointer {
  flag: EntryFlags,
  offset: u32,
  len: u32,
}

impl core::fmt::Debug for KeyPointer {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("KeyPointer")
      .field("flag", &self.flag)
      .field("offset", &self.offset)
      .field("len", &self.len)
      .finish()
  }
}

impl Clone for KeyPointer {
  #[inline]
  fn clone(&self) -> Self {
    *self
  }
}

impl Copy for KeyPointer {}

impl KeyPointer {
  const SIZE: usize = mem::size_of::<Self>();

  #[inline]
  pub(crate) fn new(flag: EntryFlags, offset: u32, len: u32) -> Self {
    Self { flag, offset, len }
  }

  #[inline]
  pub const fn offset(&self) -> usize {
    self.offset as usize
  }

  #[inline]
  pub const fn len(&self) -> usize {
    self.len as usize
  }

  #[inline]
  pub(crate) fn as_array(&self) -> [u8; Self::SIZE] {
    let mut array = [0; Self::SIZE];
    {
      let mut buf = VacantBuffer::from(array.as_mut());
      self.encode_to_buffer(&mut buf).unwrap();
    }
    array
  }
}

impl Type for KeyPointer {
  type Ref<'a> = Self;

  type Error = InsufficientBuffer;

  #[inline]
  fn encoded_len(&self) -> usize {
    Self::SIZE
  }

  #[inline]
  fn encode_to_buffer(&self, buf: &mut VacantBuffer<'_>) -> Result<usize, Self::Error> {
    buf.put_u8(self.flag.bits())?;
    buf.put_u32_le(self.offset)?;
    buf.put_u32_le(self.len).map(|_| Self::SIZE)
  }
}

impl<'a> TypeRef<'a> for KeyPointer {
  #[inline]
  unsafe fn from_slice(src: &'a [u8]) -> Self {
    let flag = EntryFlags::from_bits_retain(src[0]);
    let offset = u32::from_le_bytes(src[1..5].try_into().unwrap());
    let len = u32::from_le_bytes(src[5..Self::SIZE].try_into().unwrap());

    Self { flag, offset, len }
  }
}

/// The ARENA used to get key and value from the WAL by the pointer.
pub struct Arena<A>(A);

impl<A> Arena<A> {
  #[inline]
  pub(crate) const fn new(arena: A) -> Self {
    Self(arena)
  }
}

impl<A> Arena<A>
where
  A: rarena_allocator::Allocator,
{
  /// Get the key from the WAL by the pointer.
  #[inline]
  pub fn key(&self, kp: KeyPointer) -> &[u8] {
    unsafe { self.0.get_bytes(kp.offset as usize, kp.len as usize) }
  }

  /// Get the value from the WAL by the pointer.
  #[inline]
  pub fn value(&self, vp: ValuePointer) -> &[u8] {
    unsafe { self.0.get_bytes(vp.offset as usize, vp.len as usize) }
  }

  #[inline]
  pub(crate) fn raw_pointer(&self) -> *const u8 {
    self.0.raw_ptr()
  }
}
