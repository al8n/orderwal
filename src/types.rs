use core::{
  marker::PhantomData,
  mem,
  ops::{Bound, RangeBounds},
};

use dbutils::{
  error::InsufficientBuffer,
  leb128::encoded_u64_varint_len,
  types::{Type, TypeRef},
};
use ref_cast::RefCast as _;
use sealed::Pointee;

use crate::utils::split_lengths;

use super::{utils::merge_lengths, CHECKSUM_SIZE, RECORD_FLAG_SIZE, VERSION_SIZE};

pub use dbutils::buffer::{BufWriter, BufWriterOnce, VacantBuffer};

mod mode;
mod raw;
pub(crate) use mode::sealed;
pub use mode::{Dynamic, Generic, TypeMode};
pub(crate) use raw::*;

#[doc(hidden)]
#[derive(ref_cast::RefCast)]
#[repr(transparent)]
pub struct Query<Q: ?Sized>(pub(crate) Q);

pub struct QueryRange<Q: ?Sized, R> {
  r: R,
  _m: PhantomData<Q>,
}

impl<Q, R> From<R> for QueryRange<Q, R>
where
  R: RangeBounds<Q>,
  Q: ?Sized,
{
  #[inline]
  fn from(r: R) -> Self {
    Self { r, _m: PhantomData }
  }
}

impl<Q, R> core::ops::RangeBounds<Query<Q>> for QueryRange<Q, R>
where
  R: RangeBounds<Q>,
  Q: ?Sized,
{
  #[inline]
  fn start_bound(&self) -> Bound<&Query<Q>> {
    self.r.start_bound().map(Query::ref_cast)
  }

  #[inline]
  fn end_bound(&self) -> Bound<&Query<Q>> {
    self.r.end_bound().map(Query::ref_cast)
  }
}

#[doc(hidden)]
#[derive(ref_cast::RefCast)]
#[repr(transparent)]
pub struct RefQuery<Q> {
  pub(crate) query: Q,
}

impl<Q> RefQuery<Q> {
  #[inline]
  pub const fn new(query: Q) -> Self {
    Self { query }
  }
}

bitflags::bitflags! {
  /// The flags for each atomic write.
  pub(super) struct Flags: u8 {
    /// First bit: 1 indicates committed, 0 indicates uncommitted
    const COMMITTED = 0b00000001;
    /// Second bit: 1 indicates batching, 0 indicates single entry
    const BATCHING = 0b00000010;
  }
}

bitflags::bitflags! {
  /// The flags for each entry.
  #[derive(Debug, Copy, Clone)]
  pub struct EntryFlags: u8 {
    /// First bit: 1 indicates the entry is inserted within a batch
    const BATCHING = 0b00000001;
    /// Second bit: 1 indicates the key is pointer, the real key is stored in the offset contained in the RecordPointer.
    const KEY_POINTER = 0b00000010;
    /// Third bit: 1 indicates the value is pointer, the real value is stored in the offset contained in the ValuePointer.
    const VALUE_POINTER = 0b00000100;
    /// Fourth bit: 1 indicates the entry is a tombstone
    const REMOVED = 0b00001000;
    /// Fifth bit: 1 indicates the entry contains a version
    const VERSIONED = 0b00010000;
    /// Sixth bit: 1 indicates the entry is range deletion
    ///
    /// [Reference link](https://github.com/cockroachdb/pebble/blob/master/docs/rocksdb.md#range-deletions)
    const RANGE_DELETION = 0b00100000;
    /// Seventh bit: 1 indicates the entry is range set
    const RANGE_SET = 0b01000000;
    /// Eighth bit: 1 indicates the entry is range unset
    const RANGE_UNSET = 0b10000000;
  }
}

impl EntryFlags {
  pub(crate) const SIZE: usize = core::mem::size_of::<Self>();
}

#[derive(Debug)]
pub(crate) struct EncodedEntryMeta {
  pub(crate) packed_kvlen_size: usize,
  pub(crate) packed_kvlen: u64,
  pub(crate) entry_size: u32,
  pub(crate) klen: usize,
  pub(crate) vlen: usize,
  pub(crate) versioned: bool,
  batch: bool,
}

impl EncodedEntryMeta {
  #[inline]
  pub(crate) const fn new(key_len: usize, value_len: usize, versioned: bool) -> Self {
    // Cast to u32 is safe, because we already checked those values before calling this function.

    let len = merge_lengths(key_len as u32, value_len as u32);
    let len_size = encoded_u64_varint_len(len);
    let version_size = if versioned { VERSION_SIZE } else { 0 };
    let elen = RECORD_FLAG_SIZE as u32
      + EntryFlags::SIZE as u32
      + version_size as u32
      + len_size as u32
      + key_len as u32
      + value_len as u32
      + CHECKSUM_SIZE as u32;

    Self {
      packed_kvlen_size: len_size,
      batch: false,
      packed_kvlen: len,
      entry_size: elen,
      klen: key_len,
      vlen: value_len,
      versioned,
    }
  }

  #[inline]
  pub(crate) const fn batch(key_len: usize, value_len: usize, versioned: bool) -> Self {
    // Cast to u32 is safe, because we already checked those values before calling this function.

    let len = merge_lengths(key_len as u32, value_len as u32);
    let len_size = encoded_u64_varint_len(len);
    let version_size = if versioned { VERSION_SIZE } else { 0 };
    let elen = EntryFlags::SIZE as u32
      + version_size as u32
      + len_size as u32
      + key_len as u32
      + value_len as u32;

    Self {
      packed_kvlen_size: len_size,
      packed_kvlen: len,
      entry_size: elen,
      klen: key_len,
      vlen: value_len,
      versioned,
      batch: true,
    }
  }

  #[inline]
  pub(crate) const fn batch_zero(versioned: bool) -> Self {
    Self {
      packed_kvlen_size: 0,
      packed_kvlen: 0,
      entry_size: 0,
      klen: 0,
      vlen: 0,
      versioned,
      batch: true,
    }
  }

  #[inline]
  pub(crate) const fn entry_flag_offset(&self) -> usize {
    if self.batch {
      return 0;
    }

    RECORD_FLAG_SIZE
  }

  #[inline]
  pub(crate) const fn version_offset(&self) -> usize {
    self.entry_flag_offset() + EntryFlags::SIZE
  }

  #[inline]
  pub(crate) const fn key_offset(&self) -> usize {
    (if self.versioned {
      self.version_offset() + VERSION_SIZE
    } else {
      self.version_offset()
    }) + self.packed_kvlen_size
  }

  #[inline]
  pub(crate) const fn value_offset(&self) -> usize {
    self.key_offset() + self.klen
  }

  #[inline]
  pub(crate) const fn checksum_offset(&self) -> usize {
    if self.batch {
      self.value_offset() + self.vlen
    } else {
      self.entry_size as usize - CHECKSUM_SIZE
    }
  }
}

macro_rules! builder_ext {
  ($($name:ident),+ $(,)?) => {
    $(
      paste::paste! {
        impl<F> $name<F> {
          #[doc = "Creates a new `" $name "` with the given size and builder closure which requires `FnOnce`."]
          #[inline]
          pub const fn once<E>(size: usize, f: F) -> Self
          where
            F: for<'a> FnOnce(&mut dbutils::buffer::VacantBuffer<'a>) -> Result<usize, E>,
          {
            Self { size, f }
          }
        }
      }
    )*
  };
}

dbutils::builder!(
  /// A value builder for the wal, which requires the value size for accurate allocation and a closure to build the value.
  pub ValueBuilder;
  /// A key builder for the wal, which requires the key size for accurate allocation and a closure to build the key.
  pub KeyBuilder;
);

builder_ext!(ValueBuilder, KeyBuilder,);

/// The kind of the Write-Ahead Log.
///
/// Currently, there are two kinds of Write-Ahead Log:
/// 1. Plain: The Write-Ahead Log is plain, which means it does not support multiple versions.
/// 2. MultipleVersion: The Write-Ahead Log supports multiple versions.
#[derive(Debug, PartialEq, Eq)]
#[repr(u8)]
#[non_exhaustive]
pub enum Mode {
  /// The Write-Ahead Log is plain, which means it does not support multiple versions.
  Unique = 0,
  /// The Write-Ahead Log supports multiple versions.
  MultipleVersion = 1,
}

#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
impl TryFrom<u8> for Mode {
  type Error = crate::error::UnknownMode;

  #[inline]
  fn try_from(value: u8) -> Result<Self, Self::Error> {
    Ok(match value {
      0 => Self::Unique,
      1 => Self::MultipleVersion,
      _ => return Err(crate::error::UnknownMode(value)),
    })
  }
}

#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
impl Mode {
  #[inline]
  pub(crate) const fn display_created_err_msg(&self) -> &'static str {
    match self {
      Self::Unique => "created without multiple versions support",
      Self::MultipleVersion => "created with multiple versions support",
    }
  }

  #[inline]
  pub(crate) const fn display_open_err_msg(&self) -> &'static str {
    match self {
      Self::Unique => "opened without multiple versions support",
      Self::MultipleVersion => "opened with multiple versions support",
    }
  }
}

const U32_SIZE: usize = mem::size_of::<u32>();

/// The pointer to a record in the WAL.
#[derive(Debug, Clone, Copy)]
pub struct RecordPointer {
  offset: u32,
  len: u32,
}

impl RecordPointer {
  const SIZE: usize = mem::size_of::<Self>();

  #[inline]
  pub(crate) fn new(offset: u32, len: u32) -> Self {
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
}

impl Type for RecordPointer {
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

impl<'a> TypeRef<'a> for RecordPointer {
  #[inline]
  unsafe fn from_slice(src: &'a [u8]) -> Self {
    let offset = u32::from_le_bytes(src[..U32_SIZE].try_into().unwrap());
    let len = u32::from_le_bytes(src[U32_SIZE..Self::SIZE].try_into().unwrap());
    Self { offset, len }
  }
}

pub struct Pointer {
  offset: u32,
  len: u32,
}

impl Pointer {
  pub const SIZE: usize = U32_SIZE * 2;

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
    array[..4].copy_from_slice(&self.offset.to_le_bytes());
    array[4..].copy_from_slice(&self.len.to_le_bytes());
    array
  }

  /// # Panics
  /// Panics if the length of the slice is less than 8.
  #[inline]
  pub(crate) const fn from_slice(src: &[u8]) -> Self {
    let offset = u32::from_le_bytes([src[0], src[1], src[2], src[3]]);
    let len = u32::from_le_bytes([src[4], src[5], src[6], src[7]]);
    Self { offset, len }
  }
}
