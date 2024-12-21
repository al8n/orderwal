use core::{
  marker::PhantomData,
  mem,
  ops::{Bound, RangeBounds},
};

use dbutils::error::InsufficientBuffer;
use ref_cast::RefCast as _;
use sealed::Pointee;

use crate::utils::split_lengths;

use super::{CHECKSUM_SIZE, RECORD_FLAG_SIZE, VERSION_SIZE};

pub use dbutils::{
  buffer::{BufWriter, VacantBuffer},
  types::{Type, TypeRef},
};

mod mode;
mod raw;
pub(crate) use mode::sealed;
pub use mode::{Dynamic, Generic, TypeMode};
pub(crate) use raw::*;

#[doc(hidden)]
#[derive(ref_cast::RefCast)]
#[repr(transparent)]
pub struct Query<Q: ?Sized>(pub(crate) Q);

pub(crate) struct QueryRange<Q: ?Sized, R> {
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
    const RANGE_DELETION = 0b00010000;
    /// Sixth bit: 1 indicates the entry is range deletion
    ///
    /// [Reference link](https://github.com/cockroachdb/pebble/blob/master/docs/rocksdb.md#range-deletions)
    const RANGE_SET = 0b00100000;
    /// Seventh bit: 1 indicates the entry is range set
    const RANGE_UNSET = 0b01000000;
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
  pub(crate) batch: bool,
}

impl EncodedEntryMeta {
  #[inline]
  pub(crate) const fn placeholder() -> Self {
    Self {
      packed_kvlen_size: 0,
      packed_kvlen: 0,
      entry_size: 0,
      klen: 0,
      vlen: 0,
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
    self.version_offset() + VERSION_SIZE + self.packed_kvlen_size
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

#[derive(Debug)]
pub(crate) struct EncodedRangeEntryMeta {
  pub(crate) packed_kvlen_size: usize,
  pub(crate) packed_kvlen: u64,
  pub(crate) entry_size: u32,
  pub(crate) range_key_len: u64,
  pub(crate) range_key_len_size: usize,
  pub(crate) total_range_key_size: usize,
  /// Include Bound marker byte
  pub(crate) start_key_len: usize,
  /// Include Bound marker byte
  pub(crate) end_key_len: usize,
  pub(crate) vlen: usize,
  pub(crate) batch: bool,
}

impl EncodedRangeEntryMeta {
  #[inline]
  pub(crate) const fn placeholder() -> Self {
    Self {
      packed_kvlen_size: 0,
      packed_kvlen: 0,
      entry_size: 0,
      range_key_len: 0,
      range_key_len_size: 0,
      total_range_key_size: 0,
      start_key_len: 0,
      end_key_len: 0,
      vlen: 0,
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
  pub(crate) const fn start_key_offset(&self) -> usize {
    self.range_key_offset() + self.range_key_len_size
  }

  #[inline]
  pub(crate) const fn end_key_offset(&self) -> usize {
    self.start_key_offset() + self.start_key_len
  }

  #[inline]
  pub(crate) const fn range_key_offset(&self) -> usize {
    self.version_offset() + VERSION_SIZE + self.packed_kvlen_size
  }

  #[inline]
  pub(crate) const fn value_offset(&self) -> usize {
    self.range_key_offset() + self.total_range_key_size
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

  /// Returns the offset of the record.
  #[inline]
  pub const fn offset(&self) -> usize {
    self.offset as usize
  }

  /// Returns the size of the record.
  #[inline]
  pub const fn size(&self) -> usize {
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

/// A pointer points to a byte slice in the WAL.
pub struct Pointer {
  offset: u32,
  len: u32,
}

impl Pointer {
  /// The encoded size of the pointer.
  pub const SIZE: usize = U32_SIZE * 2;

  #[inline]
  pub(crate) const fn new(offset: u32, len: u32) -> Self {
    Self { offset, len }
  }

  /// Returns the offset to the underlying file of the pointer.
  #[inline]
  pub const fn offset(&self) -> usize {
    self.offset as usize
  }

  /// Returns the size of the byte slice of the pointer.
  #[inline]
  pub const fn size(&self) -> usize {
    self.len as usize
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
