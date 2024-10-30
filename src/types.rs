use dbutils::leb128::encoded_u64_varint_len;
pub use dbutils::{
  buffer::{BufWriter, BufWriterOnce, VacantBuffer},
  traits::MaybeStructured,
};

use crate::{utils::merge_lengths, CHECKSUM_SIZE, RECORD_FLAG_SIZE, VERSION_SIZE};

pub(crate) mod base;
pub(crate) mod multiple_version;

const ENTRY_FLAGS_SIZE: usize = core::mem::size_of::<EntryFlags>();

/// The kind of the Write-Ahead Log.
///
/// Currently, there are two kinds of Write-Ahead Log:
/// 1. Plain: The Write-Ahead Log is plain, which means it does not support multiple versions.
/// 2. MultipleVersion: The Write-Ahead Log supports multiple versions.
#[derive(Debug, PartialEq, Eq)]
#[repr(u8)]
#[non_exhaustive]
pub enum Kind {
  /// The Write-Ahead Log is plain, which means it does not support multiple versions.
  Plain = 0,
  /// The Write-Ahead Log supports multiple versions.
  MultipleVersion = 1,
}

impl TryFrom<u8> for Kind {
  type Error = crate::error::UnknownKind;

  #[inline]
  fn try_from(value: u8) -> Result<Self, Self::Error> {
    Ok(match value {
      0 => Self::Plain,
      1 => Self::MultipleVersion,
      _ => return Err(crate::error::UnknownKind(value)),
    })
  }
}

bitflags::bitflags! {
  /// The flags for each entry.
  #[derive(Debug, Copy, Clone)]
  pub struct EntryFlags: u8 {
    /// First bit: 1 indicates removed
    const REMOVED = 0b00000001;
    /// Second bit: 1 indicates the key is pointer
    const POINTER = 0b00000010;
    /// Third bit: 1 indicates the entry contains a version
    const VERSIONED = 0b00000100;
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
      + len_size as u32
      + ENTRY_FLAGS_SIZE as u32
      + version_size as u32
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
    let elen = len_size as u32
      + EntryFlags::SIZE as u32
      + version_size as u32
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
      return self.packed_kvlen_size;
    }

    RECORD_FLAG_SIZE + self.packed_kvlen_size
  }

  #[inline]
  pub(crate) const fn version_offset(&self) -> usize {
    self.entry_flag_offset() + ENTRY_FLAGS_SIZE
  }

  #[inline]
  pub(crate) const fn key_offset(&self) -> usize {
    if self.versioned {
      self.version_offset() + VERSION_SIZE
    } else {
      self.version_offset()
    }
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
