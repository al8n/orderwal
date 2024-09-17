//! An ordered Write-Ahead Log implementation for Rust.
#![doc = include_str!("../README.md")]
#![cfg_attr(not(any(feature = "std", test)), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]
#![allow(clippy::type_complexity)]

use core::{borrow::Borrow, cmp, marker::PhantomData, mem, slice};

pub use among;
use among::Among;
use crossbeam_skiplist::SkipSet;
use error::Error;
use rarena_allocator::{
  either::{self, Either},
  Allocator, ArenaOptions, Freelist, Memory, MmapOptions,
};

pub use rarena_allocator::OpenOptions;

#[cfg(feature = "std")]
extern crate std;

pub use dbutils::{
  checksum::{BuildChecksumer, Checksumer, Crc32},
  Ascend, CheapClone, Comparator, Descend,
};

#[cfg(feature = "xxhash3")]
#[cfg_attr(docsrs, doc(cfg(feature = "xxhash3")))]
pub use dbutils::checksum::XxHash3;

#[cfg(feature = "xxhash64")]
#[cfg_attr(docsrs, doc(cfg(feature = "xxhash64")))]
pub use dbutils::checksum::XxHash64;

const STATUS_SIZE: usize = mem::size_of::<u8>();
const CHECKSUM_SIZE: usize = mem::size_of::<u64>();
const CURRENT_VERSION: u16 = 0;
const MAGIC_TEXT: [u8; 6] = *b"ordwal";
const MAGIC_TEXT_SIZE: usize = MAGIC_TEXT.len();
const MAGIC_VERSION_SIZE: usize = mem::size_of::<u16>();
const HEADER_SIZE: usize = MAGIC_TEXT_SIZE + MAGIC_VERSION_SIZE;

// #[cfg(all(
//   test,
//   any(
//     all_tests,
//     test_unsync_constructor,
//     test_unsync_insert,
//     test_unsync_get,
//     test_unsync_iters,
//     test_swmr_constructor,
//     test_swmr_insert,
//     test_swmr_get,
//     test_swmr_iters,
//     test_swmr_generic_constructor,
//     test_swmr_generic_insert,
//     test_swmr_generic_get,
//     test_swmr_generic_iters,
//   )
// ))]
#[cfg(test)]
#[macro_use]
mod tests;

/// Error types.
pub mod error;

mod buffer;
pub use buffer::*;

/// Utilities.
pub mod utils;
use utils::*;

mod wal;
pub use wal::{
  Batch, BatchWithBuilders, BatchWithKeyBuilder, BatchWithValueBuilder, Builder, ImmutableWal, Wal,
};

mod options;
pub use options::Options;

/// A single writer multiple readers ordered write-ahead Log implementation.
pub mod swmr;

/// An ordered write-ahead Log implementation.
pub mod unsync;

bitflags::bitflags! {
  /// The flags of the entry.
  struct Flags: u8 {
    /// First bit: 1 indicates committed, 0 indicates uncommitted
    const COMMITTED = 0b00000001;
    /// Second bit: 1 indicates batching, 0 indicates single entry
    const BATCHING = 0b00000010;
  }
}

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
  const fn new(key_len: usize, value_len: usize, ptr: *const u8, cmp: C) -> Self {
    Self {
      ptr,
      key_len,
      value_len,
      cmp,
    }
  }

  #[inline]
  const fn as_key_slice<'a>(&self) -> &'a [u8] {
    if self.key_len == 0 {
      return &[];
    }

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    unsafe { slice::from_raw_parts(self.ptr, self.key_len) }
  }

  #[inline]
  const fn as_value_slice<'a, 'b: 'a>(&'a self) -> &'b [u8] {
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

/// An entry in the write-ahead log.
pub struct Entry<K, V, C> {
  key: K,
  value: V,
  pointer: Option<Pointer<C>>,
}

impl<K, V, C> Entry<K, V, C> {
  /// Creates a new entry.
  #[inline]
  pub const fn new(key: K, value: V) -> Self {
    Self {
      key,
      value,
      pointer: None,
    }
  }

  /// Returns the key.
  #[inline]
  pub const fn key(&self) -> &K {
    &self.key
  }

  /// Returns the value.
  #[inline]
  pub const fn value(&self) -> &V {
    &self.value
  }

  /// Consumes the entry and returns the key and value.
  #[inline]
  pub fn into_components(self) -> (K, V) {
    (self.key, self.value)
  }
}

/// An entry in the write-ahead log.
pub struct EntryWithKeyBuilder<KB, V, C> {
  kb: KeyBuilder<KB>,
  value: V,
  pointer: Option<Pointer<C>>,
}

impl<KB, V, C> EntryWithKeyBuilder<KB, V, C> {
  /// Creates a new entry.
  #[inline]
  pub const fn new(kb: KeyBuilder<KB>, value: V) -> Self {
    Self {
      kb,
      value,
      pointer: None,
    }
  }

  /// Returns the key.
  #[inline]
  pub const fn key_builder(&self) -> &KeyBuilder<KB> {
    &self.kb
  }

  /// Returns the value.
  #[inline]
  pub const fn value(&self) -> &V {
    &self.value
  }

  /// Consumes the entry and returns the key and value.
  #[inline]
  pub fn into_components(self) -> (KeyBuilder<KB>, V) {
    (self.kb, self.value)
  }
}

/// An entry in the write-ahead log.
pub struct EntryWithValueBuilder<K, VB, C> {
  key: K,
  vb: ValueBuilder<VB>,
  pointer: Option<Pointer<C>>,
}

impl<K, VB, C> EntryWithValueBuilder<K, VB, C> {
  /// Creates a new entry.
  #[inline]
  pub const fn new(key: K, vb: ValueBuilder<VB>) -> Self {
    Self {
      key,
      vb,
      pointer: None,
    }
  }

  /// Returns the key.
  #[inline]
  pub const fn value_builder(&self) -> &ValueBuilder<VB> {
    &self.vb
  }

  /// Returns the value.
  #[inline]
  pub const fn key(&self) -> &K {
    &self.key
  }

  /// Consumes the entry and returns the key and value.
  #[inline]
  pub fn into_components(self) -> (K, ValueBuilder<VB>) {
    (self.key, self.vb)
  }
}

/// An entry in the write-ahead log.
pub struct EntryWithBuilders<KB, VB, C> {
  kb: KeyBuilder<KB>,
  vb: ValueBuilder<VB>,
  pointer: Option<Pointer<C>>,
}

impl<KB, VB, C> EntryWithBuilders<KB, VB, C> {
  /// Creates a new entry.
  #[inline]
  pub const fn new(kb: KeyBuilder<KB>, vb: ValueBuilder<VB>) -> Self {
    Self {
      kb,
      vb,
      pointer: None,
    }
  }

  /// Returns the value builder.
  #[inline]
  pub const fn value_builder(&self) -> &ValueBuilder<VB> {
    &self.vb
  }

  /// Returns the key builder.
  #[inline]
  pub const fn key_builder(&self) -> &KeyBuilder<KB> {
    &self.kb
  }

  /// Consumes the entry and returns the key and value.
  #[inline]
  pub fn into_components(self) -> (KeyBuilder<KB>, ValueBuilder<VB>) {
    (self.kb, self.vb)
  }
}
