//! An ordered Write-Ahead Log implementation for Rust.
#![doc = include_str!("../README.md")]
#![cfg_attr(not(any(feature = "std", test)), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]
#![allow(clippy::type_complexity)]

use core::{borrow::Borrow, cmp, marker::PhantomData, mem, slice};

#[doc(inline)]
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

pub use dbutils::{Ascend, CheapClone, Checksumer, Comparator, Crc32, Descend};

#[cfg(feature = "xxhash3")]
pub use dbutils::XxHash3;

#[cfg(feature = "xxhash64")]
pub use dbutils::XxHash64;

const STATUS_SIZE: usize = mem::size_of::<u8>();
const CHECKSUM_SIZE: usize = mem::size_of::<u64>();
const CURRENT_VERSION: u16 = 0;
const MAGIC_TEXT: [u8; 6] = *b"ordwal";
const MAGIC_TEXT_SIZE: usize = MAGIC_TEXT.len();
const MAGIC_VERSION_SIZE: usize = mem::size_of::<u16>();
const HEADER_SIZE: usize = MAGIC_TEXT_SIZE + MAGIC_VERSION_SIZE;

#[cfg(all(test, any(feature = "test-swmr", feature = "test-unsync")))]
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
pub use wal::{Builder, Wal};

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

/// Use to avoid the mutable borrow checker, for single writer multiple readers usecase.
struct UnsafeCellChecksumer<S>(core::cell::UnsafeCell<S>);

impl<S> UnsafeCellChecksumer<S> {
  #[inline]
  const fn new(checksumer: S) -> Self {
    Self(core::cell::UnsafeCell::new(checksumer))
  }
}

impl<S> UnsafeCellChecksumer<S>
where
  S: Checksumer,
{
  #[inline]
  fn update(&self, buf: &[u8]) {
    // SAFETY: the checksumer will not be invoked concurrently.
    unsafe { (*self.0.get()).update(buf) }
  }

  #[inline]
  fn reset(&self) {
    // SAFETY: the checksumer will not be invoked concurrently.
    unsafe { (*self.0.get()).reset() }
  }

  #[inline]
  fn digest(&self) -> u64 {
    unsafe { (*self.0.get()).digest() }
  }
}
