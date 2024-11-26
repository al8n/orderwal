//! An ordered Write-Ahead Log implementation for Rust.
#![doc = include_str!("../README.md")]
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]
#![allow(clippy::type_complexity)]

use core::{mem, slice};

#[cfg(feature = "std")]
extern crate std;

#[cfg(not(feature = "std"))]
extern crate alloc as std;

pub use {
  among,
  dbutils::{
    checksum::{self, Crc32},
    equivalent::{Comparable, ComparableRangeBounds, Equivalent},
  },
};

#[cfg(feature = "xxhash3")]
#[cfg_attr(docsrs, doc(cfg(feature = "xxhash3")))]
pub use dbutils::checksum::XxHash3;

#[cfg(feature = "xxhash64")]
#[cfg_attr(docsrs, doc(cfg(feature = "xxhash64")))]
pub use dbutils::checksum::XxHash64;

pub use options::Options;
pub use skl::KeySize;

const RECORD_FLAG_SIZE: usize = mem::size_of::<types::Flags>();
const CHECKSUM_SIZE: usize = mem::size_of::<u64>();
const CURRENT_VERSION: u16 = 0;
const MAGIC_TEXT: [u8; 5] = *b"order";
const MAGIC_TEXT_SIZE: usize = MAGIC_TEXT.len();
const WAL_KIND_SIZE: usize = mem::size_of::<types::Kind>();
const MAGIC_VERSION_SIZE: usize = mem::size_of::<u16>();
const HEADER_SIZE: usize = MAGIC_TEXT_SIZE + WAL_KIND_SIZE + MAGIC_VERSION_SIZE;
/// The mvcc version size.
const VERSION_SIZE: usize = mem::size_of::<u64>();

/// Error types.
pub mod error;

mod options;
mod types;

/// Dynamic ordered write-ahead log implementation.
pub mod dynamic;

// /// Generic ordered write-ahead log implementation.
// pub mod generic;

/// The utilities functions.
pub mod utils;

/// A marker trait which indicates that such pointer has a version.
pub trait WithVersion {}

/// A marker trait which indicates that such pointer does not have a version.
pub trait WithoutVersion {}

/// A marker trait which indicates that such WAL is immutable.
pub trait Immutable {}

#[derive(Clone, Copy)]
struct Pointer {
  offset: u32,
  len: u32,
}

impl Pointer {
  #[inline]
  const fn new(offset: u32, len: u32) -> Self {
    Self { offset, len }
  }
}

struct WalComparator<P: ?Sized, C> {
  /// The start pointer of the parent ARENA.
  ptr: *const u8,
  cmp: C,
  _p: core::marker::PhantomData<P>,
}

impl<P: ?Sized, C> Clone for WalComparator<P, C>
where
  C: Clone
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ptr: self.ptr,
      cmp: self.cmp.clone(),
      _p: core::marker::PhantomData,
    }
  }
}

impl<P: ?Sized, C> Copy for WalComparator<P, C> where C: Copy {}

impl<P: ?Sized, C> core::fmt::Debug for WalComparator<P, C>
where
  C: core::fmt::Debug,
{
  #[inline]
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("WalComparator")
      .field("ptr", &self.ptr)
      .field("cmp", &self.cmp)
      .finish()
  }
}

impl<P, C> dbutils::equivalentor::Equivalentor for WalComparator<P, C>
where
  P: ?Sized,
  C: dbutils::equivalentor::Equivalentor,
{
  #[inline]
  fn equivalent(&self, a: &[u8], b: &[u8]) -> bool {
    let aoffset = u32::from_le_bytes(a[0..4].try_into().unwrap()) as usize;
    let alen = u32::from_le_bytes(a[4..8].try_into().unwrap()) as usize;

    let boffset = u32::from_le_bytes(b[0..4].try_into().unwrap()) as usize;
    let blen = u32::from_le_bytes(b[4..8].try_into().unwrap()) as usize;

    unsafe {
      let a = slice::from_raw_parts(self.ptr.add(aoffset), alen);
      let b = slice::from_raw_parts(self.ptr.add(boffset), blen);
      self.cmp.equivalent(a, b)
    }
  }
}

impl<P, C> dbutils::equivalentor::Comparator for WalComparator<P, C>
where
  P: ?Sized,
  C: dbutils::equivalentor::Comparator,
{
  #[inline]
  fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
    let aoffset = u32::from_le_bytes(a[0..4].try_into().unwrap()) as usize;
    let alen = u32::from_le_bytes(a[4..8].try_into().unwrap()) as usize;

    let boffset = u32::from_le_bytes(b[0..4].try_into().unwrap()) as usize;
    let blen = u32::from_le_bytes(b[4..8].try_into().unwrap()) as usize;

    unsafe {
      let a = slice::from_raw_parts(self.ptr.add(aoffset), alen);
      let b = slice::from_raw_parts(self.ptr.add(boffset), blen);
      self.cmp.compare(a, b)
    }
  }
}