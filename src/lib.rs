//! An ordered Write-Ahead Log implementation for Rust.
#![doc = include_str!("../README.md")]
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]
#![allow(clippy::type_complexity)]

use core::mem;

#[cfg(feature = "std")]
extern crate std;

#[cfg(not(feature = "std"))]
extern crate alloc as std;

pub use among;
pub use dbutils::{
  checksum::{self, Crc32},
  equivalent::{Comparable, ComparableRangeBounds, Equivalent},
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
const WAL_KIND_SIZE: usize = mem::size_of::<types::Mode>();
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

/// a
pub mod memtable;

// /// Generic ordered write-ahead log implementation.
// pub mod generic;

/// The utilities functions.
pub mod utils;

/// A marker trait which indicates that such pointer has a version.
pub trait WithVersion {
  /// The version.
  fn version(&self) -> u64;
}

/// A marker trait which indicates that such pointer does not have a version.
pub trait WithoutVersion {}

/// A marker trait which indicates that such WAL is immutable.
pub trait Immutable {}

/// State of the entry.
pub trait State<'a>: sealed::Sealed<'a> {}

impl<'a, T: sealed::Sealed<'a>> State<'a> for T {}

pub use skl::{Active, MaybeTombstone};

mod sealed {
  pub trait Sealed<'a>: skl::State<'a>
  where
    Self: 'a,
  {
  }

  impl<'a, T: skl::State<'a> + 'a> Sealed<'a> for T {}
}
