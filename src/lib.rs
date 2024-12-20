//! An ordered Write-Ahead Log implementation for Rust.
#![doc = include_str!("../README.md")]
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]
#![allow(clippy::type_complexity)]

use core::mem;

pub use among;

#[cfg(feature = "std")]
extern crate std;

#[cfg(not(feature = "std"))]
extern crate alloc as std;

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

const RECORD_FLAG_SIZE: usize = mem::size_of::<Flags>();
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

mod builder;
pub use builder::Builder;

/// Types
pub mod types;

mod options;
pub use options::Options;
pub use skl::KeySize;

/// Batch insertions related traits and structs.
pub mod batch;

/// A single writer multiple readers ordered write-ahead Log implementation.
mod swmr;
mod wal;
pub use swmr::*;

/// The memory table implementation.
pub mod memtable;

mod sealed;
pub use sealed::Immutable;

/// The utilities functions.
pub mod utils;

bitflags::bitflags! {
  /// The flags for each atomic write.
  struct Flags: u8 {
    /// First bit: 1 indicates committed, 0 indicates uncommitted
    const COMMITTED = 0b00000001;
    /// Second bit: 1 indicates batching, 0 indicates single entry
    const BATCHING = 0b00000010;
  }
}
