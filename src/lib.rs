//! An ordered Write-Ahead Log implementation for Rust.
#![doc = include_str!("../README.md")]
#![cfg_attr(not(any(feature = "std", test)), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]
#![allow(clippy::type_complexity)]

use core::{borrow::Borrow, marker::PhantomData, mem};

pub use among;
use among::Among;
use error::Error;
use rarena_allocator::{either::Either, Allocator, Buffer, Freelist, Options as ArenaOptions};

#[cfg(feature = "std")]
extern crate std;

pub use dbutils::{
  checksum::{self, Crc32},
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

#[cfg(all(
  test,
  any(
    all_tests,
    test_unsync_constructor,
    test_unsync_insert,
    test_unsync_get,
    test_unsync_iters,
    test_swmr_constructor,
    test_swmr_insert,
    test_swmr_get,
    test_swmr_iters,
    test_swmr_generic_constructor,
    test_swmr_generic_insert,
    test_swmr_generic_get,
    test_swmr_generic_iters,
  )
))]
#[macro_use]
mod tests;

/// Error types.
pub mod error;

mod buffer;
pub use buffer::*;

mod builder;
pub use builder::Builder;

mod entry;

/// Utilities.
pub mod utils;
use utils::*;

// mod wal;
// pub use wal::{Reader, Wal};

mod options;
pub use options::Options;

mod batch;

mod base;

mod mvcc;

mod generic;

/// A single writer multiple readers ordered write-ahead Log implementation.
pub mod swmr;

// /// An ordered write-ahead Log implementation.
// pub mod unsync;

/// Iterators for the WALs.
pub mod iter;

mod pointer;

mod sealed;

/// The mvcc version size.
const VERSION_SIZE: usize = core::mem::size_of::<u64>();

bitflags::bitflags! {
  /// The flags of the entry.
  struct Flags: u8 {
    /// First bit: 1 indicates committed, 0 indicates uncommitted
    const COMMITTED = 0b00000001;
    /// Second bit: 1 indicates batching, 0 indicates single entry
    const BATCHING = 0b00000010;
  }
}
