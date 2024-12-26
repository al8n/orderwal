//! An ordered Write-Ahead Log implementation for Rust.
#![doc = include_str!("../README.md")]
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]
#![allow(clippy::type_complexity)]

#[cfg(feature = "std")]
extern crate std;

#[cfg(not(feature = "std"))]
extern crate alloc as std;

pub use among;
pub use builder::Builder;
pub use dbutils::{checksum, equivalent, equivalentor, state};

pub use options::Options;

use core::mem;

const RECORD_FLAG_SIZE: usize = mem::size_of::<types::Flags>();
const CHECKSUM_SIZE: usize = mem::size_of::<u64>();
const CURRENT_VERSION: u16 = 0;
const MAGIC_TEXT: [u8; 6] = *b"ordwal";
const MAGIC_TEXT_SIZE: usize = MAGIC_TEXT.len();
const MAGIC_VERSION_SIZE: usize = mem::size_of::<u16>();
const HEADER_SIZE: usize = MAGIC_TEXT_SIZE + MAGIC_VERSION_SIZE;
/// The mvcc version size.
const VERSION_SIZE: usize = mem::size_of::<u64>();

/// Batch insertions related traits and structs.
pub mod batch;

/// Error types.
pub mod error;

pub(crate) mod swmr;

mod builder;
mod log;
mod options;

/// Types
pub mod types;

/// Dynamic ordered write-ahead log implementation.
pub mod dynamic;

/// Memory table related traits and structs.
pub mod memtable;

/// Generic ordered write-ahead log implementation.
pub mod generic;

/// The utilities functions.
pub mod utils;

/// A marker trait which indicates that such WAL is immutable.
pub trait Immutable {}
