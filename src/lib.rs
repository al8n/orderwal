//! An ordered Write-Ahead Log implementation for Rust.
#![doc = include_str!("../README.md")]
#![cfg_attr(not(any(feature = "std", test)), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]

use core::{borrow::Borrow, cmp, marker::PhantomData, mem, slice};

use crossbeam_skiplist::SkipSet;
use rarena_allocator::Allocator;

#[cfg(not(any(feature = "std", feature = "alloc")))]
compile_error!("`orderwal` requires either the 'std' or 'alloc' feature to be enabled");

#[cfg(not(feature = "std"))]
extern crate alloc as std;

#[cfg(feature = "std")]
extern crate std;

const STATUS_SIZE: usize = mem::size_of::<u8>();
const KEY_LEN_SIZE: usize = mem::size_of::<u32>();
const VALUE_LEN_SIZE: usize = mem::size_of::<u32>();
const CHECKSUM_SIZE: usize = mem::size_of::<u64>();

/// An lock-free ordered write-ahead Log implementation.
pub mod sync;

/// An ordered write-ahead Log implementation.
pub mod unsync;

/// Error types.
pub mod error;

bitflags::bitflags! {
  /// The flags of the entry.
  struct Flags: u8 {
    /// First bit: 1 indicates committed, 0 indicates uncommitted
    const COMMITTED = 0b00000001;
    /// Second bit: 1 indicates tombstone, 0 indicates not a tombstone
    const TOMBSTONE = 0b00000010;
  }
}

/// The comparator trait, which is used to compare two byte slices.
pub trait Comparator {
  /// Compares two byte slices.
  fn compare(a: &[u8], b: &[u8]) -> cmp::Ordering;
}


struct Pointer<C> {
  /// The pointer to the start of the entry.
  ptr: *mut u8,
  /// The length of the key.
  key_len: usize,
  /// The length of the value.
  value_len: usize,
  _m: PhantomData<C>,
}

impl<C> Pointer<C> {
  #[inline]
  const fn as_key_slice(&self) -> &[u8] {
    if self.key_len == 0 {
      return &[];
    }

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    unsafe {
      slice::from_raw_parts(self.ptr.add(STATUS_SIZE + KEY_LEN_SIZE), self.key_len)
    }
  }

  #[inline]
  const fn as_value_slice(&self) -> &[u8] {
    if self.value_len == 0 {
      return &[];
    }

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    unsafe {
      slice::from_raw_parts(
        self.ptr.add(STATUS_SIZE + KEY_LEN_SIZE + self.key_len + VALUE_LEN_SIZE),
        self.value_len,
      )
    }
  }
}

impl<C: Comparator> PartialEq for Pointer<C> {
  fn eq(&self, other: &Self) -> bool {
    C::compare(self.as_key_slice(), other.as_key_slice()).is_eq()
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
    C::compare(self.as_key_slice(), other.as_key_slice())
  }
}

impl<C> Borrow<[u8]> for Pointer<C> {
  fn borrow(&self) -> &[u8] {
    self.as_key_slice()
  }
}

pub struct Options {
  maximum_key_size: u32,
  maximum_value_size: u32,
}

impl Default for Options {
  fn default() -> Self {
    Self::new()
  }
}

impl Options {
  /// Create a new `Options` instance.
  #[inline]
  pub const fn new() -> Self {
    Self {
      maximum_key_size: u16::MAX as u32,
      maximum_value_size: u32::MAX,
    }
  }

  /// Returns the maximum key length.
  /// 
  /// The default value is `u16::MAX`.
  /// 
  /// # Example
  /// 
  /// ```rust
  /// use orderwal::Options;
  /// 
  /// let options = Options::new().with_maximum_key_size(1024);
  /// assert_eq!(options.maximum_key_size(), 1024);
  /// ```
  #[inline]
  pub const fn maximum_key_size(&self) -> u32 {
    self.maximum_key_size
  }

  /// Returns the maximum value length.
  /// 
  /// The default value is `u32::MAX`.
  /// 
  /// # Example
  /// 
  /// ```rust
  /// use orderwal::Options;
  /// 
  /// let options = Options::new().with_maximum_value_size(1024);
  /// assert_eq!(options.maximum_value_size(), 1024);
  /// ```
  #[inline]
  pub const fn maximum_value_size(&self) -> u32 {
    self.maximum_value_size
  }

  /// Sets the maximum key length.
  /// 
  /// # Example
  /// 
  /// ```rust
  /// use orderwal::Options;
  /// 
  /// let options = Options::new().with_maximum_key_size(1024);
  /// assert_eq!(options.maximum_key_size(), 1024);
  /// ```
  #[inline]
  pub const fn with_maximum_key_size(mut self, size: u32) -> Self {
    self.maximum_key_size = size;
    self
  }

  /// Sets the maximum value length.
  /// 
  /// # Example
  /// 
  /// ```rust
  /// use orderwal::Options;
  /// 
  /// let options = Options::new().with_maximum_value_size(1024);
  /// assert_eq!(options.maximum_value_size(), 1024);
  /// ```
  #[inline]
  pub const fn with_maximum_value_size(mut self, size: u32) -> Self {
    self.maximum_value_size = size;
    self
  }
}


/// Ordered write-ahead log implementation.
// ```text
// +----------------------+-------------------------+--------------------+
// | magic text (4 bytes) | magic version (2 bytes) |  version (2 bytes) |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// |     flag (1 byte)    |    key len (4 bytes)    |    key (n bytes)   | value len (4 bytes) | value (n bytes) | checksum (8 bytes) |
// +----------------------+-------------------------+--------------------+---------------------+-----------------|--------------------+
// |     flag (1 byte)    |    key len (4 bytes)    |    key (n bytes)   | value len (4 bytes) | value (n bytes) | checksum (8 bytes) |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// |     flag (1 byte)    |    key len (4 bytes)    |    key (n bytes)   | value len (4 bytes) | value (n bytes) | checksum (8 bytes) |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// |         ...          |            ...          |         ...        |          ...        |        ...      |         ...        |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// |         ...          |            ...          |         ...        |          ...        |        ...      |         ...        |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// ```
pub struct OrderedWal<A, C> {
  arena: A,
  map: SkipSet<Pointer<C>>,
  opts: Options,
}

impl<A: Allocator, C: Comparator> OrderedWal<A, C> {
  /// Returns `true` if the WAL contains the specified key.
  #[inline]
  pub fn contains_key(&self, key: &[u8]) -> bool {
    self.map.contains(key)
  }

  /// Returns the value associated with the key.
  #[inline]
  pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
    self.map.get(key).map(|ent| {
      // SAFETY: the `ptr` is a valid pointer and will never be released when the inner arena is dropped.
      unsafe {
        slice::from_raw_parts(
          ent.ptr.add(STATUS_SIZE + KEY_LEN_SIZE + ent.key_len + VALUE_LEN_SIZE),
          ent.value_len,
        )
      }
    })
  }

  /// Inserts a new entry into the WAL.
  #[inline]
  pub fn insert(&mut self, key: &[u8], value: &[u8]) -> std::io::Result<()> {
    let key_len = key.len();
    let value_len = value.len();
    let len = STATUS_SIZE + KEY_LEN_SIZE + key_len + VALUE_LEN_SIZE + value_len + CHECKSUM_SIZE;
    let ptr = self.arena.alloc_bytes(len);

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    unsafe {
      let mut offset = 0;
      ptr.add(offset).copy_from_slice(&[Flags::empty().bits]);
      offset += STATUS_SIZE;
      ptr.add(offset).copy_from_slice(&(key_len as u32).to_le_bytes());
      offset += KEY_LEN_SIZE;
      ptr.add(offset).copy_from_slice(key);
      offset += key_len;
      ptr.add(offset).copy_from_slice(&(value_len as u32).to_le_bytes());
      offset += VALUE_LEN_SIZE;
      ptr.add(offset).copy_from_slice(value);
      offset += value_len;
      ptr.add(offset).copy_from_slice(&0u64.to_le_bytes());
    }

    self.map.insert(Pointer {
      ptr,
      key_len,
      value_len,
      _m: PhantomData,
    });

    Ok(())
  }
}


impl<A, C> OrderedWal<A, C> {
  fn check(&self, key: &[u8], val: &[u8]) -> std::io::Result<()> {
    if self.opts.maximum_key_size < key.len() as u32 {
      return Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "key size exceeds the maximum key size",
      ));
    }

    if self.opts.maximum_value_size < val.len() as u32 {
      return Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "value size exceeds the maximum value size",
      ));
    }
    todo!()
  }
}