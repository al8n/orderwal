//! An ordered Write-Ahead Log implementation for Rust.
#![doc = include_str!("../README.md")]
#![cfg_attr(not(any(feature = "std", test)), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]

use core::{borrow::Borrow, cmp, marker::PhantomData, mem, slice};

use crossbeam_skiplist::SkipSet;
use rarena_allocator::{Allocator, ArenaOptions, Freelist, Memory, MmapOptions, OpenOptions};

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
const FIXED_RECORD_SIZE: usize = STATUS_SIZE + KEY_LEN_SIZE + VALUE_LEN_SIZE + CHECKSUM_SIZE;
const CURRENT_VERSION: u16 = 0;

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
  }
}

/// The comparator trait, which is used to compare two byte slices.
pub trait Comparator {
  /// Compares two byte slices.
  fn compare(a: &[u8], b: &[u8]) -> cmp::Ordering;
}


struct Pointer<C> {
  /// The pointer to the start of the entry.
  ptr: *const u8,
  /// The length of the key.
  key_len: usize,
  /// The length of the value.
  value_len: usize,
  _m: PhantomData<C>,
}

unsafe impl<C> Send for Pointer<C> {}

impl<C> Pointer<C> {
  #[inline]
  const fn new(key_len: usize, value_len: usize, ptr: *const u8) -> Self {
    Self {
      ptr,
      key_len,
      value_len,
      _m: PhantomData,
    }
  }

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
  const fn as_value_slice<'a, 'b: 'a>(&'a self) -> &'b [u8] {
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

/// Options for the WAL.
pub struct Options {
  maximum_key_size: u32,
  maximum_value_size: u32,
  read_only: bool,
  sync_on_write: bool,
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
      read_only: false,
      sync_on_write: true,
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

  /// Returns `true` if the WAL is read-only.
  ///
  /// # Example
  /// 
  /// ```rust
  /// use orderwal::Options;
  /// 
  /// let options = Options::new().with_read_only(true);
  /// assert_eq!(options.read_only(), true);
  /// ```
  #[inline]
  pub const fn read_only(&self) -> bool {
    self.read_only
  }

  /// Returns `true` if the WAL syncs on write.
  /// 
  /// The default value is `true`.
  /// 
  /// # Example
  /// 
  /// ```rust
  /// use orderwal::Options;
  /// 
  /// let options = Options::new();
  /// assert_eq!(options.sync_on_write(), true);
  /// ```
  #[inline]
  pub const fn sync_on_write(&self) -> bool {
    self.sync_on_write
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

  /// Sets the WAL to be opened in read-only mode.
  /// 
  /// # Example
  /// 
  /// ```rust
  /// use orderwal::Options;
  /// 
  /// let options = Options::new().with_read_only(true);
  /// assert_eq!(options.read_only(), true);
  /// ```
  #[inline]
  pub const fn with_read_only(mut self, ro: bool) -> Self {
    self.read_only = ro;
    self
  }

  /// Sets the WAL to sync on write.
  /// 
  /// The default value is `true`.
  /// 
  /// # Example
  /// 
  /// ```rust
  /// use orderwal::Options;
  /// 
  /// let options = Options::new().with_sync_on_write(false);
  /// assert_eq!(options.sync_on_write(), false);
  /// ```
  #[inline]
  pub const fn with_sync_on_write(mut self, sync: bool) -> Self {
    self.sync_on_write = sync;
    self
  }
}

/// The checksum trait, which is used to calculate the checksum of a byte slice.
pub trait Checksumer {
  /// Calculates the checksum of the byte slice.
  fn checksum(data: &[u8]) -> u64;
}

struct OrderWalCore<A, C> {
  arena: A,
  map: SkipSet<Pointer<C>>,
  opts: Options,
}


/// A single writer multiple readers ordered write-ahead log implementation.
/// 
/// Only the first instance of the WAL can write to the log, while the rest can only read from the log.
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
pub struct OrderWal<A, C, S> {
  core: std::sync::Arc<OrderWalCore<A, C>>,
  ro: bool,
  _s: PhantomData<S>,
}

impl<A, C, S> Clone for OrderWal<A, C, S> {
  fn clone(&self) -> Self {
    Self {
      core: self.core.clone(),
      ro: true,
      _s: PhantomData,
    }
  }
}

impl<A: Allocator, C: Comparator, S> OrderWal<A, C, S> {
  /// Returns `true` if the WAL contains the specified key.
  #[inline]
  pub fn contains_key(&self, key: &[u8]) -> bool {
    self.core.map.contains(key)
  }

  /// Returns the value associated with the key.
  #[inline]
  pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
    self.core.map.get(key).map(|ent| ent.as_value_slice())
  }
}

impl<A: Allocator, C: Comparator + 'static, S: Checksumer> OrderWal<A, C, S> {
  /// Inserts a new entry into the WAL.
  #[inline]
  pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), error::Error> {
    if self.ro || self.core.opts.read_only {
      return Err(error::Error::read_only());
    }

    self.check(key, value)?;

    let key_len = key.len();
    let value_len = value.len();
    let elen = entry_size(key_len as u32, value_len as u32);
    let buf = self.core.arena.alloc_bytes(elen);

    match buf {
      Err(e) => Err(match e {
        rarena_allocator::Error::InsufficientSpace { requested, available } => error::Error::insufficient_space(requested, available),
        rarena_allocator::Error::ReadOnly => error::Error::read_only(),
        rarena_allocator::Error::LargerThanPageSize { .. } => unreachable!(),
      }),
      Ok(mut buf) => {
        unsafe {
          // We allocate the buffer with the exact size, so it's safe to write to the buffer.
          buf.put_u8_unchecked(Flags::empty().bits());
          buf.put_u32_le_unchecked(key_len as u32);
          buf.put_slice_unchecked(key);
          buf.put_u32_le_unchecked(value_len as u32);
          buf.put_slice_unchecked(value);

          let cks = S::checksum(&buf);
          buf.put_u64_le_unchecked(cks);

          // commit the entry
          buf[0] |= Flags::COMMITTED.bits();

          if self.core.opts.sync_on_write {
            self.core.arena.flush_range(buf.offset(), elen as usize)?;
          }
          buf.detach();
          self.core.map.insert(Pointer::new(key_len, value_len, buf.as_ptr()));
          Ok(())
        }
      },
    }
  }
}

impl<A: Allocator, C, S> OrderWal<A, C, S> {
  /// Creates a new allocator backed by a mmap with the given options.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::{swmr::OrderWal, MmapOptions, Options, OpenOptions};
  ///
  /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
  /// # std::fs::remove_file(&path);
  ///
  /// let open_options = OpenOptions::default().create_new(Some(100)).read(true).write(true);
  /// let mmap_options = MmapOptions::new();
  /// let arena = OrderWal::map_mut(&path, ArenaOptions::new(), open_options, mmap_options).unwrap();
  ///
  /// # std::fs::remove_file(path);
  /// ```
  #[inline]
  pub fn map_mut<P: AsRef<std::path::Path>>(
    path: P,
    opts: Options,
    open_options: OpenOptions,
    mmap_options: MmapOptions,
  ) -> Result<Self, error::Error> {
    Ok(Self {
      core: std::sync::Arc::new(OrderWalCore {
        arena: A::map_mut(
          path,
          ArenaOptions::new().with_freelist(Freelist::None).with_magic_version(CURRENT_VERSION),
          open_options,
          mmap_options
        )?,
        map: SkipSet::new(),
        opts,
      }),
      ro: false,
      _s: PhantomData,
    })
  }

  /// Flushes the to disk.
  #[inline]
  pub fn flush(&self) -> Result<(), error::Error> {
    if self.ro || self.core.opts.read_only {
      return Err(error::Error::read_only());
    }

    self.core.arena.flush().map_err(Into::into)
  }

  /// Flushes the to disk.
  #[inline]
  pub fn flush_async(&self) -> Result<(), error::Error> {
    if self.ro || self.core.opts.read_only {
      return Err(error::Error::read_only());
    }

    self.core.arena.flush_async().map_err(Into::into)
  }
}

impl<A, C, S> OrderWal<A, C, S> {
  #[inline]
  fn check(&self, key: &[u8], val: &[u8]) -> Result<(), error::Error> {
    let klen = key.len();
    let vlen = val.len();
    let elen = klen as u64 + vlen as u64;

    if self.core.opts.maximum_key_size < klen as u32 {
      return Err(error::Error::key_too_large(klen as u32, self.core.opts.maximum_key_size));
    }

    if self.core.opts.maximum_value_size < vlen as u32 {
      return Err(error::Error::value_too_large(vlen as u32, self.core.opts.maximum_value_size));
    }

    if elen + FIXED_RECORD_SIZE as u64 > u32::MAX as u64 {
      return Err(error::Error::entry_too_large(
        elen,
        min_u64(self.core.opts.maximum_key_size as u64 + self.core.opts.maximum_value_size as u64, u32::MAX as u64),
      ));
    }

    Ok(())
  }
}

#[inline]
const fn min_u64(a: u64, b: u64) -> u64 {
  if a < b {
    a
  } else {
    b
  }
}

#[inline]
const fn entry_size(key_len: u32, value_len: u32) -> u32 {
  STATUS_SIZE as u32 + KEY_LEN_SIZE as u32 + key_len + VALUE_LEN_SIZE as u32 + value_len + CHECKSUM_SIZE as u32
}