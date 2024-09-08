//! An ordered Write-Ahead Log implementation for Rust.
#![doc = include_str!("../README.md")]
#![cfg_attr(not(any(feature = "std", test)), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]
#![allow(clippy::type_complexity)]

use core::{borrow::Borrow, cmp, marker::PhantomData, mem, slice};

use crossbeam_skiplist::SkipSet;
use rarena_allocator::{
  either, Allocator, ArenaOptions, Freelist, Memory, MmapOptions, OpenOptions,
};

#[cfg(not(any(feature = "std", feature = "alloc")))]
compile_error!("`orderwal` requires either the 'std' or 'alloc' feature to be enabled");

#[cfg(not(feature = "std"))]
extern crate alloc as std;

#[cfg(feature = "std")]
extern crate std;

pub use dbutils::{Ascend, CheapClone, Checksumer, Comparator, Crc32, Descend};

#[cfg(feature = "xxhash3")]
pub use dbutils::XxHash3;

#[cfg(feature = "xxhash64")]
pub use dbutils::XxHash64;

const STATUS_SIZE: usize = mem::size_of::<u8>();
const KEY_LEN_SIZE: usize = mem::size_of::<u32>();
const VALUE_LEN_SIZE: usize = mem::size_of::<u32>();
const CHECKSUM_SIZE: usize = mem::size_of::<u64>();
const FIXED_RECORD_SIZE: usize = STATUS_SIZE + KEY_LEN_SIZE + VALUE_LEN_SIZE + CHECKSUM_SIZE;
const CURRENT_VERSION: u16 = 0;
const MAGIC_TEXT: [u8; 6] = *b"ordwal";
const MAGIC_TEXT_SIZE: usize = MAGIC_TEXT.len();
const MAGIC_VERSION_SIZE: usize = mem::size_of::<u16>();
const HEADER_SIZE: usize = MAGIC_TEXT_SIZE + MAGIC_VERSION_SIZE;

/// Error types.
pub mod error;

mod buffer;
pub use buffer::*;

bitflags::bitflags! {
  /// The flags of the entry.
  struct Flags: u8 {
    /// First bit: 1 indicates committed, 0 indicates uncommitted
    const COMMITTED = 0b00000001;
  }
}

struct Pointer<C> {
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
  const fn as_key_slice(&self) -> &[u8] {
    if self.key_len == 0 {
      return &[];
    }

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    unsafe { slice::from_raw_parts(self.ptr.add(STATUS_SIZE + KEY_LEN_SIZE), self.key_len) }
  }

  #[inline]
  const fn as_value_slice<'a, 'b: 'a>(&'a self) -> &'b [u8] {
    if self.value_len == 0 {
      return &[];
    }

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    unsafe {
      slice::from_raw_parts(
        self
          .ptr
          .add(STATUS_SIZE + KEY_LEN_SIZE + self.key_len + VALUE_LEN_SIZE),
        self.value_len,
      )
    }
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

/// Options for the WAL.
#[derive(Debug, Clone)]
pub struct Options {
  maximum_key_size: u32,
  maximum_value_size: u32,
  sync_on_write: bool,
  cache_key: bool,
  cache_value: bool,
  magic_version: u16,
  huge: Option<u8>,
  cap: u32,
  reserved: u32,
}

impl Default for Options {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl Options {
  /// Create a new `Options` instance.
  ///
  ///
  /// # Example
  ///
  /// **Note:** If you are creating in-memory WAL, then you must specify the capacity.
  ///
  /// ```rust
  /// use orderwal::Options;
  ///
  /// let options = Options::new().with_capacity(1024 * 1024 * 8); // 8MB in-memory WAL
  /// ```
  #[inline]
  pub const fn new() -> Self {
    Self {
      maximum_key_size: u16::MAX as u32,
      maximum_value_size: u32::MAX,
      sync_on_write: true,
      cache_key: false,
      cache_value: false,
      magic_version: 0,
      huge: None,
      cap: 0,
      reserved: 0,
    }
  }

  /// Set the reserved bytes of the WAL.
  ///
  /// The `reserved` is used to configure the start position of the WAL. This is useful
  /// when you want to add some bytes as your own WAL's header.
  ///
  /// The default reserved is `0`.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::Options;
  ///
  /// let opts = Options::new().with_reserved(8);
  /// ```
  #[inline]
  pub const fn with_reserved(mut self, reserved: u32) -> Self {
    self.reserved = if self.cap as u64 <= reserved as u64 + HEADER_SIZE as u64 {
      self.cap
    } else {
      reserved
    };
    self
  }

  /// Get the reserved of the WAL.
  ///
  /// The `reserved` is used to configure the start position of the WAL. This is useful
  /// when you want to add some bytes as your own WAL's header.
  ///
  /// The default reserved is `0`.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::Options;
  ///
  /// let opts = Options::new().with_reserved(8);
  ///
  /// assert_eq!(opts.reserved(), 8);
  /// ```
  #[inline]
  pub const fn reserved(&self) -> u32 {
    self.reserved
  }

  /// Returns the magic version.
  ///
  /// The default value is `0`.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::Options;
  ///
  /// let options = Options::new().with_magic_version(1);
  /// assert_eq!(options.magic_version(), 1);
  /// ```
  #[inline]
  pub const fn magic_version(&self) -> u16 {
    self.magic_version
  }

  /// Returns the capacity of the WAL.
  ///
  /// The default value is `0`.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::Options;
  ///
  /// let options = Options::new().with_capacity(1000);
  /// assert_eq!(options.capacity(), 1000);
  /// ```
  #[inline]
  pub const fn capacity(&self) -> u32 {
    self.cap
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

  /// Returns `true`, when inserting an new entry, the owned `K` will be cached in memory.
  ///
  /// The default value is `false`.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::Options;
  ///
  /// let options = Options::new();
  /// assert_eq!(options.cache_key(), false);
  /// ```
  #[inline]
  pub const fn cache_key(&self) -> bool {
    self.cache_key
  }

  /// Returns `true`, when inserting an new entry, the owned `V` will be cached in memory.
  ///
  /// The default value is `false`.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::Options;
  ///
  /// let options = Options::new();
  /// assert_eq!(options.cache_value(), false);
  /// ```
  #[inline]
  pub const fn cache_value(&self) -> bool {
    self.cache_value
  }

  /// Returns the bits of the page size.
  ///
  /// Configures the anonymous memory map to be allocated using huge pages.
  ///
  /// This option corresponds to the `MAP_HUGETLB` flag on Linux. It has no effect on Windows.
  ///
  /// The size of the requested page can be specified in page bits.
  /// If not provided, the system default is requested.
  /// The requested length should be a multiple of this, or the mapping will fail.
  ///
  /// This option has no effect on file-backed memory maps.
  ///
  /// The default value is `None`.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::Options;
  ///
  /// let options = Options::new().with_huge(Some(64));
  /// assert_eq!(options.huge(), Some(64));
  /// ```
  #[inline]
  pub const fn huge(&self) -> Option<u8> {
    self.huge
  }

  /// Sets the capacity of the WAL.
  ///
  /// This configuration will be ignored when using file-backed memory maps.
  ///
  /// The default value is `0`.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::Options;
  ///
  /// let options = Options::new().with_capacity(100);
  /// assert_eq!(options.capacity(), 100);
  /// ```
  #[inline]
  pub const fn with_capacity(mut self, cap: u32) -> Self {
    self.cap = cap;
    self
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

  /// Sets the cache key to `true`, when inserting an new entry, the owned version `K` will be cached in memory.
  ///
  /// Only useful when using [`GenericOrderWal`](swmr::GenericOrderWal).
  ///
  /// The default value is `false`.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::Options;
  ///
  /// let options = Options::new().with_cache_key(true);
  /// assert_eq!(options.cache_key(), true);
  /// ```
  #[inline]
  pub const fn with_cache_key(mut self, cache: bool) -> Self {
    self.cache_key = cache;
    self
  }

  /// Sets the cache value to `true`, when inserting an new entry, the owned version `V` will be cached in memory.
  ///
  /// Only useful when using [`GenericOrderWal`](swmr::GenericOrderWal).
  ///
  /// The default value is `false`.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::Options;
  ///
  /// let options = Options::new().with_cache_value(true);
  /// assert_eq!(options.cache_value(), true);
  /// ```
  #[inline]
  pub const fn with_cache_value(mut self, cache: bool) -> Self {
    self.cache_value = cache;
    self
  }

  /// Returns the bits of the page size.
  ///
  /// Configures the anonymous memory map to be allocated using huge pages.
  ///
  /// This option corresponds to the `MAP_HUGETLB` flag on Linux. It has no effect on Windows.
  ///
  /// The size of the requested page can be specified in page bits.
  /// If not provided, the system default is requested.
  /// The requested length should be a multiple of this, or the mapping will fail.
  ///
  /// This option has no effect on file-backed memory maps.
  ///
  /// The default value is `None`.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::Options;
  ///
  /// let options = Options::new().with_huge(64);
  /// assert_eq!(options.huge(), Some(64));
  /// ```
  #[inline]
  pub const fn with_huge(mut self, page_bits: u8) -> Self {
    self.huge = Some(page_bits);
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

  /// Sets the magic version.
  ///
  /// The default value is `0`.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::Options;
  ///
  /// let options = Options::new().with_magic_version(1);
  /// assert_eq!(options.magic_version(), 1);
  /// ```
  #[inline]
  pub const fn with_magic_version(mut self, version: u16) -> Self {
    self.magic_version = version;
    self
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
  STATUS_SIZE as u32
    + KEY_LEN_SIZE as u32
    + key_len
    + VALUE_LEN_SIZE as u32
    + value_len
    + CHECKSUM_SIZE as u32
}

#[inline]
const fn arena_options(reserved: u32) -> ArenaOptions {
  ArenaOptions::new()
    .with_magic_version(CURRENT_VERSION)
    .with_freelist(Freelist::None)
    .with_reserved((HEADER_SIZE + reserved as usize) as u32)
    .with_unify(true)
}

macro_rules! impl_common_methods {
  () => {
    impl OrderWal {
      /// Creates a new in-memory write-ahead log backed by an aligned vec with the given capacity.
      ///
      /// # Example
      ///
      /// ```rust
      /// use orderwal::{swmr::OrderWal, Options};
      ///
      /// let wal = OrderWal::new(Options::new(), 100).unwrap();
      /// ```
      #[inline]
      pub fn new(opts: Options) -> Result<Self, Error> {
        Self::with_comparator_and_checksumer(opts, Ascend, Crc32::default())
      }

      /// Creates a new in-memory write-ahead log but backed by an anonymous mmap with the given capacity.
      ///
      /// # Example
      ///
      /// ```rust
      /// use orderwal::{swmr::OrderWal, Options, ArenaOptions, MmapOptions};
      ///
      /// let mmap_options = MmapOptions::new().len(100);
      /// let wal = OrderWal::map_anon(Options::new(), mmap_options).unwrap();
      /// ```
      pub fn map_anon(opts: Options) -> Result<Self, Error> {
        Self::map_anon_with_comparator_and_checksumer(opts, Ascend, Crc32::default())
      }

      /// Opens a write-ahead log backed by a file backed memory map in read-only mode.
      ///
      /// # Example
      ///
      /// ```rust
      /// use orderwal::{swmr::OrderWal, Options, OpenOptions, MmapOptions};
      ///
      /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
      /// # std::fs::remove_file(&path);
      ///
      /// # {
      ///   # let open_options = OpenOptions::default().create_new(Some(100)).read(true).write(true);
      ///   # let mmap_options = MmapOptions::new();
      ///   # let arena = OrderWal::map_mut(&path, Options::new(), open_options, mmap_options).unwrap();
      /// # }
      ///
      /// let open_options = OpenOptions::default().read(true);
      /// let mmap_options = MmapOptions::new();
      /// let arena = OrderWal::map(&path, open_options, mmap_options).unwrap();
      ///
      /// # std::fs::remove_file(path);
      /// ```
      pub fn map<P: AsRef<std::path::Path>>(path: P, opts: Options) -> Result<Self, error::Error> {
        Self::map_with_path_builder::<_, ()>(|| Ok(path.as_ref().to_path_buf()), opts)
          .map_err(|e| e.unwrap_right())
      }

      /// Opens a write-ahead log backed by a file backed memory map in read-only mode.
      ///
      /// # Example
      ///
      /// ```rust
      /// use rarena_allocator::{sync::Arena, Allocator, ArenaOptions, OpenOptions, MmapOptions};
      ///
      /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
      /// # std::fs::remove_file(&path);
      ///
      /// # {
      ///   # let open_options = OpenOptions::default().create_new(Some(100)).read(true).write(true);
      ///   # let mmap_options = MmapOptions::new();
      ///   # let arena = Arena::map_mut(&path, ArenaOptions::new(), open_options, mmap_options).unwrap();
      /// # }
      ///
      /// let open_options = OpenOptions::default().read(true);
      /// let mmap_options = MmapOptions::new();
      /// let arena = Arena::map_with_path_builder::<_, std::io::Error>(|| Ok(path.to_path_buf()), ArenaOptions::new(), open_options, mmap_options).unwrap();
      ///
      /// # std::fs::remove_file(path);
      /// ```
      #[inline]
      pub fn map_with_path_builder<PB, E>(
        path_builder: PB,
        opts: Options,
      ) -> Result<Self, either::Either<E, error::Error>>
      where
        PB: FnOnce() -> Result<std::path::PathBuf, E>,
      {
        Self::map_with_path_builder_and_comparator_and_checksumer(
          path_builder,
          opts,
          Ascend,
          Crc32::default(),
        )
      }

      /// Returns a write-ahead log backed by a file backed memory map with given options.
      ///
      /// # Example
      ///
      /// ```rust
      #[doc = concat!("use orderwal::{", stringify!($prefix), "::OrderWal, Options};")]
      ///
      /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
      /// # std::fs::remove_file(&path);
      ///
      /// let open_options = OpenOptions::default().create_new(Some(100)).read(true).write(true);
      /// let arena = OrderWal::map_mut(&path, Options::new(), open_options).unwrap();
      ///
      /// # std::fs::remove_file(path);
      /// ```
      #[inline]
      pub fn map_mut<P: AsRef<std::path::Path>>(
        path: P,
        opts: Options,
        open_options: OpenOptions,
      ) -> Result<Self, error::Error> {
        Self::map_mut_with_path_builder::<_, ()>(
          || Ok(path.as_ref().to_path_buf()),
          opts,
          open_options,
        )
        .map_err(|e| e.unwrap_right())
      }

      /// Returns a write-ahead log backed by a file backed memory map with given options.
      ///
      /// # Example
      ///
      /// ```rust
      #[doc = concat!("use orderwal::{", stringify!($prefix), "::OrderWal, Options};")]
      ///
      /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
      /// # std::fs::remove_file(&path);
      ///
      /// let open_options = OpenOptions::default().create_new(Some(100)).read(true).write(true);
      /// let wal = OrderWal::map_mut_with_path_builder::<_, std::io::Error>(|| Ok(path.to_path_buf()), Options::new(), open_options).unwrap();
      ///
      /// # std::fs::remove_file(path);
      /// ```
      #[inline]
      pub fn map_mut_with_path_builder<PB, E>(
        path_builder: PB,
        opts: Options,
        open_options: OpenOptions,
      ) -> Result<Self, either::Either<E, error::Error>>
      where
        PB: FnOnce() -> Result<std::path::PathBuf, E>,
      {
        Self::map_mut_with_path_builder_and_comparator_and_checksumer(
          path_builder,
          opts,
          open_options,
          Ascend,
          Crc32::default(),
        )
      }
    }
  };
  ($prefix:ident <C, S>) => {
    impl<C, S> OrderWal<C, S> {
      /// Creates a new in-memory write-ahead log backed by an aligned vec with the given [`Comparator`], [`Checksumer`] and capacity.
      ///
      /// # Example
      ///
      /// ```rust
      #[doc = concat!("use orderwal::{", stringify!($prefix), "::OrderWal, Options, Descend, Crc32};")]
      ///
      /// let wal = OrderWal::new(ArenaOptions::new(), mmap_options).unwrap();
      /// ```
      pub fn with_comparator_and_checksumer(
        opts: Options,
        cmp: C,
        cks: S,
      ) -> Result<Self, Error> {
        let arena = Arena::new(arena_options(opts.reserved()).with_capacity(opts.cap));
        OrderWalCore::new(arena, opts, cmp, cks).map(|core| Self::from_core(core, false))
      }

      /// Creates a new in-memory write-ahead log but backed by an anonymous mmap with the given [`Comparator`], [`Checksumer`] and capacity.
      ///
      /// # Example
      ///
      /// ```rust
      #[doc = concat!("use orderwal::{", stringify!($prefix), "::OrderWal, Options, Descend, Crc32};")]
      ///
      /// let arena = OrderWal::map_anon_with_comparator_and_checksumer(Options::new(), Descend, Crc32::default()).unwrap();
      /// ```
      pub fn map_anon_with_comparator_and_checksumer(
        opts: Options,
        cmp: C,
        cks: S,
      ) -> Result<Self, Error> {
        let mmap_opts = MmapOptions::new().len(opts.cap).huge(opts.huge);
        Arena::map_anon(arena_options(opts.reserved()), mmap_opts)
          .map_err(Into::into)
          .and_then(|arena| {
            OrderWalCore::new(arena, opts, cmp, cks).map(|core| Self::from_core(core, false))
          })
      }

      /// Flushes the to disk.
      #[inline]
      pub fn flush(&self) -> Result<(), error::Error> {
        if self.ro {
          return Err(error::Error::read_only());
        }

        self.core.arena.flush().map_err(Into::into)
      }

      /// Flushes the to disk.
      #[inline]
      pub fn flush_async(&self) -> Result<(), error::Error> {
        if self.ro {
          return Err(error::Error::read_only());
        }

        self.core.arena.flush_async().map_err(Into::into)
      }

      #[inline]
      fn check(&self, klen: usize, vlen: usize) -> Result<(), error::Error> {
        let elen = klen as u64 + vlen as u64;

        if self.core.opts.maximum_key_size < klen as u32 {
          return Err(error::Error::key_too_large(
            klen as u32,
            self.core.opts.maximum_key_size,
          ));
        }

        if self.core.opts.maximum_value_size < vlen as u32 {
          return Err(error::Error::value_too_large(
            vlen as u32,
            self.core.opts.maximum_value_size,
          ));
        }

        if elen + FIXED_RECORD_SIZE as u64 > u32::MAX as u64 {
          return Err(error::Error::entry_too_large(
            elen,
            min_u64(
              self.core.opts.maximum_key_size as u64 + self.core.opts.maximum_value_size as u64,
              u32::MAX as u64,
            ),
          ));
        }

        Ok(())
      }
    }
  };
  (<S: Checksumer>) => {
    impl<S: Checksumer> OrderWal<Ascend, S> {
      /// Opens a write-ahead log backed by a file backed memory map in read-only mode with the given [`Checksumer`].
      ///
      /// # Example
      ///
      /// ```rust
      #[doc = concat!("use orderwal::{", stringify!($prefix), "::OrderWal, Crc32};")]
      ///
      /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
      /// # std::fs::remove_file(&path);
      ///
      /// # {
      ///   # let open_options = OpenOptions::default().create_new(Some(100)).read(true).write(true);
      ///   # let mmap_options = MmapOptions::new();
      ///   # let arena = OrderWal::map_mut(&path, Options::new(), open_options, mmap_options).unwrap();
      /// # }
      ///
      /// let arena = OrderWal::map(&path, Crc32::default()).unwrap();
      ///
      /// # std::fs::remove_file(path);
      /// ```
      pub fn map_with_checksumer<P: AsRef<std::path::Path>>(
        path: P,
        opts: Options,
        cks: S,
      ) -> Result<Self, error::Error> {
        Self::map_with_path_builder_and_checksumer::<_, ()>(
          || Ok(path.as_ref().to_path_buf()),
          opts,
          cks,
        )
        .map_err(|e| e.unwrap_right())
      }

      /// Opens a write-ahead log backed by a file backed memory map in read-only mode with the given [`Checksumer`].
      ///
      /// # Example
      ///
      /// ```rust
      #[doc = concat!("use orderwal::{", stringify!($prefix), "::OrderWal, Options, Crc32};")]
      ///
      /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
      /// # std::fs::remove_file(&path);
      ///
      /// # {
      ///   # let open_options = OpenOptions::default().create_new(Some(100)).read(true).write(true);
      ///   # let mmap_options = MmapOptions::new();
      ///   # let arena = Arena::map_mut(&path, ArenaOptions::new(), open_options, mmap_options).unwrap();
      /// # }
      ///
      /// let arena = Arena::map_with_path_builder::<_, std::io::Error>(|| Ok(path.to_path_buf()), Crc32::default()).unwrap();
      ///
      /// # std::fs::remove_file(path);
      /// ```
      #[inline]
      pub fn map_with_path_builder_and_checksumer<PB, E>(
        path_builder: PB,
        opts: Options,
        cks: S,
      ) -> Result<Self, either::Either<E, error::Error>>
      where
        PB: FnOnce() -> Result<std::path::PathBuf, E>,
      {
        Self::map_with_path_builder_and_comparator_and_checksumer(
          path_builder,
          opts,
          Ascend,
          cks,
        )
      }

      /// Returns a write-ahead log backed by a file backed memory map with given options and [`Checksumer`].
      ///
      /// # Example
      ///
      /// ```rust
      #[doc = concat!("use orderwal::{", stringify!($prefix), "::OrderWal, Options, Crc32};")]
      ///
      /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
      /// # std::fs::remove_file(&path);
      ///
      /// let open_options = OpenOptions::default().create_new(Some(100)).read(true).write(true);
      /// let wal = OrderWal::map_mut_with_checksumer(&path, Options::new(), open_options, Crc32::default()).unwrap();
      ///
      /// # std::fs::remove_file(path);
      /// ```
      #[inline]
      pub fn map_mut_with_checksumer<P: AsRef<std::path::Path>>(
        path: P,
        opts: Options,
        open_options: OpenOptions,
        cks: S,
      ) -> Result<Self, error::Error> {
        Self::map_mut_with_path_builder_and_checksumer::<_, ()>(
          || Ok(path.as_ref().to_path_buf()),
          opts,
          open_options,
          cks,
        )
        .map_err(|e| e.unwrap_right())
      }

      /// Returns a write-ahead log backed by a file backed memory map with the given options and [`Checksumer`].
      ///
      /// # Example
      ///
      /// ```rust
      #[doc = concat!("use orderwal::{", stringify!($prefix), "::OrderWal, Options, OpenOptions, Descend, Crc32};")]
      ///
      /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
      /// # std::fs::remove_file(&path);
      ///
      /// let open_options = OpenOptions::default().create_new(Some(100)).read(true).write(true);
      /// let arena = OrderWal::map_mut_with_path_builder_and_checksumer::<_, std::io::Error>(|| Ok(path.to_path_buf()), Options::new(), open_options, Crc32::default()).unwrap();
      ///
      /// # std::fs::remove_file(path);
      /// ```
      #[inline]
      pub fn map_mut_with_path_builder_and_checksumer<PB, E>(
        path_builder: PB,
        opts: Options,
        open_options: OpenOptions,
        cks: S,
      ) -> Result<Self, either::Either<E, error::Error>>
      where
        PB: FnOnce() -> Result<std::path::PathBuf, E>,
      {
        Self::map_mut_with_path_builder_and_comparator_and_checksumer(
          path_builder,
          opts,
          open_options,
          Ascend,
          cks,
        )
      }
    }
  };
  (<C: Comparator, S>) => {
    impl<C: Comparator, S> OrderWal<C, S> {
      /// Returns `true` if the WAL contains the specified key.
      #[inline]
      pub fn contains_key<Q>(&self, key: &Q) -> bool
      where
        [u8]: Borrow<Q>,
        Q: ?Sized + Ord,
      {
        self.core.map.contains(key)
      }

      /// Returns the value associated with the key.
      #[inline]
      pub fn get<Q>(&self, key: &Q) -> Option<&[u8]>
      where
        [u8]: Borrow<Q>,
        Q: ?Sized + Ord,
      {
        self.core.map.get(key).map(|ent| ent.as_value_slice())
      }
    }
  };
  (
    Self $($mut:ident)?: where
      C: Comparator + CheapClone + $($ident:ident + )? 'static,
      S: Checksumer,
  ) => {
    impl<C, S> OrderWal<C, S>
      where
        C: Comparator + CheapClone + $($ident + )? 'static,
        S: Checksumer,
    {
      /// Opens a write-ahead log backed by a file backed memory map in read-only mode with the given [`Comparator`] and [`Checksumer`].
      ///
      /// # Example
      ///
      /// ```rust
      /// use orderwal::{swmr::OrderWal, Options, OpenOptions, MmapOptions};
      #[doc = concat!("use orderwal::{", stringify!($prefix), "::OrderWal, Descend, Crc32};")]
      ///
      /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
      /// # std::fs::remove_file(&path);
      ///
      /// # {
      ///   # let open_options = OpenOptions::default().create_new(Some(100)).read(true).write(true);
      ///   # let mmap_options = MmapOptions::new();
      ///   # let arena = OrderWal::map_mut(&path, Options::new(), open_options, mmap_options).unwrap();
      /// # }
      ///
      /// let wal = OrderWal::map(&path, Decend, Crc32::default()).unwrap();
      ///
      /// # std::fs::remove_file(path);
      /// ```
      pub fn map_with_comparator_and_checksumer<P: AsRef<std::path::Path>>(
        path: P,
        opts: Options,
        cmp: C,
        cks: S,
      ) -> Result<Self, error::Error> {
        Self::map_with_path_builder_and_comparator_and_checksumer::<_, ()>(
          || Ok(path.as_ref().to_path_buf()),
          opts,
          cmp,
          cks,
        )
        .map_err(|e| e.unwrap_right())
      }

      /// Opens a write-ahead log backed by a file backed memory map in read-only mode with the given [`Comparator`] and [`Checksumer`].
      ///
      /// # Example
      ///
      /// ```rust
      #[doc = concat!("use orderwal::{", stringify!($prefix), "::OrderWal, Descend, Crc32};")]
      ///
      /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
      /// # std::fs::remove_file(&path);
      ///
      /// # {
      ///   # let open_options = OpenOptions::default().create_new(Some(100)).read(true).write(true);
      ///   # let mmap_options = MmapOptions::new();
      ///   # let arena = Arena::map_mut(&path, ArenaOptions::new(), open_options, mmap_options).unwrap();
      /// # }
      ///
      /// let wal = Arena::map_with_path_builder::<_, std::io::Error>(|| Ok(path.to_path_buf()), Descend, Crc32::default()).unwrap();
      ///
      /// # std::fs::remove_file(path);
      /// ```
      pub fn map_with_path_builder_and_comparator_and_checksumer<PB, E>(
        path_builder: PB,
        opts: Options,
        cmp: C,
        cks: S,
      ) -> Result<Self, either::Either<E, error::Error>>
      where
        PB: FnOnce() -> Result<std::path::PathBuf, E>,
      {
        let open_options = OpenOptions::default().read(true);

        Arena::map_with_path_builder(path_builder, arena_options(opts.reserved()), open_options, MmapOptions::new())
          .map_err(|e| e.map_right(Into::into))
          .and_then(|arena| {
            OrderWalCore::replay(arena, Options::new(), true, cmp, cks)
              .map(|core| Self::from_core(core, true))
              .map_err(Either::Right)
          })
      }

      /// Returns a write-ahead log backed by a file backed memory map with the given options, [`Comparator`] and [`Checksumer`].
      ///
      /// # Example
      ///
      /// ```rust
      #[doc = concat!("use orderwal::{", stringify!($prefix), "::OrderWal, Options, OpenOptions, Descend, Crc32};")]
      ///
      /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
      /// # std::fs::remove_file(&path);
      ///
      /// let open_options = OpenOptions::default().create_new(Some(100)).read(true).write(true);
      /// let wal = OrderWal::map_mut(&path, Options::new(), open_options).unwrap();
      ///
      /// # std::fs::remove_file(path);
      /// ```
      #[inline]
      pub fn map_mut_with_comparator_and_checksumer<P: AsRef<std::path::Path>>(
        path: P,
        opts: Options,
        open_options: OpenOptions,
        cmp: C,
        cks: S,
      ) -> Result<Self, error::Error> {
        Self::map_mut_with_path_builder_and_comparator_and_checksumer::<_, ()>(
          || Ok(path.as_ref().to_path_buf()),
          opts,
          open_options,
          cmp,
          cks,
        )
        .map_err(|e| e.unwrap_right())
      }

      /// Returns a write-ahead log backed by a file backed memory map with the given options, [`Comparator`] and [`Checksumer`].
      ///
      /// # Example
      ///
      /// ```rust
      #[doc = concat!("use orderwal::{", stringify!($prefix), "::OrderWal, Options, OpenOptions, Descend, Crc32};")]
      ///
      /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
      /// # std::fs::remove_file(&path);
      ///
      /// let open_options = OpenOptions::default().create_new(Some(100)).read(true).write(true);
      /// let wal = OrderWal::map_mut_with_path_builder::<_, std::io::Error>(|| Ok(path.to_path_buf()), Options::new(), open_options, Descend, Crc32::default()).unwrap();
      ///
      /// # std::fs::remove_file(path);
      /// ```
      pub fn map_mut_with_path_builder_and_comparator_and_checksumer<PB, E>(
        path_builder: PB,
        opts: Options,
        open_options: OpenOptions,
        cmp: C,
        cks: S,
      ) -> Result<Self, either::Either<E, error::Error>>
      where
        PB: FnOnce() -> Result<std::path::PathBuf, E>,
      {
        let path = path_builder().map_err(Either::Left)?;

        let exist = path.exists();

        Arena::map_mut(path, arena_options(opts.reserved()), open_options, MmapOptions::new())
          .map_err(Into::into)
          .and_then(|arena| {
            if !exist {
              OrderWalCore::new(arena, opts, cmp, cks).map(|core| Self::from_core(core, false))
            } else {
              OrderWalCore::replay(arena, opts, false, cmp, cks).map(|core| Self::from_core(core, false))
            }
          })
          .map_err(Either::Right)
      }

      /// Get or insert a new entry into the WAL.
      #[inline]
      pub fn get_or_insert(&$($mut)? self, key: &[u8], value: &[u8]) -> Result<Option<&[u8]>, error::Error> {
        if self.ro {
          return Err(error::Error::read_only());
        }

        self.check(key.len(), value.len())?;

        if let Some(ent) = self.core.map.get(key) {
          return Ok(Some(ent.as_value_slice()));
        }

        self.insert(key, value)?;
        Ok(None)
      }

      /// Inserts a key-value pair into the WAL. This method
      /// allows the caller to build the key in place.
      ///
      /// See also [`insert_with_value_builder`](Self::insert_with_value_builder) and [`insert_with_builders`](Self::insert_with_builders).
      pub fn insert_with_key_builder<E>(
        &$($mut)? self,
        kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
        value: &[u8],
      ) -> Result<(), Either<E, Error>> {
        if self.ro {
          return Err(Either::Right(Error::read_only()));
        }

        self
          .check(kb.size() as usize, value.len())
          .map_err(Either::Right)?;

        self.insert_with_in::<E, ()>(kb, ValueBuilder::new(value.len() as u32, |buf| {
          buf.write(value).unwrap();
          Ok(())
        }))
        .map_err(|e| {
          match e {
            Among::Left(e) => Either::Left(e),
            Among::Middle(_) => unreachable!(),
            Among::Right(e) => Either::Right(e),
          }
        })
      }

      /// Inserts a key-value pair into the WAL. This method
      /// allows the caller to build the value in place.
      ///
      /// See also [`insert_with_key_builder`](Self::insert_with_key_builder) and [`insert_with_builders`](Self::insert_with_builders).
      pub fn insert_with_value_builder<E>(
        &$($mut)? self,
        key: &[u8],
        vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
      ) -> Result<(), Either<E, Error>> {
        if self.ro {
          return Err(Either::Right(Error::read_only()));
        }

        self
          .check(key.len(), vb.size() as usize)
          .map_err(Either::Right)?;

        self.insert_with_in::<(), E>(KeyBuilder::new(key.len() as u32, |buf| {
          buf.write(key).unwrap();
          Ok(())
        }), vb)
        .map_err(|e| {
          match e {
            Among::Left(_) => unreachable!(),
            Among::Middle(e) => Either::Left(e),
            Among::Right(e) => Either::Right(e),
          }
        })
      }

      /// Inserts a key-value pair into the WAL. This method
      /// allows the caller to build the key and value in place.
      pub fn insert_with_builders<KE, VE>(
        &$($mut)? self,
        kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), KE>>,
        vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), VE>>,
      ) -> Result<(), Among<KE, VE, Error>> {
        if self.ro {
          return Err(Among::Right(Error::read_only()));
        }

        self
          .check(kb.size() as usize, vb.size() as usize)
          .map_err(Among::Right)?;

        self.insert_with_in(kb, vb)
      }

      /// Inserts a key-value pair into the WAL.
      pub fn insert(&$($mut)? self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        if self.ro {
          return Err(Error::read_only());
        }

        self.check(key.len(), value.len())?;

        self.insert_with_in::<(), ()>(
          KeyBuilder::new(key.len() as u32, |buf| {
            buf.write(key).unwrap();
            Ok(())
          }),
          ValueBuilder::new(value.len() as u32, |buf| {
            buf.write(value).unwrap();
            Ok(())
          }),
        ).map_err(Among::unwrap_right)
      }

      fn insert_with_in<KE, VE>(
        &$($mut)? self,
        kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), KE>>,
        vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), VE>>,
      ) -> Result<(), Among<KE, VE, Error>> {
        let (klen, kf) = kb.into_components();
        let (vlen, vf) = vb.into_components();
        let elen = entry_size(klen, vlen);
        let buf = self.core.arena.alloc_bytes(elen);

        match buf {
          Err(e) => {
            let e = match e {
              ArenaError::InsufficientSpace {
                requested,
                available,
              } => error::Error::insufficient_space(requested, available),
              ArenaError::ReadOnly => error::Error::read_only(),
              ArenaError::LargerThanPageSize { .. } => unreachable!(),
            };
            Err(Among::Right(e))
          },
          Ok(mut buf) => {
            unsafe {
              // We allocate the buffer with the exact size, so it's safe to write to the buffer.
              let flag = Flags::COMMITTED.bits();

              self.core.cks.reset();
              self.core.cks.update(&[flag]);

              buf.put_u8_unchecked(Flags::empty().bits());
              buf.put_u32_le_unchecked(klen);

              let ko = STATUS_SIZE + KEY_LEN_SIZE;
              buf.set_len(ko + klen as usize);
              kf(&mut VacantBuffer::new(
                klen as usize,
                NonNull::new_unchecked(buf.as_mut_ptr().add(ko)),
              )).map_err(Among::Left)?;

              buf.put_u32_le_unchecked(vlen);

              let vo = STATUS_SIZE + KEY_LEN_SIZE + klen as usize + VALUE_LEN_SIZE;
              buf.set_len(vo + vlen as usize);
              vf(&mut VacantBuffer::new(
                vlen as usize,
                NonNull::new_unchecked(buf.as_mut_ptr().add(vo)),
              )).map_err(Among::Middle)?;

              let cks = {
                self.core.cks.update(&buf[1..]);
                self.core.cks.digest()
              };
              buf.put_u64_le_unchecked(cks);

              // commit the entry
              buf[0] |= Flags::COMMITTED.bits();

              if self.core.opts.sync_on_write && self.core.arena.is_ondisk() {
                self.core.arena.flush_range(buf.offset(), elen as usize).map_err(|e| Among::Right(e.into()))?;
              }
              buf.detach();
              self.core.map.insert(Pointer::new(
                klen as usize,
                vlen as usize,
                buf.as_ptr(),
                self.core.cmp.cheap_clone(),
              ));
              Ok(())
            }

          }
        }
      }
    }
  };
  (
    where
      C: Comparator + CheapClone + $($ident:ident + )? 'static,
  ) => {
    impl<C> OrderWal<C>
    where
      C: Comparator + CheapClone + $($ident + )? 'static,
    {
      /// Opens a write-ahead log backed by a file backed memory map in read-only mode with the given [`Comparator`].
      ///
      /// # Example
      ///
      /// ```rust
      #[doc = concat!("use orderwal::{", stringify!($prefix), "::OrderWal, Descend};")]
      ///
      /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
      /// # std::fs::remove_file(&path);
      ///
      /// # {
      ///   # let open_options = OpenOptions::default().create_new(Some(100)).read(true).write(true);
      ///   # let mmap_options = MmapOptions::new();
      ///   # let arena = OrderWal::map_mut(&path, Options::new(), open_options, mmap_options).unwrap();
      /// # }
      ///
      /// let arena = OrderWal::map(&path, Descend).unwrap();
      ///
      /// # std::fs::remove_file(path);
      /// ```
      pub fn map_with_comparator<P: AsRef<std::path::Path>>(
        path: P,
        opts: Options,
        cmp: C,
      ) -> Result<Self, error::Error> {
        Self::map_with_path_builder_and_comparator::<_, ()>(
          || Ok(path.as_ref().to_path_buf()),
          opts,
          cmp,
        )
        .map_err(|e| e.unwrap_right())
      }

      /// Opens a write-ahead log backed by a file backed memory map in read-only mode with the given [`Comparator`].
      ///
      /// # Example
      ///
      /// ```rust
      #[doc = concat!("use orderwal::{", stringify!($prefix), "::OrderWal, Descend};")]
      ///
      /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
      /// # std::fs::remove_file(&path);
      ///
      /// # {
      ///   # let open_options = OpenOptions::default().create_new(Some(100)).read(true).write(true);
      ///   # let mmap_options = MmapOptions::new();
      ///   # let arena = Arena::map_mut(&path, ArenaOptions::new(), open_options, mmap_options).unwrap();
      /// # }
      ///
      /// let arena = Arena::map_with_path_builder::<_, std::io::Error>(|| Ok(path.to_path_buf()), Descend).unwrap();
      ///
      /// # std::fs::remove_file(path);
      /// ```
      #[inline]
      pub fn map_with_path_builder_and_comparator<PB, E>(
        path_builder: PB,
        opts: Options,
        cmp: C,
      ) -> Result<Self, either::Either<E, error::Error>>
      where
        PB: FnOnce() -> Result<std::path::PathBuf, E>,
      {
        Self::map_with_path_builder_and_comparator_and_checksumer(
          path_builder,
          opts,
          cmp,
          Crc32::default(),
        )
      }

      /// Returns a write-ahead log backed by a file backed memory map with the given options and [`Comparator`].
      ///
      /// # Example
      ///
      /// ```rust
      #[doc = concat!("use orderwal::{", stringify!($prefix), "::OrderWal, Options, Descend};")]
      ///
      /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
      /// # std::fs::remove_file(&path);
      ///
      /// let open_options = OpenOptions::default().create_new(Some(100)).read(true).write(true);
      /// let wal = OrderWal::map_mut(&path, Options::new(), open_options).unwrap();
      ///
      /// # std::fs::remove_file(path);
      /// ```
      #[inline]
      pub fn map_mut_with_comparator<P: AsRef<std::path::Path>>(
        path: P,
        opts: Options,
        open_options: OpenOptions,
        cmp: C,
      ) -> Result<Self, error::Error> {
        Self::map_mut_with_path_builder_and_comparator::<_, ()>(
          || Ok(path.as_ref().to_path_buf()),
          opts,
          open_options,
          cmp,
        )
        .map_err(|e| e.unwrap_right())
      }

      /// Returns a write-ahead log backed by a file backed memory map with the given options and [`Comparator`].
      ///
      /// # Example
      ///
      /// ```rust
      #[doc = concat!("use orderwal::{", stringify!($prefix), "::OrderWal, Options, Descend};")]
      ///
      /// # let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
      /// # std::fs::remove_file(&path);
      ///
      /// let open_options = OpenOptions::default().create_new(Some(100)).read(true).write(true);
      /// let arena = OrderWal::map_mut_with_path_builder_and_comparator::<_, std::io::Error>(|| Ok(path.to_path_buf()), Options::new(), open_options, Descend).unwrap();
      ///
      /// # std::fs::remove_file(path);
      /// ```
      #[inline]
      pub fn map_mut_with_path_builder_and_comparator<PB, E>(
        path_builder: PB,
        opts: Options,
        open_options: OpenOptions,
        cmp: C,
      ) -> Result<Self, either::Either<E, error::Error>>
      where
        PB: FnOnce() -> Result<std::path::PathBuf, E>,
      {
        Self::map_mut_with_path_builder_and_comparator_and_checksumer(
          path_builder,
          opts,
          open_options,
          cmp,
          Crc32::default(),
        )
      }
    }
  };
  (tests $prefix:ident) => {
    #[cfg(test)]
    mod common_tests {
      use super::*;
      use tempfile::tempdir;

      const MB: usize = 1024 * 1024;

      #[test]
      fn test_construct_inmemory() {
        let mut wal = OrderWal::new(Options::new().with_capacity(MB as u32)).unwrap();
        let wal = &mut wal;
        wal.insert(b"key1", b"value1").unwrap();
      }

      #[test]
      fn test_construct_map_anon() {
        let mut wal = OrderWal::map_anon(Options::new().with_capacity(MB as u32)).unwrap();
        let wal = &mut wal;
        wal.insert(b"key1", b"value1").unwrap();
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_construct_map_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join(concat!(stringify!($prefix), "_construct_map_file"));

        {
          let mut wal = OrderWal::map_mut(
            &path,
            Options::new(),
            OpenOptions::new()
              .create_new(Some(MB as u32))
              .write(true)
              .read(true),
          )
          .unwrap();

          let wal = &mut wal;
          wal.insert(b"key1", b"value1").unwrap();
        }

        let wal = OrderWal::map(&path, Options::new()).unwrap();
        assert_eq!(wal.get(b"key1").unwrap(), b"value1");
      }
    }
  }
}

macro_rules! walcore {
  (
    $map:ident $(: $send:ident)?
  ) => {
    impl<C, S> OrderWalCore<C, S> {
      #[inline]
      fn new(arena: Arena, opts: Options, cmp: C, cks: S) -> Result<Self, Error> {
        unsafe {
          let slice = arena.reserved_slice_mut();
          slice[0..6].copy_from_slice(&MAGIC_TEXT);
          slice[6..8].copy_from_slice(&opts.magic_version.to_le_bytes());
        }

        arena
          .flush_range(0, HEADER_SIZE)
          .map(|_| Self::construct(arena, $map::new(), opts, cmp, cks))
          .map_err(Into::into)
      }
    }

    impl<C: Comparator + CheapClone + $($send +)? 'static, S: Checksumer> OrderWalCore<C, S> {
      fn replay(arena: Arena, opts: Options, ro: bool, cmp: C, checksumer: S) -> Result<Self, Error> {
        let slice = arena.reserved_slice();
        let magic_text = &slice[0..6];
        let magic_version = u16::from_le_bytes(slice[6..8].try_into().unwrap());

        if magic_text != MAGIC_TEXT {
          return Err(Error::magic_text_mismatch());
        }

        if magic_version != opts.magic_version {
          return Err(Error::magic_version_mismatch());
        }

        let mut set = $map::new();
        let map = &mut set;

        let mut cursor = arena.data_offset();
        let allocated = arena.allocated();

        loop {
          unsafe {
            // we reached the end of the arena, if we have any remaining, then if means two possibilities:
            // 1. the remaining is a partial entry, but it does not be persisted to the disk, so following the write-ahead log principle, we should discard it.
            // 2. our file may be corrupted, so we discard the remaining.
            if cursor + STATUS_SIZE + KEY_LEN_SIZE + VALUE_LEN_SIZE > allocated {
              if !ro && cursor < allocated {
                arena.rewind(ArenaPosition::Start(cursor as u32));
                arena.flush()?;
              }

              break;
            }

            let header = arena.get_bytes(cursor, STATUS_SIZE + KEY_LEN_SIZE);
            let flag = Flags::from_bits_unchecked(header[0]);
            let key_len = u32::from_le_bytes(header[1..5].try_into().unwrap()) as usize;

            // Same as above, if we reached the end of the arena, we should discard the remaining.
            if cursor + STATUS_SIZE + KEY_LEN_SIZE + key_len + VALUE_LEN_SIZE > allocated {
              if !ro {
                arena.rewind(ArenaPosition::Start(cursor as u32));
                arena.flush()?;
              }

              break;
            }

            let value_len = u32::from_le_bytes(
              arena
                .get_bytes(
                  cursor + STATUS_SIZE + KEY_LEN_SIZE + key_len,
                  VALUE_LEN_SIZE,
                )
                .try_into()
                .unwrap(),
            ) as usize;

            let elen = entry_size(key_len as u32, value_len as u32) as usize;
            // Same as above, if we reached the end of the arena, we should discard the remaining.
            if cursor + elen > allocated {
              if !ro {
                arena.rewind(ArenaPosition::Start(cursor as u32));
                arena.flush()?;
              }

              break;
            }

            let cks = u64::from_le_bytes(
              arena
                .get_bytes(cursor + elen - CHECKSUM_SIZE, CHECKSUM_SIZE)
                .try_into()
                .unwrap(),
            );

            if cks != checksumer.checksum(arena.get_bytes(cursor, elen - CHECKSUM_SIZE)) {
              return Err(Error::corrupted());
            }

            // If the entry is not committed, we should not rewind
            if !flag.contains(Flags::COMMITTED) {
              if !ro {
                arena.rewind(ArenaPosition::Start(cursor as u32));
                arena.flush()?;
              }

              break;
            }

            map.insert(Pointer::new(
              key_len,
              value_len,
              arena.get_pointer(cursor),
              cmp.cheap_clone(),
            ));
            cursor += elen;
          }
        }

        Ok(Self::construct(arena, set, opts, cmp, checksumer))
      }
    }
  };
}
/// A single writer multiple readers ordered write-ahead Log implementation.
pub mod swmr;

/// An ordered write-ahead Log implementation.
pub mod unsync;
