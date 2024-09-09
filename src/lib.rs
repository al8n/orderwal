//! An ordered Write-Ahead Log implementation for Rust.
#![doc = include_str!("../README.md")]
#![cfg_attr(not(any(feature = "std", test)), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]
#![allow(clippy::type_complexity)]

use core::{borrow::Borrow, cmp, marker::PhantomData, mem, slice};

use among::Among;
use crossbeam_skiplist::SkipSet;
use error::Error;
use rarena_allocator::{
  either::{self, Either},
  Allocator, ArenaOptions, Freelist, Memory, MmapOptions, OpenOptions,
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

mod utils;
use utils::*;

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
  const fn as_key_slice(&self) -> &[u8] {
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

mod sealed {
  use rarena_allocator::ArenaPosition;

  use super::*;

  pub trait Base<C>: Default {
    fn insert(&mut self, ele: Pointer<C>)
    where
      C: Comparator;
  }

  pub trait WalCore<C, S> {
    type Allocator: Allocator;
    type Base: Base<C>;

    fn construct(arena: Self::Allocator, base: Self::Base, opts: Options, cmp: C, cks: S) -> Self;
  }

  pub trait WalSealed<C, S>: Sized {
    type Allocator: Allocator;
    type Core: WalCore<C, S, Allocator = Self::Allocator>;

    fn new_in(arena: Self::Allocator, opts: Options, cmp: C, cks: S) -> Result<Self::Core, Error> {
      unsafe {
        let slice = arena.reserved_slice_mut();
        slice[0..6].copy_from_slice(&MAGIC_TEXT);
        slice[6..8].copy_from_slice(&opts.magic_version.to_le_bytes());
      }

      arena
        .flush_range(0, HEADER_SIZE)
        .map(|_| {
          <Self::Core as WalCore<C, S>>::construct(arena, Default::default(), opts, cmp, cks)
        })
        .map_err(Into::into)
    }

    fn replay(
      arena: Self::Allocator,
      opts: Options,
      ro: bool,
      cmp: C,
      checksumer: S,
    ) -> Result<Self::Core, Error>
    where
      C: Comparator + CheapClone,
      S: Checksumer,
    {
      let slice = arena.reserved_slice();
      let magic_text = &slice[0..6];
      let magic_version = u16::from_le_bytes(slice[6..8].try_into().unwrap());

      if magic_text != MAGIC_TEXT {
        return Err(Error::magic_text_mismatch());
      }

      if magic_version != opts.magic_version {
        return Err(Error::magic_version_mismatch());
      }

      let mut set = <Self::Core as WalCore<C, S>>::Base::default();

      let mut cursor = arena.data_offset();
      let allocated = arena.allocated();

      loop {
        unsafe {
          // we reached the end of the arena, if we have any remaining, then if means two possibilities:
          // 1. the remaining is a partial entry, but it does not be persisted to the disk, so following the write-ahead log principle, we should discard it.
          // 2. our file may be corrupted, so we discard the remaining.
          if cursor + STATUS_SIZE > allocated {
            if !ro && cursor < allocated {
              arena.rewind(ArenaPosition::Start(cursor as u32));
              arena.flush()?;
            }
            break;
          }

          let header = arena.get_u8(cursor).unwrap();
          let flag = Flags::from_bits_unchecked(header);

          let (kvsize, encoded_len) = arena.get_u64_varint(cursor + STATUS_SIZE).map_err(|_e| {
            #[cfg(feature = "tracing")]
            tracing::error!(err=%_e);

            Error::corrupted()
          })?;

          let (key_len, value_len) = split_lengths(encoded_len);
          let key_len = key_len as usize;
          let value_len = value_len as usize;
          // Same as above, if we reached the end of the arena, we should discard the remaining.
          let cks_offset = STATUS_SIZE + kvsize + key_len + value_len;
          if cks_offset + CHECKSUM_SIZE > allocated {
            if !ro {
              arena.rewind(ArenaPosition::Start(cursor as u32));
              arena.flush()?;
            }

            break;
          }

          let cks = arena.get_u64_le(cursor + cks_offset).unwrap();

          if cks != checksumer.checksum(arena.get_bytes(cursor, cks_offset)) {
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

          set.insert(Pointer::new(
            key_len,
            value_len,
            arena.get_pointer(cursor + STATUS_SIZE + kvsize),
            cmp.cheap_clone(),
          ));
          cursor += cks_offset + CHECKSUM_SIZE;
        }
      }

      Ok(<Self::Core as WalCore<C, S>>::construct(
        arena, set, opts, cmp, checksumer,
      ))
    }

    fn from_core(core: Self::Core, ro: bool) -> Self;

    fn check(&self, klen: usize, vlen: usize) -> Result<(), Error>;

    fn insert_with_in<KE, VE>(
      &mut self,
      kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), KE>>,
      vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), VE>>,
    ) -> Result<(), Among<KE, VE, Error>>
    where
      C: Comparator + CheapClone,
      S: Checksumer;
  }
}

/// A write-ahead log builder.
pub struct WalBuidler<C = Ascend, S = Crc32> {
  opts: Options,
  cmp: C,
  cks: S,
}

impl WalBuidler {
  /// Returns a new write-ahead log builder with the given options.
  #[inline]
  pub fn new(opts: Options) -> Self {
    Self {
      opts,
      cmp: Ascend,
      cks: Crc32::default(),
    }
  }
}

impl<C, S> WalBuidler<C, S> {
  /// Returns a new write-ahead log builder with the new comparator
  #[inline]
  pub fn with_comparator<NC>(self, cmp: NC) -> WalBuidler<NC, S> {
    WalBuidler {
      opts: self.opts,
      cmp,
      cks: self.cks,
    }
  }

  /// Returns a new write-ahead log builder with the new checksumer
  #[inline]
  pub fn with_checksumer<NS>(self, cks: NS) -> WalBuidler<C, NS> {
    WalBuidler {
      opts: self.opts,
      cmp: self.cmp,
      cks,
    }
  }
}

/// An abstract layer for the write-ahead log.
pub trait Wal<C, S>: Sized + sealed::WalSealed<C, S> {
  /// The iterator type.
  type Iter<'a>
  where
    Self: 'a;

  /// Creates a new in-memory write-ahead log backed by an aligned vec.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::{swmr::OrderWal, WalBuilder, Options};
  ///
  /// let wal = OrderWal::new(WalBuilder::new(Options::new())).unwrap();
  /// ```
  fn new(b: WalBuidler<C, S>) -> Result<Self, Error> {
    let WalBuidler { opts, cmp, cks } = b;
    let arena =
      <Self::Allocator as Allocator>::new(arena_options(opts.reserved()).with_capacity(opts.cap));
    <Self as sealed::WalSealed<C, S>>::new_in(arena, opts, cmp, cks)
      .map(|core| Self::from_core(core, false))
  }

  /// Creates a new in-memory write-ahead log but backed by an anonymous mmap.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::{swmr::OrderWal, WalBuilder, Options};
  ///
  /// let wal = OrderWal::map_anon(WalBuidler::new(Options::new())).unwrap();
  /// ```
  fn map_anon(b: WalBuidler<C, S>) -> Result<Self, Error> {
    let WalBuidler { opts, cmp, cks } = b;
    let mmap_opts = MmapOptions::new().len(opts.cap).huge(opts.huge);
    <Self::Allocator as Allocator>::map_anon(arena_options(opts.reserved()), mmap_opts)
      .map_err(Into::into)
      .and_then(|arena| {
        <Self as sealed::WalSealed<C, S>>::new_in(arena, opts, cmp, cks)
          .map(|core| Self::from_core(core, false))
      })
  }

  /// Opens a write-ahead log backed by a file backed memory map in read-only mode.
  fn map<P>(path: P, b: WalBuidler<C, S>) -> Result<Self, Error>
  where
    C: Comparator + CheapClone,
    S: Checksumer,
    P: AsRef<std::path::Path>,
  {
    Self::map_with_path_builder::<_, ()>(|| Ok(path.as_ref().to_path_buf()), b)
      .map_err(|e| e.unwrap_right())
  }

  /// Opens a write-ahead log backed by a file backed memory map.
  fn map_mut<P>(path: P, b: WalBuidler<C, S>, open_opts: OpenOptions) -> Result<Self, Error>
  where
    C: Comparator + CheapClone,
    S: Checksumer,
    P: AsRef<std::path::Path>,
  {
    Self::map_mut_with_path_builder::<_, ()>(|| Ok(path.as_ref().to_path_buf()), b, open_opts)
      .map_err(|e| e.unwrap_right())
  }

  /// Opens a write-ahead log backed by a file backed memory map in read-only mode.
  fn map_with_path_builder<PB, E>(
    path_builder: PB,
    b: WalBuidler<C, S>,
  ) -> Result<Self, Either<E, Error>>
  where
    PB: FnOnce() -> Result<std::path::PathBuf, E>,
    C: Comparator + CheapClone,
    S: Checksumer,
  {
    let open_options = OpenOptions::default().read(true);

    let WalBuidler { opts, cmp, cks } = b;

    <Self::Allocator as Allocator>::map_with_path_builder(
      path_builder,
      arena_options(opts.reserved()),
      open_options,
      MmapOptions::new(),
    )
    .map_err(|e| e.map_right(Into::into))
    .and_then(|arena| {
      Self::replay(arena, Options::new(), true, cmp, cks)
        .map(|core| Self::from_core(core, true))
        .map_err(Either::Right)
    })
  }

  /// Opens a write-ahead log backed by a file backed memory map.
  fn map_mut_with_path_builder<PB, E>(
    path_builder: PB,
    b: WalBuidler<C, S>,
    open_options: OpenOptions,
  ) -> Result<Self, Either<E, Error>>
  where
    PB: FnOnce() -> Result<std::path::PathBuf, E>,
    C: Comparator + CheapClone,
    S: Checksumer,
  {
    let path = path_builder().map_err(Either::Left)?;

    let exist = path.exists();

    let WalBuidler { opts, cmp, cks } = b;

    <Self::Allocator as Allocator>::map_mut(
      path,
      arena_options(opts.reserved()),
      open_options,
      MmapOptions::new(),
    )
    .map_err(Into::into)
    .and_then(|arena| {
      if !exist {
        <Self as sealed::WalSealed<C, S>>::new_in(arena, opts, cmp, cks)
          .map(|core| Self::from_core(core, false))
      } else {
        <Self as sealed::WalSealed<C, S>>::replay(arena, opts, false, cmp, cks)
          .map(|core| Self::from_core(core, false))
      }
    })
    .map_err(Either::Right)
  }

  /// Returns `true` if this WAL instance is read-only.
  fn read_only(&self) -> bool;

  /// Returns the number of entries in the WAL.
  fn len(&self) -> usize;

  /// Returns `true` if the WAL is empty.
  fn is_empty(&self) -> bool {
    self.len() == 0
  }

  /// Flushes the to disk.
  fn flush(&self) -> Result<(), Error>;

  /// Flushes the to disk.
  fn flush_async(&self) -> Result<(), Error>;

  /// Returns `true` if the WAL contains the specified key.
  fn contains_key<Q>(&self, key: &Q) -> bool
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    C: Comparator;

  /// Returns an iterator over the entries in the WAL.
  fn iter(&self) -> Self::Iter<'_>
  where
    C: Comparator;

  /// Returns the value associated with the key.
  fn get<Q>(&self, key: &Q) -> Option<&[u8]>
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    C: Comparator;

  /// Get or insert a new entry into the WAL.
  fn get_or_insert(&mut self, key: &[u8], value: &[u8]) -> Result<Option<&[u8]>, Error>
  where
    C: Comparator + CheapClone,
    S: Checksumer,
  {
    self
      .get_or_insert_with_value_builder::<()>(
        key,
        ValueBuilder::new(value.len() as u32, |buf| {
          buf.write(value).unwrap();
          Ok(())
        }),
      )
      .map_err(|e| e.unwrap_right())
  }

  /// Get or insert a new entry into the WAL.
  fn get_or_insert_with_value_builder<E>(
    &mut self,
    key: &[u8],
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
  ) -> Result<Option<&[u8]>, Either<E, Error>>
  where
    C: Comparator + CheapClone,
    S: Checksumer;

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the key in place.
  ///
  /// See also [`insert_with_value_builder`](Wal::insert_with_value_builder) and [`insert_with_builders`](Wal::insert_with_builders).
  fn insert_with_key_builder<E>(
    &mut self,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
    value: &[u8],
  ) -> Result<(), Either<E, Error>>
  where
    C: Comparator + CheapClone,
    S: Checksumer,
  {
    if self.read_only() {
      return Err(Either::Right(Error::read_only()));
    }

    self
      .check(kb.size() as usize, value.len())
      .map_err(Either::Right)?;

    self
      .insert_with_in::<E, ()>(
        kb,
        ValueBuilder::new(value.len() as u32, |buf| {
          buf.write(value).unwrap();
          Ok(())
        }),
      )
      .map_err(|e| match e {
        Among::Left(e) => Either::Left(e),
        Among::Middle(_) => unreachable!(),
        Among::Right(e) => Either::Right(e),
      })
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the value in place.
  ///
  /// See also [`insert_with_key_builder`](Wal::insert_with_key_builder) and [`insert_with_builders`](Wal::insert_with_builders).
  fn insert_with_value_builder<E>(
    &mut self,
    key: &[u8],
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
  ) -> Result<(), Either<E, Error>>
  where
    C: Comparator + CheapClone,
    S: Checksumer,
  {
    if self.read_only() {
      return Err(Either::Right(Error::read_only()));
    }

    self
      .check(key.len(), vb.size() as usize)
      .map_err(Either::Right)?;

    self
      .insert_with_in::<(), E>(
        KeyBuilder::new(key.len() as u32, |buf| {
          buf.write(key).unwrap();
          Ok(())
        }),
        vb,
      )
      .map_err(|e| match e {
        Among::Left(_) => unreachable!(),
        Among::Middle(e) => Either::Left(e),
        Among::Right(e) => Either::Right(e),
      })
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the key and value in place.
  fn insert_with_builders<KE, VE>(
    &mut self,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), KE>>,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), VE>>,
  ) -> Result<(), Among<KE, VE, Error>>
  where
    C: Comparator + CheapClone,
    S: Checksumer,
  {
    if self.read_only() {
      return Err(Among::Right(Error::read_only()));
    }

    self
      .check(kb.size() as usize, vb.size() as usize)
      .map_err(Among::Right)?;

    self.insert_with_in(kb, vb)
  }

  /// Inserts a key-value pair into the WAL.
  fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error>
  where
    C: Comparator + CheapClone,
    S: Checksumer,
  {
    if self.read_only() {
      return Err(Error::read_only());
    }

    self.check(key.len(), value.len())?;

    self
      .insert_with_in::<(), ()>(
        KeyBuilder::new(key.len() as u32, |buf| {
          buf.write(key).unwrap();
          Ok(())
        }),
        ValueBuilder::new(value.len() as u32, |buf| {
          buf.write(value).unwrap();
          Ok(())
        }),
      )
      .map_err(Among::unwrap_right)
  }
}
/// A single writer multiple readers ordered write-ahead Log implementation.
pub mod swmr;

/// An ordered write-ahead Log implementation.
pub mod unsync;

#[cfg(test)]
mod tests;
