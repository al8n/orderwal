use rarena_allocator::{Freelist, Options as ArenaOptions};

use super::{CURRENT_VERSION, HEADER_SIZE};

#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
mod memmap;

#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
pub(crate) use memmap::*;

/// Options for the WAL.
#[derive(Debug, Clone)]
pub struct Options {
  maximum_key_size: u32,
  maximum_value_size: u32,
  sync: bool,
  magic_version: u16,
  cap: Option<u32>,
  reserved: u32,

  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  pub(crate) lock_meta: bool,
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  pub(crate) read: bool,
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  pub(crate) write: bool,
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  pub(crate) create_new: bool,
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  pub(crate) create: bool,
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  pub(crate) truncate: bool,
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  pub(crate) append: bool,

  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  pub(crate) stack: bool,
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  pub(crate) populate: bool,
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  pub(crate) huge: Option<u8>,
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
  /// ## Example
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
      sync: true,
      magic_version: 0,
      cap: None,
      reserved: 0,
      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      lock_meta: false,
      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      read: false,
      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      write: false,
      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      create_new: false,
      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      create: false,
      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      truncate: false,
      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      append: false,
      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      stack: false,
      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      populate: false,
      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      huge: None,
    }
  }

  /// Set the reserved bytes of the WAL.
  ///
  /// The `reserved` is used to configure the start position of the WAL. This is useful
  /// when you want to add some bytes as your own WAL's header.
  ///
  /// The default reserved is `0`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::Options;
  ///
  /// let opts = Options::new().with_reserved(8);
  /// ```
  #[inline]
  pub const fn with_reserved(mut self, reserved: u32) -> Self {
    self.reserved = reserved;
    self
  }

  /// Get the reserved of the WAL.
  ///
  /// The `reserved` is used to configure the start position of the WAL. This is useful
  /// when you want to add some bytes as your own WAL's header.
  ///
  /// The default reserved is `0`.
  ///
  /// ## Example
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
  /// ## Example
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
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::Options;
  ///
  /// let options = Options::new().with_capacity(1000);
  /// assert_eq!(options.capacity(), 1000);
  /// ```
  #[inline]
  pub const fn capacity(&self) -> u32 {
    match self.cap {
      Some(cap) => cap,
      None => 0,
    }
  }

  /// Returns the maximum key length.
  ///
  /// The default value is `u16::MAX`.
  ///
  /// ## Example
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
  /// ## Example
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
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::Options;
  ///
  /// let options = Options::new();
  /// assert_eq!(options.sync(), true);
  /// ```
  #[inline]
  pub const fn sync(&self) -> bool {
    self.sync
  }

  /// Sets the capacity of the WAL.
  ///
  /// This configuration will be ignored when using file-backed memory maps.
  ///
  /// The default value is `0`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::Options;
  ///
  /// let options = Options::new().with_capacity(100);
  /// assert_eq!(options.capacity(), 100);
  /// ```
  #[inline]
  pub const fn with_capacity(mut self, cap: u32) -> Self {
    self.cap = Some(cap);
    self
  }

  /// Sets the maximum key length.
  ///
  /// ## Example
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
  /// ## Example
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

  /// Sets the WAL to sync on write.
  ///
  /// The default value is `true`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::Options;
  ///
  /// let options = Options::new().with_sync(false);
  /// assert_eq!(options.sync(), false);
  /// ```
  #[inline]
  pub const fn with_sync(mut self, sync: bool) -> Self {
    self.sync = sync;
    self
  }

  /// Sets the magic version.
  ///
  /// The default value is `0`.
  ///
  /// ## Example
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
pub(crate) const fn arena_options(reserved: u32) -> ArenaOptions {
  ArenaOptions::new()
    .with_magic_version(CURRENT_VERSION)
    .with_freelist(Freelist::None)
    .with_reserved((HEADER_SIZE + reserved as usize) as u32)
    .with_unify(true)
}
