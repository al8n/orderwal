use super::HEADER_SIZE;

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
