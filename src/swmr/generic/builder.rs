use std::path::{Path, PathBuf};

use rarena_allocator::sync::Arena;

use super::*;

/// A write-ahead log builder.
pub struct GenericBuilder<S = Crc32> {
  pub(super) opts: Options,
  pub(super) cks: S,
}

impl Default for GenericBuilder {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl GenericBuilder {
  /// Returns a new write-ahead log builder with the given options.
  #[inline]
  pub fn new() -> Self {
    Self {
      opts: Options::default(),
      cks: Crc32::default(),
    }
  }
}

impl<S> GenericBuilder<S> {
  /// Returns a new write-ahead log builder with the new checksumer
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{GenericBuilder, Crc32};
  ///
  /// let opts = GenericBuilder::new().with_checksumer(Crc32::new());
  /// ```
  #[inline]
  pub fn with_checksumer<NS>(self, cks: NS) -> GenericBuilder<NS> {
    GenericBuilder {
      opts: self.opts,
      cks,
    }
  }

  /// Returns a new write-ahead log builder with the new options
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{GenericBuilder, Options};
  ///
  /// let opts = GenericBuilder::new().with_options(Options::default());
  /// ```
  #[inline]
  pub fn with_options(self, opts: Options) -> Self {
    Self {
      opts,
      cks: self.cks,
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
  /// use orderwal::GenericBuilder;
  ///
  /// let opts = GenericBuilder::new().with_reserved(8);
  /// ```
  #[inline]
  pub const fn with_reserved(mut self, reserved: u32) -> Self {
    self.opts = self.opts.with_reserved(reserved);
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
  /// use orderwal::GenericBuilder;
  ///
  /// let opts = GenericBuilder::new().with_reserved(8);
  ///
  /// assert_eq!(opts.reserved(), 8);
  /// ```
  #[inline]
  pub const fn reserved(&self) -> u32 {
    self.opts.reserved()
  }

  /// Returns the magic version.
  ///
  /// The default value is `0`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::GenericBuilder;
  ///
  /// let options = GenericBuilder::new().with_magic_version(1);
  /// assert_eq!(options.magic_version(), 1);
  /// ```
  #[inline]
  pub const fn magic_version(&self) -> u16 {
    self.opts.magic_version()
  }

  /// Returns the capacity of the WAL.
  ///
  /// The default value is `0`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::GenericBuilder;
  ///
  /// let options = GenericBuilder::new().with_capacity(1000);
  /// assert_eq!(options.capacity(), 1000);
  /// ```
  #[inline]
  pub const fn capacity(&self) -> u32 {
    self.opts.capacity()
  }

  /// Returns the maximum key length.
  ///
  /// The default value is `u16::MAX`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::GenericBuilder;
  ///
  /// let options = GenericBuilder::new().with_maximum_key_size(1024);
  /// assert_eq!(options.maximum_key_size(), 1024);
  /// ```
  #[inline]
  pub const fn maximum_key_size(&self) -> u32 {
    self.opts.maximum_key_size()
  }

  /// Returns the maximum value length.
  ///
  /// The default value is `u32::MAX`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::GenericBuilder;
  ///
  /// let options = GenericBuilder::new().with_maximum_value_size(1024);
  /// assert_eq!(options.maximum_value_size(), 1024);
  /// ```
  #[inline]
  pub const fn maximum_value_size(&self) -> u32 {
    self.opts.maximum_value_size()
  }

  /// Returns `true` if the WAL syncs on write.
  ///
  /// The default value is `true`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::GenericBuilder;
  ///
  /// let options = GenericBuilder::new();
  /// assert_eq!(options.sync_on_write(), true);
  /// ```
  #[inline]
  pub const fn sync_on_write(&self) -> bool {
    self.opts.sync_on_write()
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
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::GenericBuilder;
  ///
  /// let options = GenericBuilder::new().with_huge(64);
  /// assert_eq!(options.huge(), Some(64));
  /// ```
  #[inline]
  pub const fn huge(&self) -> Option<u8> {
    self.opts.huge()
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
  /// use orderwal::GenericBuilder;
  ///
  /// let options = GenericBuilder::new().with_capacity(100);
  /// assert_eq!(options.capacity(), 100);
  /// ```
  #[inline]
  pub const fn with_capacity(mut self, cap: u32) -> Self {
    self.opts = self.opts.with_capacity(cap);
    self
  }

  /// Sets the maximum key length.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::GenericBuilder;
  ///
  /// let options = GenericBuilder::new().with_maximum_key_size(1024);
  /// assert_eq!(options.maximum_key_size(), 1024);
  /// ```
  #[inline]
  pub const fn with_maximum_key_size(mut self, size: u32) -> Self {
    self.opts = self.opts.with_maximum_key_size(size);
    self
  }

  /// Sets the maximum value length.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::GenericBuilder;
  ///
  /// let options = GenericBuilder::new().with_maximum_value_size(1024);
  /// assert_eq!(options.maximum_value_size(), 1024);
  /// ```
  #[inline]
  pub const fn with_maximum_value_size(mut self, size: u32) -> Self {
    self.opts = self.opts.with_maximum_value_size(size);
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
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::GenericBuilder;
  ///
  /// let options = GenericBuilder::new().with_huge(64);
  /// assert_eq!(options.huge(), Some(64));
  /// ```
  #[inline]
  pub const fn with_huge(mut self, page_bits: u8) -> Self {
    self.opts = self.opts.with_huge(page_bits);
    self
  }

  /// Sets the WAL to sync on write.
  ///
  /// The default value is `true`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::GenericBuilder;
  ///
  /// let options = GenericBuilder::new().with_sync_on_write(false);
  /// assert_eq!(options.sync_on_write(), false);
  /// ```
  #[inline]
  pub const fn with_sync_on_write(mut self, sync: bool) -> Self {
    self.opts = self.opts.with_sync_on_write(sync);
    self
  }

  /// Sets the magic version.
  ///
  /// The default value is `0`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::GenericBuilder;
  ///
  /// let options = GenericBuilder::new().with_magic_version(1);
  /// assert_eq!(options.magic_version(), 1);
  /// ```
  #[inline]
  pub const fn with_magic_version(mut self, version: u16) -> Self {
    self.opts = self.opts.with_magic_version(version);
    self
  }
}

impl<S: BuildChecksumer> GenericBuilder<S> {
  /// Creates a new in-memory write-ahead log backed by an aligned vec with the given capacity and options.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::swmr::{GenericOrderWal, GenericBuilder};
  ///
  /// let wal = GenericBuilder::new().with_capacity(1024).alloc::<String, String>().unwrap();
  /// ```
  #[inline]
  pub fn alloc<K, V>(self) -> Result<GenericOrderWal<K, V, S>, Error> {
    let Self { opts, cks } = self;
    let arena = Arena::new(arena_options(opts.reserved()).with_capacity(opts.capacity()))
      .map_err(Error::from_insufficient_space)?;

    GenericOrderWal::new_in(arena, opts, (), cks).map(GenericOrderWal::from_core)
  }

  /// Creates a new in-memory write-ahead log backed by an anonymous memory map with the given options.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::swmr::{GenericOrderWal, GenericBuilder};
  ///
  /// let wal = GenericBuilder::new().with_capacity(1024).map_anon::<String, String>().unwrap();
  /// ```
  #[inline]
  pub fn map_anon<K, V>(self) -> Result<GenericOrderWal<K, V, S>, Error> {
    let Self { opts, cks } = self;
    let arena = Arena::map_anon(
      arena_options(opts.reserved()),
      MmapOptions::new().len(opts.capacity()),
    )?;

    GenericOrderWal::new_in(arena, opts, (), cks).map(GenericOrderWal::from_core)
  }

  /// Open a write-ahead log backed by a file backed memory map in read only mode.
  ///
  /// ## Safety
  ///
  /// All file-backed memory map constructors are marked `unsafe` because of the potential for
  /// *Undefined Behavior* (UB) using the map if the underlying file is subsequently modified, in or
  /// out of process. Applications must consider the risk and take appropriate precautions when
  /// using file-backed maps. Solutions such as file permissions, locks or process-private (e.g.
  /// unlinked) files exist but are platform specific and limited.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{swmr::{GenericOrderWal, GenericBuilder, generic::*}, OpenOptions};
  /// # let dir = tempfile::tempdir().unwrap();
  /// # let path = dir
  /// #  .path()
  /// #  .join("generic_wal_map_mut_with_checksumer");
  /// #
  /// # let mut wal = unsafe {
  /// #   GenericBuilder::new()
  /// #      .map_mut::<String, String>(
  /// #       &path,
  /// #       OpenOptions::new()
  /// #         .create_new(Some(1024))
  /// #         .write(true)
  /// #         .read(true),
  /// #     )
  /// #     .unwrap()
  /// # };
  ///
  /// let reader = unsafe { GenericBuilder::new().map::<String, String, _>(path).unwrap() };
  /// ```
  #[inline]
  pub unsafe fn map<K, V, P: AsRef<Path>>(self, path: P) -> Result<GenericWalReader<K, V, S>, Error>
  where
    K: Type + Ord + 'static,
    for<'a> K::Ref<'a>: KeyRef<'a, K>,
    V: 'static,
  {
    self
      .map_with_path_builder::<K, V, _, ()>(|| dummy_path_builder(path))
      .map_err(|e| e.unwrap_right())
  }

  /// Open a write-ahead log backed by a file backed memory map in read only mode with the given [`Checksumer`].
  ///
  /// ## Safety
  ///
  /// All file-backed memory map constructors are marked `unsafe` because of the potential for
  /// *Undefined Behavior* (UB) using the map if the underlying file is subsequently modified, in or
  /// out of process. Applications must consider the risk and take appropriate precautions when
  /// using file-backed maps. Solutions such as file permissions, locks or process-private (e.g.
  /// unlinked) files exist but are platform specific and limited.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{swmr::{GenericOrderWal, GenericBuilder, generic::*}, OpenOptions};
  /// # let dir = tempfile::tempdir().unwrap();
  /// # let path = dir
  /// #  .path()
  /// #  .join("generic_wal_map_mut_with_checksumer");
  /// #
  /// # let mut wal = unsafe {
  /// #   GenericBuilder::new()
  /// #      .map_mut::<String, String>(
  /// #       &path,
  /// #       OpenOptions::new()
  /// #         .create_new(Some(1024))
  /// #         .write(true)
  /// #         .read(true),
  /// #     )
  /// #     .unwrap()
  /// # };
  ///
  /// let reader = unsafe { GenericBuilder::new().map_with_path_builder::<String, String, _, ()>(|| Ok(path)).unwrap() };
  /// ```
  #[inline]
  pub unsafe fn map_with_path_builder<K, V, PB, E>(
    self,
    path_builder: PB,
  ) -> Result<GenericWalReader<K, V, S>, Either<E, Error>>
  where
    K: Type + Ord + 'static,
    for<'a> K::Ref<'a>: KeyRef<'a, K>,
    V: 'static,
    PB: FnOnce() -> Result<PathBuf, E>,
  {
    let Self { cks, opts } = self;
    let open_options = OpenOptions::default().read(true);
    let arena = Arena::map_with_path_builder(
      path_builder,
      arena_options(opts.reserved()),
      open_options,
      MmapOptions::new(),
    )
    .map_err(|e| e.map_right(Into::into))?;

    GenericOrderWal::replay(arena, opts, true, (), cks)
      .map(|core| GenericWalReader::new(Arc::new(core)))
      .map_err(Either::Right)
  }

  /// Creates a new write-ahead log backed by a file backed memory map with the given options.
  ///
  /// ## Safety
  ///
  /// All file-backed memory map constructors are marked `unsafe` because of the potential for
  /// *Undefined Behavior* (UB) using the map if the underlying file is subsequently modified, in or
  /// out of process. Applications must consider the risk and take appropriate precautions when
  /// using file-backed maps. Solutions such as file permissions, locks or process-private (e.g.
  /// unlinked) files exist but are platform specific and limited.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{swmr::{GenericOrderWal, GenericBuilder generic::*}, OpenOptions};
  ///
  /// # let dir = tempfile::tempdir().unwrap();
  /// # let path = dir
  /// #  .path()
  /// #  .join("generic_wal_map_mut");
  ///
  /// let mut wal = unsafe {
  ///   GenericBuilder::new().map_mut::<String, String>(
  ///     &path,
  ///     OpenOptions::new()
  ///       .create_new(Some(1024))
  ///       .write(true)
  ///       .read(true),
  ///   )
  ///   .unwrap()
  /// };
  /// ```
  #[inline]
  pub unsafe fn map_mut<K, V, P: AsRef<Path>>(
    self,
    path: P,
    open_options: OpenOptions,
  ) -> Result<GenericOrderWal<K, V, S>, Error>
  where
    K: Type + Ord + 'static,
    for<'a> K::Ref<'a>: KeyRef<'a, K>,
    V: 'static,
  {
    self
      .map_mut_with_path_builder::<K, V, _, ()>(|| dummy_path_builder(path), open_options)
      .map_err(|e| e.unwrap_right())
  }

  /// Returns a write-ahead log backed by a file backed memory map with the given options and [`Checksumer`].
  ///
  /// ## Safety
  ///
  /// All file-backed memory map constructors are marked `unsafe` because of the potential for
  /// *Undefined Behavior* (UB) using the map if the underlying file is subsequently modified, in or
  /// out of process. Applications must consider the risk and take appropriate precautions when
  /// using file-backed maps. Solutions such as file permissions, locks or process-private (e.g.
  /// unlinked) files exist but are platform specific and limited.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{swmr::{GenericOrderWal, GenericBuilder, generic::*}, OpenOptions};
  ///
  /// let dir = tempfile::tempdir().unwrap();
  ///
  /// let mut wal = unsafe {
  ///  GenericBuilder::new().map_mut_with_path_builder::<<String, String, _, ()>(
  ///    || {
  ///       Ok(dir.path().join("generic_wal_map_mut_with_path_builder_and_checksumer"))
  ///    },
  ///    OpenOptions::new()
  ///      .create_new(Some(1024))
  ///      .write(true)
  ///      .read(true),
  ///  )
  ///  .unwrap()
  /// };
  /// ```
  pub unsafe fn map_mut_with_path_builder<K, V, PB, E>(
    self,
    path_builder: PB,
    open_options: OpenOptions,
  ) -> Result<GenericOrderWal<K, V, S>, Either<E, Error>>
  where
    K: Type + Ord + 'static,
    for<'a> K::Ref<'a>: KeyRef<'a, K>,
    V: 'static,
    PB: FnOnce() -> Result<PathBuf, E>,
  {
    let Self { opts, cks } = self;
    let path = path_builder().map_err(Either::Left)?;
    let exist = path.exists();
    let arena = Arena::map_mut_with_path_builder(
      || Ok(path),
      arena_options(opts.reserved()),
      open_options,
      MmapOptions::new(),
    )
    .map_err(|e| e.map_right(Into::into))?;

    if !exist {
      return GenericOrderWal::new_in(arena, opts, (), cks)
        .map(GenericOrderWal::from_core)
        .map_err(Either::Right);
    }

    GenericOrderWal::replay(arena, opts, false, (), cks)
      .map(GenericOrderWal::from_core)
      .map_err(Either::Right)
  }
}
