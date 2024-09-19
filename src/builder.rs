use checksum::BuildChecksumer;
use wal::{sealed::Constructor, Wal};

use super::*;

/// A write-ahead log builder.
pub struct Builder<C = Ascend, S = Crc32> {
  pub(super) opts: Options,
  pub(super) cmp: C,
  pub(super) cks: S,
}

impl Default for Builder {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl Builder {
  /// Returns a new write-ahead log builder with the given options.
  #[inline]
  pub fn new() -> Self {
    Self {
      opts: Options::default(),
      cmp: Ascend,
      cks: Crc32::default(),
    }
  }
}

impl<C, S> Builder<C, S> {
  /// Returns a new write-ahead log builder with the new comparator
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{Builder, Ascend};
  ///
  /// let opts = Builder::new().with_comparator(Ascend);
  /// ```
  #[inline]
  pub fn with_comparator<NC>(self, cmp: NC) -> Builder<NC, S> {
    Builder {
      opts: self.opts,
      cmp,
      cks: self.cks,
    }
  }

  /// Returns a new write-ahead log builder with the new checksumer
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{Builder, Crc32};
  ///
  /// let opts = Builder::new().with_checksumer(Crc32::new());
  /// ```
  #[inline]
  pub fn with_checksumer<NS>(self, cks: NS) -> Builder<C, NS> {
    Builder {
      opts: self.opts,
      cmp: self.cmp,
      cks,
    }
  }

  /// Returns a new write-ahead log builder with the new options
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{Builder, Options};
  ///
  /// let opts = Builder::new().with_options(Options::default());
  /// ```
  #[inline]
  pub fn with_options(self, opts: Options) -> Self {
    Self {
      opts,
      cmp: self.cmp,
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
  /// use orderwal::Builder;
  ///
  /// let opts = Builder::new().with_reserved(8);
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
  /// use orderwal::Builder;
  ///
  /// let opts = Builder::new().with_reserved(8);
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
  /// use orderwal::Builder;
  ///
  /// let options = Builder::new().with_magic_version(1);
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
  /// use orderwal::Builder;
  ///
  /// let options = Builder::new().with_capacity(1000);
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
  /// use orderwal::Builder;
  ///
  /// let options = Builder::new().with_maximum_key_size(1024);
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
  /// use orderwal::Builder;
  ///
  /// let options = Builder::new().with_maximum_value_size(1024);
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
  /// use orderwal::Builder;
  ///
  /// let options = Builder::new();
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
  /// use orderwal::Builder;
  ///
  /// let options = Builder::new().with_huge(64);
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
  /// use orderwal::Builder;
  ///
  /// let options = Builder::new().with_capacity(100);
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
  /// use orderwal::Builder;
  ///
  /// let options = Builder::new().with_maximum_key_size(1024);
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
  /// use orderwal::Builder;
  ///
  /// let options = Builder::new().with_maximum_value_size(1024);
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
  /// use orderwal::Builder;
  ///
  /// let options = Builder::new().with_huge(64);
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
  /// use orderwal::Builder;
  ///
  /// let options = Builder::new().with_sync_on_write(false);
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
  /// use orderwal::Builder;
  ///
  /// let options = Builder::new().with_magic_version(1);
  /// assert_eq!(options.magic_version(), 1);
  /// ```
  #[inline]
  pub const fn with_magic_version(mut self, version: u16) -> Self {
    self.opts = self.opts.with_magic_version(version);
    self
  }
}

impl<C, S> Builder<C, S> {
  /// Creates a new in-memory write-ahead log backed by an aligned vec.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{swmr::OrderWal, Builder};
  ///
  /// let wal = Builder::new()
  ///   .with_capacity(1024)
  ///   .alloc::<OrderWal>()
  ///   .unwrap();
  /// ```
  pub fn alloc<W>(self) -> Result<W, Error>
  where
    W: Wal<C, S>,
  {
    let Self { opts, cmp, cks } = self;
    arena_options(opts.reserved())
      .with_capacity(opts.capacity())
      .alloc()
      .map_err(Error::from_insufficient_space)
      .and_then(|arena| <W as Constructor<C, S>>::new_in(arena, opts, cmp, cks).map(W::from_core))
  }

  /// Creates a new in-memory write-ahead log but backed by an anonymous mmap.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{swmr::OrderWal, Builder};
  ///
  /// let wal = Builder::new()
  ///   .with_capacity(1024)
  ///   .map_anon::<OrderWal>()
  ///   .unwrap();
  /// ```
  pub fn map_anon<W>(self) -> Result<W, Error>
  where
    W: Wal<C, S>,
  {
    let Self { opts, cmp, cks } = self;
    let mmap_opts = MmapOptions::new().len(opts.capacity());
    arena_options(opts.reserved())
      .map_anon(mmap_opts)
      .map_err(Into::into)
      .and_then(|arena| <W as Constructor<C, S>>::new_in(arena, opts, cmp, cks).map(W::from_core))
  }

  /// Opens a write-ahead log backed by a file backed memory map in read-only mode.
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
  /// use orderwal::{swmr::OrderWal, Builder};
  /// # use orderwal::OpenOptions;
  ///
  /// # let dir = tempfile::tempdir().unwrap();
  /// # let path = dir.path().join("map.wal");
  ///
  /// # let wal = unsafe {
  /// #  Builder::new()
  /// #  .map_mut::<OrderWal, _>(&path, OpenOptions::default().read(true).write(true).create(Some(1000)))
  /// #  .unwrap()
  /// # };
  ///
  /// let wal = unsafe {
  ///   Builder::new()
  ///     .map::<OrderWal, _>(&path)
  ///     .unwrap()
  /// };
  pub unsafe fn map<W, P>(self, path: P) -> Result<W::Reader, Error>
  where
    C: Comparator + CheapClone + 'static,
    S: BuildChecksumer,
    P: AsRef<std::path::Path>,
    W: Wal<C, S>,
  {
    self
      .map_with_path_builder::<W, _, ()>(|| Ok(path.as_ref().to_path_buf()))
      .map_err(|e| e.unwrap_right())
  }

  /// Opens a write-ahead log backed by a file backed memory map in read-only mode.
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
  /// use orderwal::{swmr::OrderWal, Builder};
  /// # use orderwal::OpenOptions;
  ///
  /// # let dir = tempfile::tempdir().unwrap();
  /// # let path = dir.path().join("map_with_path_builder.wal");
  ///
  /// # let wal = unsafe {
  /// #  Builder::new()
  /// #  .map_mut::<OrderWal, _>(&path, OpenOptions::default().read(true).write(true).create(Some(1000)))
  /// #  .unwrap()
  /// # };
  ///
  /// let wal = unsafe {
  ///   Builder::new()
  ///     .map_with_path_builder::<OrderWal, _, ()>(|| Ok(path))
  ///     .unwrap()
  /// };
  pub unsafe fn map_with_path_builder<W, PB, E>(
    self,
    path_builder: PB,
  ) -> Result<W::Reader, Either<E, Error>>
  where
    PB: FnOnce() -> Result<std::path::PathBuf, E>,
    C: Comparator + CheapClone + 'static,
    S: BuildChecksumer,
    W: Wal<C, S>,
    W::Pointer: Ord + 'static,
  {
    let open_options = OpenOptions::default().read(true);

    let Self { opts, cmp, cks } = self;

    arena_options(opts.reserved())
      .map_with_path_builder(path_builder, open_options, MmapOptions::new())
      .map_err(|e| e.map_right(Into::into))
      .and_then(|arena| {
        <W::Reader as Constructor<C, S>>::replay(arena, Options::new(), true, cmp, cks)
          .map(<W::Reader as Constructor<C, S>>::from_core)
          .map_err(Either::Right)
      })
  }

  /// Opens a write-ahead log backed by a file backed memory map.
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
  /// use orderwal::{swmr::OrderWal, Builder, OpenOptions};
  ///
  /// let dir = tempfile::tempdir().unwrap();
  /// let path = dir.path().join("map_mut_with_path_builder_example.wal");
  ///
  /// let wal = unsafe {
  ///   Builder::new()
  ///   .map_mut::<OrderWal, _>(&path, OpenOptions::default().read(true).write(true).create(Some(1000)))
  ///   .unwrap()
  /// };
  /// ```
  pub unsafe fn map_mut<W, P>(self, path: P, open_opts: OpenOptions) -> Result<W, Error>
  where
    C: Comparator + CheapClone + 'static,
    S: BuildChecksumer,
    P: AsRef<std::path::Path>,
    W: Wal<C, S>,
  {
    self
      .map_mut_with_path_builder::<W, _, ()>(|| Ok(path.as_ref().to_path_buf()), open_opts)
      .map_err(|e| e.unwrap_right())
  }

  /// Opens a write-ahead log backed by a file backed memory map.
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
  /// use orderwal::{swmr::OrderWal, Builder, OpenOptions};
  ///
  /// let dir = tempfile::tempdir().unwrap();
  ///  
  /// let wal = unsafe {
  ///   Builder::new()
  ///   .map_mut_with_path_builder::<OrderWal, _, ()>(
  ///     || Ok(dir.path().join("map_mut_with_path_builder_example.wal")),
  ///     OpenOptions::default().read(true).write(true).create(Some(1000)),
  ///   )
  ///   .unwrap()
  /// };
  /// ```
  pub unsafe fn map_mut_with_path_builder<W, PB, E>(
    self,
    path_builder: PB,
    open_options: OpenOptions,
  ) -> Result<W, Either<E, Error>>
  where
    PB: FnOnce() -> Result<std::path::PathBuf, E>,
    C: Comparator + CheapClone + 'static,
    S: BuildChecksumer,
    W: Wal<C, S>,
  {
    let path = path_builder().map_err(Either::Left)?;
    let exist = path.exists();
    let Self { opts, cmp, cks } = self;

    arena_options(opts.reserved())
      .map_mut(path, open_options, MmapOptions::new())
      .map_err(Into::into)
      .and_then(|arena| {
        if !exist {
          <W as Constructor<C, S>>::new_in(arena, opts, cmp, cks).map(W::from_core)
        } else {
          <W as Constructor<C, S>>::replay(arena, opts, false, cmp, cks).map(W::from_core)
        }
      })
      .map_err(Either::Right)
  }
}
