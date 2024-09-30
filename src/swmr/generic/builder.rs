use std::path::{Path, PathBuf};

use crate::options::ArenaOptionsExt;

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
  /// use orderwal::{swmr::GenericBuilder, Crc32};
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
  /// use orderwal::{swmr::GenericBuilder, Options};
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
  /// use orderwal::swmr::GenericBuilder;
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
  /// use orderwal::swmr::GenericBuilder;
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
  /// use orderwal::swmr::GenericBuilder;
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
  /// use orderwal::swmr::GenericBuilder;
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
  /// use orderwal::swmr::GenericBuilder;
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
  /// use orderwal::swmr::GenericBuilder;
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
  /// use orderwal::swmr::GenericBuilder;
  ///
  /// let options = GenericBuilder::new();
  /// assert_eq!(options.sync(), true);
  /// ```
  #[inline]
  pub const fn sync(&self) -> bool {
    self.opts.sync()
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
  /// use orderwal::swmr::GenericBuilder;
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
  /// use orderwal::swmr::GenericBuilder;
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
  /// use orderwal::swmr::GenericBuilder;
  ///
  /// let options = GenericBuilder::new().with_maximum_value_size(1024);
  /// assert_eq!(options.maximum_value_size(), 1024);
  /// ```
  #[inline]
  pub const fn with_maximum_value_size(mut self, size: u32) -> Self {
    self.opts = self.opts.with_maximum_value_size(size);
    self
  }

  /// Sets the WAL to sync on write.
  ///
  /// The default value is `true`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::swmr::GenericBuilder;
  ///
  /// let options = GenericBuilder::new().with_sync(false);
  /// assert_eq!(options.sync(), false);
  /// ```
  #[inline]
  pub const fn with_sync(mut self, sync: bool) -> Self {
    self.opts = self.opts.with_sync(sync);
    self
  }

  /// Sets the magic version.
  ///
  /// The default value is `0`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::swmr::GenericBuilder;
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

impl<S> GenericBuilder<S> {
  /// Sets the option for read access.
  ///
  /// This option, when true, will indicate that the file should be
  /// `read`-able if opened.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::swmr::GenericBuilder;
  ///
  /// let opts = GenericBuilder::new().with_read(true);
  /// ```
  #[inline]
  pub fn with_read(mut self, read: bool) -> Self {
    self.opts.read = read;
    self
  }

  /// Sets the option for write access.
  ///
  /// This option, when true, will indicate that the file should be
  /// `write`-able if opened.
  ///
  /// If the file already exists, any write calls on it will overwrite its
  /// contents, without truncating it.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::swmr::GenericBuilder;
  ///
  /// let opts = GenericBuilder::new().with_write(true);
  /// ```
  #[inline]
  pub fn with_write(mut self, write: bool) -> Self {
    self.opts.write = write;
    self
  }

  /// Sets the option for the append mode.
  ///
  /// This option, when true, means that writes will append to a file instead
  /// of overwriting previous contents.
  /// Note that setting `.write(true).append(true)` has the same effect as
  /// setting only `.append(true)`.
  ///
  /// For most filesystems, the operating system guarantees that all writes are
  /// atomic: no writes get mangled because another process writes at the same
  /// time.
  ///
  /// One maybe obvious note when using append-mode: make sure that all data
  /// that belongs together is written to the file in one operation. This
  /// can be done by concatenating strings before passing them to [`write()`],
  /// or using a buffered writer (with a buffer of adequate size),
  /// and calling [`flush()`] when the message is complete.
  ///
  /// If a file is opened with both read and append access, beware that after
  /// opening, and after every write, the position for reading may be set at the
  /// end of the file. So, before writing, save the current position (using
  /// <code>[seek]\([SeekFrom](std::io::SeekFrom)::[Current]\(opts))</code>), and restore it before the next read.
  ///
  /// ## Note
  ///
  /// This function doesn't create the file if it doesn't exist. Use the
  /// [`Options::with_create`] method to do so.
  ///
  /// [`write()`]: std::io::Write::write "io::Write::write"
  /// [`flush()`]: std::io::Write::flush "io::Write::flush"
  /// [seek]: std::io::Seek::seek "io::Seek::seek"
  /// [Current]: std::io::SeekFrom::Current "io::SeekFrom::Current"
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::swmr::GenericBuilder;
  ///
  /// let opts = GenericBuilder::new().with_append(true);
  /// ```
  #[inline]
  pub fn with_append(mut self, append: bool) -> Self {
    self.opts.write = true;
    self.opts.append = append;
    self
  }

  /// Sets the option for truncating a previous file.
  ///
  /// If a file is successfully opened with this option set it will truncate
  /// the file to opts length if it already exists.
  ///
  /// The file must be opened with write access for truncate to work.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::swmr::GenericBuilder;
  ///
  /// let opts = GenericBuilder::new().with_write(true).with_truncate(true);
  /// ```
  #[inline]
  pub fn with_truncate(mut self, truncate: bool) -> Self {
    self.opts.truncate = truncate;
    self.opts.write = true;
    self
  }

  /// Sets the option to create a new file, or open it if it already exists.
  /// If the file does not exist, it is created and set the lenght of the file to the given size.
  ///
  /// In order for the file to be created, [`Options::with_write`] or
  /// [`Options::with_append`] access must be used.
  ///
  /// See also [`std::fs::write()`][std::fs::write] for a simple function to
  /// create a file with some given data.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::swmr::GenericBuilder;
  ///
  /// let opts = GenericBuilder::new().with_write(true).with_create(true);
  /// ```
  #[inline]
  pub fn with_create(mut self, val: bool) -> Self {
    self.opts.create = val;
    self
  }

  /// Sets the option to create a new file and set the file length to the given value, failing if it already exists.
  ///
  /// No file is allowed to exist at the target location, also no (dangling) symlink. In this
  /// way, if the call succeeds, the file returned is guaranteed to be new.
  ///
  /// This option is useful because it is atomic. Otherwise between checking
  /// whether a file exists and creating a new one, the file may have been
  /// created by another process (a TOCTOU race condition / attack).
  ///
  /// If `.with_create_new(true)` is set, [`.with_create()`] and [`.with_truncate()`] are
  /// ignored.
  ///
  /// The file must be opened with write or append access in order to create
  /// a new file.
  ///
  /// [`.with_create()`]: GenericBuilder::with_create
  /// [`.with_truncate()`]: GenericBuilder::with_truncate
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::swmr::GenericBuilder;
  ///
  /// let opts = GenericBuilder::new()
  ///   .with_write(true)
  ///   .with_create_new(true);
  /// ```
  #[inline]
  pub fn with_create_new(mut self, val: bool) -> Self {
    self.opts.create_new = val;
    self
  }

  /// Configures the anonymous memory map to be suitable for a process or thread stack.
  ///
  /// This option corresponds to the `MAP_STACK` flag on Linux. It has no effect on Windows.
  ///
  /// This option has no effect on file-backed memory maps and vec backed [`GenericOrderWal`].
  ///
  /// ## Example
  ///
  /// ```
  /// use orderwal::swmr::GenericBuilder;
  ///
  /// let opts = GenericBuilder::new().with_stack(true);
  /// ```
  #[inline]
  pub fn with_stack(mut self, stack: bool) -> Self {
    self.opts.stack = stack;
    self
  }

  /// Configures the anonymous memory map to be allocated using huge pages.
  ///
  /// This option corresponds to the `MAP_HUGETLB` flag on Linux. It has no effect on Windows.
  ///
  /// The size of the requested page can be specified in page bits. If not provided, the system
  /// default is requested. The requested length should be a multiple of this, or the mapping
  /// will fail.
  ///
  /// This option has no effect on file-backed memory maps and vec backed [`GenericOrderWal`].
  ///
  /// ## Example
  ///
  /// ```
  /// use orderwal::swmr::GenericBuilder;
  ///
  /// let opts = GenericBuilder::new().with_huge(Some(8));
  /// ```
  #[inline]
  pub fn with_huge(mut self, page_bits: Option<u8>) -> Self {
    self.opts.huge = page_bits;
    self
  }

  /// Populate (prefault) page tables for a mapping.
  ///
  /// For a file mapping, this causes read-ahead on the file. This will help to reduce blocking on page faults later.
  ///
  /// This option corresponds to the `MAP_POPULATE` flag on Linux. It has no effect on Windows.
  ///
  /// This option has no effect on vec backed [`GenericOrderWal`].
  ///
  /// ## Example
  ///
  /// ```
  /// use orderwal::swmr::GenericBuilder;
  ///
  /// let opts = GenericBuilder::new().with_populate(true);
  /// ```
  #[inline]
  pub fn with_populate(mut self, populate: bool) -> Self {
    self.opts.populate = populate;
    self
  }
}

impl<S> GenericBuilder<S> {
  /// Returns `true` if the file should be opened with read access.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::swmr::GenericBuilder;
  ///
  /// let opts = GenericBuilder::new().with_read(true);
  /// assert_eq!(opts.read(), true);
  /// ```
  #[inline]
  pub const fn read(&self) -> bool {
    self.opts.read
  }

  /// Returns `true` if the file should be opened with write access.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::swmr::GenericBuilder;
  ///
  /// let opts = GenericBuilder::new().with_write(true);
  /// assert_eq!(opts.write(), true);
  /// ```
  #[inline]
  pub const fn write(&self) -> bool {
    self.opts.write
  }

  /// Returns `true` if the file should be opened with append access.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::swmr::GenericBuilder;
  ///
  /// let opts = GenericBuilder::new().with_append(true);
  /// assert_eq!(opts.append(), true);
  /// ```
  #[inline]
  pub const fn append(&self) -> bool {
    self.opts.append
  }

  /// Returns `true` if the file should be opened with truncate access.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::swmr::GenericBuilder;
  ///
  /// let opts = GenericBuilder::new().with_truncate(true);
  /// assert_eq!(opts.truncate(), true);
  /// ```
  #[inline]
  pub const fn truncate(&self) -> bool {
    self.opts.truncate
  }

  /// Returns `true` if the file should be created if it does not exist.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::swmr::GenericBuilder;
  ///
  /// let opts = GenericBuilder::new().with_create(true);
  /// assert_eq!(opts.create(), true);
  /// ```
  #[inline]
  pub const fn create(&self) -> bool {
    self.opts.create
  }

  /// Returns `true` if the file should be created if it does not exist and fail if it does.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::swmr::GenericBuilder;
  ///
  /// let opts = GenericBuilder::new().with_create_new(true);
  /// assert_eq!(opts.create_new(), true);
  /// ```
  #[inline]
  pub const fn create_new(&self) -> bool {
    self.opts.create_new
  }

  /// Returns `true` if the memory map should be suitable for a process or thread stack.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::swmr::GenericBuilder;
  ///
  /// let opts = GenericBuilder::new().with_stack(true);
  /// assert_eq!(opts.stack(), true);
  /// ```
  #[inline]
  pub const fn stack(&self) -> bool {
    self.opts.stack
  }

  /// Returns the page bits of the memory map.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::swmr::GenericBuilder;
  ///
  /// let opts = GenericBuilder::new().with_huge(Some(8));
  /// assert_eq!(opts.huge(), Some(8));
  /// ```
  #[inline]
  pub const fn huge(&self) -> Option<u8> {
    self.opts.huge
  }

  /// Returns `true` if the memory map should populate (prefault) page tables for a mapping.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::swmr::GenericBuilder;
  ///
  /// let opts = GenericBuilder::new().with_populate(true);
  /// assert_eq!(opts.populate(), true);
  /// ```
  #[inline]
  pub const fn populate(&self) -> bool {
    self.opts.populate
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
  pub fn alloc<K, V>(self) -> Result<GenericOrderWal<K, V, S>, Error>
  where
    K: ?Sized,
    V: ?Sized,
  {
    let Self { opts, cks } = self;

    arena_options(opts.reserved())
      .with_capacity(opts.capacity())
      .alloc()
      .map_err(Error::from_insufficient_space)
      .and_then(|arena| {
        GenericOrderWal::new_in(arena, opts, (), cks).map(GenericOrderWal::from_core)
      })
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
  pub fn map_anon<K, V>(self) -> Result<GenericOrderWal<K, V, S>, Error>
  where
    K: ?Sized,
    V: ?Sized,
  {
    let Self { opts, cks } = self;

    arena_options(opts.reserved())
      .merge(&opts)
      .map_anon()
      .map_err(Into::into)
      .and_then(|arena| {
        GenericOrderWal::new_in(arena, opts, (), cks).map(GenericOrderWal::from_core)
      })
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
  ///
  /// use orderwal::swmr::{GenericOrderWal, GenericBuilder, generic::*};
  /// # let dir = tempfile::tempdir().unwrap();
  /// # let path = dir
  /// #  .path()
  /// #  .join("generic_wal_map_mut_with_checksumer");
  /// #
  /// # let mut wal = unsafe {
  /// #   GenericBuilder::new()
  /// #     .with_capacity(1024)
  /// #     .with_create_new(true)
  /// #     .with_read(true)
  /// #     .with_write(true)
  /// #      .map_mut::<String, String, _>(
  /// #       &path,
  /// #     )
  /// #     .unwrap()
  /// # };
  ///
  /// let reader = unsafe { GenericBuilder::new().map::<String, String, _>(path).unwrap() };
  /// ```
  #[inline]
  pub unsafe fn map<K, V, P: AsRef<Path>>(self, path: P) -> Result<GenericWalReader<K, V, S>, Error>
  where
    K: Type + Ord + ?Sized + 'static,
    for<'a> K::Ref<'a>: KeyRef<'a, K>,
    V: ?Sized + 'static,
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
  /// use orderwal::swmr::{GenericOrderWal, GenericBuilder, generic::*};
  /// # let dir = tempfile::tempdir().unwrap();
  /// # let path = dir
  /// #  .path()
  /// #  .join("generic_wal_map_mut_with_checksumer");
  /// #
  /// # let mut wal = unsafe {
  /// #   GenericBuilder::new()
  /// #     .with_capacity(1024)
  /// #     .with_create_new(true)
  /// #     .with_read(true)
  /// #     .with_write(true)
  /// #     .map_mut::<String, String, _>(
  /// #       &path,
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
    K: Type + Ord + ?Sized + 'static,
    for<'a> K::Ref<'a>: KeyRef<'a, K>,
    V: ?Sized + 'static,
    PB: FnOnce() -> Result<PathBuf, E>,
  {
    let Self { cks, opts } = self;

    arena_options(opts.reserved())
      .merge(&opts)
      .with_read(true)
      .map_with_path_builder(path_builder)
      .map_err(|e| e.map_right(Into::into))
      .and_then(|arena| {
        GenericOrderWal::replay(arena, opts, true, (), cks)
          .map(|core| GenericWalReader::new(Arc::new(core)))
          .map_err(Either::Right)
      })
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
  /// use orderwal::swmr::{GenericOrderWal, GenericBuilder, generic::*};
  ///
  /// # let dir = tempfile::tempdir().unwrap();
  /// # let path = dir
  /// #  .path()
  /// #  .join("generic_wal_map_mut");
  ///
  /// let mut wal = unsafe {
  ///   GenericBuilder::new()
  ///     .with_capacity(1024)
  ///     .with_create_new(true)
  ///     .with_read(true)
  ///     .with_write(true)
  ///     .map_mut::<String, String, _>(&path)
  ///     .unwrap()
  /// };
  /// ```
  #[inline]
  pub unsafe fn map_mut<K, V, P: AsRef<Path>>(
    self,
    path: P,
  ) -> Result<GenericOrderWal<K, V, S>, Error>
  where
    K: Type + Ord + ?Sized + 'static,
    for<'a> K::Ref<'a>: KeyRef<'a, K>,
    V: ?Sized + 'static,
  {
    self
      .map_mut_with_path_builder::<K, V, _, ()>(|| dummy_path_builder(path))
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
  /// use orderwal::swmr::{GenericOrderWal, GenericBuilder, generic::*};
  ///
  /// let dir = tempfile::tempdir().unwrap();
  ///
  /// let mut wal = unsafe {
  ///  GenericBuilder::new()
  ///   .with_create_new(true)
  ///   .with_write(true)
  ///   .with_read(true)
  ///   .with_capacity(1024)
  ///   .map_mut_with_path_builder::<String, String, _, ()>(
  ///     || {
  ///       Ok(dir.path().join("generic_wal_map_mut_with_path_builder_and_checksumer"))
  ///     },
  ///   )
  ///   .unwrap()
  /// };
  /// ```
  pub unsafe fn map_mut_with_path_builder<K, V, PB, E>(
    self,
    path_builder: PB,
  ) -> Result<GenericOrderWal<K, V, S>, Either<E, Error>>
  where
    K: Type + Ord + ?Sized + 'static,
    for<'a> K::Ref<'a>: KeyRef<'a, K>,
    V: ?Sized + 'static,
    PB: FnOnce() -> Result<PathBuf, E>,
  {
    let Self { opts, cks } = self;
    let path = path_builder().map_err(Either::Left)?;
    let exist = path.exists();
    let arena = arena_options(opts.reserved())
      .merge(&opts)
      .map_mut_with_path_builder(|| Ok(path))
      .map_err(|e| e.map_right(Into::into))?;

    if !exist {
      GenericOrderWal::new_in(arena, opts, (), cks)
    } else {
      GenericOrderWal::replay(arena, opts, false, (), cks)
    }
    .map(GenericOrderWal::from_core)
    .map_err(Either::Right)
  }
}
