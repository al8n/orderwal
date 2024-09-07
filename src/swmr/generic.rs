use core::{cmp, marker::PhantomData, slice};
use std::{
  path::{Path, PathBuf},
  sync::Arc,
};

use among::Among;
use crossbeam_skiplist::{Comparable, Equivalent, SkipSet};
use dbutils::{Checksumer, Crc32};
use rarena_allocator::{
  either::Either, sync::Arena, Allocator, ArenaPosition, Error as ArenaError, Memory, MmapOptions,
  OpenOptions,
};

use crate::{
  arena_options, entry_size,
  error::{self, Error},
  Flags, Options, UnsafeCellChecksumer, CHECKSUM_SIZE, HEADER_SIZE, KEY_LEN_SIZE, MAGIC_TEXT,
  STATUS_SIZE, VALUE_LEN_SIZE,
};

mod entry;
pub use entry::*;

mod traits;
pub use traits::*;

#[cfg(test)]
mod tests;

struct Pointer<K, V> {
  /// The pointer to the start of the entry.
  ptr: *const u8,
  /// The length of the key.
  key_len: usize,
  /// The length of the value.
  value_len: usize,

  cached_key: Option<K>,
  cached_value: Option<V>,
}

impl<K: Type, V> PartialEq for Pointer<K, V> {
  fn eq(&self, other: &Self) -> bool {
    self.as_key_slice() == other.as_key_slice()
  }
}

impl<K: Type, V> Eq for Pointer<K, V> {}

impl<K, V> PartialOrd for Pointer<K, V>
where
  K: Type + Ord,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
{
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<K, V> Ord for Pointer<K, V>
where
  K: Type + Ord,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
{
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    <K::Ref<'_> as KeyRef<K>>::compare_binary(self.as_key_slice(), other.as_key_slice())
  }
}

unsafe impl<K, V> Send for Pointer<K, V> {}
unsafe impl<K, V> Sync for Pointer<K, V> {}

impl<K, V> Pointer<K, V> {
  #[inline]
  const fn new(key_len: usize, value_len: usize, ptr: *const u8) -> Self {
    Self {
      ptr,
      key_len,
      value_len,
      cached_key: None,
      cached_value: None,
    }
  }

  #[inline]
  fn with_cached_key(mut self, key: K) -> Self {
    self.cached_key = Some(key);
    self
  }

  #[inline]
  fn with_cached_value(mut self, value: V) -> Self {
    self.cached_value = Some(value);
    self
  }

  #[inline]
  const fn as_key_slice<'a>(&self) -> &'a [u8] {
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

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Ref<'a, K, Q: ?Sized> {
  key: &'a Q,
  _k: PhantomData<K>,
}

impl<'a, K, Q: ?Sized> Ref<'a, K, Q> {
  #[inline]
  const fn new(key: &'a Q) -> Self {
    Self {
      key,
      _k: PhantomData,
    }
  }
}

impl<'a, K, Q, V> Equivalent<Pointer<K, V>> for Ref<'a, K, Q>
where
  K: Type + Ord,
  K::Ref<'a>: KeyRef<'a, K>,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
{
  fn equivalent(&self, key: &Pointer<K, V>) -> bool {
    self.compare(key).is_eq()
  }
}

impl<'a, K, Q, V> Comparable<Pointer<K, V>> for Ref<'a, K, Q>
where
  K: Type + Ord,
  K::Ref<'a>: KeyRef<'a, K>,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
{
  fn compare(&self, p: &Pointer<K, V>) -> cmp::Ordering {
    let kr = K::from_slice(p.as_key_slice());
    KeyRef::compare(&kr, self.key)
  }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Owned<'a, K, Q: ?Sized> {
  key: &'a Q,
  _k: PhantomData<K>,
}

impl<'a, K, Q: ?Sized> Owned<'a, K, Q> {
  #[inline]
  const fn new(key: &'a Q) -> Self {
    Self {
      key,
      _k: PhantomData,
    }
  }
}

impl<'a, K, Q, V> Equivalent<Pointer<K, V>> for Owned<'a, K, Q>
where
  K: Type + Ord,
  K::Ref<'a>: KeyRef<'a, K>,
  Q: ?Sized + Ord + Comparable<K> + Comparable<K::Ref<'a>>,
{
  fn equivalent(&self, key: &Pointer<K, V>) -> bool {
    self.compare(key).is_eq()
  }
}

impl<'a, K, Q, V> Comparable<Pointer<K, V>> for Owned<'a, K, Q>
where
  K: Type + Ord,
  K::Ref<'a>: KeyRef<'a, K>,
  Q: ?Sized + Ord + Comparable<K> + Comparable<K::Ref<'a>>,
{
  fn compare(&self, p: &Pointer<K, V>) -> cmp::Ordering {
    match p.cached_key.as_ref() {
      Some(k) => Comparable::compare(self.key, k),
      None => {
        let kr = K::from_slice(p.as_key_slice());
        KeyRef::compare(&kr, self.key).reverse()
      }
    }
  }
}

struct GenericOrderWalCore<K, V, S> {
  arena: Arena,
  map: SkipSet<Pointer<K, V>>,
  opts: Options,
  cks: UnsafeCellChecksumer<S>,
}

impl<K, V, S> GenericOrderWalCore<K, V, S> {
  #[inline]
  fn new(arena: Arena, opts: Options, cks: S, flush: bool) -> Result<Self, Error> {
    unsafe {
      let slice = arena.reserved_slice_mut();
      slice[0..6].copy_from_slice(&MAGIC_TEXT);
      slice[6..8].copy_from_slice(&opts.magic_version.to_le_bytes());
    }

    if !flush {
      return Ok(Self::construct(arena, SkipSet::new(), opts, cks));
    }

    arena
      .flush_range(0, HEADER_SIZE)
      .map(|_| Self::construct(arena, SkipSet::new(), opts, cks))
      .map_err(Into::into)
  }

  #[inline]
  fn construct(arena: Arena, set: SkipSet<Pointer<K, V>>, opts: Options, checksumer: S) -> Self {
    Self {
      arena,
      map: set,
      opts,
      cks: UnsafeCellChecksumer::new(checksumer),
    }
  }
}

impl<K, V, S> GenericOrderWalCore<K, V, S>
where
  K: Type + Ord + 'static,
  for<'a> <K as Type>::Ref<'a>: KeyRef<'a, K>,
  V: Type + 'static,
  S: Checksumer,
{
  fn replay(arena: Arena, opts: Options, ro: bool, checksumer: S) -> Result<Self, Error> {
    let slice = arena.reserved_slice();
    let magic_text = &slice[0..6];
    let magic_version = u16::from_le_bytes(slice[6..8].try_into().unwrap());

    if magic_text != MAGIC_TEXT {
      return Err(Error::magic_text_mismatch());
    }

    if magic_version != opts.magic_version {
      return Err(Error::magic_version_mismatch());
    }

    let map = SkipSet::new();

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

        map.insert(Pointer::new(key_len, value_len, arena.get_pointer(cursor)));
        cursor += elen;
      }
    }

    Ok(Self::construct(arena, map, opts, checksumer))
  }
}

/// Generic ordered write-ahead log implementation, which supports structured keys and values.
///
/// Only the first instance of the WAL can write to the log, while the rest can only read from the log.
pub struct GenericOrderWal<K, V, S = Crc32> {
  core: Arc<GenericOrderWalCore<K, V, S>>,
  ro: bool,
}

impl<K, V, S> Clone for GenericOrderWal<K, V, S> {
  fn clone(&self) -> Self {
    Self {
      core: self.core.clone(),
      ro: true,
    }
  }
}

impl<K, V> GenericOrderWal<K, V> {
  /// Creates a new in-memory write-ahead log backed by an aligned vec with the given capacity and options.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::{generic::GenericOrderWal, Options};
  ///
  /// let wal = GenericOrderWal::new(Options::new()).unwrap();
  /// ```
  #[inline]
  pub fn new(opts: Options) -> Self {
    Self::with_checksumer(opts, Crc32::default())
  }

  /// Creates a new in-memory write-ahead log backed by an anonymous memory map with the given options.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::{generic::GenericOrderWal, Options};
  ///
  /// let wal = GenericOrderWal::map_anon(Options::new()).unwrap();
  /// ```
  #[inline]
  pub fn map_anon(opts: Options) -> Result<Self, Error> {
    Self::map_anon_with_checksumer(opts, Crc32::default())
  }
}

impl<K, V> GenericOrderWal<K, V>
where
  K: Type + Ord + 'static,
  for<'a> <K as Type>::Ref<'a>: KeyRef<'a, K>,
  V: Type + 'static,
{
  /// Creates a new write-ahead log backed by a file backed memory map with the given options.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::{generic::GenericOrderWal, Options, OpenOptions};
  ///
  /// ```
  #[inline]
  pub fn map_mut<P: AsRef<Path>>(
    path: P,
    opts: Options,
    open_options: OpenOptions,
  ) -> Result<Self, Error> {
    Self::map_mut_with_path_builder::<_, ()>(|| Ok(path.as_ref().to_path_buf()), opts, open_options)
      .map_err(|e| e.unwrap_right())
  }

  /// Creates a new write-ahead log backed by a file backed memory map with the given options.
  #[inline]
  pub fn map_mut_with_path_builder<PB, E>(
    pb: PB,
    opts: Options,
    open_options: OpenOptions,
  ) -> Result<Self, Either<E, Error>>
  where
    PB: FnOnce() -> Result<PathBuf, E>,
  {
    Self::map_mut_with_path_builder_and_checksumer(pb, opts, open_options, Crc32::default())
  }

  /// Open a write-ahead log backed by a file backed memory map in read only mode.
  #[inline]
  pub fn map<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
    Self::map_with_path_builder::<_, ()>(|| Ok(path.as_ref().to_path_buf()))
      .map_err(|e| e.unwrap_right())
  }

  /// Open a write-ahead log backed by a file backed memory map in read only mode.
  #[inline]
  pub fn map_with_path_builder<PB, E>(pb: PB) -> Result<Self, Either<E, Error>>
  where
    PB: FnOnce() -> Result<PathBuf, E>,
  {
    Self::map_with_path_builder_and_checksumer(pb, Crc32::default())
  }
}

impl<K, V, S> GenericOrderWal<K, V, S> {
  /// Creates a new in-memory write-ahead log backed by an aligned vec with the given options and [`Checksumer`].
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::{generic::GenericOrderWal, Options, Crc32};
  ///
  /// let wal = GenericOrderWal::with_checksumer(Options::new(), Crc32::default());
  /// ```
  pub fn with_checksumer(opts: Options, cks: S) -> Self {
    let arena = Arena::new(arena_options().with_capacity(opts.cap));

    GenericOrderWalCore::new(arena, opts, cks, false)
      .map(|core| Self::from_core(core, false))
      .unwrap()
  }

  /// Creates a new in-memory write-ahead log backed by an anonymous memory map with the given options and [`Checksumer`].
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::{generic::GenericOrderWal, Options, Crc32};
  ///
  /// let wal = GenericOrderWal::map_anon_with_checksumer(Options::new(), Crc32::default()).unwrap();
  /// ```
  pub fn map_anon_with_checksumer(opts: Options, cks: S) -> Result<Self, Error> {
    let arena = Arena::map_anon(arena_options(), MmapOptions::new().len(opts.cap))?;

    GenericOrderWalCore::new(arena, opts, cks, true).map(|core| Self::from_core(core, false))
  }

  #[inline]
  fn from_core(core: GenericOrderWalCore<K, V, S>, ro: bool) -> Self {
    Self {
      core: Arc::new(core),
      ro,
    }
  }
}

impl<K, V, S> GenericOrderWal<K, V, S>
where
  K: Type + Ord + 'static,
  for<'a> <K as Type>::Ref<'a>: KeyRef<'a, K>,
  V: Type + 'static,
  S: Checksumer,
{
  /// Returns a write-ahead log backed by a file backed memory map with the given options and [`Checksumer`].
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::{generic::GenericOrderWal, Options, OpenOptions, Crc32};
  ///
  /// ```
  #[inline]
  pub fn map_mut_with_checksumer<P: AsRef<Path>>(
    path: P,
    opts: Options,
    open_options: OpenOptions,
    cks: S,
  ) -> Result<Self, Error> {
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
  /// use orderwal::{generic::GenericOrderWal, Options, OpenOptions, Crc32};
  ///
  /// ```
  pub fn map_mut_with_path_builder_and_checksumer<PB, E>(
    path_builder: PB,
    opts: Options,
    open_options: OpenOptions,
    cks: S,
  ) -> Result<Self, Either<E, Error>>
  where
    PB: FnOnce() -> Result<PathBuf, E>,
  {
    let path = path_builder().map_err(Either::Left)?;
    let exist = path.exists();
    let arena = Arena::map_mut_with_path_builder(
      || Ok(path),
      arena_options(),
      open_options,
      MmapOptions::new(),
    )
    .map_err(|e| e.map_right(Into::into))?;

    if !exist {
      return GenericOrderWalCore::new(arena, opts, cks, true)
        .map(|core| Self::from_core(core, false))
        .map_err(Either::Right);
    }

    GenericOrderWalCore::replay(arena, opts, false, cks)
      .map(|core| Self::from_core(core, false))
      .map_err(Either::Right)
  }

  /// Open a write-ahead log backed by a file backed memory map in read only mode with the given [`Checksumer`].
  #[inline]
  pub fn map_with_checksumer<P: AsRef<Path>>(path: P, cks: S) -> Result<Self, Error> {
    Self::map_with_path_builder_and_checksumer::<_, ()>(|| Ok(path.as_ref().to_path_buf()), cks)
      .map_err(|e| e.unwrap_right())
  }

  /// Open a write-ahead log backed by a file backed memory map in read only mode with the given [`Checksumer`].
  #[inline]
  pub fn map_with_path_builder_and_checksumer<PB, E>(
    path_builder: PB,
    cks: S,
  ) -> Result<Self, Either<E, Error>>
  where
    PB: FnOnce() -> Result<PathBuf, E>,
  {
    let open_options = OpenOptions::default().read(true);
    let arena = Arena::map_with_path_builder(
      path_builder,
      arena_options(),
      open_options,
      MmapOptions::new(),
    )
    .map_err(|e| e.map_right(Into::into))?;

    GenericOrderWalCore::replay(arena, Options::new(), true, cks)
      .map(|core| Self::from_core(core, true))
      .map_err(Either::Right)
  }
}

impl<K, V, S> GenericOrderWal<K, V, S>
where
  K: Type + Ord,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  V: Type,
{
  /// Gets the value associated with the key.
  #[inline]
  pub fn get<'a, 'b: 'a, Q>(&'a self, key: &'b Q) -> Option<EntryRef<'a, K, V>>
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>> + Comparable<K>,
  {
    self
      .core
      .map
      .get::<Owned<K, Q>>(&Owned::new(key))
      .map(EntryRef::new)
  }

  /// Gets the value associated with the key.
  #[inline]
  pub fn get_by_ref<'a, 'b: 'a, Q>(&'a self, key: &'b Q) -> Option<EntryRef<'a, K, V>>
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  {
    self
      .core
      .map
      .get::<Ref<K, Q>>(&Ref::new(key))
      .map(EntryRef::new)
  }
}

impl<K, V, S> GenericOrderWal<K, V, S>
where
  K: Type + Ord + for<'a> Comparable<K::Ref<'a>> + 'static,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  V: Type + 'static,
  S: Checksumer,
{
  /// Gets or insert the key value pair.
  #[inline]
  pub fn get_or_insert(
    &self,
    key: K,
    value: V,
  ) -> Option<Either<EntryRef<'_, K, V>, Result<(), Among<K::Error, V::Error, Error>>>> {
    let ent = self
      .core
      .map
      .get(&Owned::new(&key))
      .map(|e| Either::Left(EntryRef::new(e)));

    match ent {
      Some(e) => Some(e),
      None => {
        let p = self.insert_in(Either::Left(key), Either::Left(value));
        Some(Either::Right(p))
      }
    }
  }

  /// Gets or insert the key value pair.
  #[inline]
  pub fn get_or_insert_refs<'a, 'b: 'a>(
    &'a self,
    key: &'b K,
    value: &'b V,
  ) -> Option<Either<EntryRef<'a, K, V>, Result<(), Among<K::Error, V::Error, Error>>>> {
    let ent = self
      .core
      .map
      .get(&Ref::new(key))
      .map(|e| Either::Left(EntryRef::new(e)));

    match ent {
      Some(e) => Some(e),
      None => {
        let p = self.insert_in(Either::Right(key), Either::Right(value));
        Some(Either::Right(p))
      }
    }
  }
}

impl<K, V, S> GenericOrderWal<K, V, S>
where
  K: Type + Ord + 'static,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  V: Type + 'static,
  S: Checksumer,
{
  /// Inserts a key-value pair into the write-ahead log. If `cache_key` or `cache_value` is enabled, the key or value will be cached
  /// in memory for faster access.
  ///
  /// For `cache_key` or `cache_value`, see [`Options::with_cache_key`](Options::with_cache_key) and [`Options::with_cache_value`](Options::with_cache_value).
  ///
  /// See also [`insert_refs`](GenericOrderWal::insert_refs).
  #[inline]
  pub fn insert(&self, key: K, val: V) -> Result<(), Among<K::Error, V::Error, Error>> {
    self.insert_in(Either::Left(key), Either::Left(val))
  }

  /// Inserts a key-value pair into the write-ahead log.
  ///
  /// Not like [`insert`](GenericOrderWal::insert), this method does not cache the key or value in memory even if the `cache_key` or `cache_value` is enabled.
  ///
  /// For `cache_key` or `cache_value`, see [`Options::with_cache_key`](Options::with_cache_key) and [`Options::with_cache_value`](Options::with_cache_value).
  ///
  /// See also [`insert`](GenericOrderWal::insert), [`get_or_insert`](GenericOrderWal::get_or_insert) and [`get_or_insert_ref`](GenericOrderWal::get_or_insert_ref).
  #[inline]
  pub fn insert_refs(&self, key: &K, val: &V) -> Result<(), Among<K::Error, V::Error, Error>> {
    self.insert_in(Either::Right(key), Either::Right(val))
  }

  fn insert_in(
    &self,
    key: Either<K, &K>,
    val: Either<V, &V>,
  ) -> Result<(), Among<K::Error, V::Error, Error>> {
    if self.ro {
      return Err(Among::Right(Error::read_only()));
    }

    let klen = key.encoded_len();
    let vlen = val.encoded_len();

    let elen = entry_size(klen as u32, vlen as u32) as usize;

    let buf = self.core.arena.alloc_bytes(elen as u32);

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
      }
      Ok(mut buf) => {
        unsafe {
          // We allocate the buffer with the exact size, so it's safe to write to the buffer.
          let flag = Flags::COMMITTED.bits();

          self.core.cks.reset();
          self.core.cks.update(&[flag]);

          buf.put_u8_unchecked(Flags::empty().bits());
          buf.put_u32_le_unchecked(klen as u32);

          let ko = STATUS_SIZE + KEY_LEN_SIZE;
          buf.set_len(ko + klen);

          let key_buf = slice::from_raw_parts_mut(buf.as_mut_ptr().add(ko), klen);
          key.encode(key_buf).map_err(Among::Left)?;

          buf.put_u32_le_unchecked(vlen as u32);

          let vo = STATUS_SIZE + KEY_LEN_SIZE + klen + VALUE_LEN_SIZE;
          buf.set_len(vo + vlen);
          let value_buf = slice::from_raw_parts_mut(buf.as_mut_ptr().add(vo), vlen);
          val.encode(value_buf).map_err(Among::Middle)?;

          let cks = {
            self.core.cks.update(&buf[1..]);
            self.core.cks.digest()
          };
          buf.put_u64_le_unchecked(cks);

          // commit the entry
          buf[0] |= Flags::COMMITTED.bits();

          if self.core.opts.sync_on_write && self.core.arena.is_ondisk() {
            self
              .core
              .arena
              .flush_range(buf.offset(), elen as usize)
              .map_err(|e| Among::Right(e.into()))?;
          }
          buf.detach();

          let mut p = Pointer::new(klen, vlen, buf.as_ptr());
          if let Either::Left(k) = key {
            if self.core.opts.cache_key {
              p = p.with_cached_key(k);
            }
          }

          if let Either::Left(v) = val {
            if self.core.opts.cache_value {
              p = p.with_cached_value(v);
            }
          }

          self.core.map.insert(p);
          Ok(())
        }
      }
    }
  }
}
