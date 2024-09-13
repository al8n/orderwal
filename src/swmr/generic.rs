use core::{cmp, marker::PhantomData, ops::Bound, slice};
use std::{
  path::{Path, PathBuf},
  sync::Arc,
};

use among::Among;
use crossbeam_skiplist::SkipSet;
pub use dbutils::equivalent::*;
use dbutils::{Checksumer, Crc32};
use rarena_allocator::{
  either::Either, sync::Arena, Allocator, ArenaPosition, Error as ArenaError, Memory, MmapOptions,
  OpenOptions,
};

use crate::{
  arena_options, check, entry_size,
  error::{self, Error},
  split_lengths, Flags, Options, UnsafeCellChecksumer, CHECKSUM_SIZE, HEADER_SIZE, MAGIC_TEXT,
  STATUS_SIZE,
};

mod entry;
pub use entry::*;

mod traits;
pub use traits::*;

mod reader;
pub use reader::*;

mod iter;
pub use iter::*;

#[cfg(all(test, feature = "test-swmr-generic"))]
mod tests;

#[doc(hidden)]
pub struct Pointer<K, V> {
  /// The pointer to the start of the entry.
  ptr: *const u8,
  /// The length of the key.
  key_len: usize,
  /// The length of the value.
  value_len: usize,
  _m: PhantomData<(K, V)>,
}

impl<K, V> Clone for Pointer<K, V> {
  fn clone(&self) -> Self {
    *self
  }
}

impl<K, V> Copy for Pointer<K, V> {}

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
      _m: PhantomData,
    }
  }

  #[inline]
  const fn as_key_slice<'a>(&self) -> &'a [u8] {
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

struct PartialPointer<K> {
  key_len: usize,
  ptr: *const u8,
  _k: PhantomData<K>,
}

impl<K: Type> PartialEq for PartialPointer<K> {
  fn eq(&self, other: &Self) -> bool {
    self.as_key_slice() == other.as_key_slice()
  }
}

impl<K: Type> Eq for PartialPointer<K> {}

impl<K> PartialOrd for PartialPointer<K>
where
  K: Type + Ord,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
{
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<K> Ord for PartialPointer<K>
where
  K: Type + Ord,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
{
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    <K::Ref<'_> as KeyRef<K>>::compare_binary(self.as_key_slice(), other.as_key_slice())
  }
}

impl<K> PartialPointer<K> {
  #[inline]
  const fn new(key_len: usize, ptr: *const u8) -> Self {
    Self {
      key_len,
      ptr,
      _k: PhantomData,
    }
  }

  #[inline]
  fn as_key_slice<'a>(&self) -> &'a [u8] {
    if self.key_len == 0 {
      return &[];
    }

    // SAFETY: `ptr` is a valid pointer to `len` bytes.
    unsafe { slice::from_raw_parts(self.ptr, self.key_len) }
  }
}

impl<'a, K, V> Equivalent<Pointer<K, V>> for PartialPointer<K>
where
  K: Type + Ord,
  K::Ref<'a>: KeyRef<'a, K>,
{
  fn equivalent(&self, key: &Pointer<K, V>) -> bool {
    self.compare(key).is_eq()
  }
}

impl<'a, K, V> Comparable<Pointer<K, V>> for PartialPointer<K>
where
  K: Type + Ord,
  K::Ref<'a>: KeyRef<'a, K>,
{
  fn compare(&self, p: &Pointer<K, V>) -> cmp::Ordering {
    let kr: K::Ref<'_> = TypeRef::from_slice(p.as_key_slice());
    let or: K::Ref<'_> = TypeRef::from_slice(self.as_key_slice());
    KeyRef::compare(&kr, &or)
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
    let kr = TypeRef::from_slice(p.as_key_slice());
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
    let kr = <K::Ref<'_> as TypeRef<'_>>::from_slice(p.as_key_slice());
    KeyRef::compare(&kr, self.key)
  }
}

struct GenericOrderWalCore<K, V> {
  arena: Arena,
  map: SkipSet<Pointer<K, V>>,
  reserved: u32,
}

impl<K, V> GenericOrderWalCore<K, V> {
  #[inline]
  fn len(&self) -> usize {
    self.map.len()
  }

  #[inline]
  fn is_empty(&self) -> bool {
    self.map.is_empty()
  }

  #[inline]
  fn new(arena: Arena, magic_version: u16, flush: bool, reserved: u32) -> Result<Self, Error> {
    unsafe {
      let slice = arena.reserved_slice_mut();
      slice[0..6].copy_from_slice(&MAGIC_TEXT);
      slice[6..8].copy_from_slice(&magic_version.to_le_bytes());
    }

    if !flush {
      return Ok(Self::construct(arena, SkipSet::new(), reserved));
    }

    arena
      .flush_range(0, HEADER_SIZE)
      .map(|_| Self::construct(arena, SkipSet::new(), reserved))
      .map_err(Into::into)
  }

  #[inline]
  fn first(&self) -> Option<EntryRef<K, V>>
  where
    K: Type + Ord,
    for<'b> K::Ref<'b>: KeyRef<'b, K>,
  {
    self.map.front().map(EntryRef::new)
  }

  #[inline]
  fn last(&self) -> Option<EntryRef<K, V>>
  where
    K: Type + Ord,
    for<'b> K::Ref<'b>: KeyRef<'b, K>,
  {
    self.map.back().map(EntryRef::new)
  }

  #[inline]
  fn iter(&self) -> Iter<K, V>
  where
    K: Type + Ord,
    for<'b> K::Ref<'b>: KeyRef<'b, K>,
  {
    Iter::new(self.map.iter())
  }

  #[inline]
  fn range_by_ref<'a, Q>(
    &'a self,
    start_bound: Bound<&'a Q>,
    end_bound: Bound<&'a Q>,
  ) -> RefRange<'a, Q, K, V>
  where
    K: Type + Ord,
    for<'b> K::Ref<'b>: KeyRef<'b, K>,
    Q: Ord + ?Sized + Comparable<K::Ref<'a>>,
  {
    RefRange::new(
      self
        .map
        .range((start_bound.map(Ref::new), end_bound.map(Ref::new))),
    )
  }

  #[inline]
  fn range<'a, Q>(
    &'a self,
    start_bound: Bound<&'a Q>,
    end_bound: Bound<&'a Q>,
  ) -> Range<'a, Q, K, V>
  where
    K: Type + Ord,
    for<'b> K::Ref<'b>: KeyRef<'b, K>,
    Q: Ord + ?Sized + Comparable<K> + Comparable<K::Ref<'a>>,
  {
    Range::new(
      self
        .map
        .range((start_bound.map(Owned::new), end_bound.map(Owned::new))),
    )
  }

  #[inline]
  fn construct(arena: Arena, set: SkipSet<Pointer<K, V>>, reserved: u32) -> Self {
    Self {
      arena,
      map: set,
      reserved,
    }
  }
}

impl<K, V> GenericOrderWalCore<K, V>
where
  K: Type + Ord + 'static,
  for<'a> <K as Type>::Ref<'a>: KeyRef<'a, K>,
  V: Type + 'static,
{
  fn replay<S: Checksumer>(
    arena: Arena,
    opts: &Options,
    ro: bool,
    checksumer: &mut S,
  ) -> Result<Self, Error> {
    let slice = arena.reserved_slice();
    let magic_text = &slice[0..6];
    let magic_version = u16::from_le_bytes(slice[6..8].try_into().unwrap());

    if magic_text != MAGIC_TEXT {
      return Err(Error::magic_text_mismatch());
    }

    if magic_version != opts.magic_version() {
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

        map.insert(Pointer::new(
          key_len,
          value_len,
          arena.get_pointer(cursor + STATUS_SIZE + kvsize),
        ));
        cursor += cks_offset + CHECKSUM_SIZE;
      }
    }

    Ok(Self::construct(arena, map, opts.reserved()))
  }
}

impl<K, V> GenericOrderWalCore<K, V>
where
  K: Type + Ord,
  for<'a> <K as Type>::Ref<'a>: KeyRef<'a, K>,
  V: Type,
{
  #[inline]
  fn contains_key<'a, 'b: 'a, Q>(&'a self, key: &'b Q) -> bool
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>> + Comparable<K>,
  {
    self.map.contains::<Owned<K, Q>>(&Owned::new(key))
  }

  #[inline]
  fn contains_key_by_ref<'a, 'b: 'a, Q>(&'a self, key: &'b Q) -> bool
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  {
    self.map.contains::<Ref<K, Q>>(&Ref::new(key))
  }

  #[inline]
  unsafe fn contains_key_by_bytes(&self, key: &[u8]) -> bool {
    self
      .map
      .contains(&PartialPointer::new(key.len(), key.as_ptr()))
  }

  #[inline]
  fn get<'a, 'b: 'a, Q>(&'a self, key: &'b Q) -> Option<EntryRef<'a, K, V>>
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>> + Comparable<K>,
  {
    self
      .map
      .get::<Owned<K, Q>>(&Owned::new(key))
      .map(EntryRef::new)
  }

  #[inline]
  fn get_by_ref<'a, 'b: 'a, Q>(&'a self, key: &'b Q) -> Option<EntryRef<'a, K, V>>
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  {
    self.map.get::<Ref<K, Q>>(&Ref::new(key)).map(EntryRef::new)
  }

  #[inline]
  unsafe fn get_by_bytes(&self, key: &[u8]) -> Option<EntryRef<K, V>> {
    self
      .map
      .get(&PartialPointer::new(key.len(), key.as_ptr()))
      .map(EntryRef::new)
  }
}

/// Generic ordered write-ahead log implementation, which supports structured keys and values.
///
/// Both read and write operations of this WAL are zero-cost (no allocation will happen for both read and write).
///
/// Users can create multiple readers from the WAL by [`GenericOrderWal::reader`], but only one writer is allowed.
pub struct GenericOrderWal<K, V, S = Crc32> {
  core: Arc<GenericOrderWalCore<K, V>>,
  opts: Options,
  cks: UnsafeCellChecksumer<S>,
  ro: bool,
}

impl<K, V> GenericOrderWal<K, V> {
  /// Creates a new in-memory write-ahead log backed by an aligned vec with the given capacity and options.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::{swmr::GenericOrderWal, Options};
  ///
  /// let wal = GenericOrderWal::<String, String>::new(Options::new().with_capacity(1024)).unwrap();
  /// ```
  #[inline]
  pub fn new(opts: Options) -> Result<Self, Error> {
    Self::with_checksumer(opts, Crc32::default())
  }

  /// Creates a new in-memory write-ahead log backed by an anonymous memory map with the given options.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::{swmr::GenericOrderWal, Options};
  ///
  /// let wal = GenericOrderWal::<String, String>::map_anon(Options::new().with_capacity(1024)).unwrap();
  /// ```
  #[inline]
  pub fn map_anon(opts: Options) -> Result<Self, Error> {
    Self::map_anon_with_checksumer(opts, Crc32::default())
  }
}

impl<K, V, S> GenericOrderWal<K, V, S>
where
  K: Type + Ord + 'static,
  for<'a> <K as Type>::Ref<'a>: KeyRef<'a, K>,
{
  /// Returns the first key-value pair in the map. The key in this pair is the minimum key in the wal.
  #[inline]
  pub fn first(&self) -> Option<EntryRef<K, V>> {
    self.core.first()
  }

  /// Returns the last key-value pair in the map. The key in this pair is the maximum key in the wal.
  #[inline]
  pub fn last(&self) -> Option<EntryRef<K, V>> {
    self.core.last()
  }

  /// Returns an iterator over the entries in the WAL.
  #[inline]
  pub fn iter(&self) -> Iter<K, V> {
    self.core.iter()
  }

  /// Returns an iterator over a subset of the entries in the WAL.
  #[inline]
  pub fn range_by_ref<'a, Q>(
    &'a self,
    start_bound: Bound<&'a Q>,
    end_bound: Bound<&'a Q>,
  ) -> RefRange<'a, Q, K, V>
  where
    Q: Ord + ?Sized + Comparable<K::Ref<'a>>,
  {
    self.core.range_by_ref(start_bound, end_bound)
  }

  /// Returns an iterator over a subset of the entries in the WAL.
  #[inline]
  pub fn range<'a, Q>(
    &'a self,
    start_bound: Bound<&'a Q>,
    end_bound: Bound<&'a Q>,
  ) -> Range<'a, Q, K, V>
  where
    Q: Ord + ?Sized + Comparable<K> + Comparable<K::Ref<'a>>,
  {
    self.core.range(start_bound, end_bound)
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
  /// ## Safety
  ///
  /// All file-backed memory map constructors are marked `unsafe` because of the potential for
  /// *Undefined Behavior* (UB) using the map if the underlying file is subsequently modified, in or
  /// out of process. Applications must consider the risk and take appropriate precautions when
  /// using file-backed maps. Solutions such as file permissions, locks or process-private (e.g.
  /// unlinked) files exist but are platform specific and limited.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::{swmr::GenericOrderWal, Options};
  ///
  /// ```
  #[inline]
  pub unsafe fn map_mut<P: AsRef<Path>>(
    path: P,
    opts: Options,
    open_options: OpenOptions,
  ) -> Result<Self, Error> {
    Self::map_mut_with_path_builder::<_, ()>(|| Ok(path.as_ref().to_path_buf()), opts, open_options)
      .map_err(|e| e.unwrap_right())
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
  #[inline]
  pub unsafe fn map_mut_with_path_builder<PB, E>(
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
  ///
  /// ## Safety
  ///
  /// All file-backed memory map constructors are marked `unsafe` because of the potential for
  /// *Undefined Behavior* (UB) using the map if the underlying file is subsequently modified, in or
  /// out of process. Applications must consider the risk and take appropriate precautions when
  /// using file-backed maps. Solutions such as file permissions, locks or process-private (e.g.
  /// unlinked) files exist but are platform specific and limited.
  #[inline]
  pub unsafe fn map<P: AsRef<Path>>(
    path: P,
    opts: Options,
  ) -> Result<GenericWalReader<K, V>, Error> {
    Self::map_with_path_builder::<_, ()>(|| Ok(path.as_ref().to_path_buf()), opts)
      .map_err(|e| e.unwrap_right())
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
  #[inline]
  pub unsafe fn map_with_path_builder<PB, E>(
    pb: PB,
    opts: Options,
  ) -> Result<GenericWalReader<K, V>, Either<E, Error>>
  where
    PB: FnOnce() -> Result<PathBuf, E>,
  {
    Self::map_with_path_builder_and_checksumer(pb, opts, Crc32::default())
  }
}

impl<K, V, S> GenericOrderWal<K, V, S> {
  /// Returns a read-only WAL instance.
  #[inline]
  pub fn reader(&self) -> GenericWalReader<K, V> {
    GenericWalReader::new(self.core.clone())
  }

  /// Returns the path of the WAL if it is backed by a file.
  #[inline]
  pub fn path(&self) -> Option<&std::sync::Arc<std::path::PathBuf>> {
    self.core.arena.path()
  }

  /// Returns the reserved space in the WAL.
  ///
  /// # Safety
  /// - The writer must ensure that the returned slice is not modified.
  /// - This method is not thread-safe, so be careful when using it.
  #[inline]
  pub unsafe fn reserved_slice(&self) -> &[u8] {
    if self.opts.reserved() == 0 {
      return &[];
    }

    &self.core.arena.reserved_slice()[HEADER_SIZE..]
  }

  /// Returns the mutable reference to the reserved slice.
  ///
  /// # Safety
  /// - The caller must ensure that the there is no others accessing reserved slice for either read or write.
  /// - This method is not thread-safe, so be careful when using it.
  #[inline]
  pub unsafe fn reserved_slice_mut(&mut self) -> &mut [u8] {
    if self.opts.reserved() == 0 {
      return &mut [];
    }

    &mut self.core.arena.reserved_slice_mut()[HEADER_SIZE..]
  }

  /// Returns number of entries in the WAL.
  #[inline]
  pub fn len(&self) -> usize {
    self.core.len()
  }

  /// Returns `true` if the WAL is empty.
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.core.is_empty()
  }

  /// Creates a new in-memory write-ahead log backed by an aligned vec with the given options and [`Checksumer`].
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::{swmr::GenericOrderWal, Options, Crc32};
  ///
  /// let wal = GenericOrderWal::<String, String>::with_checksumer(Options::new().with_capacity(1024), Crc32::default());
  /// ```
  pub fn with_checksumer(opts: Options, cks: S) -> Result<Self, Error> {
    let arena = Arena::new(arena_options(opts.reserved()).with_capacity(opts.capacity())).map_err(
      |e| match e {
        ArenaError::InsufficientSpace {
          requested,
          available,
        } => Error::insufficient_space(requested, available),
        _ => unreachable!(),
      },
    )?;

    GenericOrderWalCore::new(arena, opts.magic_version(), false, opts.reserved())
      .map(|core| Self::from_core(core, opts, cks, false))
  }

  /// Creates a new in-memory write-ahead log backed by an anonymous memory map with the given options and [`Checksumer`].
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::{swmr::GenericOrderWal, Options, Crc32};
  ///
  /// let wal = GenericOrderWal::<String, String>::map_anon_with_checksumer(Options::new().with_capacity(1024), Crc32::default()).unwrap();
  /// ```
  pub fn map_anon_with_checksumer(opts: Options, cks: S) -> Result<Self, Error> {
    let arena = Arena::map_anon(
      arena_options(opts.reserved()),
      MmapOptions::new().len(opts.capacity()),
    )?;

    GenericOrderWalCore::new(arena, opts.magic_version(), true, opts.reserved())
      .map(|core| Self::from_core(core, opts, cks, false))
  }

  #[inline]
  fn from_core(core: GenericOrderWalCore<K, V>, opts: Options, cks: S, ro: bool) -> Self {
    Self {
      core: Arc::new(core),
      ro,
      opts,
      cks: UnsafeCellChecksumer::new(cks),
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
  /// ## Safety
  ///
  /// All file-backed memory map constructors are marked `unsafe` because of the potential for
  /// *Undefined Behavior* (UB) using the map if the underlying file is subsequently modified, in or
  /// out of process. Applications must consider the risk and take appropriate precautions when
  /// using file-backed maps. Solutions such as file permissions, locks or process-private (e.g.
  /// unlinked) files exist but are platform specific and limited.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::{swmr::GenericOrderWal, Options, Crc32};
  ///
  ///
  /// ```
  #[inline]
  pub unsafe fn map_mut_with_checksumer<P: AsRef<Path>>(
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
  /// ## Safety
  ///
  /// All file-backed memory map constructors are marked `unsafe` because of the potential for
  /// *Undefined Behavior* (UB) using the map if the underlying file is subsequently modified, in or
  /// out of process. Applications must consider the risk and take appropriate precautions when
  /// using file-backed maps. Solutions such as file permissions, locks or process-private (e.g.
  /// unlinked) files exist but are platform specific and limited.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::{swmr::GenericOrderWal, Options, Crc32};
  ///
  /// ```
  pub unsafe fn map_mut_with_path_builder_and_checksumer<PB, E>(
    path_builder: PB,
    opts: Options,
    open_options: OpenOptions,
    mut cks: S,
  ) -> Result<Self, Either<E, Error>>
  where
    PB: FnOnce() -> Result<PathBuf, E>,
  {
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
      return GenericOrderWalCore::new(arena, opts.magic_version(), true, opts.reserved())
        .map(|core| Self::from_core(core, opts, cks, false))
        .map_err(Either::Right);
    }

    GenericOrderWalCore::replay(arena, &opts, false, &mut cks)
      .map(|core| Self::from_core(core, opts, cks, false))
      .map_err(Either::Right)
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
  #[inline]
  pub unsafe fn map_with_checksumer<P: AsRef<Path>>(
    path: P,
    opts: Options,
    cks: S,
  ) -> Result<GenericWalReader<K, V>, Error> {
    Self::map_with_path_builder_and_checksumer::<_, ()>(
      || Ok(path.as_ref().to_path_buf()),
      opts,
      cks,
    )
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
  #[inline]
  pub unsafe fn map_with_path_builder_and_checksumer<PB, E>(
    path_builder: PB,
    opts: Options,
    mut cks: S,
  ) -> Result<GenericWalReader<K, V>, Either<E, Error>>
  where
    PB: FnOnce() -> Result<PathBuf, E>,
  {
    let open_options = OpenOptions::default().read(true);
    let arena = Arena::map_with_path_builder(
      path_builder,
      arena_options(opts.reserved()),
      open_options,
      MmapOptions::new(),
    )
    .map_err(|e| e.map_right(Into::into))?;

    GenericOrderWalCore::replay(arena, &opts, true, &mut cks)
      .map(|core| GenericWalReader::new(Arc::new(core)))
      .map_err(Either::Right)
  }
}

impl<K, V, S> GenericOrderWal<K, V, S>
where
  K: Type + Ord,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  V: Type,
{
  /// Returns `true` if the key exists in the WAL.
  #[inline]
  pub fn contains_key<'a, 'b: 'a, Q>(&'a self, key: &'b Q) -> bool
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>> + Comparable<K>,
  {
    self.core.contains_key(key)
  }

  /// Returns `true` if the key exists in the WAL.
  ///
  /// # Safety
  /// - The given `key` must be valid to construct to `K::Ref` without remaining.
  #[inline]
  pub unsafe fn contains_key_by_bytes(&self, key: &[u8]) -> bool {
    self.core.contains_key_by_bytes(key)
  }

  /// Returns `true` if the key exists in the WAL.
  #[inline]
  pub fn contains_key_by_ref<'a, 'b: 'a, Q>(&'a self, key: &'b Q) -> bool
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  {
    self.core.contains_key_by_ref(key)
  }

  /// Gets the value associated with the key.
  #[inline]
  pub fn get<'a, 'b: 'a, Q>(&'a self, key: &'b Q) -> Option<EntryRef<'a, K, V>>
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>> + Comparable<K>,
  {
    self.core.get(key)
  }

  /// Gets the value associated with the key.
  #[inline]
  pub fn get_by_ref<'a, 'b: 'a, Q>(&'a self, key: &'b Q) -> Option<EntryRef<'a, K, V>>
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  {
    self.core.get_by_ref(key)
  }

  /// Gets the value associated with the key.
  ///
  /// # Safety
  /// - The given `key` must be valid to construct to `K::Ref` without remaining.
  #[inline]
  pub unsafe fn get_by_bytes(&self, key: &[u8]) -> Option<EntryRef<K, V>> {
    self.core.get_by_bytes(key)
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
    &mut self,
    key: &K,
    value: &V,
  ) -> Either<EntryRef<'_, K, V>, Result<(), Among<K::Error, V::Error, Error>>> {
    let ent = self
      .core
      .map
      .get(&Owned::new(key))
      .map(|e| Either::Left(EntryRef::new(e)));

    match ent {
      Some(e) => e,
      None => {
        let p = self.insert_in(Among::Middle(key), Among::Middle(value));
        Either::Right(p)
      }
    }
  }

  /// Gets or insert the key value pair.
  #[inline]
  pub fn get_or_insert_with(
    &mut self,
    key: &K,
    value: impl FnOnce() -> V,
  ) -> Either<EntryRef<'_, K, V>, Result<(), Among<K::Error, V::Error, Error>>> {
    let ent = self
      .core
      .map
      .get(&Ref::new(key))
      .map(|e| Either::Left(EntryRef::new(e)));

    match ent {
      Some(e) => e,
      None => {
        let p = self.insert_in(Among::Middle(key), Among::Left(value()));
        Either::Right(p)
      }
    }
  }

  /// Gets or insert the key value pair.
  ///
  /// # Safety
  /// - The given `key` and `value` must be valid to construct to `K::Ref` and `V::Ref` without remaining.
  #[inline]
  pub unsafe fn get_by_bytes_or_insert_bytes(
    &mut self,
    key: &[u8],
    value: &[u8],
  ) -> Either<EntryRef<'_, K, V>, Result<(), Error>> {
    let ent = self
      .core
      .map
      .get(&PartialPointer::new(key.len(), key.as_ptr()))
      .map(|e| Either::Left(EntryRef::new(e)));

    match ent {
      Some(e) => e,
      None => match self.insert_in(Among::Right(key), Among::Right(value)) {
        Ok(_) => Either::Right(Ok(())),
        Err(Among::Right(e)) => Either::Right(Err(e)),
        _ => unreachable!(),
      },
    }
  }

  /// Gets or insert the key value pair.
  ///
  /// # Safety
  /// - The given `value` must be valid to construct to `V::Ref` without remaining.
  #[inline]
  pub unsafe fn get_or_insert_bytes(
    &mut self,
    key: &K,
    value: &[u8],
  ) -> Either<EntryRef<'_, K, V>, Result<(), Either<K::Error, Error>>> {
    let ent = self
      .core
      .map
      .get(&Owned::new(key))
      .map(|e| Either::Left(EntryRef::new(e)));

    match ent {
      Some(e) => e,
      None => match self.insert_in(Among::Middle(key), Among::Right(value)) {
        Ok(_) => Either::Right(Ok(())),
        Err(Among::Left(e)) => Either::Right(Err(Either::Left(e))),
        Err(Among::Right(e)) => Either::Right(Err(Either::Right(e))),
        _ => unreachable!(),
      },
    }
  }

  /// Gets or insert the key value pair.
  ///
  /// # Safety
  /// - The given `key` must be valid to construct to `K::Ref` without remaining.
  #[inline]
  pub unsafe fn get_by_key_bytes_or_insert(
    &mut self,
    key: &[u8],
    value: &V,
  ) -> Either<EntryRef<'_, K, V>, Result<(), Either<V::Error, Error>>> {
    let ent = self
      .core
      .map
      .get(&PartialPointer::new(key.len(), key.as_ptr()))
      .map(|e| Either::Left(EntryRef::new(e)));

    match ent {
      Some(e) => e,
      None => match self.insert_in(Among::Right(key), Among::Middle(value)) {
        Ok(_) => Either::Right(Ok(())),
        Err(Among::Middle(e)) => Either::Right(Err(Either::Left(e))),
        Err(Among::Right(e)) => Either::Right(Err(Either::Right(e))),
        _ => unreachable!(),
      },
    }
  }

  /// Gets or insert the key value pair.
  ///
  /// # Safety
  /// - The given `key` must be valid to construct to `K::Ref` without remaining.
  #[inline]
  pub unsafe fn get_by_key_bytes_or_insert_with(
    &mut self,
    key: &[u8],
    value: impl FnOnce() -> V,
  ) -> Either<EntryRef<'_, K, V>, Result<(), Either<V::Error, Error>>> {
    let ent = self
      .core
      .map
      .get(&PartialPointer::new(key.len(), key.as_ptr()))
      .map(|e| Either::Left(EntryRef::new(e)));

    match ent {
      Some(e) => e,
      None => match self.insert_in(Among::Right(key), Among::Left(value())) {
        Ok(_) => Either::Right(Ok(())),
        Err(Among::Middle(e)) => Either::Right(Err(Either::Left(e))),
        Err(Among::Right(e)) => Either::Right(Err(Either::Right(e))),
        _ => unreachable!(),
      },
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
  /// Inserts a key-value pair into the write-ahead log.
  #[inline]
  pub fn insert(&mut self, key: &K, val: &V) -> Result<(), Among<K::Error, V::Error, Error>> {
    self.insert_in(Among::Middle(key), Among::Middle(val))
  }

  /// Inserts a key in structured format and value in bytes format into the write-ahead log directly.
  ///
  /// # Safety
  /// - The given `value` must be valid to construct to `V::Ref` without remaining.
  ///
  /// # Example
  ///
  /// See [`insert_bytes`](GenericOrderWal::insert_bytes) for more details.
  #[inline]
  pub unsafe fn insert_key_with_value_bytes(
    &mut self,
    key: &K,
    value: &[u8],
  ) -> Result<(), Either<K::Error, Error>> {
    self
      .insert_in(Among::Middle(key), Among::Right(value))
      .map_err(|e| match e {
        Among::Left(e) => Either::Left(e),
        Among::Right(e) => Either::Right(e),
        _ => unreachable!(),
      })
  }

  /// Inserts a key in bytes format and value in structured format into the write-ahead log directly.
  ///
  /// # Safety
  /// - The given `key` and `value` must be valid to construct to `K::Ref` and `V::Ref` without remaining.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::{swmr::{*, generic::*}, utils::*, Options};
  /// use std::cmp;
  ///
  /// #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
  /// struct Person {
  ///   id: u64,
  ///   name: String,
  /// }
  ///
  /// #[derive(Debug)]
  /// struct PersonRef<'a> {
  ///   id: u64,
  ///   name: &'a str,
  /// }
  ///
  /// impl<'a> PartialEq for PersonRef<'a> {
  ///   fn eq(&self, other: &Self) -> bool {
  ///     self.id == other.id && self.name == other.name
  ///   }
  /// }
  ///
  /// impl<'a> Eq for PersonRef<'a> {}
  ///
  /// impl<'a> PartialOrd for PersonRef<'a> {
  ///   fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
  ///     Some(self.cmp(other))
  ///   }
  /// }
  ///
  /// impl<'a> Ord for PersonRef<'a> {
  ///   fn cmp(&self, other: &Self) -> cmp::Ordering {
  ///     self
  ///       .id
  ///       .cmp(&other.id)
  ///       .then_with(|| self.name.cmp(other.name))
  ///   }
  /// }
  ///
  /// impl Equivalent<Person> for PersonRef<'_> {
  ///   fn equivalent(&self, key: &Person) -> bool {
  ///     self.id == key.id && self.name == key.name
  ///   }
  /// }
  ///
  /// impl Comparable<Person> for PersonRef<'_> {
  ///   fn compare(&self, key: &Person) -> std::cmp::Ordering {
  ///     self.id.cmp(&key.id).then_with(|| self.name.cmp(&key.name))
  ///   }
  /// }
  ///
  /// impl Equivalent<PersonRef<'_>> for Person {
  ///   fn equivalent(&self, key: &PersonRef<'_>) -> bool {
  ///     self.id == key.id && self.name == key.name
  ///   }
  /// }
  ///
  /// impl Comparable<PersonRef<'_>> for Person {
  ///   fn compare(&self, key: &PersonRef<'_>) -> std::cmp::Ordering {
  ///     self
  ///       .id
  ///       .cmp(&key.id)
  ///       .then_with(|| self.name.as_str().cmp(key.name))
  ///   }
  /// }
  ///
  /// impl<'a> KeyRef<'a, Person> for PersonRef<'a> {
  ///   fn compare<Q>(&self, a: &Q) -> cmp::Ordering
  ///   where
  ///     Q: ?Sized + Ord + Comparable<Self>,
  ///   {
  ///     Comparable::compare(a, self)
  ///   }
  ///
  ///   fn compare_binary(this: &[u8], other: &[u8]) -> cmp::Ordering {
  ///     let (this_id_size, this_id) = decode_u64_varint(this).unwrap();
  ///     let (other_id_size, other_id) = decode_u64_varint(other).unwrap();
  ///
  ///     PersonRef {
  ///       id: this_id,
  ///       name: std::str::from_utf8(&this[this_id_size..]).unwrap(),
  ///     }
  ///     .cmp(&PersonRef {
  ///       id: other_id,
  ///       name: std::str::from_utf8(&other[other_id_size..]).unwrap(),
  ///     })
  ///   }
  /// }
  ///
  /// impl Type for Person {
  ///   type Ref<'a> = PersonRef<'a>;
  ///   type Error = EncodeVarintError;
  ///
  ///   fn encoded_len(&self) -> usize {
  ///     encoded_u64_varint_len(self.id) + self.name.len()
  ///   }
  ///
  ///   fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
  ///     let id_size = encode_u64_varint(self.id, buf)?;
  ///     buf[id_size..].copy_from_slice(self.name.as_bytes());
  ///     Ok(())
  ///   }
  /// }
  ///
  /// impl<'a> TypeRef<'a> for PersonRef<'a> {
  ///   fn from_slice(src: &'a [u8]) -> Self {
  ///     let (id_size, id) = decode_u64_varint(src).unwrap();
  ///     let name = std::str::from_utf8(&src[id_size..]).unwrap();
  ///     PersonRef { id, name }
  ///   }
  /// }
  ///
  /// let mut wal = GenericOrderWal::<Person, String>::new(Options::new().with_capacity(1024)).unwrap();
  ///
  /// # let key = Person {
  /// #  id: 1,
  /// #  name: "Alice".to_string(),
  /// # };
  ///
  ///
  /// # let mut person = vec![0; key.encoded_len()];
  /// # key.encode(&mut person).unwrap();
  ///
  /// // Assume `person` comes from somewhere else, e.g. from the network.
  /// unsafe {
  ///   wal.insert_bytes(person.as_ref(), b"Hello, Alice!").unwrap();
  /// }
  /// ```
  #[inline]
  pub unsafe fn insert_bytes(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
    self
      .insert_in(Among::Right(key), Among::Right(value))
      .map_err(|e| match e {
        Among::Right(e) => e,
        _ => unreachable!(),
      })
  }

  /// Inserts a key in bytes format and value in structured format into the write-ahead log directly.
  ///
  /// # Safety
  /// - The given `key` must be valid to construct to `K::Ref` without remaining.
  ///
  /// # Example
  ///
  /// See [`insert_bytes`](GenericOrderWal::insert_bytes) for more details.
  #[inline]
  pub unsafe fn insert_key_bytes_with_value(
    &mut self,
    key: &[u8],
    value: &V,
  ) -> Result<(), Either<V::Error, Error>> {
    self
      .insert_in(Among::Right(key), Among::Middle(value))
      .map_err(|e| match e {
        Among::Middle(e) => Either::Left(e),
        Among::Right(e) => Either::Right(e),
        _ => unreachable!(),
      })
  }

  fn insert_in(
    &self,
    key: Among<K, &K, &[u8]>,
    val: Among<V, &V, &[u8]>,
  ) -> Result<(), Among<K::Error, V::Error, Error>> {
    if self.ro {
      return Err(Among::Right(Error::read_only()));
    }

    let klen = key.encoded_len();
    let vlen = val.encoded_len();

    self.check(klen, vlen).map_err(Among::Right)?;

    let (len_size, kvlen, elen) = entry_size(klen as u32, vlen as u32);

    let buf = self.core.arena.alloc_bytes(elen);

    match buf {
      Err(e) => {
        let e = match e {
          ArenaError::InsufficientSpace {
            requested,
            available,
          } => error::Error::insufficient_space(requested, available),
          ArenaError::ReadOnly => error::Error::read_only(),
          _ => unreachable!(),
        };
        Err(Among::Right(e))
      }
      Ok(mut buf) => {
        unsafe {
          // We allocate the buffer with the exact size, so it's safe to write to the buffer.
          let flag = Flags::COMMITTED.bits();

          self.cks.reset();
          self.cks.update(&[flag]);

          buf.put_u8_unchecked(Flags::empty().bits());
          let written = buf.put_u64_varint_unchecked(kvlen);
          debug_assert_eq!(
            written, len_size,
            "the precalculated size should be equal to the written size"
          );

          let ko = STATUS_SIZE + written;
          buf.set_len(ko + klen + vlen);

          let key_buf = slice::from_raw_parts_mut(buf.as_mut_ptr().add(ko), klen);
          key.encode(key_buf).map_err(Among::Left)?;

          let vo = STATUS_SIZE + written + klen;
          let value_buf = slice::from_raw_parts_mut(buf.as_mut_ptr().add(vo), vlen);
          val.encode(value_buf).map_err(Among::Middle)?;

          let cks = {
            self.cks.update(&buf[1..]);
            self.cks.digest()
          };
          buf.put_u64_le_unchecked(cks);

          // commit the entry
          buf[0] |= Flags::COMMITTED.bits();

          if self.opts.sync_on_write() && self.core.arena.is_ondisk() {
            self
              .core
              .arena
              .flush_range(buf.offset(), elen as usize)
              .map_err(|e| Among::Right(e.into()))?;
          }
          buf.detach();

          let p = Pointer::new(klen, vlen, buf.as_ptr().add(ko));
          self.core.map.insert(p);
          Ok(())
        }
      }
    }
  }

  #[inline]
  fn check(&self, klen: usize, vlen: usize) -> Result<(), error::Error> {
    check(
      klen,
      vlen,
      self.opts.maximum_key_size(),
      self.opts.maximum_value_size(),
    )
  }
}
