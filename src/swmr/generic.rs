use core::{
  cmp,
  marker::PhantomData,
  ops::Bound,
  slice,
  sync::atomic::{AtomicPtr, Ordering},
};
use std::{
  path::{Path, PathBuf},
  sync::Arc,
};

use among::Among;
use crossbeam_skiplist::SkipSet;
use dbutils::{
  checksum::{BuildChecksumer, Checksumer, Crc32},
  leb128::encoded_u64_varint_len,
};
use rarena_allocator::{either::Either, sync::Arena, Allocator, Buffer};

use crate::{
  arena_options, check, entry_size,
  error::{self, Error},
  merge_lengths,
  pointer::GenericPointer,
  wal::sealed::Constructor,
  BatchEncodedEntryMeta, Flags, Options, CHECKSUM_SIZE, HEADER_SIZE, STATUS_SIZE,
};

pub use crate::{
  entry::{Generic, GenericEntry, GenericEntryRef},
  wal::{r#type::*, GenericBatch},
};

pub use dbutils::equivalent::{Comparable, Equivalent};

mod reader;
pub use reader::*;

mod iter;
pub use iter::*;

mod builder;
pub use builder::*;

#[cfg(all(
  test,
  any(
    all_tests,
    test_swmr_generic_constructor,
    test_swmr_generic_insert,
    test_swmr_generic_get,
    test_swmr_generic_iters,
  )
))]
mod tests;

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

impl<'a, K, V> Equivalent<GenericPointer<K, V>> for PartialPointer<K>
where
  K: Type + Ord,
  K::Ref<'a>: KeyRef<'a, K>,
{
  fn equivalent(&self, key: &GenericPointer<K, V>) -> bool {
    self.compare(key).is_eq()
  }
}

impl<'a, K, V> Comparable<GenericPointer<K, V>> for PartialPointer<K>
where
  K: Type + Ord,
  K::Ref<'a>: KeyRef<'a, K>,
{
  fn compare(&self, p: &GenericPointer<K, V>) -> cmp::Ordering {
    unsafe {
      let kr: K::Ref<'_> = TypeRef::from_slice(p.as_key_slice());
      let or: K::Ref<'_> = TypeRef::from_slice(self.as_key_slice());
      KeyRef::compare(&kr, &or).reverse()
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

impl<'a, K, Q, V> Equivalent<GenericPointer<K, V>> for Ref<'a, K, Q>
where
  K: Type + Ord,
  K::Ref<'a>: KeyRef<'a, K>,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
{
  fn equivalent(&self, key: &GenericPointer<K, V>) -> bool {
    self.compare(key).is_eq()
  }
}

impl<'a, K, Q, V> Comparable<GenericPointer<K, V>> for Ref<'a, K, Q>
where
  K: Type + Ord,
  K::Ref<'a>: KeyRef<'a, K>,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
{
  fn compare(&self, p: &GenericPointer<K, V>) -> cmp::Ordering {
    let kr = unsafe { TypeRef::from_slice(p.as_key_slice()) };
    KeyRef::compare(&kr, self.key).reverse()
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

impl<'a, K, Q, V> Equivalent<GenericPointer<K, V>> for Owned<'a, K, Q>
where
  K: Type + Ord,
  K::Ref<'a>: KeyRef<'a, K>,
  Q: ?Sized + Ord + Comparable<K> + Comparable<K::Ref<'a>>,
{
  fn equivalent(&self, key: &GenericPointer<K, V>) -> bool {
    self.compare(key).is_eq()
  }
}

impl<'a, K, Q, V> Comparable<GenericPointer<K, V>> for Owned<'a, K, Q>
where
  K: Type + Ord,
  K::Ref<'a>: KeyRef<'a, K>,
  Q: ?Sized + Ord + Comparable<K> + Comparable<K::Ref<'a>>,
{
  fn compare(&self, p: &GenericPointer<K, V>) -> cmp::Ordering {
    let kr = unsafe { <K::Ref<'_> as TypeRef<'_>>::from_slice(p.as_key_slice()) };
    KeyRef::compare(&kr, self.key).reverse()
  }
}

#[doc(hidden)]
pub struct GenericOrderWalCore<K, V, S> {
  arena: Arena,
  map: SkipSet<GenericPointer<K, V>>,
  opts: Options,
  cks: S,
}

impl<K, V, S> crate::wal::sealed::WalCore<(), S> for GenericOrderWalCore<K, V, S> {
  type Allocator = Arena;

  type Base = SkipSet<GenericPointer<K, V>>;

  type Pointer = GenericPointer<K, V>;

  #[inline]
  fn construct(arena: Self::Allocator, base: Self::Base, opts: Options, _cmp: (), cks: S) -> Self {
    Self {
      arena,
      map: base,
      opts,
      cks,
    }
  }
}

impl<K, V, S> GenericOrderWalCore<K, V, S> {
  #[inline]
  fn len(&self) -> usize {
    self.map.len()
  }

  #[inline]
  fn is_empty(&self) -> bool {
    self.map.is_empty()
  }

  #[inline]
  fn first(&self) -> Option<GenericEntryRef<'_, K, V>>
  where
    K: Type + Ord,
    for<'b> K::Ref<'b>: KeyRef<'b, K>,
  {
    self.map.front().map(GenericEntryRef::new)
  }

  #[inline]
  fn last(&self) -> Option<GenericEntryRef<'_, K, V>>
  where
    K: Type + Ord,
    for<'b> K::Ref<'b>: KeyRef<'b, K>,
  {
    self.map.back().map(GenericEntryRef::new)
  }

  #[inline]
  fn iter(&self) -> Iter<'_, K, V>
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
}

impl<K, V, S> Constructor<(), S> for GenericOrderWal<K, V, S> {
  type Allocator = Arena;

  type Core = GenericOrderWalCore<K, V, S>;

  type Pointer = GenericPointer<K, V>;

  fn allocator(&self) -> &Self::Allocator {
    &self.core.arena
  }

  fn from_core(core: Self::Core) -> Self {
    Self {
      core: Arc::new(core),
      ro: false,
    }
  }
}

impl<K, V, S> GenericOrderWalCore<K, V, S>
where
  K: Type + Ord,
  for<'a> <K as Type>::Ref<'a>: KeyRef<'a, K>,
  V: Type,
{
  #[inline]
  fn contains_key<'a, Q>(&'a self, key: &'a Q) -> bool
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>> + Comparable<K>,
  {
    self.map.contains::<Owned<'_, K, Q>>(&Owned::new(key))
  }

  #[inline]
  fn contains_key_by_ref<'a, Q>(&'a self, key: &'a Q) -> bool
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  {
    self.map.contains::<Ref<'_, K, Q>>(&Ref::new(key))
  }

  #[inline]
  unsafe fn contains_key_by_bytes(&self, key: &[u8]) -> bool {
    self
      .map
      .contains(&PartialPointer::new(key.len(), key.as_ptr()))
  }

  #[inline]
  fn get<'a, Q>(&'a self, key: &'a Q) -> Option<GenericEntryRef<'a, K, V>>
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>> + Comparable<K>,
  {
    self
      .map
      .get::<Owned<'_, K, Q>>(&Owned::new(key))
      .map(GenericEntryRef::new)
  }

  #[inline]
  fn get_by_ref<'a, Q>(&'a self, key: &'a Q) -> Option<GenericEntryRef<'a, K, V>>
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  {
    self
      .map
      .get::<Ref<'_, K, Q>>(&Ref::new(key))
      .map(GenericEntryRef::new)
  }

  #[inline]
  unsafe fn get_by_bytes(&self, key: &[u8]) -> Option<GenericEntryRef<'_, K, V>> {
    self
      .map
      .get(&PartialPointer::new(key.len(), key.as_ptr()))
      .map(GenericEntryRef::new)
  }
}

/// Generic ordered write-ahead log implementation, which supports structured keys and values.
///
/// Both read and write operations of this WAL are zero-cost (no allocation will happen for both read and write).
///
/// Users can create multiple readers from the WAL by [`GenericOrderWal::reader`], but only one writer is allowed.
pub struct GenericOrderWal<K, V, S = Crc32> {
  core: Arc<GenericOrderWalCore<K, V, S>>,
  ro: bool,
}

impl<K, V, S> GenericOrderWal<K, V, S>
where
  K: Type + Ord + 'static,
  for<'a> <K as Type>::Ref<'a>: KeyRef<'a, K>,
{
  /// Returns the first key-value pair in the map. The key in this pair is the minimum key in the wal.
  #[inline]
  pub fn first(&self) -> Option<GenericEntryRef<'_, K, V>> {
    self.core.first()
  }

  /// Returns the last key-value pair in the map. The key in this pair is the maximum key in the wal.
  #[inline]
  pub fn last(&self) -> Option<GenericEntryRef<'_, K, V>> {
    self.core.last()
  }

  /// Returns an iterator over the entries in the WAL.
  #[inline]
  pub fn iter(&self) -> Iter<'_, K, V> {
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

impl<K, V, S> GenericOrderWal<K, V, S>
where
  K: 'static,
  V: 'static,
{
  /// Returns a read-only WAL instance.
  #[inline]
  pub fn reader(&self) -> GenericWalReader<K, V, S> {
    GenericWalReader::new(self.core.clone())
  }

  /// Returns the path of the WAL if it is backed by a file.
  #[inline]
  pub fn path(&self) -> Option<&std::sync::Arc<std::path::PathBuf>> {
    self.core.arena.path()
  }

  /// Returns the reserved space in the WAL.
  ///
  /// ## Safety
  /// - The writer must ensure that the returned slice is not modified.
  /// - This method is not thread-safe, so be careful when using it.
  #[inline]
  pub unsafe fn reserved_slice(&self) -> &[u8] {
    if self.core.opts.reserved() == 0 {
      return &[];
    }

    &self.core.arena.reserved_slice()[HEADER_SIZE..]
  }

  /// Returns the mutable reference to the reserved slice.
  ///
  /// ## Safety
  /// - The caller must ensure that the there is no others accessing reserved slice for either read or write.
  /// - This method is not thread-safe, so be careful when using it.
  #[inline]
  pub unsafe fn reserved_slice_mut(&mut self) -> &mut [u8] {
    if self.core.opts.reserved() == 0 {
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
}

impl<K, V, S> GenericOrderWal<K, V, S>
where
  K: Type + Ord,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  V: Type,
{
  /// Returns `true` if the key exists in the WAL.
  #[inline]
  pub fn contains_key<'a, Q>(&'a self, key: &'a Q) -> bool
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>> + Comparable<K>,
  {
    self.core.contains_key(key)
  }

  /// Returns `true` if the key exists in the WAL.
  ///
  /// ## Safety
  /// - The given `key` must be valid to construct to `K::Ref` without remaining.
  #[inline]
  pub unsafe fn contains_key_by_bytes(&self, key: &[u8]) -> bool {
    self.core.contains_key_by_bytes(key)
  }

  /// Returns `true` if the key exists in the WAL.
  #[inline]
  pub fn contains_key_by_ref<'a, Q>(&'a self, key: &'a Q) -> bool
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  {
    self.core.contains_key_by_ref(key)
  }

  /// Gets the value associated with the key.
  #[inline]
  pub fn get<'a, Q>(&'a self, key: &'a Q) -> Option<GenericEntryRef<'a, K, V>>
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>> + Comparable<K>,
  {
    self.core.get(key)
  }

  /// Gets the value associated with the key.
  #[inline]
  pub fn get_by_ref<'a, Q>(&'a self, key: &'a Q) -> Option<GenericEntryRef<'a, K, V>>
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  {
    self.core.get_by_ref(key)
  }

  /// Gets the value associated with the key.
  ///
  /// ## Safety
  /// - The given `key` must be valid to construct to `K::Ref` without remaining.
  #[inline]
  pub unsafe fn get_by_bytes(&self, key: &[u8]) -> Option<GenericEntryRef<'_, K, V>> {
    self.core.get_by_bytes(key)
  }
}

impl<K, V, S> GenericOrderWal<K, V, S>
where
  K: Type + Ord + for<'a> Comparable<K::Ref<'a>> + 'static,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  V: Type + 'static,
  S: BuildChecksumer,
{
  /// Gets or insert the key value pair.
  #[inline]
  pub fn get_or_insert<'a>(
    &mut self,
    key: impl Into<Generic<'a, K>>,
    value: impl Into<Generic<'a, V>>,
  ) -> Either<GenericEntryRef<'_, K, V>, Result<(), Among<K::Error, V::Error, Error>>> {
    let key: Generic<'a, K> = key.into();
    let map = &self.core.map;
    let ent = match key.data() {
      Either::Left(k) => map.get(&Owned::new(k)),
      Either::Right(key) => map.get(&PartialPointer::new(key.len(), key.as_ptr())),
    };

    match ent.map(|e| Either::Left(GenericEntryRef::new(e))) {
      Some(e) => e,
      None => Either::Right(self.insert_in(key.into_among(), value.into().into_among())),
    }
  }

  /// Gets or insert the key with a value builder.
  #[inline]
  pub fn get_or_insert_with<'a>(
    &mut self,
    key: impl Into<Generic<'a, K>>,
    value: impl FnOnce() -> Generic<'a, V>,
  ) -> Either<GenericEntryRef<'_, K, V>, Result<(), Among<K::Error, V::Error, Error>>> {
    let key: Generic<'a, K> = key.into();
    let map = &self.core.map;
    let ent = match key.data() {
      Either::Left(k) => map.get(&Owned::new(k)),
      Either::Right(key) => map.get(&PartialPointer::new(key.len(), key.as_ptr())),
    };

    match ent.map(|e| Either::Left(GenericEntryRef::new(e))) {
      Some(e) => e,
      None => Either::Right(self.insert_in(key.into_among(), value().into_among())),
    }
  }
}

impl<K, V, S> GenericOrderWal<K, V, S>
where
  K: Type + Ord + 'static,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  V: Type + 'static,
  S: BuildChecksumer,
{
  /// Inserts a key-value pair into the write-ahead log.
  ///
  /// ## Example
  ///
  /// Here are three examples of how flexible the `insert` method is:
  ///
  /// The `Person` struct implementation can be found [here](https://github.com/al8n/orderwal/blob/main/src/swmr/generic/tests.rs#L24).
  ///
  /// 1. **Inserting an owned key-value pair with structured key and value**
  ///
  ///     ```rust,ignore
  ///     use orderwal::swmr::{*, generic::*};
  ///
  ///     let person = Person {
  ///       id: 1,
  ///       name: "Alice".to_string(),
  ///     };
  ///
  ///     let mut wal = GenericBuilder::new().with_capacity(1024).alloc::<Person, String>().unwrap();
  ///     let value = "Hello, Alice!".to_string();
  ///     wal.insert(&person, value);
  ///     ```
  ///
  /// 2. **Inserting a key-value pair, key is a reference, value is owned**
  ///    
  ///     ```rust,ignore
  ///     use orderwal::swmr::{*, generic::*};
  ///
  ///     let mut wal = GenericBuilder::new().with_capacity(1024).alloc::<Person, String>().unwrap();
  ///
  ///     let person = Person {
  ///       id: 1,
  ///       name: "Alice".to_string(),
  ///     };
  ///
  ///     wal.insert(&person, "value".to_string());
  ///     ```
  ///
  /// 3. **Inserting a key-value pair, both of them are in encoded format**
  ///
  ///     ```rust,ignore
  ///     use orderwal::swmr::{*, generic::*};
  ///  
  ///     let mut wal = GenericBuilder::new().with_capacity(1024).alloc::<Person, String>().unwrap();
  ///
  ///     let person = Person {
  ///       id: 1,
  ///       name: "Alice".to_string(),
  ///     }.encode_into_vec();
  ///
  ///
  ///     unsafe {
  ///       let key = Generic::from_slice(person.as_ref());
  ///       let value = Generic::from_slice("Hello, Alice!".as_bytes());
  ///       wal.insert(key, value).unwrap();
  ///     }
  ///     ```
  #[inline]
  pub fn insert<'a>(
    &mut self,
    key: impl Into<Generic<'a, K>>,
    val: impl Into<Generic<'a, V>>,
  ) -> Result<(), Among<K::Error, V::Error, Error>> {
    self.insert_in(key.into().into_among(), val.into().into_among())
  }

  /// Inserts a batch of entries into the write-ahead log.
  pub fn insert_batch<'a, 'b: 'a, B: GenericBatch<'b, Key = K, Value = V>>(
    &'a mut self,
    batch: &'b mut B,
  ) -> Result<(), Among<K::Error, V::Error, Error>> {
    // TODO: is there another way to avoid the borrow checker?
    let batch_ptr = AtomicPtr::new(batch);

    let batch = batch_ptr.load(Ordering::Acquire);
    let (num_entries, batch_encoded_size) = unsafe {
      (*batch)
        .iter_mut()
        .try_fold((0u32, 0u64), |(num_entries, size), ent| {
          let klen = ent.key.encoded_len();
          let vlen = ent.value.encoded_len();
          crate::utils::check_batch_entry(
            klen,
            vlen,
            self.core.opts.maximum_key_size(),
            self.core.opts.maximum_value_size(),
          )?;
          let merged_len = merge_lengths(klen as u32, vlen as u32);
          let merged_len_size = encoded_u64_varint_len(merged_len);

          let ent_size = klen as u64 + vlen as u64 + merged_len_size as u64;
          ent.meta = BatchEncodedEntryMeta::new(klen, vlen, merged_len, merged_len_size);
          Ok((num_entries + 1, size + ent_size))
        })
        .map_err(Among::Right)?
    };

    // safe to cast batch_encoded_size to u32 here, we already checked it's less than capacity (less than u32::MAX).
    let batch_meta = merge_lengths(num_entries, batch_encoded_size as u32);
    let batch_meta_size = encoded_u64_varint_len(batch_meta);
    let allocator = self.allocator();
    let remaining = allocator.remaining() as u64;
    let total_size =
      STATUS_SIZE as u64 + batch_meta_size as u64 + batch_encoded_size + CHECKSUM_SIZE as u64;
    if total_size > remaining {
      return Err(Among::Right(Error::insufficient_space(
        total_size,
        remaining as u32,
      )));
    }

    let mut buf = allocator
      .alloc_bytes(total_size as u32)
      .map_err(|e| Among::Right(Error::from_insufficient_space(e)))?;

    unsafe {
      let committed_flag = Flags::BATCHING | Flags::COMMITTED;
      let mut cks = self.core.cks.build_checksumer();
      let flag = Flags::BATCHING;
      buf.put_u8_unchecked(flag.bits);
      buf.put_u64_varint_unchecked(batch_meta);
      let mut cursor = STATUS_SIZE + batch_meta_size;

      {
        let batch = batch_ptr.load(Ordering::Acquire);
        for ent in (*batch).iter_mut() {
          let remaining = buf.remaining();
          if remaining < ent.meta.kvlen_size + ent.meta.klen + ent.meta.vlen {
            return Err(Among::Right(Error::larger_batch_size(total_size as u32)));
          }

          let ent_len_size = buf.put_u64_varint_unchecked(ent.meta.kvlen);
          let ko = cursor as usize + ent_len_size;
          buf.set_len(ko + ent.meta.klen + ent.meta.vlen);
          let ptr = buf.as_mut_ptr().add(ko);

          let key_buf = slice::from_raw_parts_mut(ptr, ent.meta.klen);
          ent.key.encode(key_buf).map_err(Among::Left)?;
          let value_buf = slice::from_raw_parts_mut(ptr.add(ent.meta.klen), ent.meta.vlen);
          ent.value.encode(value_buf).map_err(Among::Middle)?;

          cursor += ent_len_size + ent.meta.klen + ent.meta.vlen;
          ent.pointer = Some(GenericPointer::new(ent.meta.klen, ent.meta.vlen, ptr));
        }
      }

      if (cursor + CHECKSUM_SIZE) as u64 != total_size {
        return Err(Among::Right(Error::batch_size_mismatch(
          total_size as u32 - CHECKSUM_SIZE as u32,
          cursor as u32,
        )));
      }

      cks.update(&[committed_flag.bits]);
      cks.update(&buf[1..]);
      let checksum = cks.digest();
      buf.put_u64_le_unchecked(checksum);

      // commit the entry
      buf[0] = committed_flag.bits;
      let buf_cap = buf.capacity();

      if self.core.opts.sync_on_write() && allocator.is_ondisk() {
        allocator
          .flush_range(buf.offset(), buf_cap)
          .map_err(|e| Among::Right(e.into()))?;
      }
      buf.detach();

      {
        let batch = batch_ptr.load(Ordering::Acquire);
        (*batch).iter_mut().for_each(|ent| {
          self.core.map.insert(ent.pointer.take().unwrap());
        });
      }

      Ok(())
    }
  }

  fn insert_in(
    &self,
    key: Among<K, &K, &[u8]>,
    val: Among<V, &V, &[u8]>,
  ) -> Result<(), Among<K::Error, V::Error, Error>> {
    let klen = key.encoded_len();
    let vlen = val.encoded_len();

    self.check(klen, vlen).map_err(Among::Right)?;

    let (len_size, kvlen, elen) = entry_size(klen as u32, vlen as u32);

    let buf = self.core.arena.alloc_bytes(elen);

    match buf {
      Err(e) => Err(Among::Right(Error::from_insufficient_space(e))),
      Ok(mut buf) => {
        unsafe {
          // We allocate the buffer with the exact size, so it's safe to write to the buffer.
          let flag = Flags::COMMITTED.bits();

          let mut cks = self.core.cks.build_checksumer();
          cks.update(&[flag]);

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
            cks.update(&buf[1..]);
            cks.digest()
          };
          buf.put_u64_le_unchecked(cks);

          // commit the entry
          buf[0] |= Flags::COMMITTED.bits();

          if self.core.opts.sync_on_write() && self.core.arena.is_ondisk() {
            self
              .core
              .arena
              .flush_range(buf.offset(), elen as usize)
              .map_err(|e| Among::Right(e.into()))?;
          }
          buf.detach();

          let p = GenericPointer::new(klen, vlen, buf.as_ptr().add(ko));
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
      self.core.opts.maximum_key_size(),
      self.core.opts.maximum_value_size(),
      self.ro,
    )
  }
}

#[inline]
fn dummy_path_builder(p: impl AsRef<Path>) -> Result<PathBuf, ()> {
  Ok(p.as_ref().to_path_buf())
}
