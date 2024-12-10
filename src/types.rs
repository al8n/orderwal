use core::{marker::PhantomData, mem, ops::Bound, slice};

use dbutils::{
  error::InsufficientBuffer,
  leb128::{decode_u64_varint, encoded_u64_varint_len},
  types::{Type, TypeRef},
};
use sealed::Pointee;

use crate::utils::split_lengths;

use super::{utils::merge_lengths, CHECKSUM_SIZE, RECORD_FLAG_SIZE, VERSION_SIZE};

pub use dbutils::{
  buffer::{BufWriter, BufWriterOnce, VacantBuffer},
  types::*,
};

const UNBOUNDED: u8 = 0;
const INCLUDED: u8 = 1;
const EXCLUDED: u8 = 2;

bitflags::bitflags! {
  /// The flags for each atomic write.
  pub(super) struct Flags: u8 {
    /// First bit: 1 indicates committed, 0 indicates uncommitted
    const COMMITTED = 0b00000001;
    /// Second bit: 1 indicates batching, 0 indicates single entry
    const BATCHING = 0b00000010;
  }
}

bitflags::bitflags! {
  /// The flags for each entry.
  #[derive(Debug, Copy, Clone)]
  pub struct EntryFlags: u8 {
    /// First bit: 1 indicates the entry is inserted within a batch
    const BATCHING = 0b00000001;
    /// Second bit: 1 indicates the key is pointer, the real key is stored in the offset contained in the RecordPointer.
    const KEY_POINTER = 0b00000010;
    /// Third bit: 1 indicates the value is pointer, the real value is stored in the offset contained in the ValuePointer.
    const VALUE_POINTER = 0b00000100;
    /// Fourth bit: 1 indicates the entry is a tombstone
    const REMOVED = 0b00001000;
    /// Fifth bit: 1 indicates the entry contains a version
    const VERSIONED = 0b00010000;
    /// Sixth bit: 1 indicates the entry is range deletion
    ///
    /// [Reference link](https://github.com/cockroachdb/pebble/blob/master/docs/rocksdb.md#range-deletions)
    const RANGE_DELETION = 0b00100000;
    /// Seventh bit: 1 indicates the entry is range set
    const RANGE_SET = 0b01000000;
    /// Eighth bit: 1 indicates the entry is range unset
    const RANGE_UNSET = 0b10000000;
  }
}

impl EntryFlags {
  pub(crate) const SIZE: usize = core::mem::size_of::<Self>();
}

#[derive(Debug)]
pub(crate) struct EncodedEntryMeta {
  pub(crate) packed_kvlen_size: usize,
  pub(crate) packed_kvlen: u64,
  pub(crate) entry_size: u32,
  pub(crate) klen: usize,
  pub(crate) vlen: usize,
  pub(crate) versioned: bool,
  batch: bool,
}

impl EncodedEntryMeta {
  #[inline]
  pub(crate) const fn new(key_len: usize, value_len: usize, versioned: bool) -> Self {
    // Cast to u32 is safe, because we already checked those values before calling this function.

    let len = merge_lengths(key_len as u32, value_len as u32);
    let len_size = encoded_u64_varint_len(len);
    let version_size = if versioned { VERSION_SIZE } else { 0 };
    let elen = RECORD_FLAG_SIZE as u32
      + EntryFlags::SIZE as u32
      + version_size as u32
      + len_size as u32
      + key_len as u32
      + value_len as u32
      + CHECKSUM_SIZE as u32;

    Self {
      packed_kvlen_size: len_size,
      batch: false,
      packed_kvlen: len,
      entry_size: elen,
      klen: key_len,
      vlen: value_len,
      versioned,
    }
  }

  #[inline]
  pub(crate) const fn batch(key_len: usize, value_len: usize, versioned: bool) -> Self {
    // Cast to u32 is safe, because we already checked those values before calling this function.

    let len = merge_lengths(key_len as u32, value_len as u32);
    let len_size = encoded_u64_varint_len(len);
    let version_size = if versioned { VERSION_SIZE } else { 0 };
    let elen = EntryFlags::SIZE as u32
      + version_size as u32
      + len_size as u32
      + key_len as u32
      + value_len as u32;

    Self {
      packed_kvlen_size: len_size,
      packed_kvlen: len,
      entry_size: elen,
      klen: key_len,
      vlen: value_len,
      versioned,
      batch: true,
    }
  }

  #[inline]
  pub(crate) const fn batch_zero(versioned: bool) -> Self {
    Self {
      packed_kvlen_size: 0,
      packed_kvlen: 0,
      entry_size: 0,
      klen: 0,
      vlen: 0,
      versioned,
      batch: true,
    }
  }

  #[inline]
  pub(crate) const fn entry_flag_offset(&self) -> usize {
    if self.batch {
      return 0;
    }

    RECORD_FLAG_SIZE
  }

  #[inline]
  pub(crate) const fn version_offset(&self) -> usize {
    self.entry_flag_offset() + EntryFlags::SIZE
  }

  #[inline]
  pub(crate) const fn key_offset(&self) -> usize {
    (if self.versioned {
      self.version_offset() + VERSION_SIZE
    } else {
      self.version_offset()
    }) + self.packed_kvlen_size
  }

  #[inline]
  pub(crate) const fn value_offset(&self) -> usize {
    self.key_offset() + self.klen
  }

  #[inline]
  pub(crate) const fn checksum_offset(&self) -> usize {
    if self.batch {
      self.value_offset() + self.vlen
    } else {
      self.entry_size as usize - CHECKSUM_SIZE
    }
  }
}

macro_rules! builder_ext {
  ($($name:ident),+ $(,)?) => {
    $(
      paste::paste! {
        impl<F> $name<F> {
          #[doc = "Creates a new `" $name "` with the given size and builder closure which requires `FnOnce`."]
          #[inline]
          pub const fn once<E>(size: usize, f: F) -> Self
          where
            F: for<'a> FnOnce(&mut dbutils::buffer::VacantBuffer<'a>) -> Result<usize, E>,
          {
            Self { size, f }
          }
        }
      }
    )*
  };
}

dbutils::builder!(
  /// A value builder for the wal, which requires the value size for accurate allocation and a closure to build the value.
  pub ValueBuilder;
  /// A key builder for the wal, which requires the key size for accurate allocation and a closure to build the key.
  pub KeyBuilder;
);

builder_ext!(ValueBuilder, KeyBuilder,);

/// The kind of the Write-Ahead Log.
///
/// Currently, there are two kinds of Write-Ahead Log:
/// 1. Plain: The Write-Ahead Log is plain, which means it does not support multiple versions.
/// 2. MultipleVersion: The Write-Ahead Log supports multiple versions.
#[derive(Debug, PartialEq, Eq)]
#[repr(u8)]
#[non_exhaustive]
pub enum Mode {
  /// The Write-Ahead Log is plain, which means it does not support multiple versions.
  Unique = 0,
  /// The Write-Ahead Log supports multiple versions.
  MultipleVersion = 1,
}

#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
impl TryFrom<u8> for Mode {
  type Error = crate::error::UnknownMode;

  #[inline]
  fn try_from(value: u8) -> Result<Self, Self::Error> {
    Ok(match value {
      0 => Self::Unique,
      1 => Self::MultipleVersion,
      _ => return Err(crate::error::UnknownMode(value)),
    })
  }
}

#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
impl Mode {
  #[inline]
  pub(crate) const fn display_created_err_msg(&self) -> &'static str {
    match self {
      Self::Unique => "created without multiple versions support",
      Self::MultipleVersion => "created with multiple versions support",
    }
  }

  #[inline]
  pub(crate) const fn display_open_err_msg(&self) -> &'static str {
    match self {
      Self::Unique => "opened without multiple versions support",
      Self::MultipleVersion => "opened with multiple versions support",
    }
  }
}

const U32_SIZE: usize = mem::size_of::<u32>();

/// The pointer to a record in the WAL.
#[derive(Debug, Clone, Copy)]
pub struct RecordPointer {
  offset: u32,
  len: u32,
}

impl RecordPointer {
  const SIZE: usize = mem::size_of::<Self>();

  #[inline]
  pub(crate) fn new(offset: u32, len: u32) -> Self {
    Self { offset, len }
  }

  #[inline]
  pub const fn offset(&self) -> usize {
    self.offset as usize
  }

  #[inline]
  pub const fn len(&self) -> usize {
    self.len as usize
  }
}

impl Type for RecordPointer {
  type Ref<'a> = Self;

  type Error = InsufficientBuffer;

  #[inline]
  fn encoded_len(&self) -> usize {
    Self::SIZE
  }

  #[inline]
  fn encode_to_buffer(&self, buf: &mut VacantBuffer<'_>) -> Result<usize, Self::Error> {
    buf
      .put_u32_le(self.offset)
      .and_then(|_| buf.put_u32_le(self.len))
      .map(|_| Self::SIZE)
  }
}

impl<'a> TypeRef<'a> for RecordPointer {
  #[inline]
  unsafe fn from_slice(src: &'a [u8]) -> Self {
    let offset = u32::from_le_bytes(src[..U32_SIZE].try_into().unwrap());
    let len = u32::from_le_bytes(src[U32_SIZE..Self::SIZE].try_into().unwrap());
    Self { offset, len }
  }
}

pub struct Pointer {
  offset: u32,
  len: u32,
}

impl Pointer {
  pub const SIZE: usize = U32_SIZE * 2;

  #[inline]
  pub(crate) const fn new(offset: u32, len: u32) -> Self {
    Self { offset, len }
  }

  #[inline]
  pub const fn offset(&self) -> usize {
    self.offset as usize
  }

  #[inline]
  pub const fn len(&self) -> usize {
    self.len as usize
  }

  #[inline]
  pub(crate) fn as_array(&self) -> [u8; Self::SIZE] {
    let mut array = [0; Self::SIZE];
    array[..4].copy_from_slice(&self.offset.to_le_bytes());
    array[4..].copy_from_slice(&self.len.to_le_bytes());
    array
  }

  /// # Panics
  /// Panics if the length of the slice is less than 8.
  #[inline]
  pub(crate) const fn from_slice(src: &[u8]) -> Self {
    let offset = u32::from_le_bytes([src[0], src[1], src[2], src[3]]);
    let len = u32::from_le_bytes([src[4], src[5], src[6], src[7]]);
    Self { offset, len }
  }
}

pub trait Kind: sealed::Sealed {}

#[doc(hidden)]
#[derive(Copy, Clone)]
pub struct Dynamic;

#[doc(hidden)]
pub struct Generic<K: ?Sized, V: ?Sized>(core::marker::PhantomData<(fn() -> K, fn() -> V)>);

impl<K, V> Clone for Generic<K, V>
where
  K: ?Sized,
  V: ?Sized,
{
  fn clone(&self) -> Self {
    *self
  }
}

impl<K, V> Copy for Generic<K, V>
where
  K: ?Sized,
  V: ?Sized,
{
}

pub(crate) mod sealed {
  use skl::generic::{LazyRef, Type};

  use super::{
    Dynamic, Generic, Kind, RawEntryRef, RawRangeDeletionRef, RawRangeUpdateRef, RecordPointer,
  };

  pub trait ComparatorConstructor<C: ?Sized>: Sized {
    fn new(ptr: *const u8, cmp: triomphe::Arc<C>) -> Self;
  }

  pub trait PointComparator<C: ?Sized>: ComparatorConstructor<C> {
    fn fetch_entry<'a, T>(&self, kp: &RecordPointer) -> RawEntryRef<'a, T>
    where
      T: Kind,
      T::Key<'a>: crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Value<'a>: crate::types::sealed::Pointee<'a, Input = &'a [u8]>;
  }

  pub trait RangeComparator<C: ?Sized>: ComparatorConstructor<C> {
    fn fetch_range_update<'a, T>(&self, kp: &RecordPointer) -> RawRangeUpdateRef<'a, T>
    where
      T: Kind,
      T::Key<'a>: crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Value<'a>: crate::types::sealed::Pointee<'a, Input = &'a [u8]>;

    fn fetch_range_deletion<'a, T>(&self, kp: &RecordPointer) -> RawRangeDeletionRef<'a, T>
    where
      T: Kind,
      T::Key<'a>: crate::types::sealed::Pointee<'a, Input = &'a [u8]>;
  }

  pub trait Pointee<'a> {
    type Input;
    type Output: Copy + core::fmt::Debug;

    fn from_input(input: Self::Input) -> Self;

    fn input(&self) -> Self::Input;

    fn output(&self) -> Self::Output;
  }

  impl<'a> Pointee<'a> for &'a [u8] {
    type Input = Self;
    type Output = Self;

    #[inline]
    fn from_input(input: Self::Input) -> Self {
      input
    }

    #[inline]
    fn input(&self) -> Self::Input {
      self
    }

    #[inline]
    fn output(&self) -> Self::Output {
      self
    }
  }

  impl<'a, T> Pointee<'a> for LazyRef<'a, T>
  where
    T: Type + ?Sized,
  {
    type Input = &'a [u8];
    type Output = T::Ref<'a>;

    #[inline]
    fn from_input(input: Self::Input) -> Self {
      unsafe { LazyRef::from_raw(input) }
    }

    #[inline]
    fn input(&self) -> Self::Input {
      self.raw().unwrap()
    }

    #[inline]
    fn output(&self) -> Self::Output {
      *self.get()
    }
  }

  pub trait Sealed: Copy {
    type Key<'a>: Pointee<'a>;
    type Value<'a>: Pointee<'a>;

    type Comparator<C>: ComparatorConstructor<C>;
    type RangeComparator<C>: ComparatorConstructor<C>;
  }

  impl<T: Sealed> Kind for T {}

  impl Sealed for Dynamic {
    type Key<'a> = &'a [u8];
    type Value<'a> = &'a [u8];
    type Comparator<C> = crate::memtable::dynamic::MemtableComparator<C>;
    type RangeComparator<C> = crate::memtable::dynamic::MemtableRangeComparator<C>;
  }

  impl<K, V> Sealed for Generic<K, V>
  where
    K: Type + ?Sized,
    V: Type + ?Sized,
  {
    type Key<'a> = LazyRef<'a, K>;
    type Value<'a> = LazyRef<'a, V>;
    type Comparator<C> = crate::memtable::generic::MemtableComparator<K, C>;
    type RangeComparator<C> = crate::memtable::generic::MemtableRangeComparator<K, C>;
  }
}

pub struct RawEntryRef<'a, T: Kind> {
  flag: EntryFlags,
  key: T::Key<'a>,
  value: Option<T::Value<'a>>,
  version: Option<u64>,
}

impl<T> RawEntryRef<'_, T>
where
  T: Kind,
{
  #[inline]
  pub(crate) fn write_fmt(&self, wrapper_name: &'static str, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    let mut debugger = f.debug_struct(wrapper_name);
    debugger
      .field("flag", &self.flag)
      .field("key", &self.key.output())
      .field("value", &self.value.as_ref().map(|v| v.output()));

    if let Some(version) = self.version {
      debugger.field("version", &version);
    }

    debugger.finish()
  }
}

impl<'a, T> Clone for RawEntryRef<'a, T>
where
  T: Kind,
  T::Key<'a>: Clone,
  T::Value<'a>: Clone,
{
  fn clone(&self) -> Self {
    Self {
      flag: self.flag,
      key: self.key.clone(),
      value: self.value.clone(),
      version: self.version,
    }
  }
}

impl<'a, T> Copy for RawEntryRef<'a, T>
where
  T: Kind,
  T::Key<'a>: Copy,
  T::Value<'a>: Copy,
{
}

impl<'a, T: Kind> RawEntryRef<'a, T> {
  #[inline]
  pub const fn key(&self) -> &T::Key<'a> {
    &self.key
  }

  #[inline]
  pub const fn value(&self) -> Option<&T::Value<'a>> {
    self.value.as_ref()
  }

  #[inline]
  pub const fn version(&self) -> Option<u64> {
    self.version
  }
}

pub struct RawRangeUpdateRef<'a, T: Kind> {
  flag: EntryFlags,
  start_bound: Bound<T::Key<'a>>,
  end_bound: Bound<T::Key<'a>>,
  value: Option<T::Value<'a>>,
  version: Option<u64>,
}

impl<'a, T> Clone for RawRangeUpdateRef<'a, T>
where
  T: Kind,
  T::Key<'a>: Clone,
  T::Value<'a>: Clone,
{
  fn clone(&self) -> Self {
    Self {
      flag: self.flag,
      start_bound: self.start_bound.clone(),
      end_bound: self.end_bound.clone(),
      value: self.value.clone(),
      version: self.version,
    }
  }
}

impl<'a, T> Copy for RawRangeUpdateRef<'a, T>
where
  T: Kind,
  T::Key<'a>: Copy,
  T::Value<'a>: Copy,
{
}

impl<'a, T: Kind> RawRangeUpdateRef<'a, T> {
  #[inline]
  pub const fn start_bound(&self) -> Bound<&T::Key<'a>> {
    match &self.start_bound {
      Bound::Unbounded => Bound::Unbounded,
      Bound::Included(k) => Bound::Included(k),
      Bound::Excluded(k) => Bound::Excluded(k),
    }
  }

  #[inline]
  pub const fn end_bound(&self) -> Bound<&T::Key<'a>> {
    match &self.end_bound {
      Bound::Unbounded => Bound::Unbounded,
      Bound::Included(k) => Bound::Included(k),
      Bound::Excluded(k) => Bound::Excluded(k),
    }
  }

  #[inline]
  pub const fn value(&self) -> Option<&T::Value<'a>> {
    self.value.as_ref()
  }

  #[inline]
  pub const fn version(&self) -> Option<u64> {
    self.version
  }
}

pub struct RawRangeDeletionRef<'a, T: Kind> {
  flag: EntryFlags,
  start_bound: Bound<T::Key<'a>>,
  end_bound: Bound<T::Key<'a>>,
  version: Option<u64>,
}

impl<'a, T> Clone for RawRangeDeletionRef<'a, T>
where
  T: Kind,
  T::Key<'a>: Clone,
{
  fn clone(&self) -> Self {
    Self {
      flag: self.flag,
      start_bound: self.start_bound.clone(),
      end_bound: self.end_bound.clone(),
      version: self.version,
    }
  }
}

impl<'a, T> Copy for RawRangeDeletionRef<'a, T>
where
  T: Kind,
  T::Key<'a>: Copy,
{
}

impl<'a, T> RawRangeDeletionRef<'a, T>
where
  T: Kind,
{
  #[inline]
  pub const fn start_bound(&self) -> Bound<&T::Key<'a>> {
    match &self.start_bound {
      Bound::Unbounded => Bound::Unbounded,
      Bound::Included(k) => Bound::Included(k),
      Bound::Excluded(k) => Bound::Excluded(k),
    }
  }

  #[inline]
  pub const fn end_bound(&self) -> Bound<&T::Key<'a>> {
    match &self.end_bound {
      Bound::Unbounded => Bound::Unbounded,
      Bound::Included(k) => Bound::Included(k),
      Bound::Excluded(k) => Bound::Excluded(k),
    }
  }

  #[inline]
  pub const fn version(&self) -> Option<u64> {
    self.version
  }
}

pub(crate) struct BoundedKey {
  bound: Bound<()>,
  pointer: bool,
}

impl BoundedKey {
  #[inline]
  pub const fn new(bound: Bound<()>, pointer: bool) -> Self {
    Self { bound, pointer }
  }

  /// Decode a `u8` into a `BoundedKey`.
  #[inline]
  pub const fn decode(src: u8) -> Self {
    let bound_bits = src & 0b11; // Extract the first 2 bits for `Bound`
    let pointer_bit = (src & 0b100) != 0; // Extract the 3rd bit for `pointer`

    let bound = match bound_bits {
      0b00 => Bound::Unbounded,
      0b01 => Bound::Included(()),
      0b10 => Bound::Excluded(()),
      _ => panic!("Invalid bound encoding"),
    };

    Self {
      bound,
      pointer: pointer_bit,
    }
  }

  /// Encode the `BoundedKey` into a `u8`.
  #[inline]
  pub const fn encode(&self) -> u8 {
    let bound_bits = match self.bound {
      Bound::Unbounded => 0b00,
      Bound::Included(()) => 0b01,
      Bound::Excluded(()) => 0b10,
    };

    let pointer_bit = if self.pointer { 0b100 } else { 0 };

    bound_bits | pointer_bit
  }

  #[inline]
  pub const fn pointer(&self) -> bool {
    self.pointer
  }

  #[inline]
  pub const fn bound(&self) -> Bound<()> {
    self.bound
  }
}

/// # Safety
/// - `data_ptr` must be a valid pointer to the data.
/// - `kp` must be pointing to key which is stored in the data_ptr.
#[inline]
pub(crate) unsafe fn fetch_raw_key<'a>(data_ptr: *const u8, kp: &RecordPointer) -> &'a [u8] {
  let entry_buf = slice::from_raw_parts(data_ptr.add(kp.offset()), kp.len());
  let flag = EntryFlags::from_bits_retain(entry_buf[0]);

  debug_assert!(
    !(flag.contains(EntryFlags::RANGE_SET)
      | flag.contains(EntryFlags::RANGE_DELETION)
      | flag.contains(EntryFlags::RANGE_UNSET)),
    "unexpected range key"
  );

  let mut cursor = if flag.contains(EntryFlags::VERSIONED) {
    1 + VERSION_SIZE
  } else {
    1
  };

  let (readed, kvlen) = decode_u64_varint(&entry_buf[cursor..]).expect("");
  cursor += readed;
  let (klen, _) = split_lengths(kvlen);
  let k = &entry_buf[cursor..cursor + klen as usize];

  if !flag.contains(EntryFlags::KEY_POINTER) {
    return k;
  }

  let pointer = Pointer::from_slice(k);
  slice::from_raw_parts(
    data_ptr.add(pointer.offset() as usize),
    pointer.len() as usize,
  )
}

#[inline]
pub(crate) unsafe fn fetch_entry<'a, T>(
  data_ptr: *const u8,
  p: &RecordPointer,
) -> RawEntryRef<'a, T>
where
  T: Kind,
  T::Key<'a>: sealed::Pointee<'a, Input = &'a [u8]>,
  T::Value<'a>: sealed::Pointee<'a, Input = &'a [u8]>,
{
  let entry_buf = slice::from_raw_parts(data_ptr.add(p.offset()), p.len());
  let flag = EntryFlags::from_bits_retain(entry_buf[0]);

  debug_assert!(
    !(flag.contains(EntryFlags::RANGE_SET)
      | flag.contains(EntryFlags::RANGE_DELETION)
      | flag.contains(EntryFlags::RANGE_UNSET)),
    "unexpected range entry"
  );

  let (mut cursor, version) = if flag.contains(EntryFlags::VERSIONED) {
    let version = u64::from_le_bytes(
      entry_buf[EntryFlags::SIZE..EntryFlags::SIZE + VERSION_SIZE]
        .try_into()
        .unwrap(),
    );
    (EntryFlags::SIZE + VERSION_SIZE, Some(version))
  } else {
    (EntryFlags::SIZE, None)
  };

  let (readed, kvlen) = decode_u64_varint(&entry_buf[cursor..]).expect("");
  cursor += readed;
  let (klen, vlen) = split_lengths(kvlen);
  let k = if !flag.contains(EntryFlags::KEY_POINTER) {
    &entry_buf[cursor..cursor + klen as usize]
  } else {
    let pointer = Pointer::from_slice(&entry_buf[cursor..cursor + klen as usize]);
    slice::from_raw_parts(
      data_ptr.add(pointer.offset() as usize),
      pointer.len() as usize,
    )
  };
  cursor += klen as usize;

  let v = if flag.contains(EntryFlags::REMOVED) {
    None
  } else {
    let v = &entry_buf[cursor..cursor + vlen as usize];
    if flag.contains(EntryFlags::VALUE_POINTER) {
      let pointer = Pointer::from_slice(v);
      Some(slice::from_raw_parts(
        data_ptr.add(pointer.offset() as usize),
        pointer.len() as usize,
      ))
    } else {
      Some(v)
    }
  };

  RawEntryRef {
    flag,
    key: <T::Key<'a> as sealed::Pointee<'a>>::from_input(k),
    value: v.map(<T::Value<'a> as sealed::Pointee<'a>>::from_input),
    version,
  }
}

/// # Safety
/// - `data_ptr` must be a valid pointer to the data.
/// - `kp` must be pointing to value which is stored in the data_ptr.
#[inline]
pub(crate) unsafe fn fetch_raw_range_key_start_bound<'a, T>(
  data_ptr: *const u8,
  kp: &RecordPointer,
) -> Bound<T>
where
  T: sealed::Pointee<'a, Input = &'a [u8]>,
{
  let entry_buf = slice::from_raw_parts(data_ptr.add(kp.offset()), kp.len());
  let flag = EntryFlags::from_bits_retain(entry_buf[0]);

  debug_assert!(
    flag.contains(EntryFlags::RANGE_SET)
      | flag.contains(EntryFlags::RANGE_DELETION)
      | flag.contains(EntryFlags::RANGE_UNSET),
    "unexpected point key"
  );

  let mut cursor = if flag.contains(EntryFlags::VERSIONED) {
    EntryFlags::SIZE + VERSION_SIZE
  } else {
    EntryFlags::SIZE
  };

  let (readed, kvlen) =
    decode_u64_varint(&entry_buf[cursor..]).expect("kvlen should be decoded without error");
  cursor += readed;
  let (klen, _) = split_lengths(kvlen);

  let mut range_key_buf = &entry_buf[cursor..cursor + klen as usize];

  let (readed, range_key_len) =
    decode_u64_varint(range_key_buf).expect("range key len should be decoded without error");
  range_key_buf = &range_key_buf[readed..];
  let (start_key_len, _) = split_lengths(range_key_len);
  let start_key_buf = &range_key_buf[..start_key_len as usize];

  let start_bound = BoundedKey::decode(start_key_buf[0]);
  let raw_start_key = &start_key_buf[1..];
  let start_key = if start_bound.pointer() {
    let pointer = Pointer::from_slice(raw_start_key);
    let key = slice::from_raw_parts(
      data_ptr.add(pointer.offset() as usize),
      pointer.len() as usize,
    );
    T::from_input(key)
  } else {
    T::from_input(raw_start_key)
  };
  start_bound.bound().map(|_| start_key)
}

struct FetchRangeKey<'a, T: Pointee<'a>> {
  flag: EntryFlags,
  start_bound: Bound<T>,
  end_bound: Bound<T>,
  version: Option<u64>,
  value: Option<Pointer>,
  _m: PhantomData<&'a ()>,
}

/// # Safety
/// - `data_ptr` must be a valid pointer to the data.
/// - `kp` must be pointing to value which is stored in the data_ptr.
#[inline]
unsafe fn fetch_raw_range_key_helper<'a, T>(
  data_ptr: *const u8,
  kp: &RecordPointer,
  f: impl FnOnce(&EntryFlags),
) -> FetchRangeKey<'a, T>
where
  T: sealed::Pointee<'a, Input = &'a [u8]>,
{
  let entry_buf = slice::from_raw_parts(data_ptr.add(kp.offset()), kp.len());
  let flag = EntryFlags::from_bits_retain(entry_buf[0]);

  #[cfg(debug_assertions)]
  f(&flag);

  let (mut cursor, version) = if flag.contains(EntryFlags::VERSIONED) {
    let version = u64::from_le_bytes(
      entry_buf[EntryFlags::SIZE..EntryFlags::SIZE + VERSION_SIZE]
        .try_into()
        .unwrap(),
    );
    (EntryFlags::SIZE + VERSION_SIZE, Some(version))
  } else {
    (EntryFlags::SIZE, None)
  };

  let (readed, kvlen) =
    decode_u64_varint(&entry_buf[cursor..]).expect("kvlen should be decoded without error");
  cursor += readed;
  let (klen, vlen) = split_lengths(kvlen);

  let mut range_key_buf = &entry_buf[cursor..cursor + klen as usize];
  cursor += klen as usize;

  let (readed, range_key_len) =
    decode_u64_varint(range_key_buf).expect("range key len should be decoded without error");
  range_key_buf = &range_key_buf[readed..];
  let (start_key_len, end_key_len) = split_lengths(range_key_len);
  let start_key_buf = &range_key_buf[..start_key_len as usize];
  let end_key_buf =
    &range_key_buf[start_key_len as usize..start_key_len as usize + end_key_len as usize];

  let start_bound = BoundedKey::decode(start_key_buf[0]);
  let raw_start_key = &start_key_buf[1..];
  let start_key = if start_bound.pointer() {
    let pointer = Pointer::from_slice(raw_start_key);
    let key = slice::from_raw_parts(
      data_ptr.add(pointer.offset() as usize),
      pointer.len() as usize,
    );
    T::from_input(key)
  } else {
    T::from_input(raw_start_key)
  };
  let start_bound = start_bound.bound().map(|_| start_key);

  let end_bound = BoundedKey::decode(end_key_buf[0]);
  let raw_end_key = &end_key_buf[1..];
  let end_key = if end_bound.pointer() {
    let pointer = Pointer::from_slice(raw_end_key);
    let key = slice::from_raw_parts(
      data_ptr.add(pointer.offset() as usize),
      pointer.len() as usize,
    );
    T::from_input(key)
  } else {
    T::from_input(raw_end_key)
  };
  let end_bound = end_bound.bound().map(|_| end_key);

  let value = if flag.contains(EntryFlags::RANGE_SET) {
    Some(Pointer::new(kp.offset + cursor as u32, vlen))
  } else {
    None
  };

  FetchRangeKey {
    flag,
    start_bound,
    end_bound,
    value,
    version,
    _m: PhantomData,
  }
}

/// # Safety
/// - `data_ptr` must be a valid pointer to the data.
/// - `p` must be pointing to value which is stored in the `data_ptr`.
pub(crate) unsafe fn fetch_raw_range_key<'a, T>(
  data_ptr: *const u8,
  p: &RecordPointer,
) -> (Bound<T>, Bound<T>)
where
  T: sealed::Pointee<'a, Input = &'a [u8]>,
{
  let FetchRangeKey::<T> {
    start_bound,
    end_bound,
    ..
  } = fetch_raw_range_key_helper(data_ptr, p, |flag| {
    debug_assert!(
      flag.contains(EntryFlags::RANGE_SET)
        | flag.contains(EntryFlags::RANGE_DELETION)
        | flag.contains(EntryFlags::RANGE_UNSET),
      "unexpected point key"
    )
  });
  (start_bound, end_bound)
}

/// # Safety
/// - `data_ptr` must be a valid pointer to the data.
/// - `kp` must be pointing to value which is stored in the data_ptr.
#[inline]
pub(crate) unsafe fn fetch_raw_range_deletion_entry<'a, T>(
  data_ptr: *const u8,
  kp: &RecordPointer,
) -> RawRangeDeletionRef<'a, T>
where
  T: Kind,
  T::Key<'a>: sealed::Pointee<'a, Input = &'a [u8]>,
{
  let FetchRangeKey::<T::Key<'_>> {
    flag,
    version,
    start_bound,
    end_bound,
    ..
  } = fetch_raw_range_key_helper(data_ptr, kp, |flag| {
    debug_assert!(
      flag.contains(EntryFlags::RANGE_DELETION),
      "expected range deletion entry"
    )
  });

  RawRangeDeletionRef {
    flag,
    start_bound,
    end_bound,
    version,
  }
}

/// # Safety
/// - `data_ptr` must be a valid pointer to the data.
/// - `kp` must be pointing to value which is stored in the data_ptr.
#[inline]
pub(crate) unsafe fn fetch_raw_range_update_entry<'a, T>(
  data_ptr: *const u8,
  kp: &RecordPointer,
) -> RawRangeUpdateRef<'a, T>
where
  T: Kind,
  T::Key<'a>: sealed::Pointee<'a, Input = &'a [u8]>,
  T::Value<'a>: sealed::Pointee<'a, Input = &'a [u8]>,
{
  let FetchRangeKey::<T::Key<'_>> {
    flag,
    version,
    start_bound,
    end_bound,
    value,
    ..
  } = fetch_raw_range_key_helper(data_ptr, kp, |flag| {
    debug_assert!(
      flag.contains(EntryFlags::RANGE_DELETION),
      "expected range deletion entry"
    )
  });

  let value = value.map(|pointer| {
    let v = slice::from_raw_parts(data_ptr.add(pointer.offset()), pointer.len());
    if !flag.contains(EntryFlags::VALUE_POINTER) {
      let pointer = Pointer::from_slice(v);
      T::Value::from_input(slice::from_raw_parts(
        data_ptr.add(pointer.offset() as usize),
        pointer.len() as usize,
      ))
    } else {
      T::Value::from_input(v)
    }
  });

  RawRangeUpdateRef {
    flag,
    start_bound,
    end_bound,
    value,
    version,
  }
}
