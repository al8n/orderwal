use core::{mem, ops::Bound, slice};

use dbutils::{
  buffer::VacantBuffer,
  error::InsufficientBuffer,
  leb128::encoded_u64_varint_len,
  types::{Type, TypeRef},
};

use super::{utils::merge_lengths, CHECKSUM_SIZE, RECORD_FLAG_SIZE, VERSION_SIZE};

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

pub(crate) struct RangeKey {}

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
    let elen = 
      EntryFlags::SIZE as u32
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
    }) + self.packed_kvlen_size as usize
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
pub enum Kind {
  /// The Write-Ahead Log is plain, which means it does not support multiple versions.
  Plain = 0,
  /// The Write-Ahead Log supports multiple versions.
  MultipleVersion = 1,
}

#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
impl TryFrom<u8> for Kind {
  type Error = crate::error::UnknownKind;

  #[inline]
  fn try_from(value: u8) -> Result<Self, Self::Error> {
    Ok(match value {
      0 => Self::Plain,
      1 => Self::MultipleVersion,
      _ => return Err(crate::error::UnknownKind(value)),
    })
  }
}

#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
impl Kind {
  #[inline]
  pub(crate) const fn display_created_err_msg(&self) -> &'static str {
    match self {
      Self::Plain => "created without multiple versions support",
      Self::MultipleVersion => "created with multiple versions support",
    }
  }

  #[inline]
  pub(crate) const fn display_open_err_msg(&self) -> &'static str {
    match self {
      Self::Plain => "opened without multiple versions support",
      Self::MultipleVersion => "opened with multiple versions support",
    }
  }
}

const PTR_SIZE: usize = mem::size_of::<usize>();
const U32_SIZE: usize = mem::size_of::<u32>();

#[derive(Clone, Copy)]
pub struct ValuePointer {
  offset: u32,
  len: u32,
}

impl core::fmt::Debug for ValuePointer {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("ValuePointer")
      .field("offset", &self.offset)
      .field("len", &self.len)
      .finish()
  }
}

impl ValuePointer {
  const SIZE: usize = mem::size_of::<Self>();

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
    {
      let mut buf = VacantBuffer::from(array.as_mut());
      self.encode_to_buffer(&mut buf).unwrap();
    }
    array
  }
}

impl Type for ValuePointer {
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

impl<'a> TypeRef<'a> for ValuePointer {
  #[inline]
  unsafe fn from_slice(src: &'a [u8]) -> Self {
    let offset = u32::from_le_bytes(src[..4].try_into().unwrap());
    let len = u32::from_le_bytes(src[4..Self::SIZE].try_into().unwrap());
    Self { offset, len }
  }
}

/// The pointer to a record in the WAL.
#[derive(Clone, Copy)]
pub struct RecordPointer {
  offset: u32, 
}

impl core::fmt::Debug for RecordPointer {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("RecordPointer")
      .field("offset", &self.offset)
      .finish()
  }
}

impl RecordPointer {
  const SIZE: usize = mem::size_of::<Self>();

  #[inline]
  pub(crate) fn new(offset: u32) -> Self {
    Self { offset }
  }

  #[inline]
  pub const fn offset(&self) -> usize {
    self.offset as usize
  }

  #[inline]
  pub(crate) fn as_array(&self) -> [u8; Self::SIZE] {
    let mut array = [0; Self::SIZE];
    array[..4].copy_from_slice(&self.offset.to_le_bytes());
    array
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
    buf.put_slice(&self.as_array())
  }
}

impl<'a> TypeRef<'a> for RecordPointer {
  #[inline]
  unsafe fn from_slice(src: &'a [u8]) -> Self {
    let offset = u32::from_le_bytes([src[0], src[1], src[2], src[3]]);
    Self { offset }
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

#[derive(Copy, Clone)]
pub struct RawEntryRef<'a> {
  flag: EntryFlags,
  key: &'a [u8],
  value: Option<&'a [u8]>,
  version: Option<u64>,
}

impl<'a> RawEntryRef<'a> {
  #[inline]
  pub const fn key(&self) -> &'a [u8] {
    self.key
  }

  #[inline]
  pub const fn value(&self) -> Option<&'a [u8]> {
    self.value
  }

  #[inline]
  pub const fn version(&self) -> Option<u64> {
    self.version
  }
}

#[derive(Copy, Clone)]
pub struct RawRangeUpdateRef<'a> {
  flag: EntryFlags,
  start_bound: Bound<&'a [u8]>,
  end_bound: Bound<&'a [u8]>,
  value: Option<&'a [u8]>,
  version: Option<u64>,
}

impl<'a> RawRangeUpdateRef<'a> {
  #[inline]
  pub const fn start_bound(&self) -> Bound<&'a [u8]> {
    self.start_bound
  }

  #[inline]
  pub const fn end_bound(&self) -> Bound<&'a [u8]> {
    self.end_bound
  }

  #[inline]
  pub const fn value(&self) -> Option<&'a [u8]> {
    self.value
  }

  #[inline]
  pub const fn version(&self) -> Option<u64> {
    self.version
  }
}

#[derive(Copy, Clone)]
pub struct RawRangeDeletionRef<'a> {
  flag: EntryFlags,
  start_bound: Bound<&'a [u8]>,
  end_bound: Bound<&'a [u8]>,
  version: Option<u64>,
}

impl<'a> RawRangeDeletionRef<'a> {
  #[inline]
  pub const fn start_bound(&self) -> Bound<&'a [u8]> {
    self.start_bound
  }

  #[inline]
  pub const fn end_bound(&self) -> Bound<&'a [u8]> {
    self.end_bound
  }

  #[inline]
  pub const fn version(&self) -> Option<u64> {
    self.version
  }
}

/// Read the actual key from either the data pointer (if nested) or the key pointer.
/// And return how many bytes were read from the `key_ptr`.
#[inline]
const unsafe fn read_key_slice<'a>(
  data_ptr: *const u8,
  key_ptr: *const u8,
  flag: EntryFlags,
) -> (usize, &'a [u8]) {
  read_slice(data_ptr, key_ptr, flag, EntryFlags::KEY_POINTER)
}

/// Read the actual value from either the data pointer (if nested) or the value pointer.
/// And return how many bytes were read from the `val_ptr`.
#[inline]
const unsafe fn read_value_slice<'a>(
  data_ptr: *const u8,
  val_ptr: *const u8,
  flag: EntryFlags,
) -> (usize, &'a [u8]) {
  read_slice(data_ptr, val_ptr, flag, EntryFlags::VALUE_POINTER)
}

/// Read the a slice from either the data pointer (if nested) or the key pointer.
/// And return how many bytes were read from the `key_ptr`.
#[inline]
const unsafe fn read_slice<'a>(
  data_ptr: *const u8,
  ptr: *const u8,
  flags: EntryFlags,
  pointer_flag: EntryFlags,
) -> (usize, &'a [u8]) {
  const LEN_SIZE: usize = mem::size_of::<u32>();

  if flags.contains(pointer_flag) {
    let pbuf = slice::from_raw_parts(ptr, Pointer::SIZE);
    let pointer = Pointer::from_slice(pbuf);
    let val = slice::from_raw_parts(
      data_ptr.add(pointer.offset() as usize),
      pointer.len() as usize,
    );
    (Pointer::SIZE, val)
  } else {
    let len = u32::from_le_bytes(*ptr.cast::<[u8; LEN_SIZE]>()) as usize;
    let val = slice::from_raw_parts(ptr.add(LEN_SIZE), len);
    (LEN_SIZE + len, val)
  }
}

/// # Safety
/// - `data_ptr` must be a valid pointer to the data.
/// - `kp` must be pointing to key which is stored in the data_ptr.
#[inline]
pub(crate) const unsafe fn fetch_raw_key<'a>(data_ptr: *const u8, kp: &RecordPointer) -> &'a [u8] {
  let record_ptr = data_ptr.add(kp.offset());
  let flag = EntryFlags::from_bits_retain(*record_ptr);

  debug_assert!(
    !(flag.contains(EntryFlags::RANGE_SET)
      | flag.contains(EntryFlags::RANGE_DELETION)
      | flag.contains(EntryFlags::RANGE_UNSET)),
    "unexpected range key"
  );

  let ko = if flag.contains(EntryFlags::VERSIONED) {
    record_ptr.add(VERSION_SIZE)
  } else {
    record_ptr
  };

  read_key_slice(data_ptr, ko, flag).1
}

#[inline]
pub(crate) const unsafe fn fetch_entry<'a>(
  data_ptr: *const u8,
  p: &RecordPointer,
) -> RawEntryRef<'a> {
  let record_ptr = data_ptr.add(p.offset());
  let flag = EntryFlags::from_bits_retain(*record_ptr);
  let mut cursor = 1;

  debug_assert!(
    !(flag.contains(EntryFlags::RANGE_SET)
      | flag.contains(EntryFlags::RANGE_DELETION)
      | flag.contains(EntryFlags::RANGE_UNSET)),
    "unexpected range key"
  );

  let (ko, version) = if flag.contains(EntryFlags::VERSIONED) {
    cursor += VERSION_SIZE;
    let version = u64::from_le_bytes(*record_ptr.add(1).cast::<[u8; VERSION_SIZE]>());
    (
      record_ptr.add(EntryFlags::SIZE + VERSION_SIZE),
      Some(version),
    )
  } else {
    (record_ptr.add(EntryFlags::SIZE), None)
  };

  let (klen, raw_key) = read_key_slice(data_ptr, ko, flag);
  cursor += klen;

  let value = if flag.contains(EntryFlags::REMOVED) {
    let vo = record_ptr.add(cursor);
    let (_, raw_value) = read_value_slice(data_ptr, vo, flag);
    Some(raw_value)
  } else {
    None
  };

  RawEntryRef {
    flag,
    key: raw_key,
    value,
    version,
  }
}

/// # Safety
/// - `data_ptr` must be a valid pointer to the data.
/// - `kp` must be pointing to value which is stored in the data_ptr.
#[inline]
pub(crate) const unsafe fn fetch_raw_range_key_start_bound<'a>(
  data_ptr: *const u8,
  kp: &RecordPointer,
) -> Bound<&'a [u8]> {
  let record_ptr = data_ptr.add(kp.offset());
  let flag = EntryFlags::from_bits_retain(*record_ptr);

  debug_assert!(
    flag.contains(EntryFlags::RANGE_SET)
      | flag.contains(EntryFlags::RANGE_DELETION)
      | flag.contains(EntryFlags::RANGE_UNSET),
    "unexpected point key"
  );

  let ko = if flag.contains(EntryFlags::VERSIONED) {
    record_ptr.add(EntryFlags::SIZE + VERSION_SIZE)
  } else {
    record_ptr.add(EntryFlags::SIZE)
  };

  let bound = *ko;
  match bound {
    UNBOUNDED => Bound::Unbounded,
    INCLUDED => Bound::Included(read_key_slice(data_ptr, ko, flag).1),
    EXCLUDED => Bound::Excluded(read_key_slice(data_ptr, ko, flag).1),
    _ => panic!("unexpected bound tag"),
  }
}

struct FetchRangeKey<'a> {
  flag: EntryFlags,
  start_bound: Bound<&'a [u8]>,
  end_bound: Bound<&'a [u8]>,
  readed: usize,
  version: Option<u64>,
  ptr: *const u8,
}

/// # Safety
/// - `data_ptr` must be a valid pointer to the data.
/// - `kp` must be pointing to value which is stored in the data_ptr.
#[inline]
unsafe fn fetch_raw_range_key_helper<'a>(
  data_ptr: *const u8,
  kp: &RecordPointer,
  f: impl FnOnce(&EntryFlags),
) -> FetchRangeKey<'a> {
  let record_ptr = data_ptr.add(kp.offset());
  let flag = EntryFlags::from_bits_retain(*record_ptr);
  let mut cursor = 1;

  #[cfg(debug_assertions)]
  f(&flag);

  let (ko, version) = if flag.contains(EntryFlags::VERSIONED) {
    cursor += VERSION_SIZE;
    let version = u64::from_le_bytes(*record_ptr.add(1).cast::<[u8; VERSION_SIZE]>());
    (
      record_ptr.add(EntryFlags::SIZE + VERSION_SIZE),
      Some(version),
    )
  } else {
    (record_ptr.add(EntryFlags::SIZE), None)
  };

  let start_bound = *ko;
  cursor += 1;
  let start_bound = match start_bound {
    UNBOUNDED => Bound::Unbounded,
    INCLUDED => Bound::Included({
      let (len, key) = read_key_slice(data_ptr, ko, flag);
      cursor += len;
      key
    }),
    EXCLUDED => Bound::Excluded({
      let (len, key) = read_key_slice(data_ptr, ko, flag);
      cursor += len;
      key
    }),
    _ => panic!("unexpected bound tag"),
  };

  let end_bound = *record_ptr.add(cursor);
  let end_bound = match end_bound {
    UNBOUNDED => Bound::Unbounded,
    INCLUDED => Bound::Included({
      let (len, key) = read_key_slice(data_ptr, record_ptr.add(cursor + 1), flag);
      cursor += len;
      key
    }),
    EXCLUDED => Bound::Excluded({
      let (len, key) = read_key_slice(data_ptr, record_ptr.add(cursor + 1), flag);
      cursor += len;
      key
    }),
    _ => panic!("unexpected bound tag"),
  };

  FetchRangeKey {
    flag,
    start_bound,
    end_bound,
    readed: cursor,
    ptr: record_ptr.add(cursor),
    version,
  }
}

/// # Safety
/// - `data_ptr` must be a valid pointer to the data.
/// - `p` must be pointing to value which is stored in the `data_ptr`.
pub(crate) unsafe fn fetch_raw_range_key<'a>(
  data_ptr: *const u8,
  p: &RecordPointer,
) -> (Bound<&'a [u8]>, Bound<&'a [u8]>) {
  let FetchRangeKey {
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
pub(crate) unsafe fn fetch_raw_range_deletion_entry<'a>(
  data_ptr: *const u8,
  kp: &RecordPointer,
) -> RawRangeDeletionRef<'a> {
  let FetchRangeKey {
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
pub(crate) unsafe fn fetch_raw_range_update_entry<'a>(
  data_ptr: *const u8,
  kp: &RecordPointer,
) -> RawRangeUpdateRef<'a> {
  let FetchRangeKey {
    flag,
    version,
    start_bound,
    end_bound,
    ptr,
    ..
  } = fetch_raw_range_key_helper(data_ptr, kp, |flag| {
    debug_assert!(
      flag.contains(EntryFlags::RANGE_DELETION),
      "expected range deletion entry"
    )
  });

  let value = if flag.contains(EntryFlags::RANGE_UNSET) {
    let (_, raw_value) = read_value_slice(data_ptr, ptr, flag);
    Some(raw_value)
  } else {
    None
  };

  RawRangeUpdateRef {
    flag,
    start_bound,
    end_bound,
    value,
    version,
  }
}
