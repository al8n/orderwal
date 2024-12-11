use core::{marker::PhantomData, ops::Bound, slice};

use dbutils::leb128::decode_u64_varint;

use crate::VERSION_SIZE;

use super::{split_lengths, EntryFlags, Pointee, Pointer, RecordPointer, TypeMode};

pub struct RawEntryRef<'a, T: TypeMode> {
  flag: EntryFlags,
  key: T::Key<'a>,
  value: Option<T::Value<'a>>,
  version: Option<u64>,
}

impl<T> RawEntryRef<'_, T>
where
  T: TypeMode,
{
  #[inline]
  pub(crate) fn write_fmt(
    &self,
    wrapper_name: &'static str,
    f: &mut core::fmt::Formatter<'_>,
  ) -> core::fmt::Result {
    let mut debugger = f.debug_struct(wrapper_name);
    debugger
      .field("flags", &self.flag)
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
  T: TypeMode,
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
  T: TypeMode,
  T::Key<'a>: Copy,
  T::Value<'a>: Copy,
{
}

impl<'a, T: TypeMode> RawEntryRef<'a, T> {
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

pub struct RawRangeUpdateRef<'a, T: TypeMode> {
  flag: EntryFlags,
  start_bound: Bound<T::Key<'a>>,
  end_bound: Bound<T::Key<'a>>,
  value: Option<T::Value<'a>>,
  version: Option<u64>,
}

impl<T> RawRangeUpdateRef<'_, T>
where
  T: TypeMode,
{
  #[inline]
  pub(crate) fn write_fmt(
    &self,
    wrapper_name: &'static str,
    f: &mut core::fmt::Formatter<'_>,
  ) -> core::fmt::Result {
    let mut debugger = f.debug_struct(wrapper_name);
    debugger
      .field("flags", &self.flag)
      .field("start_bound", &self.start_bound_output())
      .field("end_bound", &self.end_bound_output())
      .field("value", &self.value.as_ref().map(|v| v.output()));

    if let Some(version) = self.version {
      debugger.field("version", &version);
    }

    debugger.finish()
  }
}

impl<'a, T> Clone for RawRangeUpdateRef<'a, T>
where
  T: TypeMode,
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
  T: TypeMode,
  T::Key<'a>: Copy,
  T::Value<'a>: Copy,
{
}

impl<'a, T: TypeMode> RawRangeUpdateRef<'a, T> {
  #[inline]
  pub const fn start_bound(&self) -> Bound<&T::Key<'a>> {
    match &self.start_bound {
      Bound::Unbounded => Bound::Unbounded,
      Bound::Included(k) => Bound::Included(k),
      Bound::Excluded(k) => Bound::Excluded(k),
    }
  }

  #[inline]
  pub fn start_bound_output(&self) -> Bound<<T::Key<'a> as Pointee<'a>>::Output> {
    match &self.start_bound {
      Bound::Unbounded => Bound::Unbounded,
      Bound::Included(k) => Bound::Included(k.output()),
      Bound::Excluded(k) => Bound::Excluded(k.output()),
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
  pub fn end_bound_output(&self) -> Bound<<T::Key<'a> as Pointee<'a>>::Output> {
    match &self.end_bound {
      Bound::Unbounded => Bound::Unbounded,
      Bound::Included(k) => Bound::Included(k.output()),
      Bound::Excluded(k) => Bound::Excluded(k.output()),
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

pub struct RawRangeDeletionRef<'a, T: TypeMode> {
  flag: EntryFlags,
  start_bound: Bound<T::Key<'a>>,
  end_bound: Bound<T::Key<'a>>,
  version: Option<u64>,
}

impl<T> RawRangeDeletionRef<'_, T>
where
  T: TypeMode,
{
  #[inline]
  pub(crate) fn write_fmt(
    &self,
    wrapper_name: &'static str,
    f: &mut core::fmt::Formatter<'_>,
  ) -> core::fmt::Result {
    let mut debugger = f.debug_struct(wrapper_name);
    debugger
      .field("flags", &self.flag)
      .field("start_bound", &self.start_bound_output())
      .field("end_bound", &self.end_bound_output());

    if let Some(version) = self.version {
      debugger.field("version", &version);
    }

    debugger.finish()
  }
}

impl<'a, T> Clone for RawRangeDeletionRef<'a, T>
where
  T: TypeMode,
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
  T: TypeMode,
  T::Key<'a>: Copy,
{
}

impl<'a, T> RawRangeDeletionRef<'a, T>
where
  T: TypeMode,
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
  pub fn start_bound_output(&self) -> Bound<<T::Key<'a> as Pointee<'a>>::Output> {
    match &self.start_bound {
      Bound::Unbounded => Bound::Unbounded,
      Bound::Included(k) => Bound::Included(k.output()),
      Bound::Excluded(k) => Bound::Excluded(k.output()),
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
  pub fn end_bound_output(&self) -> Bound<<T::Key<'a> as Pointee<'a>>::Output> {
    match &self.end_bound {
      Bound::Unbounded => Bound::Unbounded,
      Bound::Included(k) => Bound::Included(k.output()),
      Bound::Excluded(k) => Bound::Excluded(k.output()),
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
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]>,
  T::Value<'a>: Pointee<'a, Input = &'a [u8]>,
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
    key: <T::Key<'a> as Pointee<'a>>::from_input(k),
    value: v.map(<T::Value<'a> as Pointee<'a>>::from_input),
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
  T: Pointee<'a, Input = &'a [u8]>,
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
  T: Pointee<'a, Input = &'a [u8]>,
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
  T: Pointee<'a, Input = &'a [u8]>,
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
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]>,
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
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]>,
  T::Value<'a>: Pointee<'a, Input = &'a [u8]>,
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