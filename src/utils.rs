pub use dbutils::leb128::*;

use super::*;

/// Merge two `u32` into a `u64`.
///
/// - high 32 bits: `a`
/// - low 32 bits: `b`
#[inline]
pub(crate) const fn merge_lengths(a: u32, b: u32) -> u64 {
  (a as u64) << 32 | b as u64
}

/// Split a `u64` into two `u32`.
///
/// - high 32 bits: the first `u32`
/// - low 32 bits: the second `u32`
#[inline]
pub(crate) const fn split_lengths(len: u64) -> (u32, u32) {
  ((len >> 32) as u32, len as u32)
}

/// - The first `usize` is the length of the encoded `klen + vlen`
/// - The second `u64` is encoded `klen + vlen`
/// - The third `u32` is the full entry size
#[inline]
pub(crate) const fn entry_size(key_len: u32, value_len: u32) -> (usize, u64, u32) {
  let len = merge_lengths(key_len, value_len);
  let len_size = encoded_u64_varint_len(len);
  let elen = STATUS_SIZE as u32 + len_size as u32 + key_len + value_len + CHECKSUM_SIZE as u32;

  (len_size, len, elen)
}

#[inline]
pub(crate) const fn arena_options(reserved: u32) -> ArenaOptions {
  ArenaOptions::new()
    .with_magic_version(CURRENT_VERSION)
    .with_freelist(Freelist::None)
    .with_reserved((HEADER_SIZE + reserved as usize) as u32)
    // clear capacity
    .with_capacity(0)
    .with_unify(true)
}

#[inline]
pub(crate) const fn min_u64(a: u64, b: u64) -> u64 {
  if a < b {
    a
  } else {
    b
  }
}

#[inline]
pub(crate) const fn check(
  klen: usize,
  vlen: usize,
  max_key_size: u32,
  max_value_size: u32,
  ro: bool,
) -> Result<(), error::Error> {
  if ro {
    return Err(error::Error::read_only());
  }

  let max_ksize = min_u64(max_key_size as u64, u32::MAX as u64);
  let max_vsize = min_u64(max_value_size as u64, u32::MAX as u64);

  if max_ksize < klen as u64 {
    return Err(error::Error::key_too_large(klen as u64, max_key_size));
  }

  if max_vsize < vlen as u64 {
    return Err(error::Error::value_too_large(vlen as u64, max_value_size));
  }

  let (_, _, elen) = entry_size(klen as u32, vlen as u32);

  if elen == u32::MAX {
    return Err(error::Error::entry_too_large(
      elen as u64,
      min_u64(max_key_size as u64 + max_value_size as u64, u32::MAX as u64),
    ));
  }

  Ok(())
}
