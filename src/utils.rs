use dbutils::leb128;

use super::*;

#[inline]
pub(crate) const fn merge_lengths(klen: u32, vlen: u32) -> u64 {
  (klen as u64) << 32 | vlen as u64
}

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
  let len_size = leb128::encoded_len_varint(len);
  let elen = STATUS_SIZE as u32 + len_size as u32 + key_len + value_len + CHECKSUM_SIZE as u32;

  (len_size, len, elen)
}

#[inline]
pub(crate) const fn arena_options(reserved: u32) -> ArenaOptions {
  ArenaOptions::new()
    .with_magic_version(CURRENT_VERSION)
    .with_freelist(Freelist::None)
    .with_reserved((HEADER_SIZE + reserved as usize) as u32)
    .with_unify(true)
}
