pub use dbutils::leb128;

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
#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
pub(crate) const fn split_lengths(len: u64) -> (u32, u32) {
  ((len >> 32) as u32, len as u32)
}
