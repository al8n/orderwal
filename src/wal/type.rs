use core::cmp;

use among::Among;
use dbutils::equivalent::Comparable;

mod impls;
pub use impls::*;

/// The type trait for limiting the types that can be used as keys and values in the [`GenericOrderWal`].
///
/// This trait and its implementors can only be used with the [`GenericOrderWal`] type, otherwise
/// the correctness of the implementations is not guaranteed.
pub trait Type {
  /// The reference type for the type.
  type Ref<'a>: TypeRef<'a>;

  /// The error type for encoding the type into a binary format.
  type Error;

  /// Returns the length of the encoded type size.
  fn encoded_len(&self) -> usize;

  /// Encodes the type into a bytes slice, you can assume that the buf length is equal to the value returned by [`encoded_len`](Type::encoded_len).
  fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error>;

  /// Encodes the type into a [`Vec<u8>`].
  #[inline]
  fn encode_into_vec(&self) -> Result<Vec<u8>, Self::Error> {
    let mut buf = vec![0; self.encoded_len()];
    self.encode(&mut buf)?;
    Ok(buf)
  }
}

impl<T: Type> Type for &T {
  type Ref<'a> = T::Ref<'a>;
  type Error = T::Error;

  #[inline]
  fn encoded_len(&self) -> usize {
    T::encoded_len(*self)
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
    T::encode(*self, buf)
  }
}

pub(crate) trait InsertAmongExt<T: Type> {
  fn encoded_len(&self) -> usize;
  fn encode(&self, buf: &mut [u8]) -> Result<(), T::Error>;
}

impl<T: Type> InsertAmongExt<T> for Among<T, &T, &[u8]> {
  #[inline]
  fn encoded_len(&self) -> usize {
    match self {
      Among::Left(t) => t.encoded_len(),
      Among::Middle(t) => t.encoded_len(),
      Among::Right(t) => t.len(),
    }
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<(), T::Error> {
    match self {
      Among::Left(t) => t.encode(buf),
      Among::Middle(t) => t.encode(buf),
      Among::Right(t) => {
        buf.copy_from_slice(t);
        Ok(())
      }
    }
  }
}

/// The reference type trait for the [`Type`] trait.
pub trait TypeRef<'a> {
  /// Creates a reference type from a binary slice, when using it with [`GenericOrderWal`],
  /// you can assume that the slice is the same as the one returned by [`encode`](Type::encode).
  ///
  /// ## Safety
  /// - the `src` must the same as the one returned by [`encode`](Type::encode).
  unsafe fn from_slice(src: &'a [u8]) -> Self;
}

/// The key reference trait for comparing `K` in the [`GenericOrderWal`].
pub trait KeyRef<'a, K>: Ord + Comparable<K> {
  /// Compares with a type `Q` which can be borrowed from [`T::Ref`](Type::Ref).
  fn compare<Q>(&self, a: &Q) -> cmp::Ordering
  where
    Q: ?Sized + Ord + Comparable<Self>;

  /// Compares two binary formats of the `K` directly.
  fn compare_binary(a: &[u8], b: &[u8]) -> cmp::Ordering;
}
