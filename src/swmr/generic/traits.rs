use core::cmp;

use crossbeam_skiplist::Comparable;
use rarena_allocator::either::Either;

mod impls;

/// The type trait for limiting the types that can be used as keys and values in the [`GenericOrderWal`].
///
/// This trait and its implementors can only be used with the [`GenericOrderWal`] type, otherwise
/// the correctness of the implementations is not guaranteed.
pub trait Type {
  /// The reference type for the type.
  type Ref<'a>;

  /// The error type for encoding the type into a binary format.
  type Error;

  /// Returns the length of the encoded type size.
  fn encoded_len(&self) -> usize;

  /// Encodes the type into a binary slice, you can assume that the buf length is equal to the value returned by [`encoded_len`](Type::encoded_len).
  fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error>;

  /// Creates a reference type from a binary slice, when using it with [`GenericOrderWal`],
  /// you can assume that the slice is the same as the one returned by [`encode`](Type::encode).
  fn from_slice(src: &[u8]) -> Self::Ref<'_>;
}

impl<T: Type> Type for Either<T, &T> {
  type Ref<'a> = T::Ref<'a>;
  type Error = T::Error;

  fn encoded_len(&self) -> usize {
    match self {
      Either::Left(t) => t.encoded_len(),
      Either::Right(t) => t.encoded_len(),
    }
  }

  fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
    match self {
      Either::Left(t) => t.encode(buf),
      Either::Right(t) => t.encode(buf),
    }
  }

  fn from_slice(src: &[u8]) -> Self::Ref<'_> {
    T::from_slice(src)
  }
}

impl<T: Type> Type for Either<&T, T> {
  type Ref<'a> = T::Ref<'a>;
  type Error = T::Error;

  fn encoded_len(&self) -> usize {
    match self {
      Either::Left(t) => t.encoded_len(),
      Either::Right(t) => t.encoded_len(),
    }
  }

  fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
    match self {
      Either::Left(t) => t.encode(buf),
      Either::Right(t) => t.encode(buf),
    }
  }

  fn from_slice(src: &[u8]) -> Self::Ref<'_> {
    T::from_slice(src)
  }
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
