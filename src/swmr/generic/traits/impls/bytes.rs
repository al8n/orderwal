use dbutils::equivalent::*;
use std::{borrow::Cow, sync::Arc};

use super::*;

macro_rules! impls {
  ($( $(#[cfg($cfg:meta)])? $ty:ty),+ $(,)?) => {
    $(
      $(#[cfg($cfg)])?
      impl Type for $ty {
        type Ref<'a> = SliceRef<'a>;
        type Error = ();

        fn encoded_len(&self) -> usize {
          self.len()
        }

        fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
          buf.copy_from_slice(self.as_ref());
          Ok(())
        }
      }

      $(#[cfg($cfg)])?
      impl<'a> KeyRef<'a, $ty> for SliceRef<'a> {
        fn compare<Q>(&self, a: &Q) -> cmp::Ordering
        where
          Q: ?Sized + Ord + Comparable<Self>,
        {
          Comparable::compare(a, self).reverse()
        }

        fn compare_binary(a: &[u8], b: &[u8]) -> cmp::Ordering {
          a.cmp(b)
        }
      }

      $(#[cfg($cfg)])?
      impl Equivalent<SliceRef<'_>> for $ty {
        fn equivalent(&self, key: &SliceRef<'_>) -> bool {
          let this: &[u8] = self.as_ref();
          this.eq(key.0)
        }
      }

      $(#[cfg($cfg)])?
      impl Comparable<SliceRef<'_>> for $ty {
        fn compare(&self, other: &SliceRef<'_>) -> cmp::Ordering {
          let this: &[u8] = self.as_ref();
          this.cmp(other.0)
        }
      }

      $(#[cfg($cfg)])?
      impl Equivalent<$ty> for SliceRef<'_> {
        fn equivalent(&self, key: &$ty) -> bool {
          let that: &[u8] = key.as_ref();
          self.0.eq(that)
        }
      }

      $(#[cfg($cfg)])?
      impl Comparable<$ty> for SliceRef<'_> {
        fn compare(&self, other: &$ty) -> cmp::Ordering {
          let that: &[u8] = other.as_ref();
          self.0.cmp(that)
        }
      }
    )*
  };
}

impl<'a> TypeRef<'a> for &'a [u8] {
  fn from_slice(src: &'a [u8]) -> Self {
    src
  }
}

/// A wrapper type for `&'a [u8]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SliceRef<'a>(&'a [u8]);

impl<'a> TypeRef<'a> for SliceRef<'a> {
  fn from_slice(src: &'a [u8]) -> Self {
    Self(src)
  }
}

impl AsRef<[u8]> for SliceRef<'_> {
  fn as_ref(&self) -> &[u8] {
    self.0
  }
}

impl PartialEq<[u8]> for SliceRef<'_> {
  fn eq(&self, other: &[u8]) -> bool {
    self.0 == other
  }
}

impl PartialEq<&[u8]> for SliceRef<'_> {
  fn eq(&self, other: &&[u8]) -> bool {
    self.0 == *other
  }
}

impl PartialEq<SliceRef<'_>> for [u8] {
  fn eq(&self, other: &SliceRef<'_>) -> bool {
    self == other.0
  }
}

impl PartialEq<SliceRef<'_>> for &[u8] {
  fn eq(&self, other: &SliceRef<'_>) -> bool {
    *self == other.0
  }
}

impl PartialEq<Vec<u8>> for SliceRef<'_> {
  fn eq(&self, other: &Vec<u8>) -> bool {
    self.0 == other.as_slice()
  }
}

impl PartialEq<&Vec<u8>> for SliceRef<'_> {
  fn eq(&self, other: &&Vec<u8>) -> bool {
    self.0 == other.as_slice()
  }
}

impl PartialEq<SliceRef<'_>> for Vec<u8> {
  fn eq(&self, other: &SliceRef<'_>) -> bool {
    self.as_slice() == other.0
  }
}

impl PartialEq<SliceRef<'_>> for &Vec<u8> {
  fn eq(&self, other: &SliceRef<'_>) -> bool {
    self.as_slice() == other.0
  }
}

impls! {
  Cow<'_, [u8]>,
  // &'static [u8] // TODO: implement this
  Vec<u8>,
  Box<[u8]>,
  Arc<[u8]>,
  #[cfg(feature = "bytes")]
  ::bytes::Bytes,
}

#[cfg(feature = "smallvec")]
impl<const N: usize> Type for ::smallvec::SmallVec<[u8; N]> {
  type Ref<'a> = &'a [u8];
  type Error = ();

  fn encoded_len(&self) -> usize {
    self.len()
  }

  fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
    buf.copy_from_slice(self.as_ref());
    Ok(())
  }
}

#[cfg(feature = "smallvec")]
impl<'a, const N: usize> KeyRef<'a, ::smallvec::SmallVec<[u8; N]>> for [u8] {
  fn compare<Q>(&self, a: &Q) -> cmp::Ordering
  where
    Q: ?Sized + Ord + Comparable<Self>,
  {
    Comparable::compare(a, self).reverse()
  }

  fn compare_binary(a: &[u8], b: &[u8]) -> cmp::Ordering {
    a.cmp(b)
  }
}
