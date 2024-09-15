use core::borrow::Borrow;
use dbutils::equivalent::*;
use std::{borrow::Cow, sync::Arc};

use super::*;

macro_rules! impls {
  ($( $(#[cfg($cfg:meta)])? $ty:ty),+ $(,)?) => {
    $(
      $(#[cfg($cfg)])?
      const _: () = {
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

        impl Equivalent<SliceRef<'_>> for $ty {
          fn equivalent(&self, key: &SliceRef<'_>) -> bool {
            let this: &[u8] = self.as_ref();
            this.eq(key.0)
          }
        }

        impl Comparable<SliceRef<'_>> for $ty {
          fn compare(&self, other: &SliceRef<'_>) -> cmp::Ordering {
            let this: &[u8] = self.as_ref();
            this.cmp(other.0)
          }
        }

        impl Equivalent<$ty> for SliceRef<'_> {
          fn equivalent(&self, key: &$ty) -> bool {
            let that: &[u8] = key.as_ref();
            self.0.eq(that)
          }
        }

        impl Comparable<$ty> for SliceRef<'_> {
          fn compare(&self, other: &$ty) -> cmp::Ordering {
            let that: &[u8] = other.as_ref();
            self.0.cmp(that)
          }
        }
      };
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

impl Borrow<[u8]> for SliceRef<'_> {
  fn borrow(&self) -> &[u8] {
    self.0
  }
}

impl<'a> From<&'a [u8]> for SliceRef<'a> {
  fn from(src: &'a [u8]) -> Self {
    Self(src)
  }
}

impl<'a> From<SliceRef<'a>> for &'a [u8] {
  fn from(src: SliceRef<'a>) -> Self {
    src.0
  }
}

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

impl core::ops::Deref for SliceRef<'_> {
  type Target = [u8];
  fn deref(&self) -> &Self::Target {
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
  &'static [u8],
  Vec<u8>,
  Box<[u8]>,
  Arc<[u8]>,
  #[cfg(feature = "bytes")]
  ::bytes::Bytes,
}

#[cfg(feature = "smallvec")]
const _: () = {
  use smallvec::SmallVec;

  use super::*;

  impl<const N: usize> Type for SmallVec<[u8; N]> {
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

  impl<'a, const N: usize> KeyRef<'a, SmallVec<[u8; N]>> for SliceRef<'a> {
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

  impl<const N: usize> Equivalent<SliceRef<'_>> for SmallVec<[u8; N]> {
    fn equivalent(&self, key: &SliceRef<'_>) -> bool {
      let this: &[u8] = self.as_ref();
      this.eq(key.0)
    }
  }

  impl<const N: usize> Comparable<SliceRef<'_>> for SmallVec<[u8; N]> {
    fn compare(&self, other: &SliceRef<'_>) -> cmp::Ordering {
      let this: &[u8] = self.as_ref();
      this.cmp(other.0)
    }
  }

  impl<const N: usize> Equivalent<SmallVec<[u8; N]>> for SliceRef<'_> {
    fn equivalent(&self, key: &SmallVec<[u8; N]>) -> bool {
      let that: &[u8] = key.as_ref();
      self.0.eq(that)
    }
  }

  impl<const N: usize> Comparable<SmallVec<[u8; N]>> for SliceRef<'_> {
    fn compare(&self, other: &SmallVec<[u8; N]>) -> cmp::Ordering {
      let that: &[u8] = other.as_ref();
      self.0.cmp(that)
    }
  }
};
