use super::*;

mod bytes;
pub use bytes::*;
mod string;
pub use string::Str;

mod net;

impl<K, V, T> GenericBatch for T
where
  for<'a> &'a mut T: IntoIterator<Item = &'a mut GenericEntry<K, V>>,
{
  type Key = K;
  type Value = V;

  type IterMut<'a> = <&'a mut T as IntoIterator>::IntoIter where Self: 'a;

  fn iter_mut(&mut self) -> Self::IterMut<'_> {
    IntoIterator::into_iter(self)
  }
}

impl Type for () {
  type Ref<'a> = ();
  type Error = ();

  fn encoded_len(&self) -> usize {
    0
  }

  fn encode(&self, _buf: &mut [u8]) -> Result<(), Self::Error> {
    Ok(())
  }
}

impl TypeRef<'_> for () {
  unsafe fn from_slice(_buf: &[u8]) -> Self {}
}

impl<const N: usize> Type for [u8; N] {
  type Ref<'a> = Self;

  type Error = ();

  fn encoded_len(&self) -> usize {
    N
  }

  fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
    buf[..N].copy_from_slice(self.as_ref());
    Ok(())
  }
}

impl<const N: usize> TypeRef<'_> for [u8; N] {
  #[inline]
  unsafe fn from_slice(src: &'_ [u8]) -> Self {
    let mut this = [0; N];
    this.copy_from_slice(src);
    this
  }
}

macro_rules! impl_numbers {
  ($($ty:ident), +$(,)?) => {
    $(
      impl Type for $ty {
        type Ref<'a> = Self;

        type Error = ();

        #[inline]
        fn encoded_len(&self) -> usize {
          core::mem::size_of::<$ty>()
        }

        #[inline]
        fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
          const SIZE: usize = core::mem::size_of::<$ty>();
          Ok(buf[..SIZE].copy_from_slice(self.to_le_bytes().as_ref()))
        }
      }

      impl TypeRef<'_> for $ty {
        #[inline]
        unsafe fn from_slice(buf: &[u8]) -> Self {
          const SIZE: usize = core::mem::size_of::<$ty>();

          $ty::from_le_bytes(buf[..SIZE].try_into().unwrap())
        }
      }

      impl KeyRef<'_, $ty> for $ty {
        #[inline]
        fn compare<Q>(&self, a: &Q) -> core::cmp::Ordering
        where
          Q: ?Sized + Ord + Comparable<$ty> {
          Comparable::compare(a, self).reverse()
        }

        #[inline]
        fn compare_binary(a: &[u8], b: &[u8]) -> core::cmp::Ordering {
          const SIZE: usize = core::mem::size_of::<$ty>();

          let a = $ty::from_le_bytes(a[..SIZE].try_into().unwrap());
          let b = $ty::from_le_bytes(b[..SIZE].try_into().unwrap());

          a.cmp(&b)
        }
      }
    )*
  };
}

impl_numbers!(i8, i16, i32, i64, i128, u8, u16, u32, u64, u128,);

impl Type for f32 {
  type Ref<'a> = Self;

  type Error = ();

  #[inline]
  fn encoded_len(&self) -> usize {
    core::mem::size_of::<f32>()
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
    const SIZE: usize = core::mem::size_of::<f32>();
    buf[..SIZE].copy_from_slice(self.to_le_bytes().as_ref());
    Ok(())
  }
}

impl TypeRef<'_> for f32 {
  #[inline]
  unsafe fn from_slice(buf: &[u8]) -> Self {
    const SIZE: usize = core::mem::size_of::<f32>();

    f32::from_le_bytes(buf[..SIZE].try_into().unwrap())
  }
}

impl Type for f64 {
  type Ref<'a> = Self;

  type Error = ();

  #[inline]
  fn encoded_len(&self) -> usize {
    core::mem::size_of::<f64>()
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
    const SIZE: usize = core::mem::size_of::<f64>();
    buf[..SIZE].copy_from_slice(self.to_le_bytes().as_ref());
    Ok(())
  }
}

impl TypeRef<'_> for f64 {
  #[inline]
  unsafe fn from_slice(buf: &[u8]) -> Self {
    const SIZE: usize = core::mem::size_of::<f64>();

    f64::from_le_bytes(buf[..SIZE].try_into().unwrap())
  }
}

impl Type for bool {
  type Ref<'a> = Self;

  type Error = ();

  #[inline]
  fn encoded_len(&self) -> usize {
    1
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
    buf[0] = *self as u8;
    Ok(())
  }
}

impl TypeRef<'_> for bool {
  #[inline]
  unsafe fn from_slice(buf: &[u8]) -> Self {
    buf[0] != 0
  }
}

impl Type for char {
  type Ref<'a> = Self;

  type Error = ();

  #[inline]
  fn encoded_len(&self) -> usize {
    self.len_utf8()
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
    self.encode_utf8(buf);
    Ok(())
  }
}

impl TypeRef<'_> for char {
  #[inline]
  unsafe fn from_slice(buf: &[u8]) -> Self {
    core::str::from_utf8_unchecked(buf).chars().next().unwrap()
  }
}
