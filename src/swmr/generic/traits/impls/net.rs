use core::cmp;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

use super::{Comparable, KeyRef, Type, TypeRef};

impl Type for Ipv4Addr {
  type Ref<'a> = Self;

  type Error = ();

  #[inline]
  fn encoded_len(&self) -> usize {
    4
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
    buf[..4].copy_from_slice(self.octets().as_ref());
    Ok(())
  }
}

impl TypeRef<'_> for Ipv4Addr {
  #[inline]
  fn from_slice(buf: &[u8]) -> Self {
    let octets = <[u8; 4]>::from_slice(&buf[..4]);
    Ipv4Addr::from(octets)
  }
}

impl KeyRef<'_, Ipv4Addr> for Ipv4Addr {
  fn compare<Q>(&self, a: &Q) -> cmp::Ordering
  where
    Q: ?Sized + Ord + Comparable<Self>,
  {
    Comparable::compare(a, self).reverse()
  }

  fn compare_binary(a: &[u8], b: &[u8]) -> cmp::Ordering {
    let a = <Self as TypeRef>::from_slice(a);
    let b = <Self as TypeRef>::from_slice(b);
    a.cmp(&b)
  }
}

impl Type for Ipv6Addr {
  type Ref<'a> = Self;

  type Error = ();

  #[inline]
  fn encoded_len(&self) -> usize {
    16
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
    buf[..16].copy_from_slice(self.octets().as_ref());
    Ok(())
  }
}

impl TypeRef<'_> for Ipv6Addr {
  #[inline]
  fn from_slice(buf: &[u8]) -> Self {
    let octets = <[u8; 16]>::from_slice(&buf[..16]);
    Ipv6Addr::from(octets)
  }
}

impl KeyRef<'_, Ipv6Addr> for Ipv6Addr {
  fn compare<Q>(&self, a: &Q) -> cmp::Ordering
  where
    Q: ?Sized + Ord + Comparable<Self>,
  {
    Comparable::compare(a, self).reverse()
  }

  fn compare_binary(a: &[u8], b: &[u8]) -> cmp::Ordering {
    let a = <Self as TypeRef>::from_slice(a);
    let b = <Self as TypeRef>::from_slice(b);
    a.cmp(&b)
  }
}

impl Type for SocketAddrV4 {
  type Ref<'a> = Self;

  type Error = ();

  #[inline]
  fn encoded_len(&self) -> usize {
    6
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
    buf[..4].copy_from_slice(self.ip().octets().as_ref());
    buf[4..6].copy_from_slice(&self.port().to_le_bytes());
    Ok(())
  }
}

impl TypeRef<'_> for SocketAddrV4 {
  #[inline]
  fn from_slice(buf: &[u8]) -> Self {
    let octets = <[u8; 4]>::from_slice(&buf[..4]);
    let port = u16::from_le_bytes(buf[4..6].try_into().unwrap());
    SocketAddrV4::new(Ipv4Addr::from(octets), port)
  }
}

impl KeyRef<'_, SocketAddrV4> for SocketAddrV4 {
  fn compare<Q>(&self, a: &Q) -> cmp::Ordering
  where
    Q: ?Sized + Ord + Comparable<Self>,
  {
    Comparable::compare(a, self).reverse()
  }

  fn compare_binary(a: &[u8], b: &[u8]) -> cmp::Ordering {
    let a = <Self as TypeRef>::from_slice(a);
    let b = <Self as TypeRef>::from_slice(b);
    a.cmp(&b)
  }
}

impl Type for SocketAddrV6 {
  type Ref<'a> = Self;

  type Error = ();

  #[inline]
  fn encoded_len(&self) -> usize {
    18
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
    buf[..16].copy_from_slice(self.ip().octets().as_ref());
    buf[16..18].copy_from_slice(&self.port().to_le_bytes());
    Ok(())
  }
}

impl TypeRef<'_> for SocketAddrV6 {
  #[inline]
  fn from_slice(buf: &[u8]) -> Self {
    let octets = <[u8; 16]>::from_slice(&buf[..16]);
    let port = u16::from_le_bytes(buf[16..18].try_into().unwrap());
    SocketAddrV6::new(Ipv6Addr::from(octets), port, 0, 0)
  }
}

impl KeyRef<'_, SocketAddrV6> for SocketAddrV6 {
  fn compare<Q>(&self, a: &Q) -> cmp::Ordering
  where
    Q: ?Sized + Ord + Comparable<Self>,
  {
    Comparable::compare(a, self).reverse()
  }

  fn compare_binary(a: &[u8], b: &[u8]) -> cmp::Ordering {
    let a = <Self as TypeRef>::from_slice(a);
    let b = <Self as TypeRef>::from_slice(b);
    a.cmp(&b)
  }
}
