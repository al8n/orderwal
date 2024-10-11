use super::{entry::*, sealed::Constructable};

pub trait Batch<C: Constructable> {
  type Key;
  type Value;

  /// The iterator type.
  type IterMut<'a>: Iterator<Item = &'a mut Entry<Self::Key, Self::Value, C>>
  where
    Self: 'a,
    Self::Key: 'a,
    Self::Value: 'a,
    C: 'a;

  /// Returns an iterator over the keys and values.
  fn iter_mut<'a>(&'a mut self) -> Self::IterMut<'a>
  where
    Self: 'a,
    Self::Key: 'a,
    Self::Value: 'a,
    C: 'a;
}

impl<C, K, V, T> Batch<C> for T
where
  for<'a> &'a mut T: IntoIterator<Item = &'a mut Entry<K, V, C>>,
  C: Constructable,
{
  type Key = K;
  type Value = V;

  type IterMut<'a>
    = <&'a mut T as IntoIterator>::IntoIter
  where
    Self: 'a,
    Self::Key: 'a,
    Self::Value: 'a,
    C: 'a;

  fn iter_mut<'a>(&'a mut self) -> Self::IterMut<'a>
  where
    Self: 'a,
    Self::Key: 'a,
    Self::Value: 'a,
    C: 'a,
  {
    IntoIterator::into_iter(self)
  }
}
