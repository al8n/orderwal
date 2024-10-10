use crate::batch::BatchWithBuilders;

// use super::GenericEntry;

// /// The container for entries in the [`GenericBatch`].
// pub trait GenericBatch<'e, P: 'e> {
//   /// The key type.
//   type Key: 'e;

//   /// The value type.
//   type Value: 'e;

//   /// The mutable iterator type.
//   type IterMut<'a>: Iterator<Item = &'a mut GenericEntry<'e, Self::Key, Self::Value, P>>
//   where
//     Self: 'e,
//     'e: 'a;

//   /// Returns an mutable iterator over the keys and values.
//   fn iter_mut(&'e mut self) -> Self::IterMut<'e>;
// }

// impl<'e, K, V, P: 'e, T> GenericBatch<'e, P> for T
// where
//   K: 'e,
//   V: 'e,
//   for<'a> &'a mut T: IntoIterator<Item = &'a mut GenericEntry<'e, K, V, P>>,
// {
//   type Key = K;
//   type Value = V;

//   type IterMut<'a>
//     = <&'a mut T as IntoIterator>::IntoIter
//   where
//     Self: 'e,
//     'e: 'a;

//   fn iter_mut(&'e mut self) -> Self::IterMut<'e> {
//     IntoIterator::into_iter(self)
//   }
// }
