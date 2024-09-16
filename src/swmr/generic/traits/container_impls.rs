use core::iter::FusedIterator;

/// An iterator over a slice of key value tuples.
pub struct EntrySliceIter<'a, K, V>(core::slice::Iter<'a, (K, V)>);

impl<'a, K, V> Iterator for EntrySliceIter<'a, K, V> {
  type Item = (&'a K, &'a V);

  fn next(&mut self) -> Option<Self::Item> {
    self.0.next().map(|(k, v)| (k, v))
  }

  fn size_hint(&self) -> (usize, Option<usize>) {
    self.0.size_hint()
  }
}

impl<K, V> DoubleEndedIterator for EntrySliceIter<'_, K, V> {
  fn next_back(&mut self) -> Option<Self::Item> {
    self.0.next_back().map(|(k, v)| (k, v))
  }
}

impl<K, V> FusedIterator for EntrySliceIter<'_, K, V> {}

macro_rules! impl_for_vec {
  ($( $(#[cfg($cfg:meta)])? $ty:ty $(:$N:ident)? $( => $as_ref:ident)?),+ $(,)?) => {
    $(
      $(#[cfg($cfg)])?
      const _: () = {
        impl<K, V, $(const $N: usize)?> super::GenericBatch for $ty {
          type Key = K;

          type Value = V;

          type Iter<'a> = EntrySliceIter<'a, Self::Key, Self::Value> where Self: 'a;

          fn iter(&self) -> Self::Iter<'_> {
            EntrySliceIter(IntoIterator::into_iter(self $(.$as_ref())?))
          }
        }
      };
    )+
  };
}

impl_for_vec!(
  Vec<(K, V)>,
  Box<[(K, V)]>,
  &[(K, V)] => as_ref,
  std::sync::Arc<[(K, V)]> => as_ref,
  std::rc::Rc<[(K, V)]> => as_ref,
  #[cfg(feature = "smallvec-wrapper")]
  ::smallvec_wrapper::OneOrMore<(K, V)>,
  #[cfg(feature = "smallvec-wrapper")]
  ::smallvec_wrapper::TinyVec<(K, V)>,
  #[cfg(feature = "smallvec-wrapper")]
  ::smallvec_wrapper::TriVec<(K, V)>,
  #[cfg(feature = "smallvec-wrapper")]
  ::smallvec_wrapper::SmallVec<(K, V)>,
  #[cfg(feature = "smallvec-wrapper")]
  ::smallvec_wrapper::MediumVec<(K, V)>,
  #[cfg(feature = "smallvec-wrapper")]
  ::smallvec_wrapper::LargeVec<(K, V)>,
  #[cfg(feature = "smallvec-wrapper")]
  ::smallvec_wrapper::XLargeVec<(K, V)>,
  #[cfg(feature = "smallvec-wrapper")]
  ::smallvec_wrapper::XXLargeVec<(K, V)>,
  #[cfg(feature = "smallvec-wrapper")]
  ::smallvec_wrapper::XXXLargeVec<(K, V)>,
  #[cfg(feature = "smallvec")]
  ::smallvec::SmallVec<[(K, V); N]>: N,
);

impl<K, V> super::GenericBatch for std::collections::HashMap<K, V> {
  type Key = K;

  type Value = V;

  type Iter<'a> = std::collections::hash_map::Iter<'a, K, V> where
        Self: 'a;

  fn iter(&self) -> Self::Iter<'_> {
    self.iter()
  }
}

impl<K, V> super::GenericBatch for std::collections::BTreeMap<K, V> {
  type Key = K;

  type Value = V;

  type Iter<'a> = std::collections::btree_map::Iter<'a, K, V> where
        Self: 'a;

  fn iter(&self) -> Self::Iter<'_> {
    self.iter()
  }
}
