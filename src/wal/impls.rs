use core::{borrow::Borrow, iter::FusedIterator, marker::PhantomData};

/// An iterator over a slice of key value tuples.
pub struct EntrySliceIter<'a, K, V>(core::slice::Iter<'a, (K, V)>);

impl<'a, K, V> Iterator for EntrySliceIter<'a, K, V>
where
  K: Borrow<[u8]>,
  V: Borrow<[u8]>,
{
  type Item = (&'a [u8], &'a [u8]);

  fn next(&mut self) -> Option<Self::Item> {
    self.0.next().map(|(k, v)| (k.borrow(), v.borrow()))
  }

  fn size_hint(&self) -> (usize, Option<usize>) {
    self.0.size_hint()
  }
}

impl<K, V> DoubleEndedIterator for EntrySliceIter<'_, K, V>
where
  K: Borrow<[u8]>,
  V: Borrow<[u8]>,
{
  fn next_back(&mut self) -> Option<Self::Item> {
    self.0.next_back().map(|(k, v)| (k.borrow(), v.borrow()))
  }
}

impl<K, V> FusedIterator for EntrySliceIter<'_, K, V>
where
  K: Borrow<[u8]>,
  V: Borrow<[u8]>,
{
}

macro_rules! impl_for_vec {
  ($( $(#[cfg($cfg:meta)])? $ty:ty $(:$N:ident)? $( => $as_ref:ident)?),+ $(,)?) => {
    $(
      $(#[cfg($cfg)])?
      const _: () = {
        impl<K, V, $(const $N: usize)?> super::Batch for $ty
        where
          K: Borrow<[u8]>,
          V: Borrow<[u8]>,
        {
          type Iter<'a> = EntrySliceIter<'a, K, V> where Self: 'a;

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

/// An iterator over a slice of key value tuples.
pub struct EntryMapIter<'a, K, V, T> {
  iter: T,
  _m: PhantomData<&'a (K, V)>,
}

impl<K, V, T> EntryMapIter<'_, K, V, T> {
  /// Construct a new iterator.
  #[inline]
  pub const fn new(iter: T) -> Self {
    Self {
      iter,
      _m: PhantomData,
    }
  }
}

impl<'a, K, V, T> Iterator for EntryMapIter<'a, K, V, T>
where
  K: Borrow<[u8]>,
  V: Borrow<[u8]>,
  T: Iterator<Item = (&'a K, &'a V)>,
{
  type Item = (&'a [u8], &'a [u8]);

  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(|(k, v)| (k.borrow(), v.borrow()))
  }

  fn size_hint(&self) -> (usize, Option<usize>) {
    self.iter.size_hint()
  }
}

impl<'a, K, V, T> DoubleEndedIterator for EntryMapIter<'a, K, V, T>
where
  K: Borrow<[u8]>,
  V: Borrow<[u8]>,
  T: DoubleEndedIterator<Item = (&'a K, &'a V)>,
{
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(|(k, v)| (k.borrow(), v.borrow()))
  }
}

impl<'a, K, V, T> FusedIterator for EntryMapIter<'a, K, V, T>
where
  K: Borrow<[u8]>,
  V: Borrow<[u8]>,
  T: FusedIterator<Item = (&'a K, &'a V)>,
{
}

impl<K, V> super::Batch for std::collections::HashMap<K, V>
where
  K: Borrow<[u8]>,
  V: Borrow<[u8]>,
{
  type Iter<'a> = EntryMapIter<'a, K, V, std::collections::hash_map::Iter<'a, K, V>> where
        Self: 'a;

  fn iter(&self) -> Self::Iter<'_> {
    EntryMapIter::new(self.iter())
  }
}

impl<K, V> super::Batch for std::collections::BTreeMap<K, V>
where
  K: Borrow<[u8]>,
  V: Borrow<[u8]>,
{
  type Iter<'a> = EntryMapIter<'a, K, V, std::collections::btree_map::Iter<'a, K, V>> where
        Self: 'a;

  fn iter(&self) -> Self::Iter<'_> {
    EntryMapIter::new(self.iter())
  }
}
