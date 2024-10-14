// use core::{marker::PhantomData, ops::{Bound, RangeBounds}};

// use dbutils::equivalent::Comparable;
// use skl::{map::sync::{Entry as MapEntry, SkipMap}, Arena as _, Container};

// use crate::sealed;

// mod iter;
// use iter::{Iter, Range};

// pub struct ArenaTable<P> {
//   map: SkipMap,
//   _p: PhantomData<P>,
// }

// impl<P> Default for ArenaTable<P> {
//   #[inline]
//   fn default() -> Self {
//     todo!()
//   }
// }

// pub struct Entry<'a, P> {
//   entry: MapEntry<'a>,
//   _p: PhantomData<P>,
// }

// impl<'a, P> Entry<'a, P> {
//   #[inline]
//   const fn new(entry: MapEntry<'a>) -> Self {
//     Self {
//       entry,
//       _p: PhantomData,
//     }
//   }
// }

// impl<'a, P> memtable::MemtableEntry<'a> for Entry<'a, P> {
//   type Pointer = P;

//   fn pointer(&self) -> &Self::Pointer {
//     todo!()
//   }

//   fn next(&mut self) -> Option<Self> {
//     todo!()
//   }

//   fn prev(&mut self) -> Option<Self> {
//     todo!()
//   }
// }

// impl<P> memtable::Memtable for ArenaTable<P> {
//   type Pointer = P;

//   type Item<'a> = Entry<'a, Self::Pointer>
//   where
//     Self::Pointer: 'a,
//     Self: 'a;

//   type Iterator<'a> = Iter<'a, P>
//   where
//     Self::Pointer: 'a,
//     Self: 'a;

//   type Range<'a, Q, R> = Range<'a, Q, R, P>
//   where
//     Self::Pointer: 'a,
//     Self: 'a,
//     R: RangeBounds<Q>,
//     Q: ?Sized + Comparable<Self::Pointer>;

//   #[inline]
//   fn len(&self) -> usize {
//     self.map.len()
//   }

//   fn upper_bound<Q>(&self, bound: Bound<&Q>) -> Option<Self::Item<'_>>
//   where
//     Q: ?Sized + Comparable<Self::Pointer>,
//   {
//     self.map.upper_bound(bound).map(Entry::new)
//   }

//   fn lower_bound<Q>(&self, bound: Bound<&Q>) -> Option<Self::Item<'_>>
//   where
//     Q: ?Sized + Comparable<Self::Pointer>,
//   {
//     todo!()
//   }

//   fn insert(&mut self, ele: Self::Pointer) -> Result<(), crate::error::Error>
//   where
//     Self::Pointer: Ord + 'static,
//   {
//     todo!()
//   }

//   fn first(&self) -> Option<Self::Item<'_>>
//   where
//     Self::Pointer: Ord,
//   {
//     todo!()
//   }

//   fn last(&self) -> Option<Self::Item<'_>>
//   where
//     Self::Pointer: Ord,
//   {
//     todo!()
//   }

//   fn get<Q>(&self, key: &Q) -> Option<Self::Item<'_>>
//   where
//     Q: ?Sized + Comparable<Self::Pointer>,
//   {
//     todo!()
//   }

//   fn contains<Q>(&self, key: &Q) -> bool
//   where
//     Q: ?Sized + Comparable<Self::Pointer>,
//   {
//     todo!()
//   }

//   fn iter(&self) -> Self::Iterator<'_> {
//     todo!()
//   }

//   fn range<Q, R>(&self, range: R) -> Self::Range<'_, Q, R>
//   where
//     R: RangeBounds<Q>,
//     Q: ?Sized + Comparable<Self::Pointer>,
//   {
//     todo!()
//   }
// }
