use core::borrow::Borrow;
use std::sync::Arc;

use super::{MemtableComparator, MemtableRangeComparator, TableOptions};

use crate::{
  dynamic::memtable::{BaseTable, MemtableEntry, MultipleVersionMemtable},
  types::{Kind, RecordPointer},
  WithVersion,
};
use among::Among;
use core::ops::{Bound, RangeBounds};
use dbutils::{
  equivalentor::Comparator,
  types::{KeyRef, Type},
};

pub use entry::*;
pub use point_entry::*;
pub use range_deletion_entry::*;
pub use range_update_entry::*;
use range_key::*;

pub use skl::dynamic::multiple_version::sync::{Entry, Iter, Range};
use skl::{
  dynamic::{
    multiple_version::{sync::SkipMap, Map as _},
    Ascend, Builder,
  },
  generic::{
    multiple_version::{sync::SkipMap as GenericSkipMap, Map as GenericMap},
    Builder as GenericBuilder,
  },
  Arena as _, Options,
};

mod entry;
mod point_entry;
mod range_deletion_entry;
mod range_key;
mod range_update_entry;

/// A memory table implementation based on ARENA [`SkipMap`](skl).
pub struct MultipleVersionTable<C = Ascend> {
  skl: GenericSkipMap<RecordPointer, (), MemtableComparator<C>>,
  range_deletions_skl: GenericSkipMap<RecordPointer, (), MemtableRangeComparator<C>>,
  range_updates_skl: GenericSkipMap<RecordPointer, (), MemtableRangeComparator<C>>,
}

// impl<C> BaseTable for MultipleVersionTable<C>
// where
//   C: Comparator,
// {
//   type Comparator = C;

//   type Options = TableOptions<Self::Comparator>;

//   type Error = skl::error::Error;

//   type Item<'a>
//     = Entry<'a, MemtableComparator<C>>
//   where
//     Self: 'a;

//   type PointEntry<'a>
//   where
//     Self: 'a;

//   type RangeDeletionEntry<'a>
//   where
//     Self: 'a;

//   type RangeUpdateEntry<'a>
//   where
//     Self: 'a;

//   type Iterator<'a>
//     = Iter<'a, MemtableComparator<C>>
//   where
//     Self: 'a;

//   type Range<'a, Q, R>
//     = Range<'a, MemtableComparator<C>, Q, R>
//   where
//     Self: 'a,
//     R: RangeBounds<Q> + 'a,
//     Q: ?Sized + Borrow<[u8]>;

//   type PointIterator<'a>
//   where
//     Self: 'a;

//   type PointRange<'a, Q, R>
//   where
//     Self: 'a,
//     R: RangeBounds<Q> + 'a,
//     Q: ?Sized + Borrow<[u8]>;

//   type BulkDeletionsIterator<'a>
//   where
//     Self: 'a;

//   type BulkDeletionsRange<'a, Q, R>
//   where
//     Self: 'a,
//     R: RangeBounds<Q> + 'a,
//     Q: ?Sized + Borrow<[u8]>;

//   type BulkUpdatesIterator<'a>
//   where
//     Self: 'a;

//   type BulkUpdatesRange<'a, Q, R>
//   where
//     Self: 'a,
//     R: RangeBounds<Q> + 'a,
//     Q: ?Sized + Borrow<[u8]>;

//   #[inline]
//   fn new<A>(arena: Arena<A>, opts: Self::Options) -> Result<Self, Self::Error>
//   where
//     A: rarena_allocator::Allocator,
//   {
//     memmap_or_not!(opts(arena))
//   }

//   #[inline]
//   fn insert(
//     &self,
//     version: Option<u64>,
//     kp: RecordPointer,
//     vp: ValuePointer,
//   ) -> Result<(), Self::Error> {
//     self
//       .skl
//       .insert(
//         version.unwrap_or(0),
//         kp.as_array().as_ref(),
//         vp.as_array().as_ref(),
//       )
//       .map(|_| ())
//   }

//   #[inline]
//   fn remove(&self, version: Option<u64>, key: RecordPointer) -> Result<(), Self::Error> {
//     self
//       .skl
//       .get_or_remove(version.unwrap_or(0), key.as_array().as_ref())
//       .map(|_| ())
//   }

//   #[inline]
//   fn remove_range(&self, version: Option<u64>, rp: RecordPointer) -> Result<(), Self::Error> {
//     self
//       .range_deletions_skl
//       .insert(version.unwrap_or(0), rp.as_array().as_ref(), &[])
//       .map(|_| ())
//   }

//   #[inline]
//   fn update_range(
//     &self,
//     version: Option<u64>,
//     rp: RecordPointer,
//     vp: ValuePointer,
//   ) -> Result<(), Self::Error> {
//     self
//       .range_updates_skl
//       .insert(
//         version.unwrap_or(0),
//         rp.as_array().as_ref(),
//         vp.as_array().as_ref(),
//       )
//       .map(|_| ())
//   }

//   #[inline]
//   fn kind() -> Kind {
//     Kind::MultipleVersion
//   }
// }

// impl<C> MultipleVersionMemtable for MultipleVersionTable<C>
// where
//   C: Comparator + 'static,
// {
//   type MultipleVersionEntry<'a>
//     = VersionedEntry<'a, MemtableComparator<C>>
//   where
//     Self: 'a;

//   type MultipleVersionPointEntry<'a>
//   where
//     Self: 'a;

//   type MultipleVersionRangeDeletionEntry<'a>
//   where
//     Self: 'a;

//   type MultipleVersionRangeUpdateEntry<'a>
//   where
//     Self: 'a;

//   type MultipleVersionIterator<'a>
//     = IterAll<'a, MemtableComparator<C>>
//   where
//     Self: 'a;

//   type MultipleVersionRange<'a, Q, R>
//     = MultipleVersionRange<'a, MemtableComparator<C>, Q, R>
//   where
//     Self: 'a,
//     R: RangeBounds<Q> + 'a,
//     Q: ?Sized + Borrow<[u8]>;

//   type MultipleVersionPointIterator<'a>
//   where
//     Self: 'a;

//   type MultipleVersionPointRange<'a, Q, R>
//   where
//     Self: 'a,
//     R: RangeBounds<Q> + 'a,
//     Q: ?Sized + Borrow<[u8]>;

//   type MultipleVersionBulkDeletionsIterator<'a>
//   where
//     Self: 'a;

//   type MultipleVersionBulkDeletionsRange<'a, Q, R>
//   where
//     Self: 'a,
//     R: RangeBounds<Q> + 'a,
//     Q: ?Sized + Borrow<[u8]>;

//   type MultipleVersionBulkUpdatesIterator<'a>
//   where
//     Self: 'a;

//   type MultipleVersionBulkUpdatesRange<'a, Q, R>
//   where
//     Self: 'a,
//     R: RangeBounds<Q> + 'a,
//     Q: ?Sized + Borrow<[u8]>;

//   #[inline]
//   fn maximum_version(&self) -> u64 {
//     self
//       .skl
//       .maximum_version()
//       .max(self.range_deletions_skl.maximum_version())
//       .max(self.range_updates_skl.maximum_version())
//   }

//   #[inline]
//   fn minimum_version(&self) -> u64 {
//     self
//       .skl
//       .minimum_version()
//       .min(self.range_deletions_skl.minimum_version())
//       .min(self.range_updates_skl.minimum_version())
//   }

//   #[inline]
//   fn may_contain_version(&self, version: u64) -> bool {
//     self.skl.may_contain_version(version)
//       || self.range_deletions_skl.may_contain_version(version)
//       || self.range_updates_skl.may_contain_version(version)
//   }

//   fn upper_bound<Q>(&self, version: u64, bound: Bound<&Q>) -> Option<Self::Item<'_>>
//   where
//     Q: ?Sized + Borrow<[u8]>,
//   {
//     self.skl.upper_bound(version, bound)
//   }

//   fn upper_bound_versioned<Q>(
//     &self,
//     version: u64,
//     bound: Bound<&Q>,
//   ) -> Option<Self::MultipleVersionEntry<'_>>
//   where
//     Q: ?Sized + Borrow<[u8]>,
//   {
//     self.skl.upper_bound_versioned(version, bound)
//   }

//   fn lower_bound<Q>(&self, version: u64, bound: Bound<&Q>) -> Option<Self::Item<'_>>
//   where
//     Q: ?Sized + Borrow<[u8]>,
//   {
//     self.skl.lower_bound(version, bound)
//   }

//   fn lower_bound_versioned<Q>(
//     &self,
//     version: u64,
//     bound: Bound<&Q>,
//   ) -> Option<Self::MultipleVersionEntry<'_>>
//   where
//     Q: ?Sized + Borrow<[u8]>,
//   {
//     self.skl.lower_bound_versioned(version, bound)
//   }

//   fn first(&self, version: u64) -> Option<Self::Item<'_>> {
//     self.skl.first(version)
//   }

//   fn first_versioned(&self, version: u64) -> Option<Self::MultipleVersionEntry<'_>> {
//     self.skl.first_versioned(version)
//   }

//   fn last(&self, version: u64) -> Option<Self::Item<'_>> {
//     self.skl.last(version)
//   }

//   fn last_versioned(&self, version: u64) -> Option<Self::MultipleVersionEntry<'_>> {
//     self.skl.last_versioned(version)
//   }

//   fn get<Q>(&self, version: u64, key: &Q) -> Option<Self::Item<'_>>
//   where
//     Q: ?Sized + Borrow<[u8]>,
//   {
//     self.skl.get(version, key)
//   }

//   fn get_versioned<Q>(&self, version: u64, key: &Q) -> Option<Self::MultipleVersionEntry<'_>>
//   where
//     Q: ?Sized + Borrow<[u8]>,
//   {
//     self.skl.get_versioned(version, key)
//   }

//   fn contains<Q>(&self, version: u64, key: &Q) -> bool
//   where
//     Q: ?Sized + Borrow<[u8]>,
//   {
//     self.skl.contains_key(version, key)
//   }

//   fn contains_versioned<Q>(&self, version: u64, key: &Q) -> bool
//   where
//     Q: ?Sized + Borrow<[u8]>,
//   {
//     self.skl.contains_key_versioned(version, key)
//   }

//   fn iter(&self, version: u64) -> Self::Iterator<'_> {
//     self.skl.iter(version)
//   }

//   fn iter_all_versions(&self, version: u64) -> Self::MultipleVersionIterator<'_> {
//     self.skl.iter_all_versions(version)
//   }

//   fn range<'a, Q, R>(&'a self, version: u64, range: R) -> Self::Range<'a, Q, R>
//   where
//     R: RangeBounds<Q> + 'a,
//     Q: ?Sized + Borrow<[u8]>,
//   {
//     self.skl.range(version, range)
//   }

//   fn range_all_versions<'a, Q, R>(
//     &'a self,
//     version: u64,
//     range: R,
//   ) -> Self::MultipleVersionRange<'a, Q, R>
//   where
//     R: RangeBounds<Q> + 'a,
//     Q: ?Sized + Borrow<[u8]>,
//   {
//     self.skl.range_all_versions(version, range)
//   }

//   fn point_iter(&self, version: u64) -> Self::PointIterator<'_> {
//     todo!()
//   }

//   fn point_iter_all_versions(&self, version: u64) -> Self::MultipleVersionPointIterator<'_> {
//     todo!()
//   }

//   fn point_range<'a, Q, R>(&'a self, version: u64, range: R) -> Self::PointRange<'a, Q, R>
//   where
//     R: RangeBounds<Q> + 'a,
//     Q: ?Sized + Borrow<[u8]>,
//   {
//     todo!()
//   }

//   fn point_range_all_versions<'a, Q, R>(
//     &'a self,
//     version: u64,
//     range: R,
//   ) -> Self::MultipleVersionPointRange<'a, Q, R>
//   where
//     R: RangeBounds<Q> + 'a,
//     Q: ?Sized + Borrow<[u8]>,
//   {
//     todo!()
//   }

//   fn bulk_deletions_iter(&self, version: u64) -> Self::BulkDeletionsIterator<'_> {
//     todo!()
//   }

//   fn bulk_deletions_iter_all_versions(
//     &self,
//     version: u64,
//   ) -> Self::MultipleVersionBulkDeletionsIterator<'_> {
//     todo!()
//   }

//   fn bulk_deletions_range<'a, Q, R>(
//     &'a self,
//     version: u64,
//     range: R,
//   ) -> Self::BulkDeletionsRange<'a, Q, R>
//   where
//     R: RangeBounds<Q> + 'a,
//     Q: ?Sized + Borrow<[u8]>,
//   {
//     todo!()
//   }

//   fn bulk_deletions_range_all_versions<'a, Q, R>(
//     &'a self,
//     version: u64,
//     range: R,
//   ) -> Self::MultipleVersionBulkDeletionsRange<'a, Q, R>
//   where
//     R: RangeBounds<Q> + 'a,
//     Q: ?Sized + Borrow<[u8]>,
//   {
//     todo!()
//   }

//   fn bulk_updates_iter(&self, version: u64) -> Self::BulkUpdatesIterator<'_> {
//     todo!()
//   }

//   fn bulk_updates_iter_all_versions(
//     &self,
//     version: u64,
//   ) -> Self::MultipleVersionBulkUpdatesIterator<'_> {
//     todo!()
//   }

//   fn bulk_updates_range<'a, Q, R>(
//     &'a self,
//     version: u64,
//     range: R,
//   ) -> Self::BulkUpdatesRange<'a, Q, R>
//   where
//     R: RangeBounds<Q> + 'a,
//     Q: ?Sized + Borrow<[u8]>,
//   {
//     todo!()
//   }

//   fn bulk_updates_range_all_versions<'a, Q, R>(
//     &'a self,
//     version: u64,
//     range: R,
//   ) -> Self::MultipleVersionBulkUpdatesRange<'a, Q, R>
//   where
//     R: RangeBounds<Q> + 'a,
//     Q: ?Sized + Borrow<[u8]>,
//   {
//     todo!()
//   }
// }
