use core::{
  convert::Infallible,
  ops::ControlFlow,
  sync::atomic::{AtomicUsize, Ordering},
};

use crossbeam_skiplist_mvcc::nested::SkipMap;
use dbutils::{
  equivalentor::{Comparator, QueryComparator},
  state::{Active, MaybeTombstone, State},
};
use ref_cast::RefCast;
use triomphe::Arc;

use crate::types::{
  sealed::{ComparatorConstructor, PointComparator, Pointee, RangeComparator},
  Mode, Query, RecordPointer, RefQuery, Remove, Update,
};

use super::{sealed, Entry, Memtable, MutableMemtable, RangeEntry, RangeEntryExt, Transfer};

pub use entry::*;
pub use iter::*;
pub use point::*;
pub use range_entry::*;

mod entry;
mod iter;
mod point;
mod range_entry;

/// A memory table implementation based on ARENA [`SkipMap`](crossbeam_skiplist_mvcc::nested::SkipMap).
pub struct Table<C, T>
where
  T: Mode,
{
  pub(in crate::memtable) skl: SkipMap<RecordPointer, RecordPointer, T::Comparator<C>>,
  pub(in crate::memtable) range_deletions_skl:
    SkipMap<RecordPointer, RecordPointer, T::RangeComparator<C>>,
  pub(in crate::memtable) range_updates_skl:
    SkipMap<RecordPointer, RecordPointer, T::RangeComparator<C>>,
  len: AtomicUsize,
}

impl<C, T> Memtable for Table<C, T>
where
  C: 'static,
  T: Mode,
  T::Comparator<C>: 'static,
  T::RangeComparator<C>: 'static,
{
  type Options = C;
  type Error = Infallible;

  #[inline]
  fn new<A>(arena: A, opts: Self::Options) -> Result<Self, Self::Error>
  where
    Self: Sized,
    A: rarena_allocator::Allocator,
  {
    let cmp = Arc::new(opts);
    let ptr = arena.raw_ptr();
    let points_cmp = <T::Comparator<C> as ComparatorConstructor<_>>::new(ptr, cmp.clone());
    let range_del_cmp = <T::RangeComparator<C> as ComparatorConstructor<_>>::new(ptr, cmp.clone());
    let range_update_cmp =
      <T::RangeComparator<C> as ComparatorConstructor<_>>::new(ptr, cmp.clone());

    Ok(Self {
      skl: SkipMap::with_comparator(points_cmp),
      range_deletions_skl: SkipMap::with_comparator(range_del_cmp),
      range_updates_skl: SkipMap::with_comparator(range_update_cmp),
      len: AtomicUsize::new(0),
    })
  }

  #[inline]
  fn len(&self) -> usize {
    self.len.load(Ordering::Acquire)
  }
}

impl<C, T> MutableMemtable for Table<C, T>
where
  C: 'static,
  T: Mode,
  T::Comparator<C>: Comparator<RecordPointer> + Send + 'static,
  T::RangeComparator<C>: Comparator<RecordPointer> + Send + 'static,
{
  #[inline]
  fn insert(&self, version: u64, pointer: RecordPointer) -> Result<(), Self::Error> {
    self.skl.insert_unchecked(version, pointer, pointer);
    self.len.fetch_add(1, Ordering::Release);
    Ok(())
  }

  #[inline]
  fn remove(&self, version: u64, key: RecordPointer) -> Result<(), Self::Error> {
    self.skl.remove_unchecked(version, key);
    self.len.fetch_add(1, Ordering::Release);
    Ok(())
  }

  #[inline]
  fn range_remove(&self, version: u64, pointer: RecordPointer) -> Result<(), Self::Error> {
    self
      .range_deletions_skl
      .insert_unchecked(version, pointer, pointer);
    self.len.fetch_add(1, Ordering::Release);
    Ok(())
  }

  #[inline]
  fn range_set(&self, version: u64, pointer: RecordPointer) -> Result<(), Self::Error> {
    self
      .range_updates_skl
      .insert_unchecked(version, pointer, pointer);
    self.len.fetch_add(1, Ordering::Release);
    Ok(())
  }

  #[inline]
  fn range_unset(&self, version: u64, key: RecordPointer) -> Result<(), Self::Error> {
    self.range_updates_skl.remove_unchecked(version, key);
    self.len.fetch_add(1, Ordering::Release);
    Ok(())
  }
}

impl<'a, C, T> Table<C, T>
where
  C: 'static,
  T: Mode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]>,
  T::Comparator<C>: PointComparator<C>
    + Comparator<RecordPointer>
    + Comparator<Query<<T::Key<'a> as Pointee<'a>>::Output>>
    + 'static,
  T::RangeComparator<C>: Comparator<RecordPointer>
    + QueryComparator<RecordPointer, RefQuery<<T::Key<'a> as Pointee<'a>>::Output>>
    + RangeComparator<C>
    + 'static,
  RangeEntryRef<'a, Active, Remove, C, T>:
    RangeEntry<'a, Remove, Key = <T::Key<'a> as Pointee<'a>>::Output>,
{
  pub(in crate::memtable) fn validate<S>(
    &'a self,
    query_version: u64,
    ent: PointEntryRef<'a, S, C, T>,
  ) -> ControlFlow<Option<EntryRef<'a, S, C, T>>, PointEntryRef<'a, S, C, T>>
  where
    S: Transfer<'a, T::Value<'a>>,
    S::Data<'a, S::Value>: 'a,
    PointEntryRef<'a, S, C, T>: Entry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
    MaybeTombstone: Transfer<'a, T::Value<'a>>,
    RangeEntryRef<'a, MaybeTombstone, Update, C, T>: RangeEntry<
      'a,
      Update,
      Key = <T::Key<'a> as Pointee<'a>>::Output,
      Value = <MaybeTombstone as State>::Data<
        'a,
        <MaybeTombstone as sealed::Sealed<'a, T::Value<'a>>>::Value,
      >,
    >,
  {
    let key = ent.key();
    let cmp = ent.ent.comparator();
    let version = ent.ent.version();
    let query = RefQuery::new(key);
    let shadow = self
      .range_deletions_skl
      .range(query_version, ..=&query)
      .any(|ent| {
        let del_ent_version = ent.version();
        if !(version <= del_ent_version && del_ent_version <= query_version) {
          return false;
        }
        let ent = RangeEntryRef::<Active, Remove, C, T>::new(ent);
        dbutils::equivalentor::RangeComparator::contains(
          cmp,
          &ent.query_range(),
          Query::ref_cast(&query.query),
        )
      });
    if shadow {
      return ControlFlow::Continue(ent);
    }
    let range_ent = self
      .range_updates_skl
      .range_all(query_version, ..=&query)
      .filter_map(|ent| {
        let range_ent_version = ent.version();
        if !(version <= range_ent_version && range_ent_version <= query_version) {
          return None;
        }
        let ent = RangeEntryRef::<MaybeTombstone, Update, C, T>::new(ent);
        if dbutils::equivalentor::RangeComparator::contains(
          cmp,
          &ent.query_range(),
          Query::ref_cast(&query.query),
        ) {
          Some(ent)
        } else {
          None
        }
      })
      .max_by_key(|e| e.version());
    if let Some(range_ent) = range_ent {
      let version = range_ent.version();
      if let Some(val) = range_ent.into_value() {
        return ControlFlow::Break(Some(EntryRef::new(
          self,
          query_version,
          ent,
          key,
          Some(S::data(val)),
          version,
        )));
      }
    }
    let version = ent.version();
    ControlFlow::Break(Some(EntryRef::new(
      self,
      query_version,
      ent,
      key,
      None,
      version,
    )))
  }
}
