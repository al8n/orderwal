memtable!(multiple_version(version));

use core::ops::ControlFlow;

use ref_cast::RefCast;
use skl::{
  generic::{Comparator, LazyRef, TypeRefComparator, TypeRefQueryComparator},
  Active, MaybeTombstone, Transformable,
};

use crate::{
  memtable::{
    MemtableEntry, RangeDeletionEntry as RangeDeletionEntryTrait, RangeEntry, RangeEntryExt as _,
    RangeUpdateEntry as RangeUpdateEntryTrait,
  },
  types::{
    sealed::{PointComparator, Pointee, RangeComparator},
    Query, RefQuery, TypeMode,
  },
  State, WithVersion,
};

pub use entry::*;
pub use iter::*;
pub use point::*;
pub use range_deletion::*;
pub use range_update::*;

mod entry;
mod iter;
mod point;
mod range_deletion;
mod range_update;

impl<'a, C, T> Table<C, T>
where
  C: 'static,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]>,
  T::Comparator<C>: PointComparator<C>
    + TypeRefComparator<RecordPointer>
    + Comparator<Query<<T::Key<'a> as Pointee<'a>>::Output>>
    + 'static,
  T::RangeComparator<C>: TypeRefComparator<RecordPointer>
    + TypeRefQueryComparator<RecordPointer, RefQuery<<T::Key<'a> as Pointee<'a>>::Output>>
    + RangeComparator<C>
    + 'static,
  RangeDeletionEntry<'a, Active, C, T>:
    RangeDeletionEntryTrait<'a> + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
{
  pub(in crate::memtable) fn validate<S>(
    &'a self,
    query_version: u64,
    ent: PointEntry<'a, S, C, T>,
  ) -> ControlFlow<Option<Entry<'a, S, C, T>>, PointEntry<'a, S, C, T>>
  where
    S: State,
    S::Data<'a, LazyRef<'a, ()>>: Clone + Transformable<Input = Option<&'a [u8]>>,
    S::Data<'a, T::Value<'a>>: Transformable<Input = Option<&'a [u8]>> + 'a,
    <MaybeTombstone as State>::Data<'a, T::Value<'a>>: Transformable<Input = Option<&'a [u8]>> + 'a,
    RangeUpdateEntry<'a, MaybeTombstone, C, T>: RangeUpdateEntryTrait<
        'a,
        Value = Option<<S::Data<'a, T::Value<'a>> as Transformable>::Output>,
      > + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  {
    let key = ent.key();
    let cmp = ent.ent.comparator();
    let version = ent.ent.version();
    let query = RefQuery::new(key);

    // check if the next entry is visible.
    // As the range_del_skl is sorted by the start key, we can use the lower_bound to find the first
    // deletion range that may cover the next entry.

    let shadow = self
      .range_deletions_skl
      .range(query_version, ..=&query)
      .any(|ent| {
        let del_ent_version = ent.version();
        if !(version <= del_ent_version && del_ent_version <= query_version) {
          return false;
        }

        let ent = RangeDeletionEntry::<Active, C, T>::new(ent);
        dbutils::equivalentor::RangeComparator::contains(
          cmp,
          &ent.query_range(),
          Query::ref_cast(&query.query),
        )
      });

    if shadow {
      return ControlFlow::Continue(ent);
    }

    // find the range key entry with maximum version that shadow the next entry.
    let range_ent = self
      .range_updates_skl
      .range_all(query_version, ..=&query)
      .filter_map(|ent| {
        let range_ent_version = ent.version();
        if !(version <= range_ent_version && range_ent_version <= query_version) {
          return None;
        }

        let ent = RangeUpdateEntry::<MaybeTombstone, C, T>::new(ent);
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

    // check if the next entry's value should be shadowed by the range key entries.
    if let Some(range_ent) = range_ent {
      let version = range_ent.version();
      if let Some(val) = range_ent.into_value() {
        return ControlFlow::Break(Some(Entry::new(
          self,
          query_version,
          ent,
          key,
          Some(S::data(val)),
          version,
        )));
      }

      // if value is None, the such range is unset, so we should return the value of the point entry.
    }

    let version = ent.version();
    ControlFlow::Break(Some(Entry::new(
      self,
      query_version,
      ent,
      key,
      None,
      version,
    )))
  }
}
