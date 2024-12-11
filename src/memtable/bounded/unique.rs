memtable!(unique());

use core::ops::ControlFlow;

use skl::{
  generic::{Comparator, TypeRefComparator, TypeRefQueryComparator},
  Active,
};

use crate::{
  memtable::{
    MemtableEntry, RangeDeletionEntry as RangeDeletionEntryTrait, RangeEntry,
    RangeUpdateEntry as RangeUpdateEntryTrait,
  },
  types::{
    sealed::{PointComparator, Pointee, RangeComparator},
    TypeMode,
  },
  State,
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
  T::Value<'a>: Pointee<'a, Input = &'a [u8]>,
  <T::Key<'a> as Pointee<'a>>::Output: 'a,
  <T::Value<'a> as Pointee<'a>>::Output: 'a,
  T::Comparator<C>: PointComparator<C>
    + TypeRefComparator<'a, RecordPointer>
    + Comparator<<T::Key<'a> as Pointee<'a>>::Output>
    + 'static,
  T::RangeComparator<C>: TypeRefComparator<'a, RecordPointer>
    + TypeRefQueryComparator<'a, RecordPointer, <T::Key<'a> as Pointee<'a>>::Output>
    + RangeComparator<C>
    + 'static,
  RangeDeletionEntry<'a, Active, C, T>:
    RangeDeletionEntryTrait<'a> + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  RangeUpdateEntry<'a, Active, C, T>: RangeUpdateEntryTrait<'a, Value = <T::Value<'a> as Pointee<'a>>::Output>
    + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
{
  pub(in crate::memtable) fn validate(
    &'a self,
    ent: PointEntry<'a, Active, C, T>,
  ) -> ControlFlow<Option<Entry<'a, Active, C, T>>, PointEntry<'a, Active, C, T>> {
    let key = ent.key();
    let cmp = ent.ent.comparator();

    // check if the next entry is visible.
    // As the range_del_skl is sorted by the end key, we can use the lower_bound to find the first
    // deletion range that may cover the next entry.

    let shadow = self.range_deletions_skl.range(..=key).any(|ent| {
      let ent = RangeDeletionEntry::<Active, C, T>::new(ent);
      dbutils::equivalentor::RangeComparator::contains(cmp, &ent.range(), &key)
    });

    if shadow {
      return ControlFlow::Continue(ent);
    }

    // find the range key entry with maximum version that shadow the next entry.
    let range_ent = self.range_updates_skl.range(..=key).find_map(|ent| {
      let ent = RangeUpdateEntry::<Active, C, T>::new(ent);

      if dbutils::equivalentor::RangeComparator::contains(cmp, &ent.range(), &key) {
        Some(ent)
      } else {
        None
      }
    });

    // check if the next entry's value should be shadowed by the range key entries.
    if let Some(range_ent) = range_ent {
      let val = range_ent.value();
      return ControlFlow::Break(Some(Entry::new(self, ent, key, val)));

      // if value is None, the such range is unset, so we should return the value of the point entry.
    }

    let val = ent.value();
    ControlFlow::Break(Some(Entry::new(self, ent, key, val)))
  }
}
