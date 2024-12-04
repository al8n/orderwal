use core::{cell::OnceCell, ops::Bound};

use skl::{dynamic::BytesComparator, generic::{multiple_version::sync::Entry, GenericValue, LazyRef}};

use crate::{dynamic::memtable::{bounded::MemtableRangeComparator, RangeEntry}, types::{RawRangeDeletionRef, RecordPointer}, WithVersion};

/// Range update entry.
pub struct RangeDeletionEntry<'a, L, C>
where
  L: GenericValue<'a>,
{
  ent: Entry<'a, RecordPointer, L, MemtableRangeComparator<C>>,
  data: OnceCell<RawRangeDeletionRef<'a>>,
}

impl<'a, L, C> Clone for RangeDeletionEntry<'a, L, C>
where
  L: GenericValue<'a> + Clone
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      data: self.data.clone(),
    }
  }
}

impl<'a, L, C> RangeEntry<'a> for RangeDeletionEntry<'a, L, C>
where
  C: BytesComparator,
  L: GenericValue<'a> + 'a,
{
  #[inline]
  fn start_bound(&self) -> Bound<&'a [u8]> {
    let ent = self.data.get_or_init(|| {
      self.ent.comparator().fetch_range_deletion(self.ent.key())
    });
    ent.start_bound()
  }

  #[inline]
  fn end_bound(&self) -> Bound<&'a [u8]> {
    let ent = self.data.get_or_init(|| {
      self.ent.comparator().fetch_range_deletion(self.ent.key())
    });
    ent.end_bound()
  }

  #[inline]
  fn next(&mut self) -> Option<Self> {
    self.ent.next().map(|ent| Self {
      ent,
      data: OnceCell::new(),
    })
  }

  #[inline]
  fn prev(&mut self) -> Option<Self> {
    self.ent.prev().map(|ent| Self {
      ent,
      data: OnceCell::new(),
    })
  }
}

impl<'a, C> crate::dynamic::memtable::RangeDeletionEntry<'a> for RangeDeletionEntry<'a, LazyRef<'a, ()>, C>
where
  C: BytesComparator,
{
}


impl<'a, L, C> WithVersion for RangeDeletionEntry<'a, L, C>
where
  C: BytesComparator,
  L: GenericValue<'a> + 'a,
{
  #[inline]
  fn version(&self) -> u64 {
    self.ent.version()
  }
}
