use skl::generic::Ascend;
pub use skl::Height;

use crate::{
  memtable::{
    Memtable, MemtableEntry, RangeDeletionEntry as RangeDeletionEntryTrait, RangeEntry,
    RangeEntryExt as _, RangeUpdateEntry as RangeUpdateEntryTrait,
  },
  types::{
    sealed::{ComparatorConstructor, PointComparator, Pointee, RangeComparator},
    Query, RecordPointer, RefQuery, TypeMode,
  },
  State, WithVersion,
};
use core::ops::ControlFlow;
use ref_cast::RefCast;
use skl::{
  generic::{Comparator, LazyRef, TypeRefComparator, TypeRefQueryComparator},
  Active, MaybeTombstone, Transformable,
};

use among::Among;
use skl::{
  either::Either,
  generic::multiple_version::{sync::SkipMap, Map},
};
use triomphe::Arc;

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

/// Options to configure the [`Table`] or [`MultipleVersionTable`].
#[derive(Debug, Copy, Clone)]
pub struct TableOptions<C = Ascend> {
  capacity: u32,
  map_anon: bool,
  max_height: Height,
  pub(in crate::memtable) cmp: C,
}

impl<C: Default> Default for TableOptions<C> {
  #[inline]
  fn default() -> Self {
    Self::with_comparator(Default::default())
  }
}

impl TableOptions {
  /// Creates a new instance of `TableOptions` with the default options.
  #[inline]
  pub const fn new() -> Self {
    Self {
      capacity: 8192,
      map_anon: false,
      max_height: Height::new(),
      cmp: Ascend::new(),
    }
  }
}

impl<C> TableOptions<C> {
  /// Creates a new instance of `TableOptions` with the default options.
  #[inline]
  pub const fn with_comparator(cmp: C) -> TableOptions<C> {
    Self {
      capacity: 8192,
      map_anon: false,
      max_height: Height::new(),
      cmp,
    }
  }

  /// Sets the capacity of the table.
  ///
  /// Default is `8KB`.
  #[inline]
  pub const fn with_capacity(mut self, capacity: u32) -> Self {
    self.capacity = capacity;
    self
  }

  /// Sets the table to use anonymous memory.
  #[inline]
  pub const fn with_map_anon(mut self, map_anon: bool) -> Self {
    self.map_anon = map_anon;
    self
  }

  /// Sets the maximum height of the table.
  ///
  /// Default is `20`.
  #[inline]
  pub const fn with_max_height(mut self, max_height: Height) -> Self {
    self.max_height = max_height;
    self
  }

  /// Returns the capacity of the table.
  #[inline]
  pub const fn capacity(&self) -> u32 {
    self.capacity
  }

  /// Returns `true` if the table is using anonymous memory.
  #[inline]
  pub const fn map_anon(&self) -> bool {
    self.map_anon
  }

  /// Returns the maximum height of the table.
  #[inline]
  pub const fn max_height(&self) -> Height {
    self.max_height
  }
}

/// A memory table implementation based on ARENA [`SkipMap`](skl).
pub struct Table<C, T>
where
  T: TypeMode,
{
  pub(in crate::memtable) skl: SkipMap<RecordPointer, (), T::Comparator<C>>,
  pub(in crate::memtable) range_deletions_skl: SkipMap<RecordPointer, (), T::RangeComparator<C>>,
  pub(in crate::memtable) range_updates_skl: SkipMap<RecordPointer, (), T::RangeComparator<C>>,
}
impl<C, T> Memtable for Table<C, T>
where
  C: 'static,
  T: TypeMode,
  T::Comparator<C>: TypeRefComparator<RecordPointer> + 'static,
  T::RangeComparator<C>: TypeRefComparator<RecordPointer> + 'static,
{
  type Options = TableOptions<C>;
  type Error = skl::error::Error;
  #[inline]
  fn new<A>(arena: A, opts: Self::Options) -> Result<Self, Self::Error>
  where
    Self: Sized,
    A: rarena_allocator::Allocator,
  {
    {
      use skl::Arena;
      let arena_opts = skl::Options::new()
        .with_capacity(opts.capacity())
        .with_freelist(skl::options::Freelist::None)
        .with_compression_policy(skl::options::CompressionPolicy::None)
        .with_unify(false)
        .with_max_height(opts.max_height());

      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      let mmap = opts.map_anon();
      let cmp = Arc::new(opts.cmp);
      let ptr = arena.raw_ptr();
      let points_cmp = <T::Comparator<C> as ComparatorConstructor<_>>::new(ptr, cmp.clone());
      let range_del_cmp =
        <T::RangeComparator<C> as ComparatorConstructor<_>>::new(ptr, cmp.clone());
      let range_update_cmp =
        <T::RangeComparator<C> as ComparatorConstructor<_>>::new(ptr, cmp.clone());
      let b = skl::generic::Builder::with(points_cmp).with_options(arena_opts);
      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      let points: SkipMap<_, _, _> = {
        if mmap {
          b.map_anon().map_err(skl::error::Error::IO)
        } else {
          b.alloc()
        }
      }?;
      let allocator = points.allocator().clone();
      let range_del_skl =
        SkipMap::<_, _, _>::create_from_allocator(allocator.clone(), range_del_cmp)?;
      let range_key_skl = SkipMap::<_, _, _>::create_from_allocator(allocator, range_update_cmp)?;
      Ok(Self {
        skl: points,
        range_updates_skl: range_key_skl,
        range_deletions_skl: range_del_skl,
      })
    }
  }
  #[inline]
  fn len(&self) -> usize {
    self.skl.len() + self.range_deletions_skl.len() + self.range_updates_skl.len()
  }
  #[inline]
  fn insert(&self, _version: Option<u64>, pointer: RecordPointer) -> Result<(), Self::Error> {
    self
      .skl
      .insert(_version.unwrap(), &pointer, &())
      .map(|_| ())
      .map_err(Among::unwrap_right)
  }
  #[inline]
  fn remove(&self, _version: Option<u64>, key: RecordPointer) -> Result<(), Self::Error> {
    self
      .skl
      .get_or_remove(_version.unwrap(), &key)
      .map(|_| ())
      .map_err(Either::unwrap_right)
  }
  #[inline]
  fn range_remove(&self, _version: Option<u64>, pointer: RecordPointer) -> Result<(), Self::Error> {
    self
      .range_deletions_skl
      .insert(_version.unwrap(), &pointer, &())
      .map(|_| ())
      .map_err(Among::unwrap_right)
  }
  #[inline]
  fn range_set(&self, _version: Option<u64>, pointer: RecordPointer) -> Result<(), Self::Error> {
    self
      .range_updates_skl
      .insert(_version.unwrap(), &pointer, &())
      .map(|_| ())
      .map_err(Among::unwrap_right)
  }
  #[inline]
  fn range_unset(&self, _version: Option<u64>, key: RecordPointer) -> Result<(), Self::Error> {
    self
      .range_updates_skl
      .get_or_remove(_version.unwrap(), &key)
      .map(|_| ())
      .map_err(Either::unwrap_right)
  }
}

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
