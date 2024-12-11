pub trait TypeMode: sealed::Sealed {}

#[doc(hidden)]
#[derive(Copy, Clone)]
pub struct Dynamic;

#[doc(hidden)]
pub struct Generic<K: ?Sized, V: ?Sized>(core::marker::PhantomData<(fn() -> K, fn() -> V)>);

impl<K, V> Clone for Generic<K, V>
where
  K: ?Sized,
  V: ?Sized,
{
  fn clone(&self) -> Self {
    *self
  }
}

impl<K, V> Copy for Generic<K, V>
where
  K: ?Sized,
  V: ?Sized,
{
}

pub(crate) mod sealed {
  use skl::generic::{LazyRef, Type};

  use super::{
    Dynamic, Generic, TypeMode, super::{RawEntryRef, RawRangeDeletionRef, RawRangeUpdateRef, RecordPointer},
  };

  pub trait ComparatorConstructor<C: ?Sized>: Sized {
    fn new(ptr: *const u8, cmp: triomphe::Arc<C>) -> Self;
  }

  pub trait PointComparator<C: ?Sized>: ComparatorConstructor<C> {
    fn fetch_entry<'a, T>(&self, kp: &RecordPointer) -> RawEntryRef<'a, T>
    where
      T: TypeMode,
      T::Key<'a>: crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Value<'a>: crate::types::sealed::Pointee<'a, Input = &'a [u8]>;
  }

  pub trait RangeComparator<C: ?Sized>: ComparatorConstructor<C> {
    fn fetch_range_update<'a, T>(&self, kp: &RecordPointer) -> RawRangeUpdateRef<'a, T>
    where
      T: TypeMode,
      T::Key<'a>: crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Value<'a>: crate::types::sealed::Pointee<'a, Input = &'a [u8]>;

    fn fetch_range_deletion<'a, T>(&self, kp: &RecordPointer) -> RawRangeDeletionRef<'a, T>
    where
      T: TypeMode,
      T::Key<'a>: crate::types::sealed::Pointee<'a, Input = &'a [u8]>;
  }

  pub trait Pointee<'a> {
    type Input;
    type Output: Copy + core::fmt::Debug;

    fn from_input(input: Self::Input) -> Self;

    fn input(&self) -> Self::Input;

    fn output(&self) -> Self::Output;
  }

  impl<'a> Pointee<'a> for &'a [u8] {
    type Input = Self;
    type Output = Self;

    #[inline]
    fn from_input(input: Self::Input) -> Self {
      input
    }

    #[inline]
    fn input(&self) -> Self::Input {
      self
    }

    #[inline]
    fn output(&self) -> Self::Output {
      self
    }
  }

  impl<'a, T> Pointee<'a> for LazyRef<'a, T>
  where
    T: Type + ?Sized,
  {
    type Input = &'a [u8];
    type Output = T::Ref<'a>;

    #[inline]
    fn from_input(input: Self::Input) -> Self {
      unsafe { LazyRef::from_raw(input) }
    }

    #[inline]
    fn input(&self) -> Self::Input {
      self.raw().unwrap()
    }

    #[inline]
    fn output(&self) -> Self::Output {
      *self.get()
    }
  }

  pub trait Sealed: Copy {
    type Key<'a>: Pointee<'a>;
    type Value<'a>: Pointee<'a>;

    type Comparator<C>: ComparatorConstructor<C>;
    type RangeComparator<C>: ComparatorConstructor<C>;
  }

  impl<T: Sealed> TypeMode for T {}

  impl Sealed for Dynamic {
    type Key<'a> = &'a [u8];
    type Value<'a> = &'a [u8];
    type Comparator<C> = crate::memtable::dynamic::MemtableComparator<C>;
    type RangeComparator<C> = crate::memtable::dynamic::MemtableRangeComparator<C>;
  }

  impl<K, V> Sealed for Generic<K, V>
  where
    K: Type + ?Sized,
    V: Type + ?Sized,
  {
    type Key<'a> = LazyRef<'a, K>;
    type Value<'a> = LazyRef<'a, V>;
    type Comparator<C> = crate::memtable::generic::MemtableComparator<K, C>;
    type RangeComparator<C> = crate::memtable::generic::MemtableRangeComparator<K, C>;
  }
}