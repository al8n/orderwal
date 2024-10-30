pub use multiple_version::MultipleVersionTable;
pub use table::Table;

macro_rules! match_op {
  ($self:ident.$op:ident($($args:ident),*) $(.map($associated_ty:ident))?) => {{
    match $self {
      Self::Arena(e) => e.$op($($args,)*) $(.map(Self::$associated_ty::Arena))?,
      Self::Linked(e) => e.$op($($args,)*) $(.map(Self::$associated_ty::Linked))?,
    }}
  };
  (Dispatch::$associated_ty:ident($self:ident.$op:ident($($args:ident),*))) => {{
    match $self {
      Self::Arena(e) => Self::$associated_ty::Arena(e.$op($($args,)*)),
      Self::Linked(e) => Self::$associated_ty::Linked(e.$op($($args,)*)),
    }}
  };
  (new($opts:ident)) => {{
    match $opts {
      Self::Options::Arena(opts) => ArenaTable::new(opts).map(Self::Arena).map_err(Self::Error::Arena),
      Self::Options::Linked => LinkedTable::new(())
        .map(Self::Linked)
        .map_err(|_| Self::Error::Linked),
    }
  }};
  (update($self:ident.$op:ident($($args:ident),*))) => {{
    match $self {
      Self::Arena(t) => t.$op($($args,)*).map_err(Self::Error::Arena),
      Self::Linked(t) => t.$op($($args,)*).map_err(|_| Self::Error::Linked),
    }
  }};
}

macro_rules! iter {
  (enum $name:ident {
    Arena($arena:ident),
    Linked($linked:ident),
  } -> $ent:ident) => {
    /// A sum type of iter for different memtable implementations.
    #[non_exhaustive]
    pub enum $name<'a, K, V>
    where
      K: ?Sized + Type + Ord,
      KeyPointer<K>: Type<Ref<'a> = KeyPointer<K>> + KeyRef<'a, KeyPointer<K>>,
      V: ?Sized + Type,
    {
      /// Arena iter
      Arena($arena<'a, KeyPointer<K>, ValuePointer<V>>),
      /// Linked iter
      Linked($linked<'a, KeyPointer<K>, ValuePointer<V>>),
    }

    impl<'a, K, V> Iterator for $name<'a, K, V>
    where
      K: ?Sized + Type + Ord + 'static,
      KeyPointer<K>: Type<Ref<'a> = KeyPointer<K>> + KeyRef<'a, KeyPointer<K>>,
      V: ?Sized + Type + 'static,
    {
      type Item = $ent<'a, K, V>;

      #[inline]
      fn next(&mut self) -> Option<Self::Item> {
        match_op!(self.next().map(Item))
      }
    }

    impl<'a, K, V> DoubleEndedIterator for $name<'a, K, V>
    where
      K: ?Sized + Type + Ord + 'static,
      KeyPointer<K>: Type<Ref<'a> = KeyPointer<K>> + KeyRef<'a, KeyPointer<K>>,
      V: ?Sized + Type + 'static,
    {
      #[inline]
      fn next_back(&mut self) -> Option<Self::Item> {
        match_op!(self.next_back().map(Item))
      }
    }
  };
}

macro_rules! range {
  (enum $name:ident {
    Arena($arena:ident),
    Linked($linked:ident),
  } -> $ent:ident) => {
    /// A sum type of range for different memtable implementations.
    #[non_exhaustive]
    pub enum $name<'a, K, V, Q, R>
    where
      R: RangeBounds<Q>,
      Q: ?Sized + Comparable<KeyPointer<K>>,
      K: ?Sized + Type + Ord,
      KeyPointer<K>: Type<Ref<'a> = KeyPointer<K>> + KeyRef<'a, KeyPointer<K>>,
      V: ?Sized + Type,
    {
      /// Arena range
      Arena($arena<'a, KeyPointer<K>, ValuePointer<V>, Q, R>),
      /// Linked range
      Linked($linked<'a, Q, R, KeyPointer<K>, ValuePointer<V>>),
    }

    impl<'a, K, V, Q, R> Iterator for $name<'a, K, V, Q, R>
    where
      R: RangeBounds<Q>,
      Q: ?Sized + Comparable<KeyPointer<K>>,
      K: ?Sized + Type + Ord + 'a,
      KeyPointer<K>: Type<Ref<'a> = KeyPointer<K>> + KeyRef<'a, KeyPointer<K>>,
      V: ?Sized + Type + 'a,
    {
      type Item = $ent<'a, K, V>;

      #[inline]
      fn next(&mut self) -> Option<Self::Item> {
        match_op!(self.next().map(Item))
      }
    }

    impl<'a, K, V, Q, R> DoubleEndedIterator for $name<'a, K, V, Q, R>
    where
      R: RangeBounds<Q>,
      Q: ?Sized + Comparable<KeyPointer<K>>,
      K: ?Sized + Type + Ord + 'a,
      KeyPointer<K>: Type<Ref<'a> = KeyPointer<K>> + KeyRef<'a, KeyPointer<K>>,
      V: ?Sized + Type + 'a,
    {
      fn next_back(&mut self) -> Option<Self::Item> {
        match_op!(self.next_back().map(Item))
      }
    }
  };
}

macro_rules! base_entry {
  (enum $name:ident {
    Arena($arena:ident),
    Linked($linked:ident),
  }) => {
    /// A sum type of entry for different memtable implementations.
    #[derive(Debug)]
    #[non_exhaustive]
    pub enum $name<'a, K, V>
    where
      K: ?Sized,
      V: ?Sized,
    {
      /// Arena entry
      Arena($arena<'a, KeyPointer<K>, ValuePointer<V>>),
      /// Linked entry
      Linked($linked<'a, KeyPointer<K>, ValuePointer<V>>),
    }

    impl<K: ?Sized, V: ?Sized> Clone for $name<'_, K, V> {
      #[inline]
      fn clone(&self) -> Self {
        match self {
          Self::Arena(e) => Self::Arena(e.clone()),
          Self::Linked(e) => Self::Linked(e.clone()),
        }
      }
    }

    impl<'a, K, V> BaseEntry<'a> for $name<'a, K, V>
    where
      K: ?Sized + Type + Ord,
      KeyPointer<K>: Type<Ref<'a> = KeyPointer<K>> + KeyRef<'a, KeyPointer<K>>,
      V: ?Sized + Type,
    {
      type Key = K;

      type Value = V;

      #[inline]
      fn key(&self) -> KeyPointer<Self::Key> {
        *match_op!(self.key())
      }

      fn next(&mut self) -> Option<Self> {
        match self {
          Self::Arena(e) => e.next().map(Self::Arena),
          Self::Linked(e) => e.next().map(Self::Linked),
        }
      }

      fn prev(&mut self) -> Option<Self> {
        match self {
          Self::Arena(e) => e.prev().map(Self::Arena),
          Self::Linked(e) => e.prev().map(Self::Linked),
        }
      }
    }
  };
}

/// The sum type for different memtable implementations options.
#[derive(Debug, Default)]
#[non_exhaustive]
pub enum TableOptions {
  /// The options for the arena memtable.
  Arena(super::arena::TableOptions),
  /// The options for the linked memtable.
  #[default]
  Linked,
}

impl From<super::arena::TableOptions> for TableOptions {
  #[inline]
  fn from(opts: super::arena::TableOptions) -> Self {
    Self::Arena(opts)
  }
}

impl TableOptions {
  /// Create a new arena memtable options with the default values.
  #[inline]
  pub const fn arena() -> Self {
    Self::Arena(super::arena::TableOptions::new())
  }

  /// Create a new linked memtable options with the default values.
  #[inline]
  pub const fn linked() -> Self {
    Self::Linked
  }
}

/// The sum type of error for different memtable implementations.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
  /// The error for the arena memtable.
  Arena(skl::Error),
  /// The error for the linked memtable.
  Linked,
}

impl From<skl::Error> for Error {
  #[inline]
  fn from(e: skl::Error) -> Self {
    Self::Arena(e)
  }
}

impl core::fmt::Display for Error {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Arena(e) => write!(f, "{e}"),
      Self::Linked => Ok(()),
    }
  }
}

impl core::error::Error for Error {}

mod multiple_version;
mod table;
