

use crate::dynamic::types::State;

use super::PointEntry;

/// a
pub struct Entry<'a, S, C>
where
  S: State<'a>,
{
  ent: PointEntry<'a, S, C>,
}
