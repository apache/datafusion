use std::cmp::Ordering;
use crate::types::{logical_map, LogicalFieldRef, LogicalTypeRef};

/// TODO logical-types
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct LogicalMap {
    value_type: LogicalFieldRef,
    // TODO logical-types
}

impl LogicalMap {
    /// Returns the logical type of this value.
    pub fn logical_type(&self) -> LogicalTypeRef {
        logical_map(self.value_type.clone())
    }
}


impl PartialOrd for LogicalMap {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        todo!("logical-types")
    }
}