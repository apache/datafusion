use std::cmp::Ordering;
use crate::scalar::LogicalScalar;
use crate::types::{logical_union, LogicalTypeRef, LogicalUnionFields};

/// TODO logical-types
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct LogicalUnion {
    fields: LogicalUnionFields,
    type_id: i8,
    value: Box<LogicalScalar>,
}

impl LogicalUnion {
    /// Returns the [`LogicalTypeRef`] for [`self`].
    pub fn logical_type(&self) -> LogicalTypeRef {
        logical_union(self.fields.clone())
    }
}

impl PartialOrd for LogicalUnion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        todo!("logical-types")
    }
}