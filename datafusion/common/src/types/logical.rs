use core::fmt;
use std::{cmp::Ordering, hash::Hash, sync::Arc};

use super::NativeType;

/// A reference counted [`LogicalType`]
pub type LogicalTypeRef = Arc<dyn LogicalType>;

pub trait LogicalType: fmt::Debug {
    fn native(&self) -> &NativeType;
    fn name(&self) -> Option<&str>;
}

impl PartialEq for dyn LogicalType {
    fn eq(&self, other: &Self) -> bool {
        self.native().eq(other.native()) && self.name().eq(&other.name())
    }
}

impl Eq for dyn LogicalType {}

impl PartialOrd for dyn LogicalType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for dyn LogicalType {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name()
            .cmp(&other.name())
            .then(self.native().cmp(other.native()))
    }
}

impl Hash for dyn LogicalType {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.native().hash(state);
    }
}
