use crate::error::_internal_err;
use crate::types::{logical_fixed_size_binary, LogicalTypeRef};
use bigdecimal::ToPrimitive;

/// TODO logical-types
#[derive(Debug, Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct LogicalFixedSizeBinary {
    /// The value of the fixed size binary scalar
    value: Vec<u8>,
}

impl LogicalFixedSizeBinary {
    /// TODO logical-types
    pub fn try_new(value: Vec<u8>) -> crate::Result<Self> {
        if value.len().to_i32().is_none() {
            return _internal_err!("Value too big for fixed-size binary.");
        }
        Ok(Self { value })
    }

    /// TODO logical-types
    pub fn len(&self) -> i32 {
        self.value.len() as i32
    }

    /// TODO logical-types
    pub fn value(&self) -> &[u8] {
        self.value.as_ref()
    }

    /// TODO logical-types
    pub fn into_value(self) -> Vec<u8> {
        self.value
    }

    /// TODO logical-types
    pub fn logical_type(&self) -> LogicalTypeRef {
        // LogicalFixedSizeBinary::new guarantees that the cast succeeds
        logical_fixed_size_binary(self.len())
    }
}
