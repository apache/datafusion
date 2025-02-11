use crate::error::_internal_err;
use crate::types::{logical_fixed_size_binary, LogicalTypeRef};
use bigdecimal::ToPrimitive;
use std::fmt::{Display, Formatter};

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
        // LogicalFixedSizeBinary::new guarantees that the cast succeeds
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

    /// Returns the logical type of this value
    pub fn logical_type(&self) -> LogicalTypeRef {
        logical_fixed_size_binary(self.len())
    }
}

impl Display for LogicalFixedSizeBinary {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // print up to first 10 bytes, with trailing ... if needed
        for b in self.value.iter().take(10) {
            write!(f, "{b:02X}")?;
        }
        if self.value.len() > 10 {
            write!(f, "...")?;
        }
        Ok(())
    }
}
