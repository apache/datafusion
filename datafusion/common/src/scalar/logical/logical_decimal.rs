use crate::error::_internal_err;
use crate::types::{logical_decimal, LogicalTypeRef};
use crate::Result;
use bigdecimal::{BigDecimal, ToPrimitive};
use std::fmt::{Display, Formatter};

/// TODO logical-types
#[derive(Debug, Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct LogicalDecimal {
    /// The actual value of the logical decimal
    value: BigDecimal,
}

impl LogicalDecimal {
    /// Creates a new [`LogicalDecimal`] from the given value.
    ///
    /// # Errors
    ///
    /// This function returns an error if [`value`] violates one of the following:
    /// - The digits of the decimal must be representable in 256 bits.
    /// - The scale must be representable with an `i8`.
    pub fn try_new(value: BigDecimal) -> Result<Self> {
        const MAX_PHYSICAL_DECIMAL_BYTES: usize = 256 / 8;
        if value.digits().to_ne_bytes().len() > MAX_PHYSICAL_DECIMAL_BYTES {
            return _internal_err!("Too many bytes for logical decimal");
        }

        if value.fractional_digit_count().to_i8().is_none() {
            return _internal_err!("Scale not supported for logical decimal");
        }

        Ok(Self { value })
    }

    /// Returns the value of this logical decimal.
    pub fn value(&self) -> &BigDecimal {
        &self.value
    }

    /// Returns the logical type of this value.
    pub fn logical_type(&self) -> LogicalTypeRef {
        // LogicalDecimal::new guarantees that the casts succeed
        logical_decimal(
            self.value.digits() as u8,
            self.value.fractional_digit_count() as i8,
        )
    }
}

impl Display for LogicalDecimal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value())
    }
}
