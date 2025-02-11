// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
