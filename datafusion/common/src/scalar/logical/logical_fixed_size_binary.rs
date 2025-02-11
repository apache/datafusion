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

    /// Returns the length of the binary value.
    pub fn len(&self) -> i32 {
        // LogicalFixedSizeBinary::new guarantees that the cast succeeds
        self.value.len() as i32
    }

    /// Returns whether the length of the binary value is zero.
    pub fn is_empty(&self) -> bool {
        self.value.len() == 0
    }

    /// Returns a read-only view to the value of the binary value.
    pub fn value(&self) -> &[u8] {
        self.value.as_ref()
    }

    /// Returns the buffer of the binary value.
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
