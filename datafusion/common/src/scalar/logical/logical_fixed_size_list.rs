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
use crate::scalar::logical::logical_list::LogicalList;
use crate::scalar::LogicalScalar;
use crate::types::{logical_fixed_size_list, LogicalTypeRef};
use crate::DataFusionError;
use bigdecimal::ToPrimitive;
use std::fmt::{Display, Formatter};

#[derive(Clone, PartialEq, Eq, Hash, Debug, PartialOrd)]
pub struct LogicalFixedSizeList {
    /// The inner list with a fixed size.
    inner: LogicalList,
}

impl LogicalFixedSizeList {
    /// Tries to create a new [`LogicalFixedSizeList`].
    ///
    /// # Errors
    ///
    /// An error is returned if `list` is too big.
    pub fn try_new(inner: LogicalList) -> crate::Result<Self> {
        if inner.len().to_i32().is_none() {
            return _internal_err!("List too big for fixed-size list.");
        }

        Ok(Self { inner })
    }

    /// Returns the logical type of this value.
    pub fn logical_type(&self) -> LogicalTypeRef {
        // LogicalFixedSizeList::new guarantees that the cast succeeds
        logical_fixed_size_list(self.inner.element_field(), self.inner.len() as i32)
    }

    /// Returns the logical values of this list.
    pub fn values(&self) -> &[LogicalScalar] {
        self.inner.values()
    }
}

/// A [LogicalList] may be turned into a [LogicalFixedSizeList].
///
/// # Errors
///
/// Returns an error if the [LogicalList] is too big.
impl TryFrom<LogicalList> for LogicalFixedSizeList {
    type Error = DataFusionError;

    fn try_from(value: LogicalList) -> Result<Self, Self::Error> {
        LogicalFixedSizeList::try_new(value)
    }
}

impl Display for LogicalFixedSizeList {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}
