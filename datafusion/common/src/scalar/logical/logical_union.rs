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
use crate::scalar::LogicalScalar;
use crate::types::{logical_union, LogicalTypeRef, LogicalUnionFields};
use crate::{Result, _internal_datafusion_err};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

/// TODO logical-types
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct LogicalUnion {
    fields: LogicalUnionFields,
    type_id: i8,
    value: Box<LogicalScalar>,
}

impl LogicalUnion {
    /// Creates a new [LogicalUnion].
    ///
    /// # Errors
    ///
    /// The function returns an error in the following conditions:
    /// - `type_id` does not refer to a valid field in `fields`
    /// - `value` is not compatible with the field
    pub fn try_new(
        fields: LogicalUnionFields,
        type_id: i8,
        value: LogicalScalar,
    ) -> Result<LogicalUnion> {
        let field = fields
            .find_by_type_id(type_id)
            .ok_or(_internal_datafusion_err!(
                "TypeId not within the given fields."
            ))?;

        if !value.is_compatible_with_field(field) {
            return _internal_err!(
                "Value is not compatible with field '{}'.",
                field.name()
            );
        }

        Ok(LogicalUnion {
            fields,
            type_id,
            value: Box::new(value),
        })
    }

    /// Returns the logical type of this value.
    pub fn logical_type(&self) -> LogicalTypeRef {
        logical_union(self.fields.clone())
    }
}

impl PartialOrd for LogicalUnion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.fields != other.fields || self.type_id != other.type_id {
            return None;
        }
        self.value.partial_cmp(&other.value)
    }
}

impl Display for LogicalUnion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.type_id, self.value)
    }
}
