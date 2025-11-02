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

use crate::Result;
use crate::ScalarValue;
use arrow::array::Array;
use std::fmt::Debug;

/// Implements pretty printing for a set of types.
///
/// For example, the default pretty-printer for a byte array might not be adequate for a UUID type,
/// which is physically stored as a fixed-length byte array. This extension allows the user to
/// override the default pretty-printer for a given type.
pub trait ValuePrettyPrinter: Debug + Sync + Send {
    /// Pretty print a scalar value.
    ///
    /// # Error
    ///
    /// Will return an error if the given `df_type` is not supported by this pretty printer.
    fn pretty_print_scalar(&self, value: &ScalarValue) -> Result<String>;

    /// Pretty print a specific value of a given array.
    ///
    /// # Error
    ///
    /// Will return an error if the given `df_type` is not supported by this pretty printer.
    fn pretty_print_array(&self, array: &dyn Array, index: usize) -> Result<String> {
        let value = ScalarValue::try_from_array(array, index)?;
        self.pretty_print_scalar(&value)
    }
}

/// The default pretty printer.
///
/// Uses the arrow implementation of printing values.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DefaultValuePrettyPrinter;

impl ValuePrettyPrinter for DefaultValuePrettyPrinter {
    fn pretty_print_scalar(&self, value: &ScalarValue) -> Result<String> {
        Ok(value.to_string())
    }
}
