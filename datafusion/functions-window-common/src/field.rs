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

use datafusion_common::arrow::datatypes::DataType;

/// Contains metadata which may be useful for a user-defined window
/// function to specify the field of the final result of evaluating the
/// user-defined window function.
pub struct FieldArgs<'a> {
    /// The data types of input expressions.
    input_types: &'a [DataType],
    /// The display name.
    schema_name: &'a str,
}

impl<'a> FieldArgs<'a> {
    /// Create an instance of [`FieldArgs`].
    ///
    /// # Arguments
    ///
    /// * `input_types` - The data types derived from the input
    /// expressions to the window function.
    /// * `schema_name` - The formatted display name for the window
    /// function derived from the input schema.
    ///
    /// # Returns
    ///
    /// A new [`FieldArgs`] instance.
    pub fn new(input_types: &'a [DataType], schema_name: &'a str) -> Self {
        FieldArgs {
            input_types,
            schema_name,
        }
    }

    /// Returns the data types of input expressions to the window
    /// function.
    pub fn input_types(&self) -> &[DataType] {
        self.input_types
    }

    /// Returns the name of the output field of this window function.
    pub fn name(&self) -> &str {
        self.schema_name
    }

    /// Returns an `Option<DataType>` containing the data type of the
    /// input expression at the specified index. It returns `None` when
    /// the index is out of bounds.
    pub fn get_input_type(&self, index: usize) -> Option<DataType> {
        self.input_types.get(index).cloned()
    }
}
