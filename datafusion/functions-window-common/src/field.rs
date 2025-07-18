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

use datafusion_common::arrow::datatypes::FieldRef;

/// Metadata for defining the result field from evaluating a
/// user-defined window function.
pub struct WindowUDFFieldArgs<'a> {
    /// The fields corresponding to the arguments to the
    /// user-defined window function.
    input_fields: &'a [FieldRef],
    /// The display name of the user-defined window function.
    display_name: &'a str,
}

impl<'a> WindowUDFFieldArgs<'a> {
    /// Create an instance of [`WindowUDFFieldArgs`].
    ///
    /// # Arguments
    ///
    /// * `input_fields` - The fields corresponding to the
    ///   arguments to the user-defined window function.
    /// * `function_name` - The qualified schema name of the
    ///   user-defined window function expression.
    ///
    pub fn new(input_fields: &'a [FieldRef], display_name: &'a str) -> Self {
        WindowUDFFieldArgs {
            input_fields,
            display_name,
        }
    }

    /// Returns the field of input expressions passed as arguments
    /// to the user-defined window function.
    pub fn input_fields(&self) -> &[FieldRef] {
        self.input_fields
    }

    /// Returns the name for the field of the final result of evaluating
    /// the user-defined window function.
    pub fn name(&self) -> &str {
        self.display_name
    }

    /// Returns `Some(Field)` of input expression at index, otherwise
    /// returns `None` if the index is out of bounds.
    pub fn get_input_field(&self, index: usize) -> Option<FieldRef> {
        self.input_fields.get(index).cloned()
    }
}
