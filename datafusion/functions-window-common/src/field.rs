// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
