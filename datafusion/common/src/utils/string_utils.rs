// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Utilities for working with strings

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;

/// Convenient function to convert an Arrow string array to a vector of strings
pub fn string_array_to_vec(array: &dyn Array) -> Vec<Option<&str>> {
    match array.data_type() {
        DataType::Utf8 => array.as_string::<i32>().iter().collect(),
        DataType::LargeUtf8 => array.as_string::<i64>().iter().collect(),
        DataType::Utf8View => array.as_string_view().iter().collect(),
        _ => unreachable!(),
    }
}
